"""
clip_index.py -- sqlite-backed index of every clip the bot posts, powering
the AI search command and daily recaps.

Primary key is (clip_id, uid), NOT clip_id alone: two Discord users can watch
the same streamer, so one Twitch clip gets posted twice -- once per user's
auto channel, each with its own channel/msg ids and lifecycle.

AI columns (transcript / summary / embedding) arrive later via a background
enrichment sweep and may never arrive at all -- there is no working AI key on
this box right now -- so search() has a pure-keyword fallback that needs no
embeddings, and upsert_clip() must never clobber AI fields already set
(a plain INSERT OR REPLACE would wipe them on every re-post).

Concurrency: callers hit this from one asyncio process via asyncio.to_thread,
so every function opens a short-lived WAL-mode connection; no global handle.

Standalone self-test (runs against a temp db, never touches clips.db):
    .venv/bin/python clip_index.py
"""
import json
import math
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

BASE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE, "twitch.env")


def _env(key):
    """Env var first (systemd EnvironmentFile), twitch.env fallback (CLI runs)."""
    val = os.environ.get(key)
    if val:
        return val
    try:
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if line.startswith(key + "="):
                    return line.split("=", 1)[1].strip()
    except FileNotFoundError:
        pass
    return None


def _local_tz():
    """The bot's idea of 'today' (the server runs UTC, the user doesn't) -- same
    CLIPS_TZ convention as twitch_api.local_tz, duplicated to stay standalone."""
    try:
        return ZoneInfo(_env("CLIPS_TZ") or "America/New_York")
    except Exception:
        return datetime.now().astimezone().tzinfo


def _db_path():
    """Resolved per call (not at import) so the self-test can point CLIPS_DB
    at a temp file without ever opening the real clips.db."""
    return _env("CLIPS_DB") or os.path.join(BASE, "clips.db")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS clips (
    clip_id    TEXT NOT NULL,
    uid        TEXT NOT NULL,
    login      TEXT,
    platform   TEXT,
    title      TEXT,
    url        TEXT,
    view_count INTEGER,
    duration   REAL,
    game       TEXT,
    creator    TEXT,
    created_at TEXT,
    posted_day TEXT,
    channel_id INTEGER,
    msg_id     INTEGER,
    thumbnail_url TEXT,
    transcript TEXT,
    summary    TEXT,
    embedding  TEXT,
    PRIMARY KEY (clip_id, uid)
)
"""


def _connect():
    """Short-lived WAL connection; schema is idempotently ensured every open
    (cheap, and it means no separate init step for callers or tests)."""
    conn = sqlite3.connect(_db_path(), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute(_SCHEMA)
    for col in ("thumbnail_url TEXT", "liked INTEGER", "dismissed INTEGER"):
        try:                                 # dbs created before the column
            conn.execute(f"ALTER TABLE clips ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass                             # already there
    conn.execute("CREATE INDEX IF NOT EXISTS idx_clips_uid_day "
                 "ON clips (uid, posted_day)")
    return conn


def _row_dict(row):
    """sqlite3.Row -> plain dict, with the embedding json decoded back to a
    list of floats (or None if absent/corrupt) so callers never see raw json."""
    d = dict(row)
    if d.get("embedding"):
        try:
            d["embedding"] = json.loads(d["embedding"])
        except (json.JSONDecodeError, TypeError):
            d["embedding"] = None
    return d


# ---- writes ------------------------------------------------------------------

_COLS = ("clip_id", "uid", "login", "platform", "title", "url", "view_count",
         "duration", "game", "creator", "created_at", "posted_day",
         "channel_id", "msg_id", "thumbnail_url")


def upsert_clip(rec):
    """Insert/refresh a posted clip by (clip_id, uid). Only the non-AI columns
    come from rec; ON CONFLICT updates just those, so a re-post or metadata
    refresh never erases transcript/summary/embedding already enriched."""
    vals = ([str(rec["clip_id"]), str(rec["uid"])]
            + [rec.get(c) for c in _COLS[2:]])
    sql = (f"INSERT INTO clips ({','.join(_COLS)}) "
           f"VALUES ({','.join('?' * len(_COLS))}) "
           "ON CONFLICT(clip_id, uid) DO UPDATE SET "
           + ",".join(f"{c}=excluded.{c}" for c in _COLS[2:]))
    with closing(_connect()) as conn, conn:
        conn.execute(sql, vals)


def set_ai(clip_id, uid, transcript, summary, embedding):
    """Attach enrichment output. embedding is a list of floats or None (no
    working key -> the sweep passes None and we keep the row keyword-searchable)."""
    emb = json.dumps(embedding) if embedding is not None else None
    with closing(_connect()) as conn, conn:
        conn.execute("UPDATE clips SET transcript=?, summary=?, embedding=? "
                     "WHERE clip_id=? AND uid=?",
                     (transcript, summary, emb, str(clip_id), str(uid)))


def set_flag(clip_id, uid, liked=None, dismissed=None):
    """✅/❌ card buttons: liked marks a keeper, dismissed means the card was
    deleted and the digest must never repost that clip for that user."""
    sets, vals = [], []
    if liked is not None:
        sets.append("liked=?")
        vals.append(int(liked))
    if dismissed is not None:
        sets.append("dismissed=?")
        vals.append(int(dismissed))
    if not sets:
        return
    with closing(_connect()) as conn, conn:
        conn.execute(f"UPDATE clips SET {','.join(sets)} "
                     "WHERE clip_id=? AND uid=?",
                     (*vals, str(clip_id), str(uid)))


def dismissed_ids(uid, login):
    """Clip ids this user ❌-dismissed for one streamer (repost guard)."""
    with closing(_connect()) as conn:
        rows = conn.execute("SELECT clip_id FROM clips WHERE uid=? AND "
                            "login=? AND dismissed=1",
                            (str(uid), login)).fetchall()
    return {r["clip_id"] for r in rows}


def liked_rows(uid, limit=50):
    """The user's ✅-saved clips, newest first."""
    with closing(_connect()) as conn:
        rows = conn.execute("SELECT * FROM clips WHERE uid=? AND liked=1 "
                            "ORDER BY created_at DESC LIMIT ?",
                            (str(uid), limit)).fetchall()
    return [_row_dict(r) for r in rows]


def bump_views(clip_id, uid, view_count):
    """Cheap view-count refresh (the digest re-fetches clips hours later)."""
    with closing(_connect()) as conn, conn:
        conn.execute("UPDATE clips SET view_count=? WHERE clip_id=? AND uid=?",
                     (view_count, str(clip_id), str(uid)))


def purge_older_than(days=90):
    """Housekeeping: drop rows whose posted_day is older than the cutoff.
    Rows with a NULL posted_day are kept (NULL < x is never true in sqlite).
    Returns the number of rows deleted."""
    cutoff = (datetime.now(_local_tz()).date() - timedelta(days=days)).isoformat()
    with closing(_connect()) as conn, conn:
        cur = conn.execute("DELETE FROM clips WHERE posted_day < ?", (cutoff,))
        return cur.rowcount


# ---- reads -------------------------------------------------------------------

def get(clip_id, uid):
    """One row as a dict (embedding decoded), or None."""
    with closing(_connect()) as conn:
        row = conn.execute("SELECT * FROM clips WHERE clip_id=? AND uid=?",
                           (str(clip_id), str(uid))).fetchone()
    return _row_dict(row) if row else None


def get_by_msg(msg_id):
    """The card row for a Discord message id (reaction handlers only get the
    message id, not the clip id)."""
    with closing(_connect()) as conn:
        row = conn.execute("SELECT * FROM clips WHERE msg_id=? LIMIT 1",
                           (int(msg_id),)).fetchone()
    return _row_dict(row) if row else None


def get_any(clip_id):
    """Any user's row for this clip id (url/platform/title are identical
    across uids) -- for stateless button handlers that only carry the id."""
    with closing(_connect()) as conn:
        row = conn.execute("SELECT * FROM clips WHERE clip_id=? LIMIT 1",
                           (str(clip_id),)).fetchone()
    return _row_dict(row) if row else None


def missing_ai(limit=20):
    """Rows still lacking a summary, oldest first, for the background
    enrichment sweep (oldest first so a backlog drains in posting order)."""
    with closing(_connect()) as conn:
        rows = conn.execute("SELECT * FROM clips WHERE summary IS NULL "
                            "AND (dismissed IS NULL OR dismissed=0) "
                            "ORDER BY created_at ASC, rowid ASC LIMIT ?",
                            (limit,)).fetchall()
    return [_row_dict(r) for r in rows]


def day_stats(uid, login, day):
    """Recap numbers for one user's streamer on one posted_day (YYYY-MM-DD):
    {'clip_count', 'total_views', 'top' (highest-view row or None),
     'summaries' (non-null, capped at 40 so a recap prompt stays small)}."""
    with closing(_connect()) as conn:
        rows = [_row_dict(r) for r in conn.execute(
            "SELECT * FROM clips WHERE uid=? AND login=? AND posted_day=?",
            (str(uid), login, day))]
    return {
        "clip_count": len(rows),
        "total_views": sum(r["view_count"] or 0 for r in rows),
        "top": max(rows, key=lambda r: r["view_count"] or 0, default=None),
        "summaries": [r["summary"] for r in rows if r.get("summary")][:40],
    }


# ---- search ------------------------------------------------------------------

_TERM_RE = re.compile(r"[a-z0-9]+")

# field -> keyword weight. Title/login/game are the strongest user intent
# signals; transcripts are long and noisy so a raw hit there counts least.
_KW_FIELDS = (("title", 3.0), ("summary", 2.0), ("login", 2.0),
              ("game", 2.0), ("transcript", 1.0), ("creator", 1.0))

# In embedding mode keywords are only a tie-breaking nudge on top of cosine
# (which lives in [-1, 1]); capped so a spammy transcript can't outvote meaning.
_KW_BONUS_SCALE = 0.02
_KW_BONUS_CAP = 0.2


def _kw_score(row, terms):
    score = 0.0
    for field, weight in _KW_FIELDS:
        text = (row.get(field) or "").lower()
        if not text:
            continue
        for t in terms:
            score += text.count(t) * weight
    return score


def _cosine(a, b):
    """Pure-python cosine similarity; 0.0 on any shape mismatch or zero vector
    (bad stored embedding must rank low, never crash the search command)."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = na = nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / math.sqrt(na * nb)


def search(uid, query, query_embedding=None, limit=8):
    """Search one user's clips. With a query_embedding: cosine similarity over
    rows that have embeddings, plus a small capped keyword bonus. Without one
    (no working AI key -- the degraded but always-available path): weighted
    keyword occurrence counts, and a row must match at least once to appear.
    Returns row dicts with a '_score' key, best first."""
    terms = _TERM_RE.findall((query or "").lower())
    with closing(_connect()) as conn:
        rows = [_row_dict(r) for r in
                conn.execute("SELECT * FROM clips WHERE uid=? AND "
                             "(dismissed IS NULL OR dismissed=0)",
                             (str(uid),))]
    scored = []
    if query_embedding is not None:
        for row in rows:
            if not row.get("embedding"):
                continue
            bonus = min(_kw_score(row, terms) * _KW_BONUS_SCALE, _KW_BONUS_CAP)
            row["_score"] = _cosine(query_embedding, row["embedding"]) + bonus
            scored.append(row)
    else:
        for row in rows:
            kw = _kw_score(row, terms)
            if kw > 0:
                row["_score"] = kw
                scored.append(row)
    scored.sort(key=lambda r: r["_score"], reverse=True)
    return scored[:limit]


# ---- self-test ---------------------------------------------------------------

if __name__ == "__main__":
    import shutil
    import tempfile

    tmp = tempfile.mkdtemp(prefix="clip_index_test_")
    os.environ["CLIPS_DB"] = os.path.join(tmp, "test_clips.db")
    print(f"self-test db: {_db_path()}", flush=True)

    today = datetime.now(_local_tz()).date().isoformat()
    fakes = [
        {"clip_id": "AceClutchXYZ", "uid": "111", "login": "jynxzi",
         "platform": "twitch", "title": "INSANE 1v5 ace clutch",
         "url": "https://clips.twitch.tv/AceClutchXYZ", "view_count": 4200,
         "duration": 28.5, "game": "Rainbow Six Siege", "creator": "someguy",
         "created_at": "2026-07-08T14:00:00Z", "posted_day": today,
         "channel_id": 1001, "msg_id": 5001},
        {"clip_id": "SharedTableABC", "uid": "111", "login": "caseoh_",
         "platform": "twitch", "title": "table breaks AGAIN",
         "url": "https://clips.twitch.tv/SharedTableABC", "view_count": 9000,
         "duration": 15.0, "game": "Just Chatting", "creator": "clipper42",
         "created_at": "2026-07-08T15:00:00Z", "posted_day": today,
         "channel_id": 1001, "msg_id": 5002},
        # same twitch clip, second user -> exercises the composite PK
        {"clip_id": "SharedTableABC", "uid": "222", "login": "caseoh_",
         "platform": "twitch", "title": "table breaks AGAIN",
         "url": "https://clips.twitch.tv/SharedTableABC", "view_count": 9000,
         "duration": 15.0, "game": "Just Chatting", "creator": "clipper42",
         "created_at": "2026-01-01T15:00:00Z", "posted_day": "2026-01-01",
         "channel_id": 2002, "msg_id": 6001},
    ]
    for rec in fakes:
        upsert_clip(rec)
    print(f"inserted {len(fakes)} fake clips (2 uids, 1 shared clip_id)",
          flush=True)

    set_ai("AceClutchXYZ", "111",
           transcript="oh my god he actually aced them, one tap headshots",
           summary="jynxzi pulls off a 1v5 ace clutch with headshots",
           embedding=[0.1, 0.2, 0.3, 0.4])
    print("set_ai on AceClutchXYZ/111 (4-dim fake embedding)", flush=True)

    # re-upsert must NOT wipe the AI fields
    fakes[0]["view_count"] = 5000
    upsert_clip(fakes[0])
    row = get("AceClutchXYZ", "111")
    print(f"after re-upsert: views={row['view_count']} "
          f"summary_kept={row['summary'] is not None} "
          f"embedding={row['embedding']}", flush=True)

    bump_views("SharedTableABC", "111", 12345)
    print(f"bump_views -> {get('SharedTableABC', '111')['view_count']}",
          flush=True)
    print(f"get(missing) -> {get('NopeNope', '111')}", flush=True)

    hits = search("111", "ace clutch")
    print(f"keyless search 'ace clutch' -> {len(hits)} hit(s): "
          + ", ".join(f"{h['clip_id']} score={h['_score']:.1f}" for h in hits),
          flush=True)
    hits = search("111", "table")
    print(f"keyless search 'table' -> {len(hits)} hit(s): "
          + ", ".join(f"{h['clip_id']} score={h['_score']:.1f}" for h in hits),
          flush=True)
    hits = search("222", "ace clutch")
    print(f"keyless search other uid 'ace clutch' -> {len(hits)} hit(s) "
          "(scoping check, expect 0)", flush=True)

    hits = search("111", "clutch", query_embedding=[0.1, 0.2, 0.3, 0.4])
    print(f"embedding search -> {len(hits)} hit(s): "
          + ", ".join(f"{h['clip_id']} score={h['_score']:.3f}" for h in hits),
          flush=True)

    stats = day_stats("111", "jynxzi", today)
    print(f"day_stats(111, jynxzi, {today}) -> clips={stats['clip_count']} "
          f"views={stats['total_views']} top={stats['top']['clip_id']} "
          f"summaries={stats['summaries']}", flush=True)

    pending = missing_ai()
    print(f"missing_ai -> {[(r['clip_id'], r['uid']) for r in pending]}",
          flush=True)

    deleted = purge_older_than(days=90)
    print(f"purge_older_than(90) deleted {deleted} row(s); "
          f"uid 222 row now: {get('SharedTableABC', '222')}", flush=True)

    shutil.rmtree(tmp)
    print("self-test OK (temp db removed)", flush=True)
