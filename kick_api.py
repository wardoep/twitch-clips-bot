"""
kick_api.py -- Kick.com client for the clips bot, shaped like twitch_api.py.

Kick has no official public API; this hits the site's own JSON endpoints
(/api/v2/...), which sit behind Cloudflare and 403 plain curl but answer
fine with a browser User-Agent (verified from this box 2026-07-08). Because
that welcome can be revoked at Cloudflare's whim, every call degrades to
None/empty with a loud print instead of raising -- the bot must keep serving
Twitch even when Kick sulks.

Clips come back normalized to the same dict shape the bot already consumes
from twitch_api.get_top_clips, so downstream code treats platforms uniformly.
Kick reports the category name inline (no id->name lookup like Helix), so
game_id is always None and game_name is filled directly.

Standalone test:
    .venv/bin/python kick_api.py [kick:name | kick.com/name URL]
"""
import json
import os
import re
import time
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE, "twitch.env")

SITE = "https://kick.com/api/v2"
# Chrome UA is what gets us past Cloudflare; plain python-requests/curl UAs 403.
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

PAGE_SLEEP = 0.4      # courtesy pause between paginated clip requests
LIVE_SLEEP = 0.5      # courtesy pause between per-slug is_live requests


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


_session = None


def _http():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": UA, "Accept": "application/json"})
    return _session


def _get(path, params=None):
    """GET a site-API path -> parsed JSON, or None on any trouble (404, 403,
    network, non-JSON). Never raises: a Kick outage must not take the bot's
    Twitch side down with it."""
    try:
        r = _http().get(f"{SITE}/{path}", params=params, timeout=15)
    except requests.RequestException as e:
        print(f"[kick] request failed on {path}: {e}", flush=True)
        return None
    if r.status_code == 404:
        return None
    if r.status_code == 403:
        print(f"[kick] !!! 403 on {path} -- Cloudflare is blocking us "
              f"(UA/IP mood change). Returning nothing.", flush=True)
        return None
    if r.status_code != 200:
        print(f"[kick] unexpected HTTP {r.status_code} on {path}", flush=True)
        return None
    try:
        return r.json()
    except ValueError:
        print(f"[kick] non-JSON body on {path} (Cloudflare interstitial?)",
              flush=True)
        return None


# ---- public helpers ----------------------------------------------------------

def local_tz():
    """The bot's idea of 'today' (the server runs UTC, the user doesn't)."""
    try:
        return ZoneInfo(_env("CLIPS_TZ") or "America/New_York")
    except Exception:
        return datetime.now().astimezone().tzinfo


# Kick slugs are lowercase; underscores and hyphens both occur in the wild
# (e.g. musa_usa). Reserved site paths must not be mistaken for channels.
_SLUG_RE = re.compile(r"^[a-z0-9_-]{3,30}$")
_NOT_CHANNELS = {"video", "videos", "categories", "category", "search",
                 "browse", "clips", "community-guidelines", "dashboard"}


def resolve_login(text):
    """Pull a Kick slug out of user input -- a kick.com URL, 'kick:name', or
    'kick name'. Bare names return None on purpose: in this bot an unmarked
    name means Twitch, and silently stealing it would break -watch."""
    text = text.strip().rstrip("/").split("?")[0]
    m = re.search(r"kick\.com/([A-Za-z0-9_-]+)", text)
    if not m:
        m = re.match(r"(?i)^kick[:\s]+([A-Za-z0-9_-]+)$", text.strip())
    if not m:
        return None
    slug = m.group(1).lower()
    if slug in _NOT_CHANNELS or not _SLUG_RE.match(slug):
        return None
    return slug


def get_user(slug):
    """-> {'id', 'login', 'display_name', 'profile_image_url'} or None.
    Matches twitch_api.get_user's shape; id is the channel id as a string
    (that's what the clips endpoint keys on, and the bot stores ids as str)."""
    data = _get(f"channels/{slug}")
    if not data or "id" not in data:
        return None
    user = data.get("user") or {}
    return {"id": str(data["id"]),
            "login": data.get("slug", slug),
            "display_name": user.get("username") or slug,
            "profile_image_url": user.get("profile_pic")}


def _normalize(c, slug):
    """Raw Kick clip dict -> the bot's normalized clip shape. The 'kick_'
    id prefix keeps posted.json keys collision-free across platforms; the
    ?clip= URL is what kick.com itself shares and yt-dlp's kick:clips
    extractor resolves it (verified with --simulate on this box)."""
    cat = c.get("category") or {}
    creator = c.get("creator") or {}
    return {"id": f"kick_{c['id']}",
            "url": f"https://kick.com/{slug}?clip={c['id']}",
            "title": c.get("title") or "",
            "view_count": int(c.get("view_count") or c.get("views") or 0),
            "duration": float(c.get("duration") or 0),
            "created_at": c.get("created_at") or "",
            "creator_name": creator.get("username") or "",
            "game_id": None,
            "game_name": cat.get("name") or "?",
            "thumbnail_url": c.get("thumbnail_url") or ""}


def _fetch_clips(slug, time_window, max_pages):
    """Walk /channels/{slug}/clips pages (20/page, view-sorted desc by the
    API). nextCursor is a JSON blob -- an object on page 1's response, a
    string on later ones -- passed back verbatim as the cursor param."""
    clips, cursor = [], "0"
    for _ in range(max_pages):
        data = _get(f"channels/{slug}/clips",
                    {"cursor": cursor, "sort": "view", "time": time_window})
        if data is None:
            break
        batch = data.get("clips") or []
        clips.extend(batch)
        nxt = data.get("nextCursor")
        if not nxt or not batch:
            break
        cursor = json.dumps(nxt, separators=(",", ":")) if isinstance(nxt, dict) else nxt
        time.sleep(PAGE_SLEEP)
    return clips


def get_top_clips(slug, top_n=100):
    """Today's top clips for a channel, view-sorted desc, normalized.
    Kick's time=day filter is a trailing ~24h window, not a calendar day
    (verified: it returned clips 26h old), so we still filter client-side
    on created_at >= local midnight. Because the API sorts by views, we can
    stop paging as soon as top_n of today's clips are in hand -- everything
    after has fewer views. Page cap keeps a busy channel from turning this
    into a crawl."""
    now = datetime.now(local_tz())
    midnight = datetime.combine(now.date(), dtime.min, tzinfo=now.tzinfo)
    kept = []
    cap = max(3, top_n // 20 + 5)
    clips, cursor = [], "0"
    for _ in range(cap):
        data = _get(f"channels/{slug}/clips",
                    {"cursor": cursor, "sort": "view", "time": "day"})
        if data is None:
            break
        batch = data.get("clips") or []
        for c in batch:
            try:
                created = datetime.fromisoformat(c["created_at"])
            except (KeyError, ValueError):
                continue
            if created >= midnight:
                kept.append(_normalize(c, slug))
        nxt = data.get("nextCursor")
        if len(kept) >= top_n or not nxt or not batch:
            break
        cursor = json.dumps(nxt, separators=(",", ":")) if isinstance(nxt, dict) else nxt
        time.sleep(PAGE_SLEEP)
    kept.sort(key=lambda c: c["view_count"], reverse=True)
    return kept[:top_n]


def is_live(slugs):
    """Which of these slugs are live right now -> set of slugs, or None when
    EVERY probe failed (Cloudflare mood) -- callers must treat None as
    'unknown, keep previous state', never as 'everyone offline', or live
    channels get rename-flapped into Discord's 2-per-10-min rename limit."""
    live, failures, total = set(), 0, 0
    for i, slug in enumerate(sorted(set(s for s in slugs if s))):
        if i:
            time.sleep(LIVE_SLEEP)
        total += 1
        data = _get(f"channels/{slug}")
        if data is None:
            failures += 1
        elif data.get("livestream"):
            live.add(slug)
    if total and failures == total:
        return None
    return live


if __name__ == "__main__":
    import subprocess
    import sys
    query = sys.argv[1] if len(sys.argv) > 1 else "kick:adinross"
    slug = resolve_login(query)
    print(f"slug: {slug}")
    print(f"bare name stays Twitch: resolve_login('xqc') = {resolve_login('xqc')}")
    if not slug:
        sys.exit("could not resolve a kick slug from input")
    user = get_user(slug)
    if not user:
        sys.exit(f"no such kick channel: {slug}")
    print(f"user: {user['display_name']} (id {user['id']})")

    clips = get_top_clips(slug, top_n=5)
    print(f"{len(clips)} clips today:")
    for i, c in enumerate(clips, 1):
        print(f"  #{i}  {c['view_count']:>7,} views  {c['duration']:>5.1f}s  "
              f"[{c['game_name']}]  {c['title'][:50]}  {c['url']}")
    test_url = clips[0]["url"] if clips else None
    if not clips:
        print("(none today -- showing time=all top 3 to prove parsing)")
        raw = _fetch_clips(slug, "all", max_pages=1)
        for i, c in enumerate((_normalize(r, slug) for r in raw[:3]), 1):
            print(f"  #{i}  {c['view_count']:>7,} views  {c['duration']:>5.1f}s  "
                  f"[{c['game_name']}]  {c['title'][:50]}  {c['url']}")
        if raw:
            test_url = _normalize(raw[0], slug)["url"]

    print(f"is_live([{slug!r}, 'xqc']): {is_live([slug, 'xqc'])}")

    if test_url:
        print(f"yt-dlp --simulate check on {test_url}")
        r = subprocess.run([sys.executable, "-m", "yt_dlp", "--simulate",
                            "--print", "title", test_url],
                           capture_output=True, text=True, timeout=120)
        print(f"  -> exit {r.returncode}, title: {r.stdout.strip()!r}")
        if r.returncode:
            print(f"  stderr: {r.stderr.strip()[-300:]}")
