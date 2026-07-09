"""
twitch_api.py -- thin Twitch Helix client for the clips bot.

App-access token via client-credentials flow (no user login), cached in
token.json next to this file and refreshed automatically on expiry/401.
TwitchTracker is only an input format the user pastes; all data comes from
the official Helix API.

Standalone test (uses twitch.env in this directory if env vars unset):
    .venv/bin/python twitch_api.py <streamer name or twitchtracker URL>
"""
import json
import os
import re
import time
from datetime import datetime, time as dtime, timezone
from zoneinfo import ZoneInfo

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
TOKEN_PATH = os.path.join(BASE, "token.json")
ENV_PATH = os.path.join(BASE, "twitch.env")

HELIX = "https://api.twitch.tv/helix"
OAUTH = "https://id.twitch.tv/oauth2/token"


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


def _client_creds():
    cid, secret = _env("TWITCH_CLIENT_ID"), _env("TWITCH_CLIENT_SECRET")
    if not cid or not secret:
        raise RuntimeError("TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET not set "
                           "(fill twitch.env -- see dev.twitch.tv/console)")
    return cid, secret


def _get_token(force=False):
    if not force:
        try:
            with open(TOKEN_PATH) as f:
                tok = json.load(f)
            if tok.get("expires_at", 0) > time.time() + 60:
                return tok["access_token"]
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            pass
    cid, secret = _client_creds()
    r = requests.post(OAUTH, data={"client_id": cid, "client_secret": secret,
                                   "grant_type": "client_credentials"},
                      timeout=15)
    r.raise_for_status()
    data = r.json()
    tok = {"access_token": data["access_token"],
           "expires_at": time.time() + data.get("expires_in", 3600)}
    with open(TOKEN_PATH, "w") as f:
        json.dump(tok, f)
    os.chmod(TOKEN_PATH, 0o600)
    return tok["access_token"]


def _helix(path, params):
    cid = _client_creds()[0]
    for attempt in (1, 2):
        r = requests.get(f"{HELIX}/{path}", params=params, timeout=15,
                         headers={"Client-Id": cid,
                                  "Authorization": f"Bearer {_get_token(force=attempt == 2)}"})
        if r.status_code == 401 and attempt == 1:
            continue                      # stale token -- refresh and retry once
        r.raise_for_status()
        return r.json().get("data", [])
    return []


# ---- public helpers ----------------------------------------------------------

def local_tz():
    """The bot's idea of 'today' (the server runs UTC, the user doesn't)."""
    try:
        return ZoneInfo(_env("CLIPS_TZ") or "America/New_York")
    except Exception:
        return datetime.now().astimezone().tzinfo


_LOGIN_RE = re.compile(r"^[a-z0-9_]{3,25}$")


def resolve_login(text):
    """Pull a Twitch login out of whatever the user pasted: a twitchtracker.com
    URL, a twitch.tv URL, or a bare channel name. Returns None if unparseable."""
    text = text.strip().rstrip("/").split("?")[0]
    m = re.search(r"(?:twitchtracker\.com|twitch\.tv)/([A-Za-z0-9_]+)", text)
    login = (m.group(1) if m else text).lower()
    return login if _LOGIN_RE.match(login) else None


def get_user(login):
    """-> {'id', 'login', 'display_name', 'profile_image_url'} or None."""
    data = _helix("users", {"login": login})
    return data[0] if data else None


def get_top_clips(broadcaster_id, top_n=5, since=None):
    """Top clips (view-sorted by Helix) created since `since` (default: local
    midnight today). Returns the raw Helix clip dicts, at most top_n."""
    if since is None:
        now = datetime.now(local_tz())
        since = datetime.combine(now.date(), dtime.min, tzinfo=now.tzinfo)
    started = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ended = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return _helix("clips", {"broadcaster_id": broadcaster_id, "first": top_n,
                            "started_at": started, "ended_at": ended})


def get_live_user_ids(user_ids):
    """Which of these broadcaster ids are live right now -> set of ids
    (single batched /streams call)."""
    ids = [u for u in set(user_ids) if u]
    if not ids:
        return set()
    data = _helix("streams", [("user_id", u) for u in ids]
                  + [("first", str(min(len(ids), 100)))])
    return {s["user_id"] for s in data}


def get_game_names(game_ids):
    """{game_id: name} for a set of game ids (single batched call)."""
    ids = [g for g in set(game_ids) if g]
    if not ids:
        return {}
    data = _helix("games", [("id", g) for g in ids])
    return {g["id"]: g["name"] for g in data}


if __name__ == "__main__":
    import sys
    query = sys.argv[1] if len(sys.argv) > 1 else "jynxzi"
    login = resolve_login(query)
    print(f"login: {login}")
    user = get_user(login)
    if not user:
        sys.exit(f"no such streamer: {login}")
    print(f"user: {user['display_name']} (id {user['id']})")
    clips = get_top_clips(user["id"])
    games = get_game_names(c["game_id"] for c in clips)
    print(f"{len(clips)} clips today:")
    for i, c in enumerate(clips, 1):
        print(f"  #{i}  {c['view_count']:>7,} views  {c['duration']:>5.1f}s  "
              f"[{games.get(c['game_id'], '?')}]  {c['title'][:60]}  {c['url']}")
