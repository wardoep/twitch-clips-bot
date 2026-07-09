"""
clips_bot.py -- Twitch top-clips Discord bot. One gateway process, three jobs:

  1. DM commands:
       -watch <twitchtracker URL | twitch URL | channel name>
       -unwatch <name>
       -cliplist
       -top [streamer] [n]   (today's top n clips as a list, default 10;
                              streamer optional when only one is watched)
       -digest               (force a refresh right now)
  2. Rolling refresh every CLIPS_POLL_MINUTES (default 30): for every watched
     streamer, fetch today's top CLIPS_TOP_N clips (official Helix API,
     view-sorted). New entrants get posted into that streamer's auto-created
     #clips-<name> channel WITH a compact playable MP4 attached (480p/360p,
     whatever fits the upload limit) so clips play in-channel before anyone
     downloads; already-posted cards are EDITED in place so their rank label
     (Top #1..#N) and view count stay current all day -- one card per clip
     per day, no repost spam. Day rolls over at local midnight (CLIPS_TZ).
  3. Download button: fetches the MP4 with yt-dlp and attaches it in-channel;
     if the file is over the guild upload limit, replies with a fresh signed
     direct URL instead (Twitch clip CDN links expire, so they are always
     resolved at press time -- the button's custom_id only holds the clip id,
     which is why it keeps working across bot restarts with no state).

Runs as a systemd user service (twitch-clips.service). Same gateway template
as plug_gateway_bot.py: no privileged intents, DM content still delivered,
blocking work off the loop via asyncio.to_thread.
"""
import asyncio
import json
import os
import secrets
from datetime import datetime, time as dtime, timedelta

import discord
from discord import app_commands
from discord.ext import tasks

import ai
import clip_index
import downloader
import kick_api
import twitch_api

BASE = os.path.dirname(os.path.abspath(__file__))
WATCHLIST_PATH = os.path.join(BASE, "watchlist.json")
POSTED_PATH = os.path.join(BASE, "posted.json")
SUBS_PATH = os.path.join(BASE, "live_subs.json")   # {login: [user ids to ping]}
STREAM_LOG_PATH = os.path.join(BASE, "stream_log.json")  # {login: [sessions]}

TOKEN = os.environ.get("CLIPS_DISCORD_TOKEN")
GUILD_ID = int(os.environ.get("CLIPS_GUILD_ID", "0"))
CATEGORY_NAME = os.environ.get("CLIPS_CATEGORY_NAME", "Twitch Clips")
TOP_N = int(os.environ.get("CLIPS_TOP_N", "5"))
MAX_NEW = int(os.environ.get("CLIPS_MAX_PER_REFRESH", "15"))  # flood guard
                                                              # for loose filters
TZ = twitch_api.local_tz()                  # CLIPS_TZ, default America/New_York
POLL_MINUTES = int(os.environ.get("CLIPS_POLL_MINUTES", "30"))
SWEEP_MINUTES = int(os.environ.get("CLIPS_SWEEP_MINUTES", "2"))
_rh, _rm = (os.environ.get("CLIPS_RECAP_TIME", "00:10")).split(":")

MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
LIVE_PREFIX = "🔴"                          # channel-name marker while live

ADD_CHANNEL = os.environ.get("CLIPS_ADD_CHANNEL", "add-streamers")
PING_CHANNEL = os.environ.get("CLIPS_PING_CHANNEL", "🔔┃live-pings")
MSG_INTENT = os.environ.get("CLIPS_MSG_INTENT") == "1"

intents = discord.Intents.none()
intents.guilds = True
intents.dm_messages = True                  # -watch / -unwatch / -digest DMs
intents.reactions = True                    # ✅ save / ❌ dismiss on cards
# privileged: lets the bot read bare pastes in #add-streamers / #live-pings.
# Needs the "Message Content Intent" toggle in the dev portal Bot page first.
# guild_messages delivers the message EVENTS; message_content fills in .content
intents.guild_messages = MSG_INTENT
intents.message_content = MSG_INTENT
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)     # /watch etc., usable anywhere
GUILD = discord.Object(id=GUILD_ID or 1)

_dl_locks = {}                              # clip_id -> Lock (double-click guard)


# ---- tiny JSON state ---------------------------------------------------------

def _load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


# ---- watchlist commands (DM) ---------------------------------------------------

def _wl_all_pairs(wl):
    """watchlist.json is {discord_user_id: {key: entry}} -> flat list of
    (uid, key, entry). Keys are the twitch login, or 'kick:<slug>' for Kick
    streamers (twitch keys stay bare for back-compat with pre-kick data)."""
    return [(uid, key, e) for uid, keys in wl.items()
            for key, e in keys.items()]


def _split_key(key):
    """watchlist key -> (platform, login/slug)."""
    if key.startswith("kick:"):
        return "kick", key[5:]
    return "twitch", key


def _watch_url(key):
    platform, slug = _split_key(key)
    return (f"https://kick.com/{slug}" if platform == "kick"
            else f"https://twitch.tv/{slug}")


def _channel_owner(channel_id):
    """Reverse-map a channel id -> (uid, login) if it's someone's private
    streamer channel."""
    for uid, login, e in _wl_all_pairs(_load(WATCHLIST_PATH)):
        if e.get("channel_id") == channel_id:
            return uid, login
    return None


def _private_overwrites(guild, member):
    """A user's streamer channel: invisible to everyone else; the owner can
    see it and type (filters); the bot posts."""
    return {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(view_channel=True,
                                            send_messages=True),
        guild.me: discord.PermissionOverwrite(view_channel=True,
                                              send_messages=True),
    }


async def _pin(msg):
    try:
        await msg.pin()
    except discord.Forbidden:
        print(f"[pin] missing Manage Messages in #{msg.channel.name}",
              flush=True)


FILTERS_CMD_ID = None                        # set after tree.sync in on_ready


def _pin_embed(display):
    """The pinned how-it-works embed for a streamer channel (design spec)."""
    f_ref = (f"</filters:{FILTERS_CMD_ID}>" if FILTERS_CMD_ID
             else "`/filters`")
    return discord.Embed(
        color=0x9146FF, title=f"Your {display} clip feed",
        description=(
            f"**🎬 What posts here** — every new {display} clip, ranked by "
            f"views. 🥇🥈🥉 mark the day's top 3.\n\n"
            f"**🎛️ Filter it** — {f_ref} or the button below. A Top pick "
            f"and a view minimum combine.\n\n"
            f"**⬇️ On every clip** — **Download** saves the MP4 to your "
            f"device.\n\n"
            f"-# A date divider posts each morning. Quiet channel? Your "
            f"filter might be strict — check {f_ref}."))


def _pin_view():
    view = discord.ui.View(timeout=None)
    view.add_item(discord.ui.Button(style=discord.ButtonStyle.primary,
                                    emoji="🎛️", label="Set filters",
                                    custom_id="open_filters"))
    view.add_item(discord.ui.Button(style=discord.ButtonStyle.secondary,
                                    emoji="❓", label="Help",
                                    custom_id="open_help"))
    return view


async def _post_clips_explainer(chan, display):
    """Post + pin the how-this-works message in a streamer's clips channel."""
    await _pin(await chan.send(embed=_pin_embed(display), view=_pin_view()))


async def _ensure_channel(key, display, user_id):
    """Create the user's private #clips-<name> channel. Callers only invoke
    this when the user's watchlist entry has no live channel (names may
    repeat across users, so no find-by-name)."""
    platform, slug = _split_key(key)
    guild = client.get_guild(GUILD_ID)
    if guild is None:
        raise RuntimeError(f"bot is not in guild {GUILD_ID}")
    member = guild.get_member(int(user_id)) or await guild.fetch_member(
        int(user_id))
    cat = discord.utils.find(lambda c: c.name.lower() == CATEGORY_NAME.lower(),
                             guild.categories)
    if cat is None:
        cat = await guild.create_category(CATEGORY_NAME)
    chan = await guild.create_text_channel(
        f"clips-{slug}", category=cat,
        overwrites=_private_overwrites(guild, member))
    await _post_clips_explainer(chan, display or slug)
    return chan


async def cmd_watch(arg, user_id):
    kslug = kick_api.resolve_login(arg) if arg else None
    if kslug:                                # explicit kick.com/-marker input
        platform, login, key = "kick", kslug, f"kick:{kslug}"
        user = await asyncio.to_thread(kick_api.get_user, kslug)
        if not user:
            return f"no Kick channel called `{kslug}` -- check the spelling?"
    else:
        login = twitch_api.resolve_login(arg) if arg else None
        if not login:
            return ("usage: paste a twitchtracker URL, twitch/kick URL, or "
                    "channel name -- e.g. `twitchtracker.com/jynxzi` or "
                    "`kick.com/adinross`")
        platform, key = "twitch", login
        user = await asyncio.to_thread(twitch_api.get_user, login)
        if not user:
            return f"no Twitch channel called `{login}` -- check the spelling?"
    wl = _load(WATCHLIST_PATH)
    mine = wl.setdefault(str(user_id), {})
    if key in mine:
        return (f"you're already watching **{mine[key]['display_name']}** "
                f"-> <#{mine[key]['channel_id']}>")
    chan = await _ensure_channel(key, user["display_name"], user_id)
    # channel creation took multiple awaited Discord calls -- re-load so we
    # don't clobber watchlist writes (filters, other watches) made meanwhile
    wl = _load(WATCHLIST_PATH)
    mine = wl.setdefault(str(user_id), {})
    mine[key] = {"user_id": user["id"],
                 "display_name": user["display_name"],
                 "platform": platform, "login": login,
                 "channel_id": chan.id,
                 "min_views": None, "top_n": None,
                 "added_at": datetime.now(TZ).isoformat()}
    _save(WATCHLIST_PATH, wl)
    return (f"watching **{user['display_name']}** -- your private channel is "
            f"<#{chan.id}> (only you can see it). New clips land within "
            f"~{SWEEP_MINUTES} min; views/ranks refresh every "
            f"{POLL_MINUTES} min. Default: **every clip of the day**; use "
            f"`/filters` in that channel to narrow it (how-to pinned there).")


def _resolve_watch_key(arg, mine):
    """User text -> the watchlist key it refers to (exact key, twitch login,
    or kick slug in any accepted form), or None."""
    if arg in mine:
        return arg
    kslug = kick_api.resolve_login(arg)
    if kslug and f"kick:{kslug}" in mine:
        return f"kick:{kslug}"
    login = twitch_api.resolve_login(arg)
    if login and login in mine:
        return login
    if login and f"kick:{login}" in mine:    # bare name, kick-only watch
        return f"kick:{login}"
    return None


async def cmd_unwatch(arg, user_id):
    wl = _load(WATCHLIST_PATH)
    mine = wl.get(str(user_id), {})
    key = _resolve_watch_key(arg.strip(), mine) if arg else None
    if not key:
        names = ", ".join(f"`{k}`" for k in mine) or "(none)"
        return f"you're not watching that. yours: {names}"
    entry = mine.pop(key)
    _save(WATCHLIST_PATH, wl)
    return (f"stopped watching **{entry['display_name']}** "
            f"(<#{entry['channel_id']}> left in place)")


async def cmd_top(arg, requester_id):
    """-top [streamer] [n] -- a compact list of today's top n clips
    (default 10, max 25). Streamer optional when you watch exactly one."""
    mine = _load(WATCHLIST_PATH).get(str(requester_id), {})
    key, n = None, 10
    for tok in arg.split():
        if tok.isdigit():
            n = max(1, min(25, int(tok)))
        else:
            key = (_resolve_watch_key(tok, mine)
                   or twitch_api.resolve_login(tok))
    if not key:
        if len(mine) == 1:
            key = next(iter(mine))
        else:
            return ("usage: `-top <streamer> [count]`  e.g. `-top jynxzi 15`")
    platform, slug = _split_key(key)
    if key in mine:
        user_id, display = mine[key]["user_id"], mine[key]["display_name"]
    elif platform == "twitch":               # works for unwatched twitch too
        user = await asyncio.to_thread(twitch_api.get_user, slug)
        if not user:
            return f"no Twitch channel called `{slug}`"
        user_id, display = user["id"], user["display_name"]
    else:
        user_id, display = None, slug
    if platform == "kick":
        clips = await asyncio.to_thread(kick_api.get_top_clips, slug, n)
    else:
        clips = await asyncio.to_thread(twitch_api.get_top_clips, user_id, n)
    if not clips:
        return f"**{display}** has no clips yet today"
    lines = [f"**{display} -- top {len(clips)} clips today**"]
    for i, c in enumerate(clips, 1):
        title = (c["title"] or "(untitled)").replace("[", "(").replace("]", ")")
        lines.append(f"`#{i:>2}` {MEDALS.get(i, '·')} **{c['view_count']:,}**"
                     f" views · {_fmt_duration(c['duration'])} · "
                     f"[{title[:70]}](<{c['url']}>)")
    return "\n".join(lines)


async def cmd_livesub(arg, user_id):
    """Typed in the live-pings channel: toggle a go-live ping for yourself."""
    wl = _load(WATCHLIST_PATH)
    watched = {k: e for _, k, e in _wl_all_pairs(wl)}   # by anyone
    login = _resolve_watch_key(arg.strip(), watched) if arg else None
    if not login and not arg:
        return ("type a streamer name (or link) to get pinged when they go "
                "live -- type it again to stop")
    if not login:
        return (f"that streamer isn't being watched yet -- paste them in "
                f"the add-streamers channel first, then subscribe here")
    subs = _load(SUBS_PATH)
    lst = subs.setdefault(login, [])
    if user_id in lst:
        lst.remove(user_id)
        verb = "will no longer"
    else:
        lst.append(user_id)
        verb = "will"
    _save(SUBS_PATH, subs)
    return (f"<@{user_id}> {verb} be pinged when "
            f"**{watched[login]['display_name']}** goes live")


def cmd_mypings(user_id):
    subs = _load(SUBS_PATH)
    names = {l: e["display_name"]
             for _, l, e in _wl_all_pairs(_load(WATCHLIST_PATH))}
    mine = [names.get(l, l)
            for l, users in subs.items() if user_id in users]
    if not mine:
        return ("you have no live pings set -- type a streamer's name here "
                "to add one")
    return ("you get pinged when these go live: "
            + ", ".join(f"**{n}**" for n in sorted(mine))
            + "\n`-stop <name>` turns one off")


def cmd_stop_ping(arg, user_id):
    subs = _load(SUBS_PATH)
    # subs are keyed like the watchlist ("kick:slug" included) -- resolve
    # the user's text against the sub keys themselves
    key = _resolve_watch_key(arg.strip(), subs) if arg else None
    if key and user_id in (subs.get(key) or []):
        subs[key].remove(user_id)
        _save(SUBS_PATH, subs)
        names = {k: e["display_name"]
                 for _, k, e in _wl_all_pairs(_load(WATCHLIST_PATH))}
        return f"done -- no more pings for **{names.get(key, key)}**"
    return ("you don't have a ping set for that one -- `-mypings` shows "
            "what you have")


def _filter_desc(entry):
    mv, tn = entry.get("min_views"), entry.get("top_n")
    if mv and tn:
        return f"top {tn} with ≥{mv:,} views"
    if mv:
        return f"all clips with ≥{mv:,} views"
    if tn:
        return f"top {tn} of the day"
    return "every clip of the day"


def cmd_cliplist(user_id):
    mine = _load(WATCHLIST_PATH).get(str(user_id), {})
    if not mine:
        return ("you're not watching anyone -- paste a streamer in the "
                "add-streamers channel")
    lines = [f"• **{v['display_name']}** -> <#{v['channel_id']}> "
             f"({_filter_desc(v)})" for v in mine.values()]
    return "you're watching:\n" + "\n".join(lines)


async def cmd_filter(uid, login, text):
    """Typed in the user's own streamer channel: set what gets posted.
    Understands `-top 8`, `-minviews 50`, natural phrases like 'more than 50
    views' / 'at least 3 views', `-filters`, `-reset`."""
    import re
    wl = _load(WATCHLIST_PATH)
    entry = wl.get(uid, {}).get(login)
    if entry is None:
        return None
    low = text.lower().strip()
    if low.startswith("-filter"):            # "-filter top 8", "-filters", …
        low = low[len("-filter"):].lstrip("s").lstrip(" :").strip()
    elif low.startswith("-settings"):
        low = ""
    m_top = re.match(r"^-?top\s*(\d+)$", low)
    m_views = re.search(
        r"(?:-?minviews\s*|(?:more than|over|at least|min(?:imum)?)\s+)(\d+)",
        low) or re.match(r"^(\d+)\s*\+?\s*views?", low)
    if low in ("", "show", "current"):
        reply = (f"Current: **{_filter_label(entry)}**\n-# change it: "
                 f"`more than 50 views` · `top 8` · `reset` -- or /filters")
    elif low in ("-reset", "reset", "off", "default", "none"):
        entry["min_views"], entry["top_n"] = None, None
        reply = "filter removed -- **⭐ Everything** gets posted"
    elif m_top:                              # combines with a views minimum
        entry["top_n"] = max(1, min(25, int(m_top.group(1))))
        reply = f"got it -- **{_filter_label(entry)}**"
    elif m_views:                            # combines with a top-N
        entry["min_views"] = max(1, int(m_views.group(1)))
        reply = f"got it -- **{_filter_label(entry)}**"
    else:
        return ("filters for this channel: `more than 50 views` (or "
                "`-minviews 50`) · `-top 8` · `-filters` shows current · "
                "`-reset` back to default")
    _save(WATCHLIST_PATH, wl)
    if low not in ("", "show", "current"):   # a filter was changed
        _spawn(run_digest(force_login=login, force_uid=uid),
               f"filter refresh {login}")
        reply += " -- refreshing now"
    return reply


# ---- clip cards ----------------------------------------------------------------

def _fmt_duration(seconds):
    s = int(round(seconds))
    return f"{s // 60}:{s % 60:02d}"         # always M:SS per copy rules


class _DownloadView(discord.ui.View):
    """Card buttons; presses are handled globally in on_interaction
    (custom_ids survive restarts). ✅/❌ sit on GREY buttons -- the emoji
    carries the color, without the loud green/red button fills."""
    def __init__(self, clip_id):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="Download",
                                        style=discord.ButtonStyle.primary,
                                        custom_id=f"dl:{clip_id}"))
        self.add_item(discord.ui.Button(emoji="✅",
                                        style=discord.ButtonStyle.secondary,
                                        custom_id=f"like:{clip_id}"))
        self.add_item(discord.ui.Button(emoji="❌",
                                        style=discord.ButtonStyle.secondary,
                                        custom_id=f"nope:{clip_id}"))


def _clip_content(clip, rank, game_name, summary=None):
    """The info panel, as message text so it renders ABOVE the attached
    video (Discord always puts embeds below attachments). Line 1: rank +
    views + linked title; line 2: small grey details; line 3 (when the AI
    sweep has caught up): a one-line summary of what happens."""
    title = (clip["title"] or "(untitled clip)").strip()
    title = title.replace("[", "(").replace("]", ")").replace("`", "'")
    medal = MEDALS.get(rank)                 # no medal past top 3
    line1 = (f"**{medal + ' ' if medal else ''}TOP {rank} · "
             f"{clip['view_count']:,} views**")
    bits = [f"[**{title[:90]}**](<{clip['url']}>)",
            _fmt_duration(clip["duration"]), game_name,
            f"clipped by {clip['creator_name'] or '?'}"]
    out = line1 + "\n-# " + " · ".join(b for b in bits if b)
    if summary:
        out += f"\n💬 ***{summary}***"       # bold-italic, full-size white
    return out


def _thumb_embed(clip):
    """Fallback visual when no playable MP4 could be attached."""
    e = discord.Embed(color=0x9146FF)        # twitch purple
    if clip.get("thumbnail_url"):
        e.set_image(url=clip["thumbnail_url"])
    return e


def _fetch_preview(ref, limit):
    """Blocking: grab a compact MP4 that fits the channel upload limit for
    the in-channel player. Twitch offers a quality ladder (480p, then 360p);
    Kick clips (URL refs) expose ONE source format to yt-dlp -- height caps
    are a no-op there, so we fetch once and ffmpeg-downscale if oversized.
    None if nothing fits."""
    if ref.startswith("http"):               # kick: single-format source
        path = downloader.fetch(ref, 480)
        if os.path.getsize(path) <= limit:
            return path
        small = None
        try:
            small = downloader.transcode_to_fit(path, 480)
        except Exception as e:
            print(f"[preview] transcode failed for {ref}: {e}", flush=True)
        os.remove(path)
        if small and os.path.getsize(small) <= limit:
            return small
        if small:
            os.remove(small)
        return None
    for max_h in (480, 360):
        path = downloader.fetch(ref, max_h)
        if os.path.getsize(path) <= limit:
            return path
        os.remove(path)
    return None


def _select_clips(clips, entry):
    """Apply the channel's filter -> [(rank, clip)]. Rank = position in the
    full view-sorted list. NO filter = every clip of the day; min_views and
    top_n combine (top N of the clips over the threshold)."""
    out = list(enumerate(clips, 1))
    if entry.get("min_views"):
        out = [(i, c) for i, c in out
               if c["view_count"] >= entry["min_views"]]
    if entry.get("top_n"):
        out = out[:entry["top_n"]]
    return out


_digest_lock = asyncio.Lock()                # sweep/refresh never interleave
_bg_tasks = set()                            # strong refs: bare create_task
                                             # results can be GC'd mid-flight


def _spawn(coro, what):
    """Fire-and-forget with a strong reference + exception logging."""
    task = asyncio.get_running_loop().create_task(coro)
    _bg_tasks.add(task)

    def _done(t):
        _bg_tasks.discard(t)
        if not t.cancelled() and t.exception():
            print(f"[bg] {what} failed: {t.exception()!r}", flush=True)
    task.add_done_callback(_done)


async def run_digest(force_login=None, force_uid=None, edit_existing=True):
    """Refresh clips for every (user, streamer) pair: fetch each streamer
    once, then apply each user's filter to their private channel -- post new
    matches; when edit_existing, also edit posted cards so rank + views stay
    current (the fast sweep skips that -- it's the expensive Discord part).
    Cards NEVER move: Discord message order is fixed at post time; edits only
    update the text in place."""
    async with _digest_lock:
        return await _run_digest_locked(force_login, force_uid, edit_existing)


async def _run_digest_locked(force_login, force_uid, edit_existing):
    wl = _load(WATCHLIST_PATH)
    pairs = _wl_all_pairs(wl)
    if force_login or force_uid:
        pairs = [p for p in pairs
                 if (force_login is None or p[1] == force_login)
                 and (force_uid is None or p[0] == str(force_uid))]
    posted = _load(POSTED_PATH)
    today = datetime.now(TZ).strftime("%Y-%m-%d")
    posted = {d: v for d, v in posted.items()           # prune old days
              if d >= (datetime.now(TZ) - timedelta(days=3)).strftime("%Y-%m-%d")}
    day = posted.setdefault(today, {})
    by_login = {}
    for uid, login, entry in pairs:
        by_login.setdefault(login, []).append((uid, entry))
    summary = []
    for login, watchers in by_login.items():
        platform, slug = _split_key(login)
        # one API fetch per streamer, wide enough for the loosest filter
        # (100 = page max on both platforms; no-filter channels get it all)
        fetch_n = 100
        if all(w[1].get("top_n") for w in watchers):
            fetch_n = max(w[1]["top_n"] for w in watchers)
        try:
            if platform == "kick":
                clips = await asyncio.to_thread(kick_api.get_top_clips,
                                                slug, fetch_n)
                games = {}                    # kick clips carry game_name
            else:
                clips = await asyncio.to_thread(
                    twitch_api.get_top_clips,
                    watchers[0][1]["user_id"], fetch_n)
                games = await asyncio.to_thread(
                    twitch_api.get_game_names,
                    [c["game_id"] for c in clips])
        except Exception as e:
            print(f"[digest] {login}: {platform} API error {e}", flush=True)
            summary.append(f"{login}: {platform} API error")
            continue
        for uid, entry in watchers:
            key = f"{uid}:{login}"
            cards = day.get(key) or {}
            selected = _select_clips(clips, entry)
            if not selected and not cards:
                summary.append(f"{login}: no matching clips yet today")
                continue
            chan = client.get_channel(entry["channel_id"])
            if chan is None:
                print(f"[digest] {key}: channel {entry['channel_id']} gone,"
                      f" recreating", flush=True)
                chan = await _ensure_channel(login, entry["display_name"],
                                             uid)
                entry["channel_id"] = chan.id
                full = _load(WATCHLIST_PATH)
                full.setdefault(uid, {})[login] = entry
                _save(WATCHLIST_PATH, full)
            if selected and not cards:        # first clips of the day
                date_label = datetime.now(TZ).strftime("%B %-d, %Y")
                await chan.send(f"-# ── **{date_label}** ──────────")
            # summaries live in the index (background AI sweep fills them);
            # one thread-hop grabs everything this channel's edits will need
            ai_rows = {}
            if edit_existing and cards:
                ai_rows = await asyncio.to_thread(
                    lambda: {cid: clip_index.get(cid, uid) for cid in cards})
            # ❌-dismissed clips: never repost, never edit
            dis = await asyncio.to_thread(clip_index.dismissed_ids,
                                          uid, login)
            new = updated = skipped = 0
            bumps = []
            for rank, clip in selected:
                if clip["id"] in dis:
                    continue                  # user ❌'d it -- stays gone
                game = clip.get("game_name") or games.get(clip["game_id"])
                rec = cards.get(clip["id"])
                if rec and not edit_existing:
                    continue                  # fast sweep: new clips only
                row_ai = ai_rows.get(clip["id"])
                if rec and rec.get("m") and (row_ai is None
                                             or not row_ai.get("summary")):
                    # snapshot may predate the AI sweep by minutes -- a stale
                    # None here would strip a freshly-appended 💬 line
                    row_ai = (await asyncio.to_thread(
                        clip_index.get, clip["id"], uid)) or row_ai
                clip_summary = (row_ai or {}).get("summary")
                content = _clip_content(clip, rank, game, clip_summary)
                if (row_ai or {}).get("liked"):
                    content += " · ✅ saved"
                if rec and rec.get("m"):
                    try:
                        pm = chan.get_partial_message(rec["m"])
                        if rec.get("v"):      # video card: panel text only
                            await pm.edit(content=content, embeds=[])
                        else:                 # thumbnail fallback card
                            await pm.edit(content=content,
                                          embed=_thumb_embed(clip))
                        updated += 1
                        bumps.append((clip["id"], clip["view_count"]))
                    except discord.NotFound:
                        cards.pop(clip["id"])  # deleted -> repost next cycle
                    except discord.HTTPException as e:
                        print(f"[digest] {key}: edit {rec['m']} failed {e}",
                              flush=True)
                    continue
                elif rec:
                    continue                  # pre-migration card: leave as-is
                if new >= MAX_NEW:            # flood guard for loose filters
                    skipped += 1
                    continue
                # new clip: attach a playable compact MP4
                ref = clip["url"] if platform == "kick" else clip["id"]
                limit = getattr(chan.guild, "filesize_limit",
                                10 * 1024 * 1024)
                path = None
                try:
                    path = await asyncio.to_thread(_fetch_preview, ref, limit)
                except Exception as e:
                    print(f"[digest] {key}: preview fetch {clip['id']} "
                          f"failed {e}", flush=True)
                if path:
                    msg = await chan.send(
                        content=content,
                        file=discord.File(path,
                                          filename=f"{clip['id']}.mp4"),
                        view=_DownloadView(clip["id"]))
                    os.remove(path)
                    cards[clip["id"]] = {"m": msg.id, "v": True}
                else:                         # too big / fetch failed
                    msg = await chan.send(content=content,
                                          embed=_thumb_embed(clip),
                                          view=_DownloadView(clip["id"]))
                    cards[clip["id"]] = {"m": msg.id, "v": False}
                new += 1
                await asyncio.to_thread(clip_index.upsert_clip, {
                    "clip_id": clip["id"], "uid": uid, "login": login,
                    "platform": platform, "title": clip["title"],
                    "url": clip["url"], "view_count": clip["view_count"],
                    "duration": clip["duration"], "game": game,
                    "creator": clip["creator_name"],
                    "created_at": clip["created_at"], "posted_day": today,
                    "channel_id": chan.id, "msg_id": msg.id,
                    "thumbnail_url": clip.get("thumbnail_url")})
            if bumps:
                await asyncio.to_thread(
                    lambda: [clip_index.bump_views(c, uid, v)
                             for c, v in bumps])
            if skipped:
                await chan.send(f"…{skipped} more clips match your filter -- "
                                f"they'll post over the next refreshes "
                                f"({MAX_NEW} per {POLL_MINUTES}min)")
            day[key] = cards
            _save(POSTED_PATH, posted)
            summary.append(f"{login}: {new} new, {updated} refreshed")
            if new:
                print(f"[digest] {key}: {new} new, {updated} refreshed",
                      flush=True)
    return "\n".join(summary) or "watchlist is empty"


# discord.ext.tasks PERMANENTLY stops a loop whose body raises anything
# outside its blessed connection-error set -- one Forbidden/NotFound must
# never kill the pipeline, so every loop body gets a blanket guard.

@tasks.loop(minutes=POLL_MINUTES)
async def clip_refresh():
    try:
        await run_digest()                   # full pass: new + view/rank edits
    except Exception as e:
        print(f"[refresh] cycle failed: {e!r}", flush=True)


@tasks.loop(minutes=SWEEP_MINUTES)
async def clip_sweep():
    try:
        await run_digest(edit_existing=False)  # fast pass: new clips only
        # heartbeat: the bot's member-list status always shows when it last
        # checked, so "no clips" is visibly different from "not running"
        n = len({p[1] for p in _wl_all_pairs(_load(WATCHLIST_PATH))})
        stamp = datetime.now(TZ).strftime("%-I:%M %p").lower()
        await client.change_presence(activity=discord.Activity(
            type=discord.ActivityType.watching,
            name=f"{n} streamers · last check {stamp} ET"))
    except Exception as e:
        print(f"[sweep] cycle failed: {e!r}", flush=True)


# ---- AI enrichment (summaries + embeddings, trickled in the background) ---------

@tasks.loop(minutes=3)
async def ai_enrich():
    try:
        await _ai_enrich_pass()
    except Exception as e:
        print(f"[ai] enrich cycle failed: {e!r}", flush=True)


async def _ai_enrich_pass():
    """Give recent cards their 💬 summary: pull audio from a small copy of
    the clip, transcribe, summarize, embed for search, then append the line
    to the card. Failures store summary='' so a dead clip never loops."""
    if not ai.enabled():
        return
    rows = await asyncio.to_thread(clip_index.missing_ai, 4)
    for row in rows:
        cid, uid = row["clip_id"], row["uid"]
        # a twin row (same clip, other user) may already be enriched
        twin = await asyncio.to_thread(clip_index.get_any, cid)
        if twin and twin.get("summary"):
            transcript, summary = twin.get("transcript"), twin["summary"]
            embedding = twin.get("embedding")
        else:
            ref = row["url"] if row["platform"] == "kick" else cid
            transcript = None
            try:
                path = await asyncio.to_thread(downloader.fetch, ref, 360)
                transcript = await asyncio.to_thread(ai.transcribe, path)
                os.remove(path)
            except Exception as e:
                print(f"[ai] {cid}: fetch/transcribe failed {e}", flush=True)
            meta = {"title": row["title"], "streamer": row["login"],
                    "game": row["game"], "duration": row["duration"],
                    "view_count": row["view_count"]}
            summary = await asyncio.to_thread(ai.summarize, meta, transcript)
            embedding = await asyncio.to_thread(
                ai.embed, f"{row['login']} {row['title']} {summary or ''} "
                          f"{(transcript or '')[:1500]}") if summary else None
        # summary='' (not NULL) marks a permanent miss -> drops out of queue
        await asyncio.to_thread(clip_index.set_ai, cid, uid,
                                transcript, summary or "", embedding)
        if not summary:
            continue
        chan = client.get_channel(row["channel_id"])
        if chan is None:
            continue
        try:
            msg = await chan.fetch_message(row["msg_id"])
            if "💬" not in (msg.content or ""):
                await msg.edit(content=msg.content
                               + f"\n💬 ***{summary}***")
            print(f"[ai] {cid}: summarized", flush=True)
        except discord.HTTPException as e:
            print(f"[ai] {cid}: card edit failed {e}", flush=True)


# ---- daily recap ---------------------------------------------------------------

def _recap_embed(entry, key, day, stats, hours, paragraph, hidden):
    pretty = datetime.strptime(day, "%Y-%m-%d").strftime("%B %-d, %Y")
    quiet = not stats["clip_count"] and hours is None
    if quiet and not paragraph:
        paragraph = (f"All quiet — no stream and no clips today. I checked "
                     f"every {SWEEP_MINUTES} minutes. ✅")
    e = discord.Embed(color=0x9146FF,
                      title=f"📊 {entry['display_name']} — {pretty}",
                      description=paragraph or None)
    if hours is not None:
        h, m = int(hours), int(round((hours % 1) * 60))
        e.add_field(name="⏱ Stream time", value=f"{h}h {m:02d}m")
    else:
        e.add_field(name="⏱ Stream time", value="—")
    e.add_field(name="🎬 Clips", value=f"{stats['clip_count']:,}")
    e.add_field(name="👁 Clip views", value=f"{stats['total_views']:,}")
    if hidden:
        e.add_field(name="🚫 Hidden by your filter",
                    value=f"{hidden:,} clips didn't pass "
                          f"**{_filter_desc(entry)}** — `/filters` to loosen",
                    inline=False)
    top = stats.get("top")
    if top:
        t = (top["title"] or "(untitled)").replace("[", "(").replace("]", ")")
        e.add_field(name="🏆 Top clip",
                    value=f"[{t[:80]}](<{top['url']}>) · "
                          f"{top['view_count']:,} views", inline=False)
    e.set_footer(text=f"posts daily · bot checks every {SWEEP_MINUTES} min")
    return e


async def _post_recap(uid, key, entry, day):
    """One channel's recap for one local day. ALWAYS posts -- a quiet-day
    recap is the daily proof-of-life ('no clips' must look different from
    'bot is down'). Returns True if posted."""
    stats = await asyncio.to_thread(clip_index.day_stats, uid, key, day)
    hours = _stream_hours(key, day)
    hidden = 0
    try:                                    # clips made vs shown that day
        platform, slug = _split_key(key)
        since = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=TZ)
        if platform == "kick":
            clips = await asyncio.to_thread(kick_api.get_top_clips, slug, 100)
        else:
            clips = await asyncio.to_thread(twitch_api.get_top_clips,
                                            entry["user_id"], 100, since)
        hidden = max(0, len(clips) - len(_select_clips(clips, entry)))
    except Exception:
        pass
    paragraph = None
    if ai.enabled() and (stats["clip_count"] or hours):
        top = stats.get("top") or {}
        paragraph = await asyncio.to_thread(ai.recap_paragraph, {
            "streamer": entry["display_name"], "date": day,
            "stream_hours": hours, "clip_count": stats["clip_count"],
            "total_views": stats["total_views"],
            "top_clip_title": top.get("title")}, stats["summaries"])
    chan = client.get_channel(entry["channel_id"])
    if chan is None:
        return False
    await chan.send(embed=_recap_embed(entry, key, day, stats, hours,
                                       paragraph, hidden))
    return True


@tasks.loop(time=dtime(int(_rh), int(_rm), tzinfo=TZ))
async def daily_recap():
    try:
        day = (datetime.now(TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
        for uid, key, entry in _wl_all_pairs(_load(WATCHLIST_PATH)):
            try:
                if await _post_recap(uid, key, entry, day):
                    print(f"[recap] {uid}:{key} posted for {day}",
                          flush=True)
            except Exception as e:
                print(f"[recap] {uid}:{key} failed {e}", flush=True)
    except Exception as e:
        print(f"[recap] cycle failed: {e!r}", flush=True)


# ---- AI search (-search / /search with a ◀ ▶ pager) ----------------------------

_searches = {}                               # token -> {uid, rows, i}


def _search_embed(row, i, n):
    e = discord.Embed(
        color=0x9146FF,
        title=(row["title"] or "(untitled)")[:200],
        description=((f"💬 {row['summary']}\n" if row.get("summary") else "")
                     + f"[Jump to clip ↗](https://discord.com/channels/"
                       f"{GUILD_ID}/{row['channel_id']}/{row['msg_id']})"))
    e.add_field(name="Views", value=f"{row['view_count'] or 0:,}")
    e.add_field(name="Length", value=_fmt_duration(row["duration"] or 0))
    e.add_field(name="Streamer", value=row["login"] or "?")
    if row.get("thumbnail_url"):
        e.set_image(url=row["thumbnail_url"])
    e.set_footer(text=f"result {i + 1}/{n} · {row['platform']} · "
                      f"{row.get('posted_day') or ''}")
    return e


def _search_view(token, i, n, row):
    view = discord.ui.View(timeout=None)
    view.add_item(discord.ui.Button(style=discord.ButtonStyle.secondary,
                                    emoji="◀", row=0, disabled=i <= 0,
                                    custom_id=f"srch:{token}:p"))
    view.add_item(discord.ui.Button(style=discord.ButtonStyle.secondary,
                                    label=f"{i + 1}/{n}", row=0,
                                    disabled=True,
                                    custom_id=f"srch:{token}:c"))
    view.add_item(discord.ui.Button(style=discord.ButtonStyle.secondary,
                                    emoji="▶", row=0, disabled=i >= n - 1,
                                    custom_id=f"srch:{token}:n"))
    view.add_item(discord.ui.Button(style=discord.ButtonStyle.primary,
                                    label="Download", row=0,
                                    custom_id=f"dl:{row['clip_id']}"))
    if row.get("url"):
        view.add_item(discord.ui.Button(style=discord.ButtonStyle.link,
                                        label="Watch", row=0,
                                        url=row["url"]))
    return view


async def _send_search_pager(channel, uid, query):
    """Text-command flavor of /search (DMs and your own private channels)."""
    if not query:
        await channel.send('usage: `-search "jynxzi getting trolled"`')
        return
    rows, token = await _run_search(uid, query)
    if not rows:
        await channel.send(f"no clips matched **{query[:80]}**")
        return
    await channel.send(embed=_search_embed(rows[0], 0, len(rows)),
                       view=_search_view(token, 0, len(rows), rows[0]))


async def _run_search(uid, query):
    """-> (rows, token) or ([], None). Embedding search when AI is up, with
    a keyword fallback so search always answers something."""
    emb = None
    if ai.enabled():
        emb = await asyncio.to_thread(ai.embed, query)
    rows = await asyncio.to_thread(clip_index.search, str(uid), query,
                                   emb, 12)
    if emb is not None and not rows:         # nothing embedded yet
        rows = await asyncio.to_thread(clip_index.search, str(uid), query,
                                       None, 12)
    if not rows:
        return [], None
    token = secrets.token_hex(4)
    _searches[token] = {"uid": str(uid), "rows": rows, "i": 0}
    while len(_searches) > 60:               # drop oldest sessions
        _searches.pop(next(iter(_searches)))
    return rows, token


# ---- live indicator (🔴 channel-name prefix while streaming) --------------------

def _log_stream_state(log, login, is_live):
    """Track stream sessions off the 5-min live poll (feeds the daily recap's
    stream-length stat). 'last' advances every tick while live so a crash
    mid-stream still leaves an accurate end estimate. Mutates log in place."""
    sessions = log.setdefault(login, [])
    now = datetime.now(TZ).isoformat()
    open_s = (sessions[-1] if sessions and sessions[-1].get("end") is None
              else None)
    if is_live:
        if open_s:
            open_s["last"] = now
        else:
            sessions.append({"start": now, "last": now, "end": None})
    elif open_s:
        open_s["end"] = open_s.get("last", now)
    cutoff = (datetime.now(TZ) - timedelta(days=8)).isoformat()
    log[login] = [s for s in sessions
                  if (s.get("end") or s.get("last") or s["start"]) >= cutoff]


def _stream_hours(login, day):
    """Hours streamed on local day YYYY-MM-DD, or None if nothing logged."""
    d0 = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=TZ)
    d1 = d0 + timedelta(days=1)
    total = timedelta()
    for s in _load(STREAM_LOG_PATH).get(login) or []:
        start = datetime.fromisoformat(s["start"])
        end = datetime.fromisoformat(s.get("end") or s.get("last")
                                     or s["start"])
        lo, hi = max(start, d0), min(end, d1)
        if hi > lo:
            total += hi - lo
    return round(total.total_seconds() / 3600, 1) if total else None


@tasks.loop(minutes=5)
async def live_status():
    try:
        await _live_status_pass()
    except Exception as e:
        print(f"[live] cycle failed: {e!r}", flush=True)


async def _live_status_pass():
    """Prefix every watcher's channel with 🔴 while the streamer is live and
    ping subscribers once per go-live. Renames only on state transitions
    (Discord caps channel renames at 2 per 10 min). A platform check that
    FAILED yields unknown (None) -- state is left alone rather than treating
    everyone as offline, which would rename-flap live channels on API blips."""
    pairs = _wl_all_pairs(_load(WATCHLIST_PATH))
    if not pairs:
        return
    tw_ids = [e["user_id"] for _, k, e in pairs
              if _split_key(k)[0] == "twitch"]
    kick_slugs = sorted({_split_key(k)[1] for _, k, e in pairs
                         if _split_key(k)[0] == "kick"})
    try:
        live_tw = (await asyncio.to_thread(twitch_api.get_live_user_ids,
                                           tw_ids) if tw_ids else set())
    except Exception as e:
        print(f"[live] Helix error {e}", flush=True)
        live_tw = None                        # unknown, not "all offline"
    live_kick = (await asyncio.to_thread(kick_api.is_live, kick_slugs)
                 if kick_slugs else set())    # None on total Kick failure

    def _pair_live(key, entry):
        """True/False, or None when that platform's check failed."""
        platform, slug = _split_key(key)
        if platform == "kick":
            return None if live_kick is None else slug in live_kick
        return None if live_tw is None else entry["user_id"] in live_tw

    slog = _load(STREAM_LOG_PATH)             # session tracking for recaps
    for key in {p[1] for p in pairs}:
        entry = next(e for u, k, e in pairs if k == key)
        state = _pair_live(key, entry)
        if state is not None:
            _log_stream_state(slog, key, state)
    _save(STREAM_LOG_PATH, slog)
    pinged = set()                            # one ping per streamer per tick
    for uid, login, entry in pairs:
        chan = client.get_channel(entry["channel_id"])
        if chan is None:
            continue
        is_live = _pair_live(login, entry)
        if is_live is None:
            continue                          # unknown -> leave marker as-is
        marked = chan.name.startswith(LIVE_PREFIX)
        try:
            if is_live and not marked:
                await chan.edit(name=f"{LIVE_PREFIX}{chan.name}")
                print(f"[live] {login} went LIVE", flush=True)
                if login in pinged:
                    continue
                pinged.add(login)
                subs = _load(SUBS_PATH).get(login) or []
                pchan = discord.utils.get(chan.guild.text_channels,
                                          name=PING_CHANNEL)
                if subs and pchan:
                    mentions = " ".join(f"<@{u}>" for u in subs)
                    await pchan.send(
                        f"🔴 **{entry['display_name']}** just went live -- "
                        f"{_watch_url(login)}\n{mentions}")
            elif not is_live and marked:
                await chan.edit(name=chan.name.removeprefix(LIVE_PREFIX))
                print(f"[live] {login} went offline", flush=True)
        except discord.HTTPException as e:
            print(f"[live] {login}: rename failed {e}", flush=True)


# ---- slash commands (same handlers as the DM commands) --------------------------

def _chunks(text, size=1900):
    """Split a reply on line boundaries to stay under Discord's 2000 cap."""
    out, chunk = [], ""
    for line in text.splitlines():
        if len(chunk) + len(line) > size:
            out.append(chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk.strip():
        out.append(chunk)
    return out


def _slash_place(interaction):
    """Where was a slash command used? -> ('add' | 'mine', login | None).
    'mine' = the caller's own streamer channel."""
    if getattr(interaction.channel, "name", "") == ADD_CHANNEL:
        return "add", None
    own = _channel_owner(interaction.channel_id)
    if own and own[0] == str(interaction.user.id):
        return "mine", own[1]
    return None, None


async def _wrong_channel(interaction, what="that"):
    guild = interaction.guild
    add = (discord.utils.get(guild.text_channels, name=ADD_CHANNEL)
           if guild else None)
    where = f"<#{add.id}>" if add else "the add-streamers channel"
    await interaction.response.send_message(
        f"use {what} in {where} (or your own streamer channel)",
        ephemeral=True)


class _WatchModal(discord.ui.Modal, title="Add a streamer"):
    target = discord.ui.TextInput(
        label="TwitchTracker/Twitch link or channel name",
        placeholder="twitchtracker.com/jynxzi")

    async def on_submit(self, interaction):
        await interaction.response.defer()
        await interaction.followup.send(
            await cmd_watch(str(self.target.value), interaction.user.id))


def _streamer_select(uid, prefix, placeholder):
    """Ephemeral dropdown of the user's own streamers (None if empty)."""
    mine = _load(WATCHLIST_PATH).get(uid, {})
    if not mine:
        return None
    view = discord.ui.View(timeout=None)
    view.add_item(discord.ui.Select(
        custom_id=f"{prefix}:{uid}", placeholder=placeholder,
        options=[discord.SelectOption(label=e["display_name"], value=l,
                                      description=_filter_desc(e))
                 for l, e in list(mine.items())[:25]]))
    return view


@tree.command(name="watch", description="Add a streamer -- you get a private"
              " channel with their top clips", guild=GUILD)
@app_commands.describe(streamer="TwitchTracker URL, twitch.tv URL, or name "
                       "(leave empty for an input box)")
async def slash_watch(interaction: discord.Interaction,
                      streamer: str = None):
    if _slash_place(interaction)[0] != "add":
        await _wrong_channel(interaction, "/watch")
        return
    if not streamer:
        await interaction.response.send_modal(_WatchModal())
        return
    await interaction.response.defer()
    await interaction.followup.send(
        await cmd_watch(streamer, interaction.user.id))


@tree.command(name="unwatch", description="Stop watching a streamer",
              guild=GUILD)
@app_commands.describe(streamer="Streamer name (leave empty to pick from "
                       "a list)")
async def slash_unwatch(interaction: discord.Interaction,
                        streamer: str = None):
    if _slash_place(interaction)[0] != "add":
        await _wrong_channel(interaction, "/unwatch")
        return
    if not streamer:
        uid = str(interaction.user.id)
        view = _streamer_select(uid, "unw", "pick who to stop watching…")
        if view is None:
            await interaction.response.send_message(
                "you're not watching anyone yet", ephemeral=True)
            return
        await interaction.response.send_message("stop watching who?",
                                                view=view, ephemeral=True)
        return
    await interaction.response.send_message(
        await cmd_unwatch(streamer, interaction.user.id))


@tree.command(name="watching", description="List your watched streamers",
              guild=GUILD)
async def slash_watching(interaction: discord.Interaction):
    if _slash_place(interaction)[0] != "add":
        await _wrong_channel(interaction, "/watching")
        return
    chunks = _chunks(cmd_cliplist(interaction.user.id))
    await interaction.response.send_message(chunks[0])
    for extra in chunks[1:]:                 # 25+ streamers can pass 2000
        await interaction.followup.send(extra)


_TOP_EMOJI = {3: "🥇", 5: "🏅", 10: "🔟", 25: "📈"}
_VIEW_EMOJI = {3: "👀", 5: "👀", 10: "🔥", 20: "🚀"}
_VIEW_PRESETS = (3, 5, 10, 20)
_TOP_PRESETS = (3, 5, 10, 25)


def _filter_label(entry):
    """Emoji-vocabulary rendering of the current filter, e.g.
    '🔟 Top 10 · 🔥 50+ views' or '✏️ 75+ views (custom)'."""
    mv, tn = entry.get("min_views"), entry.get("top_n")
    parts = []
    if tn:
        parts.append(f"{_TOP_EMOJI.get(tn, '📈')} Top {tn}")
    if mv:
        if mv in _VIEW_EMOJI:
            parts.append(f"{_VIEW_EMOJI[mv]} {mv:,}+ views")
        else:
            parts.append(f"✏️ {mv:,}+ views (custom)")
    return " · ".join(parts) if parts else "⭐ Everything"


def _filter_panel_embed(entry, footer=None):
    e = discord.Embed(
        color=0x9146FF, title="🎛️ Clip filters",
        description=f"Current: **{_filter_label(entry)}**\n"
                    f"-# A Top pick and a view minimum combine. Click a lit "
                    f"button to turn it off.")
    if footer:
        e.set_footer(text=footer)
    return e


def _filter_panel_view(uid, login, entry, prev=None):
    """The big always-visible control panel: one button per option, active
    picks lit (blurple), everything one click away -- no dropdown scrolling.
    `prev` = (min_views, top_n) before the last change; Undo stays enabled
    and toggles back and forth (click it twice = redo)."""
    tn, mv = entry.get("top_n"), entry.get("min_views")
    view = discord.ui.View(timeout=None)

    def add(row, emoji, label, action, active, disabled=False):
        style = (discord.ButtonStyle.primary if active
                 else discord.ButtonStyle.secondary)
        view.add_item(discord.ui.Button(
            style=style, emoji=emoji, label=label, row=row,
            disabled=disabled, custom_id=f"fbtn:{uid}:{login}:{action}"))

    add(0, "⭐", "Everything", "all", not tn and not mv)
    for n in _TOP_PRESETS:
        add(0, _TOP_EMOJI[n], f"Top {n}", f"top{n}", tn == n)
    for n in _VIEW_PRESETS:
        add(1, _VIEW_EMOJI[n], f"{n}+ views", f"v{n}", mv == n)
    custom_active = bool(mv) and mv not in _VIEW_PRESETS
    add(1, "✏️", f"{mv:,}+ (custom)" if custom_active else "Custom…",
        "custom", custom_active)
    if prev is not None:
        view.add_item(discord.ui.Button(
            style=discord.ButtonStyle.secondary, emoji="↩️", label="Undo",
            row=2,
            custom_id=f"fund:{uid}:{login}:{prev[0] or 0}:{prev[1] or 0}"))
    else:
        view.add_item(discord.ui.Button(
            style=discord.ButtonStyle.secondary, emoji="↩️", label="Undo",
            row=2, disabled=True, custom_id="fund:disabled"))
    return view


async def _confirm_footer(entry):
    """'With this filter, X of today's N clips would have posted · …'"""
    tail = "Applies to new clips only — nothing is deleted."
    try:
        if entry.get("platform") == "kick":
            clips = await asyncio.to_thread(kick_api.get_top_clips,
                                            entry["login"], 100)
        else:
            clips = await asyncio.to_thread(twitch_api.get_top_clips,
                                            entry["user_id"], 100)
        if not clips:
            return "No clips yet today — this applies to the next one."
        n = len(_select_clips(clips, entry))
        return (f"With this filter, {n} of today's {len(clips)} clips "
                f"would have posted · {tail}")
    except Exception:
        return tail


class _MinViewsModal(discord.ui.Modal, title="Custom minimum views"):
    amount = discord.ui.TextInput(label="Only post clips with at least…",
                                  placeholder="e.g. 75", required=True,
                                  min_length=1, max_length=7)

    def __init__(self, uid, login, prev_mv, prev_tn):
        super().__init__(custom_id="filter_custom_modal")
        self.uid, self.login = uid, login
        self.prev = (prev_mv, prev_tn)

    async def on_submit(self, interaction):
        raw = str(self.amount.value).strip().replace(",", "")
        if not raw.isdigit() or int(raw) < 1:
            await interaction.response.send_message(
                "That needs to be a number, e.g. **75**.\n"
                "-# Nothing was changed.", ephemeral=True)
            return
        wl = _load(WATCHLIST_PATH)
        entry = wl.get(self.uid, {}).get(self.login)
        if entry is None:
            await interaction.response.send_message(
                "that streamer isn't on your list anymore", ephemeral=True)
            return
        entry["min_views"] = int(raw)
        _save(WATCHLIST_PATH, wl)
        await interaction.response.defer(ephemeral=True)   # 3s ack window
        _spawn(run_digest(force_login=self.login, force_uid=self.uid),
               f"filter refresh {self.login}")
        await interaction.followup.send(
            embed=_filter_panel_embed(entry, await _confirm_footer(entry)),
            view=_filter_panel_view(self.uid, self.login, entry,
                                    prev=self.prev),
            ephemeral=True)


async def _filter_gui(interaction):
    place, login = _slash_place(interaction)
    if place != "mine":
        await interaction.response.send_message(
            "use this inside one of your own streamer channels",
            ephemeral=True)
        return
    uid = str(interaction.user.id)
    entry = _load(WATCHLIST_PATH).get(uid, {}).get(login, {})
    await interaction.response.defer(ephemeral=True)       # 3s ack window
    await interaction.followup.send(
        embed=_filter_panel_embed(entry, await _confirm_footer(entry)),
        view=_filter_panel_view(uid, login, entry), ephemeral=True)


@tree.command(name="filters", description="Pick what this channel shows "
              "(clickable menu -- combine a top-N with a view minimum)",
              guild=GUILD)
async def slash_filters(interaction: discord.Interaction):
    await _filter_gui(interaction)


def _help_text(guild):
    add = discord.utils.get(guild.text_channels, name=ADD_CHANNEL)
    ping = discord.utils.get(guild.text_channels, name=PING_CHANNEL)
    add_m = f"<#{add.id}>" if add else "**#add-streamers**"
    ping_m = f"<#{ping.id}>" if ping else "**#live-pings**"
    return (
        f"## 🎬 twitch bot — commands\n"
        f"**{add_m}** — manage your streamers\n"
        f"• paste a TwitchTracker/Twitch link, **kick.com link**, or name → "
        f"you get a **private clips channel** for them\n"
        f"• `/watch` `/unwatch` `/watching` — add / remove / list yours\n"
        f"**{ping_m}** — go-live pings\n"
        f"• type a streamer's name → get @pinged when they go live "
        f"(type it again to stop)\n"
        f"• `-stop <name>` turn a ping off · `-mypings` list yours\n"
        f"**your streamer channels** (private to you)\n"
        f"• default: **every clip of the day** gets posted\n"
        f"• `/filters` → clickable menu to narrow it -- pick one, or "
        f"combine two (e.g. Top 10 **+** 50+ views)\n"
        f"• typing works too: `more than 50 views` · `-top 8` · "
        f"`-reset` (back to everything)\n"
        f"• `/top` → list beyond the cards · `/digest` → refresh right now\n"
        f"• 🔎 `/search` → find a clip by describing it (\"jynxzi getting "
        f"trolled\") -- ◀ ▶ to flip through matches\n"
        f"• 📊 `/recap` → today's numbers now; a full recap posts "
        f"automatically each night (stream time, clips, views, AI summary)\n"
        f"• 💬 cards grow a one-line AI summary a few minutes after "
        f"posting\n"
        f"**the cards**\n"
        f"• 🥇 rank + view count update live all day · video plays "
        f"in-channel\n"
        f"• ⬇️ **Download** sends the full-quality MP4 to your DMs\n"
        f"• ✅ save a clip (`/saved` lists them; click again to unsave) · "
        f"❌ delete the card for good (never reposts)\n"
        f"• 🔴 in a channel name = that streamer is live right now")


@tree.command(name="help", description="All twitch bot commands and how the "
              "channels work", guild=GUILD)
async def slash_help(interaction: discord.Interaction):
    await interaction.response.send_message(_help_text(interaction.guild),
                                            ephemeral=True)


@tree.command(name="top", description="Today's top clips as a list",
              guild=GUILD)
@app_commands.describe(streamer="Streamer (optional in a streamer channel)",
                       count="How many, up to 25 (default 10)")
async def slash_top(interaction: discord.Interaction,
                    streamer: str = "", count: int = 10):
    place, login = _slash_place(interaction)
    if place is None:
        await _wrong_channel(interaction, "/top")
        return
    if not streamer and login:
        streamer = login                     # default to this channel's guy
    uid = str(interaction.user.id)
    mine = _load(WATCHLIST_PATH).get(uid, {})
    if not streamer and len(mine) != 1:      # ambiguous -> pick from a list
        view = _streamer_select(uid, f"tops.{count}",
                                "whose top clips?")
        if view is None:
            await interaction.response.send_message(
                "you're not watching anyone yet", ephemeral=True)
            return
        await interaction.response.send_message("whose top clips?",
                                                view=view, ephemeral=True)
        return
    await interaction.response.defer()
    reply = await cmd_top(f"{streamer} {count}".strip(),
                          interaction.user.id)
    for chunk in _chunks(reply):
        await interaction.followup.send(chunk)


@tree.command(name="search", description="Find a clip by describing it "
              "(searches your clips' titles, transcripts, and AI summaries)",
              guild=GUILD)
@app_commands.describe(query='e.g. "Jynxzi getting trolled by Hivise"')
async def slash_search(interaction: discord.Interaction, query: str):
    place, _ = _slash_place(interaction)
    if place is None:
        await _wrong_channel(interaction, "/search")
        return
    await interaction.response.defer(ephemeral=True)
    rows, token = await _run_search(interaction.user.id, query)
    if not rows:
        await interaction.followup.send(
            f"no clips matched **{query[:80]}** -- I can only search clips "
            f"posted since the index went live", ephemeral=True)
        return
    await interaction.followup.send(
        embed=_search_embed(rows[0], 0, len(rows)),
        view=_search_view(token, 0, len(rows), rows[0]), ephemeral=True)


@tree.command(name="saved", description="Your ✅-saved clips", guild=GUILD)
async def slash_saved(interaction: discord.Interaction):
    if _slash_place(interaction)[0] is None:
        await _wrong_channel(interaction, "/saved")
        return
    rows = await asyncio.to_thread(clip_index.liked_rows,
                                   str(interaction.user.id))
    if not rows:
        await interaction.response.send_message(
            "no saved clips yet -- hit ✅ on a card to keep it",
            ephemeral=True)
        return
    lines = ["**your ✅ saved clips**"]
    for r in rows:
        t = (r["title"] or "(untitled)").replace("[", "(").replace("]", ")")
        jump = (f"https://discord.com/channels/{GUILD_ID}/"
                f"{r['channel_id']}/{r['msg_id']}")
        lines.append(f"• [{t[:60]}](<{r['url'] or jump}>) · "
                     f"{r['view_count'] or 0:,} views · {r['login']} · "
                     f"[jump ↗](<{jump}>)")
    chunks = _chunks("\n".join(lines))
    await interaction.response.send_message(chunks[0], ephemeral=True)
    for extra in chunks[1:]:
        await interaction.followup.send(extra, ephemeral=True)


@tree.command(name="recap", description="Post this channel's recap for "
              "today so far (stream time, clips, views, AI summary)",
              guild=GUILD)
async def slash_recap(interaction: discord.Interaction):
    place, key = _slash_place(interaction)
    if place != "mine":
        await interaction.response.send_message(
            "use /recap inside one of your own streamer channels",
            ephemeral=True)
        return
    uid = str(interaction.user.id)
    entry = _load(WATCHLIST_PATH).get(uid, {}).get(key)
    await interaction.response.defer(ephemeral=True)
    day = datetime.now(TZ).strftime("%Y-%m-%d")
    if await _post_recap(uid, key, entry, day):
        await interaction.followup.send("recap posted ⬆", ephemeral=True)
    else:
        await interaction.followup.send(
            "nothing to recap yet today -- no clips or stream time logged",
            ephemeral=True)


@tree.command(name="digest", description="Refresh your clips right now",
              guild=GUILD)
async def slash_digest(interaction: discord.Interaction):
    place, login = _slash_place(interaction)
    if place is None:
        await _wrong_channel(interaction, "/digest")
        return
    await interaction.response.defer()
    reply = await run_digest(force_login=login,
                             force_uid=interaction.user.id)
    try:
        for chunk in _chunks(reply):
            await interaction.followup.send(chunk)
    except discord.HTTPException as e:       # webhook token expires at 15min
        print(f"[digest] /digest reply failed: {e}", flush=True)


# ---- download button -----------------------------------------------------------

async def _handle_download(interaction, clip_id):
    """Fetch the MP4 and DM it to whoever pressed the button -- the channel
    stays clean (the press itself only shows an ephemeral 'sent to your DMs').
    Falls back to an ephemeral reply if their DMs are closed."""
    lock = _dl_locks.setdefault(clip_id, asyncio.Lock())
    if lock.locked():
        await interaction.response.send_message(
            "already fetching that one -- hold on", ephemeral=True)
        return
    await interaction.response.defer(thinking=True, ephemeral=True)
    async with lock:
        ref = clip_id                        # twitch slug works directly...
        if clip_id.startswith("kick_"):      # ...kick needs its page URL
            row = await asyncio.to_thread(clip_index.get_any, clip_id)
            if not row or not row.get("url"):
                await interaction.followup.send(
                    "can't find that clip's source anymore", ephemeral=True)
                return
            ref = row["url"]
        limit = 10 * 1024 * 1024             # DM upload limit (no boost perk)
        path, size = None, None
        try:
            # source quality first, then step down until it fits the limit
            for max_h in (None, 720, 480):
                if path:
                    os.remove(path)
                path = await asyncio.to_thread(downloader.fetch,
                                               ref, max_h)
                size = os.path.getsize(path)
                if size <= limit:
                    break
        except Exception as e:
            print(f"[dl] {clip_id}: fetch failed {e}", flush=True)
            await interaction.followup.send(f"couldn't fetch that clip: `{e}`",
                                            ephemeral=True)
            return
        try:
            if size <= limit:
                payload = {"file": discord.File(path,
                                                filename=f"{clip_id}.mp4")}
                note = f"attached ({size:,}B)"
            elif clip_id.startswith("kick_"):
                # kick's only yt-dlp format is an m3u8 playlist -- a "direct
                # link" would download an unplayable text file; send the page
                payload = {"content":
                           f"clip is {size / 1e6:.1f}MB (over the DM upload "
                           f"limit) -- watch/save it on Kick: {ref}"}
                note = f"too big ({size:,}B), sent kick page"
            else:
                url = await asyncio.to_thread(downloader.direct_url, ref)
                payload = {"content":
                           f"clip is {size / 1e6:.1f}MB even at 480p (over "
                           f"the DM upload limit) -- [direct MP4 link]({url})"
                           f" (expires, grab it soon)"}
                note = f"too big ({size:,}B), sent link"
            try:
                await interaction.user.send(**payload)
                await interaction.followup.send("📬 sent it to your DMs",
                                                ephemeral=True)
            except discord.Forbidden:        # DMs closed -> ephemeral instead
                if "file" in payload:        # File objects are single-use
                    payload = {"file": discord.File(
                        path, filename=f"{clip_id}.mp4")}
                await interaction.followup.send(ephemeral=True, **payload)
                note += " (DMs closed, ephemeral)"
            print(f"[dl] {clip_id} -> {interaction.user.id}: {note}",
                  flush=True)
        finally:
            try:
                if path:
                    os.remove(path)
            except OSError:
                pass
    if not lock.locked():                    # unbounded-dict housekeeping
        _dl_locks.pop(clip_id, None)


@client.event
async def on_interaction(interaction):
    if interaction.type != discord.InteractionType.component:
        return
    custom_id = (interaction.data or {}).get("custom_id", "")
    if custom_id.startswith("dl:"):
        await _handle_download(interaction, custom_id[3:])
    elif custom_id.startswith(("like:", "nope:")):   # card ✅ / ❌
        cid = custom_id[5:]
        own = _channel_owner(interaction.channel_id)
        if not own or own[0] != str(interaction.user.id):
            await interaction.response.send_message(
                "that card belongs to someone else", ephemeral=True)
            return
        uid, key = own
        row = await asyncio.to_thread(clip_index.get, cid, uid)
        if row is None:                      # pre-index card: minimal row
            await asyncio.to_thread(clip_index.upsert_clip, {
                "clip_id": cid, "uid": uid, "login": key,
                "platform": _split_key(key)[0], "title": None, "url": None,
                "view_count": None, "duration": None, "game": None,
                "creator": None, "created_at": None,
                "posted_day": datetime.now(TZ).strftime("%Y-%m-%d"),
                "channel_id": interaction.channel_id,
                "msg_id": interaction.message.id, "thumbnail_url": None})
        if custom_id.startswith("like:"):
            liked = 0 if (row and row.get("liked")) else 1
            await asyncio.to_thread(clip_index.set_flag, cid, uid,
                                    liked=liked)
            marker = " · ✅ saved"
            content = interaction.message.content or ""
            if liked and marker not in content:
                content += marker
            else:
                content = content.replace(marker, "")
            await interaction.response.edit_message(content=content)
        else:                                # ❌: delete + never repost
            await asyncio.to_thread(clip_index.set_flag, cid, uid,
                                    dismissed=1)
            await interaction.response.defer()
            try:
                await interaction.message.delete()
            except discord.HTTPException as e:
                print(f"[nope] delete failed {e}", flush=True)
            print(f"[nope] {uid} dismissed {cid}", flush=True)
    elif custom_id.startswith("srch:"):      # search pager arrows
        _, token, direction = custom_id.split(":")
        sess = _searches.get(token)
        if sess is None:
            await interaction.response.send_message(
                "that search expired -- run /search again", ephemeral=True)
            return
        if str(interaction.user.id) != sess["uid"]:
            await interaction.response.send_message(
                "that search belongs to someone else", ephemeral=True)
            return
        sess["i"] = max(0, min(len(sess["rows"]) - 1,
                               sess["i"] + (1 if direction == "n" else -1)))
        row, i, n = sess["rows"][sess["i"]], sess["i"], len(sess["rows"])
        await interaction.response.edit_message(
            embed=_search_embed(row, i, n), view=_search_view(token, i, n,
                                                              row))
    elif custom_id.startswith(("fbtn:", "fund:")):   # the filter panel
        if custom_id == "fund:disabled":
            return
        # kick watchlist keys contain ':' ("kick:xqc"), so parse from the
        # OUTSIDE in: prefix + uid from the left, action/state from the right
        prefix, _, rest = custom_id.partition(":")
        if prefix == "fund":
            head, mv_s, tn_s = rest.rsplit(":", 2)
        else:
            head, _, action = rest.rpartition(":")
        uid, _, login = head.partition(":")
        if str(interaction.user.id) != uid:
            await interaction.response.send_message(
                "that panel belongs to someone else", ephemeral=True)
            return
        wl = _load(WATCHLIST_PATH)
        entry = wl.get(uid, {}).get(login)
        if entry is None:
            await interaction.response.edit_message(
                content="that streamer isn't on your list anymore",
                embed=None, view=None)
            return
        prev = (entry.get("min_views"), entry.get("top_n"))
        if prefix == "fund":                 # undo: swap to the stored state
            entry["min_views"] = int(mv_s) or None
            entry["top_n"] = int(tn_s) or None
            footer_note = "Restored · Applies to new clips only."
        else:
            if action == "custom":           # number typed in the modal
                await interaction.response.send_modal(
                    _MinViewsModal(uid, login, *prev))
                return
            if action == "all":
                entry["min_views"], entry["top_n"] = None, None
            elif action.startswith("top"):   # click a lit button = toggle off
                n = int(action[3:])
                entry["top_n"] = None if entry.get("top_n") == n else n
            elif action.startswith("v"):
                n = int(action[1:])
                entry["min_views"] = (None if entry.get("min_views") == n
                                      else n)
            footer_note = None
        _save(WATCHLIST_PATH, wl)
        # ack NOW -- _confirm_footer does a live clips fetch that can blow
        # the 3-second component-ack window (token refresh, kick paging)
        await interaction.response.defer()
        _spawn(run_digest(force_login=login, force_uid=uid),
               f"filter refresh {login}")
        footer = footer_note or await _confirm_footer(entry)
        await interaction.edit_original_response(
            content=None, embed=_filter_panel_embed(entry, footer),
            view=_filter_panel_view(uid, login, entry, prev=prev))
    elif custom_id == "open_filters":        # pinned message button
        own = _channel_owner(interaction.channel_id)
        if not own or own[0] != str(interaction.user.id):
            await interaction.response.send_message(
                "this channel belongs to someone else", ephemeral=True)
            return
        uid, login = own
        entry = _load(WATCHLIST_PATH).get(uid, {}).get(login, {})
        await interaction.response.defer(ephemeral=True)   # 3s ack window
        await interaction.followup.send(
            embed=_filter_panel_embed(entry, await _confirm_footer(entry)),
            view=_filter_panel_view(uid, login, entry), ephemeral=True)
    elif custom_id == "open_help":           # pinned message button
        await interaction.response.send_message(
            _help_text(interaction.guild), ephemeral=True)
    elif custom_id.startswith(("unw:", "tops.")):   # streamer pickers
        prefix, _, uid = custom_id.partition(":")
        if str(interaction.user.id) != uid:
            await interaction.response.send_message(
                "that menu belongs to someone else", ephemeral=True)
            return
        login = (interaction.data.get("values") or [""])[0]
        if prefix == "unw":
            reply = await cmd_unwatch(login, interaction.user.id)
            await interaction.response.edit_message(content=reply, view=None)
        else:                                # tops.<count>
            await interaction.response.defer()   # cmd_top does network I/O
            count = prefix.split(".", 1)[1]
            chunks = _chunks(await cmd_top(f"{login} {count}",
                                           interaction.user.id))
            await interaction.edit_original_response(content=chunks[0],
                                                     view=None)
            for extra in chunks[1:]:
                await interaction.followup.send(extra, ephemeral=True)


# ---- DM dispatch ----------------------------------------------------------------

@client.event
async def on_message(message):
    if message.author.bot:
        return
    chan_name = getattr(message.channel, "name", "")
    in_add = message.guild is not None and chan_name == ADD_CHANNEL
    in_ping = message.guild is not None and chan_name == PING_CHANNEL
    owned = (_channel_owner(message.channel.id)
             if message.guild is not None and not (in_add or in_ping)
             else None)
    if message.guild is not None and not (in_add or in_ping or owned):
        return    # guild msgs only in add/ping/your own streamer channels
    text = (message.content or "").strip()
    low = text.lower()
    uid = message.author.id
    reply = None
    if owned:      # your own streamer channel: filters / search / digest
        owner_uid, login = owned
        if str(uid) != owner_uid:
            return
        if low.startswith("-search"):
            await _send_search_pager(message.channel, uid,
                                     text[7:].strip().strip('"'))
            return
        if low.startswith("-digest"):
            reply = await run_digest(force_login=login, force_uid=uid)
        else:
            reply = await cmd_filter(owner_uid, login, text)
    elif in_ping:                            # live-pings channel commands only
        if low.startswith("-mypings"):
            reply = cmd_mypings(uid)
        elif low.startswith("-stop"):
            reply = cmd_stop_ping(text[5:].strip(), uid)
        elif text:                           # bare paste = toggle go-live ping
            reply = await cmd_livesub(text, uid)
    else:                                    # DMs + the add-streamers channel
        if low.startswith("-watch"):
            reply = await cmd_watch(text[6:].strip(), uid)
        elif low.startswith("-unwatch"):
            reply = await cmd_unwatch(text[8:].strip(), uid)
        elif low.startswith("-cliplist"):
            reply = cmd_cliplist(uid)
        elif low.startswith("-top"):
            reply = await cmd_top(text[4:].strip(), uid)
        elif low.startswith("-digest"):
            arg = text[7:].strip()
            force = None
            if arg:                          # name -> that channel only
                mine = _load(WATCHLIST_PATH).get(str(uid), {})
                force = _resolve_watch_key(arg, mine)
                if force is None:
                    await message.channel.send(
                        "you're not watching that one -- `-cliplist` shows "
                        "yours")
                    return
            await message.channel.send("refreshing your channels…")
            reply = await run_digest(force_login=force, force_uid=uid)
        elif low.startswith("-search") and message.guild is None:
            await _send_search_pager(message.channel, uid,
                                     text[7:].strip().strip('"'))
        elif low.startswith("-mypings") and message.guild is None:
            reply = cmd_mypings(uid)
        elif low.startswith("-stop") and message.guild is None:
            reply = cmd_stop_ping(text[5:].strip(), uid)
        elif in_add and text:                # bare paste = add the streamer
            reply = await cmd_watch(text, uid)
    if reply:
        for chunk in _chunks(reply):
            await message.channel.send(chunk)
        print(f"[dm] {message.author.id}: {low.split()[0]} -> "
              f"{reply.splitlines()[0][:80]}", flush=True)


async def _ensure_prompt_channel(name, topic, greeting):
    """Create a typeable helper channel under the clips category if missing."""
    guild = client.get_guild(GUILD_ID)
    if guild is None or discord.utils.get(guild.text_channels, name=name):
        return
    cat = discord.utils.find(lambda c: c.name.lower() == CATEGORY_NAME.lower(),
                             guild.categories)
    if cat is None:
        cat = await guild.create_category(CATEGORY_NAME)
    chan = await guild.create_text_channel(name, category=cat, topic=topic)
    await _pin(await chan.send(greeting))


_SAVED_MARK = " · ✅ saved"


@client.event
async def on_raw_reaction_add(payload):
    """✅ react = save the clip · ❌ react = delete the card + never repost.
    Only the channel owner's reacts count (their private channel anyway)."""
    if client.user and payload.user_id == client.user.id:
        return                               # our own seed reactions
    emoji = str(payload.emoji)
    if emoji not in ("✅", "❌"):
        return
    own = _channel_owner(payload.channel_id)
    if not own or own[0] != str(payload.user_id):
        return
    uid, key = own
    row = await asyncio.to_thread(clip_index.get_by_msg, payload.message_id)
    if row is None or str(row["uid"]) != uid:
        return                               # not one of this user's cards
    cid = row["clip_id"]
    chan = client.get_channel(payload.channel_id)
    if chan is None:
        return
    if emoji == "✅":
        await asyncio.to_thread(clip_index.set_flag, cid, uid, liked=1)
        try:
            msg = await chan.fetch_message(payload.message_id)
            if _SAVED_MARK not in (msg.content or ""):
                await msg.edit(content=msg.content + _SAVED_MARK)
        except discord.HTTPException:
            pass
        print(f"[like] {uid} saved {cid}", flush=True)
    else:
        await asyncio.to_thread(clip_index.set_flag, cid, uid, dismissed=1)
        try:
            await chan.get_partial_message(payload.message_id).delete()
        except discord.HTTPException as e:
            print(f"[nope] delete failed {e}", flush=True)
        print(f"[nope] {uid} dismissed {cid}", flush=True)


@client.event
async def on_raw_reaction_remove(payload):
    """Un-reacting ✅ un-saves the clip."""
    if str(payload.emoji) != "✅":
        return
    own = _channel_owner(payload.channel_id)
    if not own or own[0] != str(payload.user_id):
        return
    uid, _ = own
    row = await asyncio.to_thread(clip_index.get_by_msg, payload.message_id)
    if row is None or str(row["uid"]) != uid:
        return
    await asyncio.to_thread(clip_index.set_flag, row["clip_id"], uid,
                            liked=0)
    chan = client.get_channel(payload.channel_id)
    try:
        msg = await chan.fetch_message(payload.message_id)
        if _SAVED_MARK in (msg.content or ""):
            await msg.edit(content=msg.content.replace(_SAVED_MARK, ""))
    except (discord.HTTPException, AttributeError):
        pass
    print(f"[like] {uid} unsaved {row['clip_id']}", flush=True)


@client.event
async def on_ready():
    global FILTERS_CMD_ID
    synced = await tree.sync(guild=GUILD)    # instant guild-scoped registration
    FILTERS_CMD_ID = next((c.id for c in synced if c.name == "filters"), None)
    if MSG_INTENT:
        await _ensure_prompt_channel(
            ADD_CHANNEL,
            "Paste a TwitchTracker, twitch.tv, or kick.com link (or a "
            "channel name) to start watching that streamer. "
            "-unwatch <name> removes.",
            "**Paste a TwitchTracker/Twitch link, kick.com link, or channel "
            "name here** and you'll get a **private channel** (only you can "
            "see it) with that streamer's clips of the day, auto-refreshed. "
            "`-unwatch <name>` to remove one, `-cliplist` to see yours. "
            "Filters (like 'only clips over 50 views') are set inside your "
            "streamer channel -- how-to is pinned there.")
        await _ensure_prompt_channel(
            PING_CHANNEL,
            "Type a streamer's name to get @pinged when they go live. "
            "-stop <name> or type it again to stop. -mypings lists yours.",
            "**Want a ping when someone goes live?**\n"
            "• Type their name here (e.g. `jynxzi`) → I'll @you when they "
            "start streaming\n"
            "• `-stop <name>` (or type the name again) → turn a ping off\n"
            "• `-mypings` → see which pings you have set\n"
            "Their clips channel also shows 🔴 in its name while they're "
            "live.")
    if not clip_refresh.is_running():
        clip_refresh.start()                 # first run fires immediately
    if not clip_sweep.is_running():
        clip_sweep.start()
    if not live_status.is_running():
        live_status.start()
    if not ai_enrich.is_running():
        ai_enrich.start()
    if not daily_recap.is_running():
        daily_recap.start()
    pairs = _wl_all_pairs(_load(WATCHLIST_PATH))
    print(f"twitch-clips ready as {client.user} -- "
          f"{len(pairs)} streamer channel(s) across "
          f"{len({p[0] for p in pairs})} user(s), refreshing every "
          f"{POLL_MINUTES}min, slash commands synced", flush=True)


if __name__ == "__main__":
    if not TOKEN or not GUILD_ID:
        # exit 0 so Restart=on-failure doesn't loop before twitch.env is filled
        print("CLIPS_DISCORD_TOKEN / CLIPS_GUILD_ID not set -- fill twitch.env"
              " then: systemctl --user restart twitch-clips", flush=True)
        raise SystemExit(0)
    try:
        client.run(TOKEN)
    except discord.PrivilegedIntentsRequired:
        # exit 0 so this doesn't crash-loop before the portal toggle is set
        print("CLIPS_MSG_INTENT=1 but the Message Content Intent toggle is "
              "OFF in the dev portal (Bot page). Flip it and restart, or "
              "remove CLIPS_MSG_INTENT from twitch.env.", flush=True)
        raise SystemExit(0)
