# Twitch Clips Bot

A self-hosted Discord bot that monitors Twitch and Kick streamers and delivers
each day's top clips as playable video cards, with filters, search, and a
nightly recap — running unattended as a Linux (systemd) service.

## Overview

Users add a streamer through a slash command, DM command, or by pasting a
channel URL. The bot creates a private Discord channel per user per streamer
and keeps it current all day: new clips are posted within about two minutes,
and a slower full pass edits already-posted cards in place so rank labels and
view counts stay accurate without repost spam. Each card carries a compact MP4
that plays directly in Discord, plus a Download button that fetches the
full-quality file on demand.

An optional AI layer (used only when API keys are configured, and degrading
cleanly when they are not) transcribes clips with Whisper, writes one-line
summaries, embeds them for semantic `/search`, and generates a short
end-of-day recap paragraph per channel.

## Architecture

```
                      +--> Twitch Helix API (OAuth2 app token)
Discord bot           |      clips / users / live status / games
(clips_bot.py) -------+
  scheduled loops     +--> Kick site API (unofficial, degrades gracefully)
  slash + DM commands        clips / live status
        |
        +--> downloader.py (yt-dlp + ffmpeg)
        |      quality-stepped MP4s sized to fit Discord's upload limit
        |
        +--> ai.py (optional: Whisper, summaries, embeddings, recaps)
        |
        +--> clip_index.py (SQLite: cards, AI fields, search, day stats)
        |
        +--> daily digest / recap posted back to Discord
```

| File | Role |
|---|---|
| `clips_bot.py` | Bot process: gateway, scheduled loops, commands, cards, filters, search UI |
| `twitch_api.py` | Twitch Helix client — app-token auth, clips, live status, game lookups |
| `kick_api.py` | Kick client — pagination, normalization, fail-soft error handling |
| `downloader.py` | yt-dlp fetch, fresh signed URLs, ffmpeg downscale |
| `ai.py` | Transcription, summaries, embeddings, recap text (plain REST, no SDKs) |
| `clip_index.py` | SQLite index with keyword + embedding-based search and retention purge |

## Key features

- **OAuth2 client-credentials flow** against Twitch's token endpoint: the app
  token is cached to disk with its expiry, refreshed automatically, and
  retried once on a 401 from a stale token.
- **Quality-stepping downloads**: in-channel previews try 480p then 360p;
  the Download button tries source, then 720p, then 480p, stopping at the
  first file under Discord's 10 MB limit. Kick clips expose a single format,
  so oversized ones are re-encoded with ffmpeg instead. If nothing fits, the
  bot falls back to a freshly resolved signed URL (Twitch CDN links expire,
  so they are never baked into cards).
- **Rate-limit and failure awareness**: batched Helix lookups, courtesy
  delays between paginated Kick requests, a single retry on HTTP 429/5xx for
  AI calls, per-clip locks against double-clicked downloads, and live-status
  logic that treats "all probes failed" as unknown rather than offline (a
  wrong answer would trip Discord's channel-rename limit).
- **Scheduled jobs** via `discord.ext.tasks`: a 2-minute fast sweep for new
  clips, a 15-minute full pass that edits cards in place, a 3-minute AI
  enrichment pass, a 5-minute live-status check, and a nightly recap at a
  configurable local time and timezone.
- **Per-user, per-channel filters** (top-N, minimum views, combinable)
  through a button panel or typed commands, plus semantic search across
  transcripts and summaries with a paged results UI.
- **State kept simple**: JSON files for watchlists and posted-card tracking
  (written atomically via temp file + rename), SQLite for the clip index,
  with a 90-day retention purge.

## Tech stack

Python 3 · discord.py · requests · yt-dlp · ffmpeg · SQLite ·
OpenAI (Whisper, embeddings) and Anthropic APIs via plain REST · systemd

## What I learned

- **OAuth2 in practice.** Implementing the client-credentials flow by hand —
  requesting app tokens, caching them with expiry timestamps, and handling
  the 401-refresh-retry cycle — taught me more about token lifecycles than
  any tutorial. It transfers directly to troubleshooting auth against any
  enterprise API.
- **Working with REST APIs, official and not.** Twitch's documented Helix
  API and Kick's undocumented site endpoints demanded opposite mindsets: one
  is a contract you can rely on, the other can change or block you at any
  time, so every Kick call returns a safe default instead of raising. That
  "assume the dependency will fail" habit now shapes how I write anything
  that touches a network.
- **Respecting rate limits.** I learned to batch requests, pause between
  paginated calls, retry only on retryable statuses (429/5xx), and design
  around platform limits I could not change — like Discord's two renames per
  channel per ten minutes, which forced careful state handling.
- **Running a long-lived Linux service.** The bot runs 24/7 under a systemd
  user unit with `Restart=on-failure`. I learned to write code that survives
  restarts without duplicating work (idempotent card posting keyed on clip
  IDs), to keep blocking work off the event loop, and to clean up temp files
  even on error paths.
- **Secrets management.** All credentials live in an env file loaded through
  systemd's `EnvironmentFile`, with a committed `twitch.env.example` as the
  template and the real file (plus cached tokens and user data) excluded by
  `.gitignore` and locked to `0600`.
- **Debugging from logs.** With no debugger attached to a running service,
  structured log lines with clear prefixes (`[dl]`, `[ai]`, `[kick]`) and
  captured HTTP status details became the primary diagnostic tool — reading
  `journalctl`/log tails to reconstruct what a failed cycle actually did.

## Setup

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp twitch.env.example twitch.env    # fill in credentials, then: chmod 600 twitch.env
.venv/bin/python clips_bot.py
```

`twitch.env.example` documents every setting: the Discord bot token and guild
ID, Twitch client ID/secret (dev.twitch.tv/console), an optional OpenAI key
for the AI features, and cadence/timezone tuning. ffmpeg must be on `PATH`.
For unattended operation, run it as a systemd user service with
`EnvironmentFile=twitch.env` and `Restart=on-failure`.

---
Built and maintained by **Edward J. Penna** — [github.com/wardoep](https://github.com/wardoep)
