# twitch clips bot

A Discord bot that turns a server into a personal Twitch/Kick clips hub:
paste a streamer in the public **➕ add-streamers** channel and you get a
**private channel** where every clip of their day arrives as a playable card —
ranked by views, AI-summarized, filterable, searchable, and recapped nightly.

## What it does

- **Two-speed clip feed** — new clips land within ~2 minutes (fast sweep);
  every 15 minutes a full pass edits posted cards in place so rank labels and
  view counts stay current. Cards never move or repost.
- **Cards** — bold header (`🥇 TOP 1 · 2,211 views`), grey metadata line with
  the linked title / length / game / clipper, a full-size bold-italic
  `💬 AI summary` of what happens in the clip, the video attached and playable
  in-channel (auto-downscaled to fit Discord's upload limit), and buttons:
  **Download** (full-quality MP4 to your DMs) · **✅ save** · **❌ delete +
  never repost**.
- **Filters** — `/filters` opens a button panel per channel: Everything
  (default), Top 3/5/10/25, minimum views (3/5/10/20+ or custom), combinable,
  instant apply with Undo. Typed forms work too (`more than 50 views`).
- **AI search** — `/search query:"jynxzi getting trolled"` searches meaning
  (Whisper transcripts + summaries + embeddings) across your clips, with a
  ◀ ▶ pager, jump links to the cards, and Download.
- **Daily recap** — every night each channel gets stream time, clips created,
  total views, top clip, clips hidden by your filter, and an AI-written
  paragraph. Posts even on quiet days as proof-of-life; `/recap` for
  today-so-far.
- **Live awareness** — 🔴 prefix on the channel name while the streamer is
  live; a public **🔔 live-pings** channel where anyone opts into go-live
  @pings per streamer.
- **Multi-user** — every Discord user gets their own private channels and
  filters, even for the same streamer. `/help` everywhere.
- **Platforms** — Twitch (official Helix API) and Kick (site API). YouTube has
  no public clips API.

## Stack

Python 3.14 · discord.py · yt-dlp + ffmpeg · OpenAI (whisper-1, gpt-4o-mini,
text-embedding-3-small) via raw requests · sqlite for the clip index · JSON
files for state · systemd user service.

| file | role |
|---|---|
| `clips_bot.py` | the bot: gateway, loops, commands, cards, filters, search UI |
| `twitch_api.py` | Twitch Helix client (app token, clips, live, games) |
| `kick_api.py` | Kick site-API client (clips, live), browser-UA |
| `downloader.py` | yt-dlp fetch / direct URLs / ffmpeg downscale |
| `ai.py` | transcribe · summarize · embed · recap paragraphs |
| `clip_index.py` | sqlite index: cards, AI fields, search, day stats, flags |

## Run

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp twitch.env.example twitch.env   # fill in tokens, chmod 600
.venv/bin/python clips_bot.py      # or the systemd unit below
```

systemd user unit (`~/.config/systemd/user/twitch-clips.service`):
`Type=simple`, `EnvironmentFile=twitch.env`, `Restart=on-failure`,
stdout appended to `logs/clips_bot.log`.
