"""
ai.py -- optional AI layer for the clips bot: Whisper transcription, one-line
clip summaries, embeddings, and daily recap paragraphs.

Everything here is best-effort garnish on top of the clip cards, so every
function degrades gracefully: no key / dead key / API hiccup -> return None
(never raise), and callers just render the card without the AI bits. All HTTP
goes through requests directly (no SDKs) to keep the dependency footprint
identical to the rest of the bot.

Provider split: transcription + embeddings need OpenAI (Whisper / embeddings
have no Anthropic equivalent here); text generation prefers Anthropic Haiku
when an ANTHROPIC_API_KEY exists, else falls back to OpenAI gpt-4o-mini.

Standalone test (safe without working keys -- shows the None paths):
    .venv/bin/python ai.py [optional clip.mp4 to try transcribing]
"""
import os
import subprocess
import time

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE, "twitch.env")

OPENAI = "https://api.openai.com/v1"
ANTHROPIC = "https://api.anthropic.com/v1"
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
OPENAI_CHAT_MODEL = "gpt-4o-mini"

# Transcribe uploads audio, so it gets a longer read window; everything else
# is small JSON and should fail fast rather than stall a pipeline cycle.
TRANSCRIBE_TIMEOUT = (30, 60)   # (connect, read)
TIMEOUT = 20
FFMPEG_TIMEOUT = 30             # clips are 5-60s; extraction takes ~1s


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


def _openai_key():
    """The key if it at least looks like an OpenAI key. Format check only --
    a dead key still passes here and fails at call time (logged, -> None)."""
    key = _env("OPENAI_API_KEY")
    return key if key and key.startswith("sk-") and len(key) > 20 else None


def _anthropic_key():
    key = _env("ANTHROPIC_API_KEY")
    return key if key and key.startswith("sk-ant-") and len(key) > 20 else None


def enabled():
    """True when some AI is plausibly usable (OpenAI key for transcribe/embed
    /summaries, or Anthropic key for summaries). Plausible != verified: we
    only check the key looks right, so callers must still handle None from
    the individual functions."""
    return _openai_key() is not None or _anthropic_key() is not None


def _post(url, headers, timeout, label, json_body=None, data=None, files=None):
    """POST with one retry on 429/5xx (short sleep). Transient hiccups are
    common enough that a single retry saves most failed cycles; anything else
    (401 dead key, 400 bad request) logs and returns None so callers can shrug
    it off. files must be bytes tuples, not open handles, so the retry can
    re-send the body."""
    for attempt in (1, 2):
        try:
            r = requests.post(url, headers=headers, json=json_body, data=data,
                              files=files, timeout=timeout)
        except requests.RequestException as e:
            print(f"[ai] {label}: request failed ({e})", flush=True)
            return None
        if r.status_code == 200:
            return r
        if (r.status_code == 429 or r.status_code >= 500) and attempt == 1:
            print(f"[ai] {label}: HTTP {r.status_code}, retrying once", flush=True)
            time.sleep(2)
            continue
        print(f"[ai] {label}: HTTP {r.status_code} {r.text[:200]}", flush=True)
        return None
    return None


# ---- transcription -------------------------------------------------------------

def _extract_audio(mp4_path):
    """ffmpeg-extract a 48kbps MP3 next to the clip. Whisper only needs audio,
    and stripping the video shrinks the upload ~10x (a 60s clip -> ~360KB).
    Caller deletes the file. Returns the mp3 path or None."""
    mp3_path = os.path.splitext(mp4_path)[0] + ".ai.mp3"
    try:
        proc = subprocess.run(
            ["/usr/bin/ffmpeg", "-y", "-i", mp4_path, "-vn",
             "-acodec", "libmp3lame", "-b:a", "48k", mp3_path],
            capture_output=True, timeout=FFMPEG_TIMEOUT)
    except (subprocess.TimeoutExpired, OSError) as e:
        print(f"[ai] transcribe: ffmpeg failed ({e})", flush=True)
        return None
    if proc.returncode != 0 or not os.path.exists(mp3_path):
        tail = proc.stderr.decode(errors="replace")[-300:]
        print(f"[ai] transcribe: ffmpeg exit {proc.returncode}: {tail}", flush=True)
        return None
    return mp3_path


def transcribe(mp4_path):
    """Clip MP4 -> transcript text via OpenAI Whisper, or None on any failure.
    Clips are 5-60s so this is one small multipart POST, no chunking."""
    key = _openai_key()
    if not key:
        print("[ai] transcribe: no OPENAI_API_KEY, skipping", flush=True)
        return None
    if not os.path.isfile(mp4_path):
        print(f"[ai] transcribe: no such file {mp4_path}", flush=True)
        return None
    mp3_path = _extract_audio(mp4_path)
    if not mp3_path:
        return None
    try:
        # read into memory (tiny) so _post can safely re-send on retry
        with open(mp3_path, "rb") as f:
            audio = f.read()
        r = _post(f"{OPENAI}/audio/transcriptions",
                  headers={"Authorization": f"Bearer {key}"},
                  timeout=TRANSCRIBE_TIMEOUT, label="transcribe",
                  data={"model": "whisper-1"},
                  files={"file": (os.path.basename(mp3_path), audio, "audio/mpeg")})
    finally:
        try:
            os.remove(mp3_path)   # temp artifact -- never leave it in clip_cache
        except OSError:
            pass
    if r is None:
        return None
    try:
        text = r.json()["text"].strip()
    except (ValueError, KeyError):
        print("[ai] transcribe: unexpected response shape", flush=True)
        return None
    return text or None


# ---- text generation (summaries / recaps) --------------------------------------

def _llm(system, user, max_tokens, label):
    """One system+user turn -> text. Anthropic Haiku when a key exists (spec'd
    provider preference), else OpenAI gpt-4o-mini, else None. No cross-provider
    fallback on failure -- a dead provider should be visible in the logs, not
    papered over with doubled spend."""
    akey = _anthropic_key()
    if akey:
        r = _post(f"{ANTHROPIC}/messages",
                  headers={"x-api-key": akey,
                           "anthropic-version": "2023-06-01",
                           "content-type": "application/json"},
                  timeout=TIMEOUT, label=label,
                  json_body={"model": ANTHROPIC_MODEL,
                             "max_tokens": max_tokens,
                             "system": system,
                             "messages": [{"role": "user", "content": user}]})
        if r is None:
            return None
        try:
            blocks = r.json().get("content", [])
            text = " ".join(b.get("text", "") for b in blocks
                            if b.get("type") == "text").strip()
        except ValueError:
            print(f"[ai] {label}: unexpected anthropic response", flush=True)
            return None
        return text or None
    okey = _openai_key()
    if okey:
        r = _post(f"{OPENAI}/chat/completions",
                  headers={"Authorization": f"Bearer {okey}"},
                  timeout=TIMEOUT, label=label,
                  json_body={"model": OPENAI_CHAT_MODEL,
                             "max_tokens": max_tokens,
                             "messages": [{"role": "system", "content": system},
                                          {"role": "user", "content": user}]})
        if r is None:
            return None
        try:
            text = (r.json()["choices"][0]["message"]["content"] or "").strip()
        except (ValueError, KeyError, IndexError):
            print(f"[ai] {label}: unexpected openai response", flush=True)
            return None
        return text or None
    print(f"[ai] {label}: no AI key configured, skipping", flush=True)
    return None


def summarize(meta, transcript):
    """One punchy card line (<=~140 chars) for a clip, or None. meta needs
    title/streamer/game/duration/view_count; transcript is optional -- the
    model works from metadata alone when a clip had no usable audio."""
    system = ("You caption Twitch clips for a Discord card. Reply with exactly "
              "one punchy sentence, at most 140 characters, describing what "
              "happens in the clip -- name who did what. No hashtags, no "
              "quotation marks around the sentence, and don't just restate "
              "the clip title.")
    lines = [f"Streamer: {meta.get('streamer')}",
             f"Clip title: {meta.get('title')}",
             f"Game: {meta.get('game')}",
             f"Duration: {meta.get('duration')}s",
             f"Views: {meta.get('view_count')}"]
    if transcript:
        lines.append(f"Transcript: {transcript[:4000]}")
    else:
        lines.append("Transcript: (none -- caption from the metadata)")
    text = _llm(system, "\n".join(lines), 100, "summarize")
    if not text:
        return None
    text = text.strip().strip('"').strip()
    if len(text) > 200:                      # card layout guard -- model
        text = text[:197].rstrip() + "..."   # occasionally ignores the cap
    return text or None


def recap_paragraph(stats, summaries):
    """2-3 sentence end-of-day recap for a streamer's channel, or None.
    stats: streamer, date, stream_hours (float|None), clip_count, total_views,
    top_clip_title. summaries = that day's clip summaries (may be empty)."""
    system = ("You write a short end-of-day recap for a Twitch streamer's "
              "Discord channel. Reply with 2-3 sentences summarizing how the "
              "day went, weaving in the highlights. Conversational, no "
              "hashtags, no bullet points, no quotation marks around it.")
    hours = stats.get("stream_hours")
    lines = [f"Streamer: {stats.get('streamer')}",
             f"Date: {stats.get('date')}",
             f"Hours streamed: {hours if hours is not None else 'unknown'}",
             f"Clips today: {stats.get('clip_count')}",
             f"Total clip views: {stats.get('total_views')}",
             f"Top clip: {stats.get('top_clip_title')}"]
    if summaries:
        lines.append("Clip summaries:")
        lines += [f"- {s}" for s in summaries[:20]]
    text = _llm(system, "\n".join(lines), 250, "recap")
    return text.strip().strip('"').strip() or None if text else None


# ---- embeddings ----------------------------------------------------------------

def embed(text):
    """Text -> embedding vector (list of floats) via OpenAI
    text-embedding-3-small, or None. Input truncated to ~6000 chars -- plenty
    for a title+transcript, and keeps us clear of token limits."""
    key = _openai_key()
    if not key:
        print("[ai] embed: no OPENAI_API_KEY, skipping", flush=True)
        return None
    if not text or not text.strip():
        return None
    r = _post(f"{OPENAI}/embeddings",
              headers={"Authorization": f"Bearer {key}"},
              timeout=TIMEOUT, label="embed",
              json_body={"model": "text-embedding-3-small",
                         "input": text[:6000]})
    if r is None:
        return None
    try:
        return r.json()["data"][0]["embedding"]
    except (ValueError, KeyError, IndexError):
        print("[ai] embed: unexpected response shape", flush=True)
        return None


if __name__ == "__main__":
    import sys
    print(f"enabled: {enabled()}  (openai key: {bool(_openai_key())}, "
          f"anthropic key: {bool(_anthropic_key())})")

    meta = {"title": "INSANE 1v5 clutch", "streamer": "jynxzi",
            "game": "Rainbow Six Siege", "duration": 28.5, "view_count": 12345}
    print("summarize ->", summarize(meta, "oh my god no way he actually hit that"))

    vec = embed("jynxzi insane 1v5 clutch rainbow six siege")
    print("embed ->", f"{len(vec)} floats" if vec else None)

    stats = {"streamer": "jynxzi", "date": "2026-07-08", "stream_hours": 6.5,
             "clip_count": 4, "total_views": 51234,
             "top_clip_title": "INSANE 1v5 clutch"}
    print("recap ->", recap_paragraph(stats, ["He clutched a 1v5 to win the map.",
                                              "Chat exploded over a ranked ace."]))

    if len(sys.argv) > 1:
        print("transcribe ->", transcribe(sys.argv[1]))
