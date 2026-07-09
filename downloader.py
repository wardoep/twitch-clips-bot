"""
downloader.py -- fetch a Twitch clip MP4 (or a fresh signed direct URL) via
yt-dlp used as a library. Twitch clip CDN URLs are signed and expire, so the
Download button resolves them fresh on every press instead of baking a link
into the card. ffmpeg is on PATH system-wide.
"""
import os
import subprocess
import uuid

import yt_dlp

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE, "clip_cache")


def clip_url(clip_ref):
    """A ref is either a bare Twitch clip slug or a full URL (Kick clips and
    anything else yt-dlp can handle come through as URLs)."""
    if clip_ref.startswith(("http://", "https://")):
        return clip_ref
    return f"https://clips.twitch.tv/{clip_ref}"


def _ydl(**extra):
    opts = {"quiet": True, "no_warnings": True, "noprogress": True,
            "format": "mp4/best", **extra}
    return yt_dlp.YoutubeDL(opts)


def fetch(clip_ref, max_height=None):
    """Download the clip MP4 into clip_cache/. Returns the file path.
    max_height caps the quality (1080p clips easily exceed Discord's 10MB
    upload limit; the bot retries at 720/480 before falling back to a link).
    Caller deletes the file after upload."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    fmt = (f"mp4[height<={max_height}]/best[height<={max_height}]/mp4/best"
           if max_height else "mp4/best")
    # unique per call: the sweep, the AI sweep, and Download presses can all
    # fetch the same clip concurrently -- shared names cross-delete/corrupt
    out = os.path.join(CACHE_DIR,
                       f"%(id)s_{max_height or 'src'}_{uuid.uuid4().hex[:8]}"
                       f".%(ext)s")
    with _ydl(outtmpl=out, format=fmt) as ydl:
        info = ydl.extract_info(clip_url(clip_ref), download=True)
        path = ydl.prepare_filename(info)
        # kick clips come down as m3u8->mp4 remuxes; ext can differ from
        # the template's %(ext)s guess -- trust the downloads record if so
        if not os.path.exists(path) and info.get("requested_downloads"):
            path = info["requested_downloads"][0]["filepath"]
        return path


def transcode_to_fit(path, height=480):
    """Re-encode a too-big clip down to `height` (Kick clips expose no
    quality ladder to yt-dlp, so the only way to shrink them is ffmpeg).
    Returns the new path; caller deletes both files."""
    out = path.rsplit(".", 1)[0] + f"_t{height}.mp4"
    subprocess.run(
        ["ffmpeg", "-y", "-i", path, "-vf", f"scale=-2:{height}",
         "-c:v", "libx264", "-preset", "veryfast", "-crf", "28",
         "-c:a", "aac", "-b:a", "96k", out],
        check=True, capture_output=True, timeout=120)
    return out


def direct_url(clip_ref):
    """Resolve a fresh signed MP4 URL without downloading (fallback when the
    file is over Discord's upload limit). Expires after a while -- say so."""
    with _ydl() as ydl:
        info = ydl.extract_info(clip_url(clip_ref), download=False)
        return info["url"]


if __name__ == "__main__":
    import sys
    cid = sys.argv[1]
    print("direct url:", direct_url(cid))
    path = fetch(cid)
    print(f"downloaded: {path} ({os.path.getsize(path):,} bytes)")
