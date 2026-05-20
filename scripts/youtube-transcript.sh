#!/usr/bin/env bash
# Fetch a YouTube video's auto-caption transcript via yt-dlp, strip timestamps,
# and emit plain text on stdout. Designed for n8n Execute Command nodes.
#
# Usage:    youtube-transcript.sh <youtube-url>
# Exits 0 on success with transcript on stdout.
# Exits 1 if yt-dlp not installed.
# Exits 2 if no captions available.
# Exits 3 on any other failure.
#
# Requires: yt-dlp (pip install -U yt-dlp), and a writable tmpdir.

set -euo pipefail

URL="${1:-}"
if [ -z "$URL" ]; then
  echo "usage: $0 <youtube-url>" >&2
  exit 3
fi

if ! command -v yt-dlp >/dev/null 2>&1; then
  echo "yt-dlp not installed" >&2
  exit 1
fi

TMPDIR="$(mktemp -d -t yt-transcript.XXXXXX)"
trap 'rm -rf "$TMPDIR"' EXIT

# --write-auto-sub: auto-generated captions (most videos have them)
# --write-sub: human-uploaded captions (preferred if available)
# --sub-lang en: prefer English; fall back to whatever's first
# --skip-download: no audio/video, just the .vtt
yt-dlp \
  --quiet --no-warnings \
  --write-auto-sub --write-sub \
  --sub-lang "en.*,en" --sub-format "vtt" \
  --skip-download \
  --output "$TMPDIR/cap.%(ext)s" \
  "$URL" >/dev/null 2>&1 || true

VTT="$(find "$TMPDIR" -name 'cap.*.vtt' | head -n1 || true)"
if [ -z "$VTT" ] || [ ! -s "$VTT" ]; then
  echo "no captions available for $URL" >&2
  exit 2
fi

# Strip WEBVTT header, timestamp cues, cue settings, and dedupe consecutive lines.
# Auto-captions repeat each phrase across overlapping windows — uniq cleans that up.
awk '
  /^WEBVTT/ { next }
  /^[0-9]+$/ { next }                                  # cue sequence numbers
  /-->/ { next }                                       # timestamp lines
  /^[[:space:]]*$/ { next }                            # blank lines
  {
    gsub(/<[^>]*>/, "");                               # inline tags <c>, <00:00:01.500>
    gsub(/^[[:space:]]+|[[:space:]]+$/, "");           # trim
    if ($0 != prev) { print; prev = $0 }
  }
' "$VTT"
