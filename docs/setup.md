# Setup

This doc gets filled in for real during **Sprint 1 — Provisioning**. Below is the skeleton — replace each `TODO` block as you go through bring-up.

## Prerequisites

- n8n running on your homelab and reachable via HTTPS (you already have a Cloudflare Tunnel — confirm it terminates at n8n)
- `yt-dlp` available on the n8n host (Sprint 3 dependency; install with `pip install -U yt-dlp` or `brew install yt-dlp`)
- Syncthing (or iCloud Drive) configured to sync the Obsidian vault folder between homelab and Mac
- Obsidian installed on Mac

## 1. Telegram bot

1. Open Telegram, message `@BotFather`.
2. `/newbot` → choose a name and a username (must end in `bot`, e.g. `mtn_hivemind_bot`).
3. Save the token in `.env` as `TELEGRAM_BOT_TOKEN`.
4. Message `@userinfobot` to get your own numeric Telegram user ID → save as `TELEGRAM_ALLOWED_USER_ID`. The bot will ignore any other sender.

## 2. Supabase

1. Go to https://supabase.com, create a free-tier project.
2. Project Settings → API → copy `URL` (→ `SUPABASE_URL`) and `service_role` key (→ `SUPABASE_SERVICE_KEY`).
3. Project Settings → Database → Connection string → URI → copy → `SUPABASE_POSTGRES_URL`.
4. SQL editor → paste contents of `supabase/schema.sql` → Run.
5. Confirm the table exists: `select count(*) from bookmarks;`

## 3. Gemini API key (free tier)

1. Go to https://aistudio.google.com/apikey
2. Create API key. **Do NOT enable billing on the GCP project** — that flips you off free tier.
3. Save as `GEMINI_API_KEY`.
4. (Optional, recommended) In AI Studio Settings, opt out of prompts being used for model training if available in your region.

## 4. Obsidian vault

1. On the n8n host, pick a path inside your Syncthing-synced folder: e.g. `~/Sync/obsidian/n8n-bookmarks`.
2. Copy `obsidian-vault-template/` contents there.
3. Save the absolute path as `OBSIDIAN_VAULT_PATH`.
4. On the Mac side, open the synced folder as an Obsidian vault (`Open another vault` → `Open folder as vault`).
5. Verify a test file written into a workspace folder shows up in Obsidian within ~10s.

## 5. n8n credentials

In n8n → Credentials, create:

- **Telegram API**: name `telegram-hivemind`, token = `TELEGRAM_BOT_TOKEN`.
- **Postgres**: name `supabase-bookmarks`, parse from `SUPABASE_POSTGRES_URL`. Test the connection.
- **HTTP Header Auth** (for Gemini): name `gemini-api-key`. Header name: `x-goog-api-key`, value: `GEMINI_API_KEY`.

## 6. Ping test (last step of Sprint 1)

Build a one-off n8n workflow:
- Telegram Trigger → IF sender matches `TELEGRAM_ALLOWED_USER_ID` → Postgres `SELECT count(*) FROM bookmarks` → Telegram Send Message `pong, {{ $json.count }} bookmarks`.

Activate it. Send `/ping` (or any message) to the bot. Expect: `pong, 0 bookmarks`.

If that works, the full stack is reachable. Move to Sprint 2.

---

## Troubleshooting placeholder

Fill in as gotchas hit during real bring-up. Common issues to expect:
- Cloudflare Tunnel not forwarding the webhook path → check tunnel config
- Supabase pooler vs direct connection (n8n usually wants direct)
- yt-dlp missing from the n8n container (Sprint 3) → either install in the host or run yt-dlp in a sidecar container
