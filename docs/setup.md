# Setup

Bring-up notes. Real values captured during Sprint 1 (2026-05-20).

## Stack at a glance

| Piece | Where |
|---|---|
| n8n | Docker Compose on Raspberry Pi homelab (`n8n-compose/`) — container name `n8n-compose-n8n-1` |
| n8n public URL | `https://n8n.mtnleo-n8n.org/` (Cloudflare Tunnel) |
| Telegram bot | `@<configured-in-BotFather>` — token stored only in n8n credentials |
| Supabase | Pi Server Monitor project (`zkirbpiobilzzhzlrlcc`), org Mtnleo (`eirnmmwtbjoifndlqcvb`), region `us-east-2` |
| Schema | `hivemind` (isolated from other apps sharing the project) |
| Obsidian vault | Host: `/home/mtnleo/n8n-compose/local-files/obsidian-vault` — Container: `/files/obsidian-vault` |
| Sync to Mac | Not yet configured (deferred — see "Open items") |

## 1. Telegram bot — done

Created via `@BotFather` → `/newbot`. Token + own user ID stored in n8n credentials (`telegram-hivemind`). Allowed user ID filter is set on the Telegram Trigger so the bot ignores any other sender.

## 2. Supabase — done (sharing existing project)

We hit the free-tier 2-project limit, so we reused the existing **Pi Server Monitor** project with a dedicated `hivemind` schema for hard namespace isolation.

Schema applied via MCP `apply_migration` (name: `create_hivemind_schema_and_bookmarks`). Verify:

```sql
select schemaname, tablename from pg_tables where schemaname='hivemind';
-- → hivemind.bookmarks
```

## 3. Gemini API key — done

Created in Google AI Studio (https://aistudio.google.com/apikey) with billing OFF (free tier). Stored only in the n8n Gemini credential (added in Sprint 2 — not used by ping).

## 4. Obsidian vault — partially done

Vault folder + workspace template copied to host at `/home/mtnleo/n8n-compose/local-files/obsidian-vault`. n8n container sees it as `/files/obsidian-vault` thanks to the existing bind mount in `docker-compose.yml`.

**Not yet done:** sync to Mac. See "Open items" below.

## 5. n8n credentials — done

Three credentials needed in n8n UI (n8n MCP cannot create credentials — UI only).

### `telegram-hivemind` (Telegram API)
- Access Token: `<bot token from BotFather>`

### `supabase-hivemind` (Postgres)
**Critical:** the pooler host for this project is `aws-1-...`, not `aws-0-...`.

| Field | Value |
|---|---|
| Host | `aws-1-us-east-2.pooler.supabase.com` |
| Port | `5432` (session pooler — supports prepared statements) |
| Database | `postgres` |
| User | `postgres.zkirbpiobilzzhzlrlcc` |
| Password | `<DB password from Supabase dashboard>` |
| SSL | `require` + "Ignore SSL Issues" ON (Supabase cert chain is self-signed from n8n's PoV) |

Free-tier direct host `db.zkirbpiobilzzhzlrlcc.supabase.co` is IPv6-only and unreachable from the Pi — must use pooler.

### `gemini-api-key` (HTTP Header Auth)
Created in Sprint 1, used from Sprint 2 onward.
- Header name: `x-goog-api-key`
- Header value: `<Gemini API key>`

## 6. Ping workflow — done

n8n workflow `Hivemind — Ping` (ID `NAp209DrgQQHN5ec`):

```
Telegram Trigger (userIds=<allowed>) → Postgres executeQuery (count from hivemind.bookmarks) → Telegram sendMessage ("pong — N bookmark(s) saved")
```

Activated 2026-05-20 — confirmed end-to-end:
- Telegram → Cloudflare Tunnel → n8n container → Supabase pooler → n8n → Telegram reply

## Open items (not blocking S2 web ingest, but needed before browsing)

- **Sync vault to Mac.** Three options to pick from in S2 or earlier:
  - **Syncthing**: install on Pi + Mac, mesh sync, works off-LAN, conflict-safe.
  - **SMB share**: Pi exposes folder, Mac mounts as network drive. Only works on home Wi-Fi.
  - **SSHFS / rsync**: Mac pulls periodically. Read-only on Mac is fine since Obsidian is a derived view.
- **Markdown writes from n8n**: in S2 we'll use the Read/Write Files from Disk node pointed at `/files/obsidian-vault/...`.

## Troubleshooting (real issues seen)

| Symptom | Cause | Fix |
|---|---|---|
| n8n Postgres credential test: `self-signed certificate in certificate chain` | SSL set to `verify-full` or `verify-ca` | Set SSL to `require`, or toggle "Ignore SSL Issues" ON |
| n8n Postgres credential test: `Host not found` | Wrong pooler host — used `aws-0-...` instead of `aws-1-...` for this project | Use `aws-1-us-east-2.pooler.supabase.com` |
| Postgres connect: `tenant/user X not found` | Same as above — supavisor couldn't resolve tenant on wrong pooler region | Same fix |
| Postgres connect: `ENETUNREACH ... IPv6 ...` | Tried direct host `db.<ref>.supabase.co` on free tier (IPv6-only) | Use pooler instead |
| Sent `/ping` twice, only first one responded | Workflow was in test mode (one-shot listener) | Toggle Active = ON, or use MCP `publish_workflow` |
