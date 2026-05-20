# Architecture

Three n8n workflows. One database. One Obsidian vault. One Telegram bot. Free-tier Gemini.

## Diagram

```
                              ┌──────────────┐
                              │ Telegram bot │
                              └──────┬───────┘
                                     │ webhook (Cloudflare Tunnel)
                                     ▼
                         ┌────────────────────────┐
                         │      n8n (homelab)     │
                         │                        │
        URL ───────────► │  WF A: Ingest          │ ──┐
        /ask <q> ──────► │  WF B: Ask             │   │
        /star /list ───► │  WF C: Commands        │   │
                         └────────────────────────┘   │
                                     │                │
                  ┌──────────────────┼────────────────┘
                  │                  │
                  ▼                  ▼
        ┌───────────────────┐   ┌──────────────────────────┐
        │  Supabase Cloud   │   │  Obsidian vault          │
        │  (pgvector)       │   │  (Syncthing to Mac)      │
        │                   │   │                          │
        │  bookmarks table  │   │  <workspace>/<slug>.md   │
        │  match_bookmarks  │   │  [[wikilinks]] → graph   │
        └───────────────────┘   └──────────────────────────┘
                  ▲
                  │ embeddings + summaries
                  │
        ┌───────────────────┐
        │  Gemini 2.5 Flash │
        │  text-embedding-  │
        │  004              │
        └───────────────────┘
```

## Workflow A — Ingest

Trigger: any non-command Telegram message containing a URL.

```
Telegram Trigger
  ↓
Extract URL (regex)
  ↓
Normalize URL + sha256 → url_hash
  ↓
Supabase lookup by url_hash ────► found? → reply "Already saved on <date> to <workspace>" → END
  ↓ not found
Detect source_type (host regex)
  ↓
Switch:
  • youtube     → Execute Command: scripts/youtube-transcript.sh <url>
  • x / twitter → HTTP GET https://cdn.syndication.twimg.com/tweet-result?id=<id>&lang=en&token=...
  • instagram   → HTTP GET https://r.jina.ai/<url> (fallback only)
  • web         → HTTP GET https://r.jina.ai/<url>
  ↓
Gemini 2.5 Flash (classify.md as system + content as user) → JSON
  ↓
Gemini text-embedding-004 on (title + summary + key_points joined)
  ↓
Supabase INSERT bookmarks (...)
  ↓
Function node: render Obsidian markdown from template
  ↓
Filesystem write to OBSIDIAN_VAULT_PATH/<workspace>/<YYYY-MM-DD>-<slug>.md
  ↓
Telegram reply:
  ✅ Saved to <workspace>
  <first 2 sentences of summary>
  /star <id> to mark must-read
```

Failure modes — all fall to Inbox + a reply that names the failure:
- Extractor empty → "Couldn't read content, saved URL to Inbox."
- Gemini error → "AI summary failed, saved URL to Inbox."
- Supabase down → reply with retry hint; do NOT write Obsidian (DB is source of truth for IDs).

## Workflow B — Ask

Trigger: Telegram message starting with `/ask ` or ending with `?`.

```
Telegram Trigger
  ↓
Extract question text (strip /ask prefix)
  ↓
Gemini text-embedding-004 on question
  ↓
Supabase RPC: match_bookmarks(embedding, threshold=0.55, k=8)
  ↓
Build context: JSON.stringify(matches)
  ↓
Gemini 2.5 Flash (rag.md as system + {question, matches} as user)
  ↓
Telegram reply (plain text, no markdown)
```

## Workflow C — Commands

Trigger: Telegram message starting with `/`.

| Command | n8n branch |
|---|---|
| `/start`, `/help` | reply with static usage text |
| `/star <id>` | Supabase `UPDATE bookmarks SET must_read = NOT must_read WHERE id=$1 RETURNING *` → patch Obsidian frontmatter via filesystem read+write |
| `/list <workspace>` | `SELECT id, title, url, must_read FROM bookmarks WHERE workspace=$1 ORDER BY created_at DESC LIMIT 10` → format as text |
| `/recent` | same, no workspace filter, LIMIT 5 |
| `/ask ...` | route to Workflow B (or share trigger if you prefer one workflow with switch) |

## Why Supabase + Obsidian (both)

| Need | Solved by |
|---|---|
| Semantic search from Telegram | Supabase + pgvector |
| Hand-browsable graph view | Obsidian + wikilinks |
| Long-form summaries in nice typography | Obsidian markdown |
| Stable IDs for `/star <id>` | Supabase bigserial |
| Sync across phone + laptop | Syncthing on the vault folder |
| Backup | Supabase auto-backup + vault is in Syncthing |

Supabase is the source of truth for IDs and embeddings. Obsidian is a derived view, regeneratable from the DB if ever lost.
