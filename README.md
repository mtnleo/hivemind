# n8n-bookmarks

Personal AI-powered bookmark hive mind. Send any URL (article, YouTube, tweet, Instagram) to a Telegram bot — it gets fetched, summarized by Gemini, classified into a workspace, stored in Supabase + an Obsidian vault, and retrievable by natural-language `/ask` queries.

## What it does

- **Ingest**: forward any URL to the Telegram bot.
- **Extract**: real content is fetched — YouTube transcripts via `yt-dlp`, tweets via the syndication endpoint, articles via [Jina Reader](https://r.jina.ai), Instagram via Jina fallback.
- **Process**: Gemini 2.5 Flash classifies into one of the predefined workspaces and produces a summary + key points + tags.
- **Store twice**: Supabase row (with `text-embedding-004` embedding for semantic search) **and** an Obsidian markdown file with `[[wikilinks]]` so the graph view builds itself.
- **Retrieve**: `/ask what did I save about X` → embedding search → Gemini answers with cited source URLs.

## Stack

| Layer | Tool |
|---|---|
| Trigger | Telegram bot |
| Orchestrator | n8n (homelab, behind Cloudflare Tunnel) |
| AI | Gemini 2.5 Flash + `text-embedding-004` (free tier) |
| Vector DB | Supabase Cloud (Postgres + pgvector) |
| Browsing / graph view | Obsidian vault (Syncthing-synced) |

## Repo layout

```
.
├── README.md                       you are here
├── .env.example                    secrets template
├── docs/                           architecture, setup, workspaces, prompt design
├── supabase/schema.sql             database schema + match_bookmarks RPC
├── prompts/                        source of truth for Gemini prompts
├── n8n/workflows/                  exported workflow JSON
├── scripts/                        yt-dlp + URL normalization helpers
└── obsidian-vault-template/        starter vault structure
```

## Quick start

1. Follow [`docs/setup.md`](docs/setup.md) — provisions Supabase, Telegram bot, Gemini API key, n8n credentials.
2. Import the three workflows from `n8n/workflows/` into your n8n instance.
3. Send a URL to your bot.

## Required n8n container config

The ingest workflow writes Obsidian markdown files into `/files/obsidian-vault/`
inside the n8n container (host bind mount). n8n's `restrictFileAccessTo` defaults
to `~/.n8n-files`, which blocks the vault path with a misleading
**"is not writable"** error.

Add to the n8n service env in `docker-compose.yml`:

```yaml
environment:
  - N8N_RESTRICT_FILE_ACCESS_TO=/files
```

Then `docker compose up -d` to recreate the container. The workspace subdirs
(`AI & LLMs`, `Dev Tools`, …) must also exist on the host beforehand — n8n's
`readWriteFile` node does **not** create parents.

## Deploying ingest workflow

n8n's MCP `create_workflow_from_code` has reproducible 500 bugs. Deploy via raw
REST API instead:

```bash
# Update
curl -X PUT https://<your-n8n>/api/v1/workflows/<id> \
  -H "X-N8N-API-KEY: ..." -H "Content-Type: application/json" \
  --data @workflow.json
# Activate
curl -X POST https://<your-n8n>/api/v1/workflows/<id>/activate \
  -H "X-N8N-API-KEY: ..."
```

Body must include `name`, `nodes`, `connections`, `settings` only. Readonly
fields (`availableInMCP`, `binaryMode`, `staticData`, `versionId`, etc.) cause
400 errors if passed back unchanged.

## Build status

See [`docs/architecture.md`](docs/architecture.md) for the full architecture and current build sprint.

## Workspaces

See [`docs/workspaces.md`](docs/workspaces.md) for the taxonomy. Edit `prompts/classify.md` to change them.
