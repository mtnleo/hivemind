# n8n workflows

Three workflows live under `workflows/` as exported JSON. They are imported into your n8n instance via the UI: **Workflows → ⋯ → Import from File**.

| File | Purpose | Built in sprint |
|---|---|---|
| `workflows/01-ingest.json` | Telegram URL → fetch → Gemini classify+summarize → Supabase + Obsidian write → reply | S2 + S3 + S4 |
| `workflows/02-ask.json` | `/ask` → embed → pgvector match → Gemini RAG → reply | S5 |
| `workflows/03-commands.json` | `/star`, `/list`, `/recent`, `/help` | S6 |

Files don't exist yet — they get exported from n8n at the end of each sprint that builds them.

## Conventions for the workflows

- **Credentials** are referenced by ID, not embedded. After import, n8n will prompt to re-bind: pick the ones you set up in `docs/setup.md` (`telegram-hivemind`, `supabase-bookmarks`, `gemini-api-key`).
- **Static config** (workspace list, OBSIDIAN_VAULT_PATH, similarity threshold) lives in a `Set` node at the top of each workflow. That's the one place to edit per-environment values.
- **Errors** route to a single `Telegram Send` node at the bottom that replies `⚠️ <step>: <error>`. Each major step's `On Error` is set to "Continue (use error output)".
- **Prompts** are NOT inlined into the AI node. They're stored as static text in a `Set` node so updating `prompts/classify.md` only requires re-pasting in one place.

## Re-exporting after edits

After modifying a workflow in n8n:
1. Workflow page → ⋯ → Download
2. Replace the corresponding file in `workflows/`
3. Commit with a message like `n8n(01-ingest): add Instagram fallback branch`

n8n's export does NOT include credential secrets — only credential IDs and names — so the JSON is safe to commit.
