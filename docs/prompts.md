# Prompt design notes

Two prompts power the system. Both live in `prompts/` as their own files (source of truth — n8n reads them as static text and embeds in API calls).

| File | Used by | Model |
|---|---|---|
| `prompts/classify.md` | Workflow A — Ingest | Gemini 2.5 Flash |
| `prompts/rag.md` | Workflow B — Ask | Gemini 2.5 Flash |

## Design principles

- **JSON-out for classify**, plain-text-out for RAG. JSON is parseable by n8n's Code/Function node; plain text is what Telegram renders cleanly.
- **System instruction + user message split**. The full prompt content (the .md file body, minus the H1) goes in `system_instruction`. The user message holds only the payload (URL + content for classify; question + matches for RAG). Keeps the cache-friendly bit stable.
- **No few-shot bloat**. One example each, max. Flash follows tight schemas well; more examples just burn tokens on free tier.
- **Explicit failure paths in the prompt**. The classify prompt tells the model what to do when extraction failed (set Inbox + failure summary), so n8n doesn't need to branch on errors before the AI call.
- **Workspace list is hardcoded in the prompt**, not passed as a variable. Reason: if the list changes, you want to also re-read the rules and example — `prompts/classify.md` is the one place to edit.

## When to revise

Revise `classify.md` when:
- You see ≥10 items mis-classified into the same wrong workspace → tighten the relevant rule.
- You add/remove a workspace → also update `docs/workspaces.md` and the Obsidian template.
- Tag quality drifts (too generic, too noisy) → adjust the tag rule.

Revise `rag.md` when:
- Answers start citing items that aren't actually used in the answer → reinforce "only list matches you used".
- Answers ramble → tighten the sentence-count rule.
- Telegram rendering breaks → adjust formatting rules (no markdown, plain bullets only).

## Token cost estimate (free tier)

Per ingest call (classify):
- System: ~1.2k tokens (the prompt)
- Content: 1k–15k tokens (transcript or article body, truncated to 50k chars upstream)
- Output: ~300 tokens (JSON)

Per ask call (RAG):
- System: ~600 tokens
- Matches (8 items × ~200 tokens summary): ~1.6k tokens
- Output: ~200 tokens

Free-tier daily ceiling on Flash 2.5 (~250 RPD as of late 2025) easily covers >50 ingests + >50 asks per day. Embeddings are essentially free at 1500 RPM.
