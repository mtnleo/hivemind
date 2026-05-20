# Workspaces

The classifier picks exactly one workspace per saved item. The list is small on purpose — too many categories and the AI thrashes, too few and everything ends up in one folder.

## Current taxonomy (v1)

| Workspace | What goes here | Examples |
|---|---|---|
| `AI & LLMs` | Model releases, prompting, agents, evals, LLM-focused papers | Opus release notes, "How to use multi-agent setups", DeepMind paper |
| `Dev Tools` | Claude Code, MCP, IDEs, n8n, CLIs, dev productivity | Cursor tips, MCP server tutorial, n8n workflow patterns |
| `System Design & Architecture` | Distributed systems, DBs, infra, design patterns | "Designing Data-Intensive Applications" notes, postmortems, infra talks |
| `Productivity & Workflow` | Note-taking, PKM, personal systems, time management | Tiago Forte essays, Obsidian setups, journaling methods |
| `Fitness & Gym` | Exercises, programming, form, recovery | Squat form video, push-pull-legs guide, mobility drills |
| `Health & Nutrition` | Diet, sleep, supplements, biomarkers | Andrew Huberman clips, creatine studies, sleep optimization |
| `Career & Leadership` | Interviews, hiring, management, career strategy | Lenny Rachitsky posts, hiring framework, IC vs manager tracks |
| `Inbox` | Fallback when confidence <70% or content too short / generic | Random short tweets, ambiguous links |

## Editing the taxonomy

To add, remove, or rename a workspace:

1. Edit `prompts/classify.md` — change the workspace list AND keep the rules / example aligned.
2. Update this file's table.
3. Create the matching folder in the Obsidian vault + a stub file in `obsidian-vault-template/_workspaces/`.
4. No DB migration needed — `workspace` is a text column, new values just appear.

## Rule of thumb

- Add a workspace only when you've seen 5+ items in `Inbox` that clearly belong together but don't fit any existing one.
- Resist topical creep ("AI Ethics", "AI Tools", "AI Papers" → just `AI & LLMs`). Use **tags** for subtopics.
- Tags are free-form (kebab-case) and provide the long tail. Workspaces provide the shelves.

## Reclassification

v1 has no `/move` command. To re-categorize:
1. Open the item in Obsidian, change `workspace:` in frontmatter, move the file to the new folder.
2. `UPDATE bookmarks SET workspace = '<new>' WHERE id = <id>;` in Supabase SQL editor.

v2 will add `/move <id> <workspace>` to do both automatically.
