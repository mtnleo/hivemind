# Hive Mind

This is the home of everything you've forwarded to your Telegram bot. The bot processes each URL with Gemini and drops a markdown file into the matching workspace folder. The Graph view (Cmd+G) builds itself from the `[[tag]]` wikilinks at the bottom of each note.

## Workspaces

- [[_workspaces/AI & LLMs|AI & LLMs]]
- [[_workspaces/Dev Tools|Dev Tools]]
- [[_workspaces/System Design & Architecture|System Design & Architecture]]
- [[_workspaces/Productivity & Workflow|Productivity & Workflow]]
- [[_workspaces/Fitness & Gym|Fitness & Gym]]
- [[_workspaces/Health & Nutrition|Health & Nutrition]]
- [[_workspaces/Career & Leadership|Career & Leadership]]
- [[_workspaces/Inbox|Inbox]]

## Saved query: must-reads

```dataview
TABLE workspace, url
FROM ""
WHERE must_read = true
SORT file.ctime DESC
```

(Requires the Dataview community plugin — install once and queries like this work everywhere.)

## How to use

- **Browse**: click any workspace above, then any item inside it.
- **Search**: Cmd+O to fuzzy-find by title; Cmd+Shift+F to search content.
- **Graph**: Cmd+G — clusters of tags form naturally.
- **Mark as must-read**: from Telegram, `/star <id>` (id is in the saved file's frontmatter as `supabase_id`).
