# Inbox

Fallback workspace — items the classifier wasn't confident about, or where content extraction failed. Triage manually: move to a real workspace, or delete.

## Recent

```dataview
TABLE summary, url
FROM "Inbox"
SORT file.ctime DESC
LIMIT 30
```
