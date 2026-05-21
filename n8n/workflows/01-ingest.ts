// Hivemind — Ingest (web + YouTube + X + Instagram)
// Sprint 3: Telegram URL → Extract URL (host-detect) → Has URL? → Route Source (switch)
//           → per-source Jina fetch → Gemini classify+summarize → Supabase + Obsidian → reply.
//
// Per-source fetch nodes today all use Jina Reader (r.jina.ai/<url>). Same API surface,
// distinct nodes so each branch can be swapped later (e.g. yt-dlp transcript service for
// YouTube, syndication endpoint for X). Switch routes on `sourceType` set by Extract URL.
//
// Source-of-truth for the live n8n workflow (workflow id NAp209DrgQQHN5ec).
// MCP `create_workflow_from_code` has reproducible 500 bugs — deploy via raw REST API:
// PUT https://n8n.mtnleo-n8n.org/api/v1/workflows/<id> with body { name, nodes, connections, settings }
// then POST /activate. See README for full procedure.
//
// Credentials referenced in n8n UI:
//   - Telegram account            (telegramApi)
//   - Postgres account            (postgres → Supabase pooler aws-1-us-east-2:5432, ssl=require)
//   - Google Gemini(PaLM) Api account  (googlePalmApi → reused for HTTP node via nodeCredentialType)
//
// REQUIRED n8n env var (else writeFile fails with "is not writable"):
//   N8N_RESTRICT_FILE_ACCESS_TO=/files
// Default is `~/.n8n-files` which blocks the vault path.
//
// Vault path inside the n8n container is `/files/obsidian-vault` (host bind mount on the Pi).

import { workflow, node, trigger, ifElse, switchNode, newCredential, expr } from '@n8n/workflow-sdk';

// ---------------------------------------------------------------------------
// Inline classify system prompt — mirrors prompts/classify.md.
// n8n convention: prompts live in the workflow, not loaded from disk.
// ---------------------------------------------------------------------------
const CLASSIFY_SYSTEM_PROMPT = `You are a personal knowledge curator. Your job is to read a piece of content the user just saved and produce a structured JSON record for their hive-mind library.

You will receive:
- url: the source URL
- source_type: one of youtube, x, web, instagram, note
- content: the extracted full text (transcript, tweet body, article body, or the user's own note)

You MUST return ONE JSON object, nothing else — no prose, no markdown fence, no comments. Schema:

{
  "workspace": "<one of the workspaces below>",
  "title": "<short, specific, max 80 chars. Prefer the original title if good; otherwise rewrite>",
  "summary": "<3 to 6 sentences. Capture the actual insight, not just a description. Past tense, third-person>",
  "key_points": ["<bullet 1>", "<bullet 2>", "..."],
  "tags": ["<kebab-case>", "..."],
  "suggested_must_read": true | false
}

Workspaces (pick exactly one — case-sensitive):
- AI & LLMs
- Dev Tools
- System Design & Architecture
- Productivity & Workflow
- Fitness & Gym
- Health & Nutrition
- Career & Leadership
- Inbox  (fallback when <70% confident OR content too short / generic)

Rules:
- One workspace only. If a piece spans two, pick the dominant one.
- tags: 3-7 entries, kebab-case. Specific over generic.
- suggested_must_read = true only for unusually high-signal items. Default false.
- summary: write what the reader will learn, not "the article discusses...".
- title: never include the source name. Just the substance.
- If content is empty / extraction failed: set workspace = "Inbox", summary = "Extraction failed — open URL manually.", key_points = [], tags = [source_type], suggested_must_read = false.`;

const GEMINI_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent';
const VAULT_BASE = '/files/obsidian-vault';
const RAW_CONTENT_TRUNCATE = 50000;

// ---------------------------------------------------------------------------
// 1. Telegram Trigger — same userIds filter pattern as 00-ping.ts
// ---------------------------------------------------------------------------
const telegramTrigger = trigger({
  type: 'n8n-nodes-base.telegramTrigger',
  version: 1.2,
  config: {
    name: 'Telegram Trigger',
    parameters: {
      updates: ['message'],
      additionalFields: {
        userIds: '8837789534'
      }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [200, 400]
  },
  output: [{
    update_id: 1,
    message: {
      message_id: 42,
      from: { id: 8837789534, first_name: 'Martin' },
      chat: { id: 8837789534, type: 'private' },
      date: 1736000000,
      text: 'https://www.anthropic.com/news/example-article'
    }
  }]
});

// ---------------------------------------------------------------------------
// 2. Extract URL — regex first URL + simplified normalize + sha256 hash.
// ---------------------------------------------------------------------------
const extractUrl = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Extract URL',
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      // URL class is unreliable in n8n Code sandbox — use pure regex for host detection.
      // Tracking param strip via regex for dedup hash (UNIQUE constraint on url_hash).
      jsCode: `const msg = $json.message || {};
const text = (msg.text || msg.caption || '').trim();
const match = text.match(/https?:\\/\\/[^\\s]+/);
if (!match) {
  return {
    json: {
      hasUrl: false,
      chatId: msg.chat ? msg.chat.id : null,
      telegramMsgId: msg.message_id || null,
      rawText: text
    }
  };
}
const rawUrl = match[0];
const hostMatch = rawUrl.match(/^https?:\\/\\/([^/?#]+)/);
const rawHost = hostMatch ? hostMatch[1].toLowerCase() : '';
const host = rawHost.replace(/^www\\./, '');
function detectSource(h) {
  if (!h) return 'web';
  if (h === 'youtube.com' || h === 'm.youtube.com' || h === 'youtu.be' || h.endsWith('.youtube.com')) return 'youtube';
  if (h === 'x.com' || h === 'twitter.com' || h === 'mobile.twitter.com' || h === 'mobile.x.com') return 'x';
  if (h === 'instagram.com' || h.endsWith('.instagram.com')) return 'instagram';
  return 'web';
}
const sourceType = detectSource(host);
const TRACKING = /[?&](utm_source|utm_medium|utm_campaign|utm_term|utm_content|utm_id|gclid|fbclid|mc_cid|mc_eid|igshid|ref|ref_src|ref_url|si|feature|app)=[^&]*/gi;
const normalized = rawUrl.replace(TRACKING, '').replace(/[?&]$/, '').replace(/\\?&/, '?');
return {
  json: {
    hasUrl: true,
    url: rawUrl,
    normalizedUrl: normalized,
    urlHash: normalized,
    host,
    sourceType,
    chatId: msg.chat.id,
    telegramMsgId: msg.message_id
  }
};`
    },
    position: [440, 400]
  },
  output: [{
    hasUrl: true,
    url: 'https://www.anthropic.com/news/example-article',
    urlHash: 'deadbeef',
    sourceType: 'web',
    chatId: 8837789534,
    telegramMsgId: 42
  }]
});

// ---------------------------------------------------------------------------
// 3. URL gate — ifElse routes to fetch+save (true) or no-URL reply (false).
// ---------------------------------------------------------------------------
const urlGate = ifElse({
  type: 'n8n-nodes-base.if',
  version: 2.3,
  config: {
    name: 'Has URL?',
    parameters: {
      conditions: {
        conditions: [{
          leftValue: expr('={{ $json.hasUrl }}'),
          rightValue: true,
          operator: { type: 'boolean', operation: 'true' }
        }]
      }
    },
    position: [680, 400]
  }
});

// ---------------------------------------------------------------------------
// 4a. Telegram reply on no-URL branch (terminal).
// ---------------------------------------------------------------------------
const telegramNoUrl = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Reply: No URL',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('={{ $json.chatId }}'),
      text: 'No URL detected in that message. Send me a web link to save.',
      additionalFields: { appendAttribution: false }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [920, 560]
  },
  output: [{ ok: true }]
});

// ---------------------------------------------------------------------------
// 4b. Route Source — Switch v3 branches by sourceType set in Extract URL.
//     All 4 branches currently use Jina Reader (same fetch, separate nodes so
//     each can be swapped later — e.g. yt-dlp transcript service for YouTube).
// ---------------------------------------------------------------------------
const routeSource = node({
  type: 'n8n-nodes-base.switch',
  version: 3.2,
  config: {
    name: 'Route Source',
    parameters: {
      rules: {
        values: [
          {
            conditions: {
              options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
              conditions: [{ leftValue: expr('={{ $json.sourceType }}'), rightValue: 'web', operator: { type: 'string', operation: 'equals' } }],
              combinator: 'and'
            },
            renameOutput: true, outputKey: 'web'
          },
          {
            conditions: {
              options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
              conditions: [{ leftValue: expr('={{ $json.sourceType }}'), rightValue: 'youtube', operator: { type: 'string', operation: 'equals' } }],
              combinator: 'and'
            },
            renameOutput: true, outputKey: 'youtube'
          },
          {
            conditions: {
              options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
              conditions: [{ leftValue: expr('={{ $json.sourceType }}'), rightValue: 'x', operator: { type: 'string', operation: 'equals' } }],
              combinator: 'and'
            },
            renameOutput: true, outputKey: 'x'
          },
          {
            conditions: {
              options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
              conditions: [{ leftValue: expr('={{ $json.sourceType }}'), rightValue: 'instagram', operator: { type: 'string', operation: 'equals' } }],
              combinator: 'and'
            },
            renameOutput: true, outputKey: 'instagram'
          }
        ]
      },
      options: { fallbackOutput: 'extra', renameFallbackOutput: 'default' }
    },
    position: [920, 280]
  }
});

// Jina fetch nodes — one per source so each branch is independently swappable.
const jinaFetch = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.4,
  config: {
    name: 'Jina Reader',
    parameters: {
      method: 'GET',
      url: expr('=https://r.jina.ai/{{ $json.url }}'),
      options: { response: { response: { responseFormat: 'text' } }, timeout: 30000 }
    },
    position: [1160, 100]
  },
  output: [{ data: '# Example article\n\nBody text' }]
});

const jinaYoutube = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.4,
  config: {
    name: 'Jina (YouTube)',
    // Jina returns page description for YT videos (no transcript). Gemini classifies
    // from title+description — lands in Inbox when content too thin. Future: swap
    // this node for a yt-dlp transcript service.
    parameters: {
      method: 'GET',
      url: expr('=https://r.jina.ai/{{ $json.url }}'),
      options: { response: { response: { responseFormat: 'text' } }, timeout: 30000 }
    },
    position: [1160, 240]
  },
  output: [{ data: 'YouTube video page text' }]
});

const jinaX = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.4,
  config: {
    name: 'Jina (X)',
    // Jina works for public X/Twitter posts. Syndication fallback not needed for MVP.
    parameters: {
      method: 'GET',
      url: expr('=https://r.jina.ai/{{ $json.url }}'),
      options: { response: { response: { responseFormat: 'text' } }, timeout: 30000 }
    },
    position: [1160, 380]
  },
  output: [{ data: 'Tweet text' }]
});

const jinaInstagram = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.4,
  config: {
    name: 'Jina (Instagram)',
    // Jina for IG: works for public posts, returns minimal text for login-walled.
    // Login-walled → Gemini gets near-empty content → workspace=Inbox (by design).
    parameters: {
      method: 'GET',
      url: expr('=https://r.jina.ai/{{ $json.url }}'),
      options: { response: { response: { responseFormat: 'text' } }, timeout: 30000 }
    },
    position: [1160, 520]
  },
  output: [{ data: 'Instagram post text' }]
});

// ---------------------------------------------------------------------------
// 5. Classify + summarize via Gemini 2.5 Flash. Single JSON object output.
// ---------------------------------------------------------------------------
const geminiClassify = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.4,
  config: {
    name: 'Gemini Classify',
    parameters: {
      method: 'POST',
      url: GEMINI_URL,
      authentication: 'predefinedCredentialType',
      // Reuse the existing `Google Gemini(PaLM) Api account` cred via HTTP node.
      // n8n sends `x-goog-api-key` header automatically. Avoids needing a
      // separate httpHeaderAuth cred.
      nodeCredentialType: 'googlePalmApi',
      sendBody: true,
      contentType: 'json',
      specifyBody: 'json',
      jsonBody: expr(`={{ JSON.stringify({
  system_instruction: { parts: [{ text: ${JSON.stringify(CLASSIFY_SYSTEM_PROMPT)} }] },
  contents: [{
    role: 'user',
    parts: [{
      text: JSON.stringify({
        url: $('Extract URL').item.json.url,
        source_type: $('Extract URL').item.json.sourceType || 'web',
        content: (($json.data || $json.body || '') + '').slice(0, ${RAW_CONTENT_TRUNCATE})
      })
    }]
  }],
  generationConfig: { responseMimeType: 'application/json', temperature: 0.2 }
}) }}`),
      options: { timeout: 60000 }
    },
    credentials: { googlePalmApi: newCredential('Google Gemini(PaLM) Api account') },
    // Gemini API occasionally returns 503; retry transparently.
    retryOnFail: true,
    maxTries: 4,
    waitBetweenTries: 3000,
    position: [1400, 280]
  },
  output: [{
    candidates: [{
      content: { parts: [{ text: '{"workspace":"AI & LLMs","title":"x","summary":"x","key_points":[],"tags":[],"suggested_must_read":false}' }] }
    }]
  }]
});

// ---------------------------------------------------------------------------
// 6. Parse Gemini JSON + derive slug, date, obsidian_path. Truncate raw.
// ---------------------------------------------------------------------------
const parseClassification = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Parse + Derive',
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      jsCode: `const ex = $('Extract URL').item.json;
// Read fetched text from whichever branch ran.
const SOURCE_NODE = { web: 'Jina Reader', youtube: 'Jina (YouTube)', x: 'Jina (X)', instagram: 'Jina (Instagram)' };
const fetchNodeName = SOURCE_NODE[ex.sourceType] || 'Jina Reader';
let jina = '';
try { const it = $(fetchNodeName).item; jina = (it && (it.json.data || it.json.body || '')) + ''; } catch (e) {}
let parsed;
try {
  const text = $json.candidates[0].content.parts[0].text;
  parsed = JSON.parse(text);
} catch (e) {
  parsed = {
    workspace: 'Inbox',
    title: ex.url,
    summary: 'AI summary failed — open URL manually.',
    key_points: [],
    tags: [ex.sourceType || 'web'],
    suggested_must_read: false
  };
}
const VALID_WS = new Set(['AI & LLMs','Dev Tools','System Design & Architecture','Productivity & Workflow','Fitness & Gym','Health & Nutrition','Career & Leadership','Inbox']);
const workspace = VALID_WS.has(parsed.workspace) ? parsed.workspace : 'Inbox';
function slugify(s) {
  return (s || '')
    .toLowerCase()
    .normalize('NFKD').replace(/[\\u0300-\\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60) || 'untitled';
}
const date = new Date().toISOString().slice(0, 10);
const slug = slugify(parsed.title);
const filename = date + '-' + slug + '.md';
const obsidianPath = '${VAULT_BASE}/' + workspace + '/' + filename;
const rawContent = (jina + '').slice(0, ${RAW_CONTENT_TRUNCATE});
const pgArray = '{' + (parsed.tags || []).map(t => '"' + String(t).replace(/"/g, '\\\\"') + '"').join(',') + '}';
return {
  json: {
    url: ex.url,
    url_hash: ex.urlHash,
    source_type: ex.sourceType || 'web',
    workspace: workspace,
    title: parsed.title,
    summary: parsed.summary,
    key_points: parsed.key_points || [],
    tags: pgArray,
    must_read: !!parsed.suggested_must_read,
    raw_content: rawContent,
    telegram_msg_id: ex.telegramMsgId,
    obsidian_path: obsidianPath,
    chat_id: ex.chatId,
    _filename: filename,
    _date: date,
    _slug: slug,
    _key_points: parsed.key_points || [],
    _tags: parsed.tags || []
  }
};`
    },
    position: [1640, 280]
  },
  output: [{ workspace: 'AI & LLMs', title: 'x', obsidian_path: '/files/obsidian-vault/AI & LLMs/2026-05-20-x.md' }]
});

// ---------------------------------------------------------------------------
// 7. Postgres INSERT into hivemind.bookmarks with RETURNING id.
// ---------------------------------------------------------------------------
const supabaseInsert = node({
  type: 'n8n-nodes-base.postgres',
  version: 2.6,
  config: {
    name: 'Insert Bookmark',
    parameters: {
      resource: 'database',
      operation: 'executeQuery',
      // n8n's `queryReplacement` is naive comma-split — any value containing a
      // comma (titles, summaries) shatters the param list. Collapse all values
      // into a single `$1::jsonb` parameter and extract columns inside SQL.
      query: `with payload as (select $1::jsonb as p)
insert into hivemind.bookmarks
  (url, url_hash, source_type, workspace, title, summary, key_points, tags, must_read, raw_content, telegram_msg_id, obsidian_path)
select
  p->>'url',
  p->>'url_hash',
  p->>'source_type',
  p->>'workspace',
  p->>'title',
  p->>'summary',
  p->'key_points',
  (p->>'tags')::text[],
  (p->>'must_read')::bool,
  p->>'raw_content',
  (p->>'telegram_msg_id')::int,
  p->>'obsidian_path'
from payload
on conflict (url_hash) do update set
  url = excluded.url,
  workspace = excluded.workspace,
  title = excluded.title,
  summary = excluded.summary,
  key_points = excluded.key_points,
  tags = excluded.tags,
  must_read = excluded.must_read,
  raw_content = excluded.raw_content,
  obsidian_path = excluded.obsidian_path
returning id;`,
      options: {
        queryReplacement: expr('={{ JSON.stringify($json) }}')
      }
    },
    credentials: { postgres: newCredential('supabase-hivemind') },
    position: [1880, 280]
  },
  output: [{ id: 1 }]
});

// ---------------------------------------------------------------------------
// 8. Render Obsidian markdown + prepare binary payload for the file write.
// ---------------------------------------------------------------------------
const renderMarkdown = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Render Markdown',
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      jsCode: `const meta = $('Parse + Derive').item.json;
const supabaseId = $json.id;

const fm = [
  '---',
  'supabase_id: ' + supabaseId,
  'url: ' + JSON.stringify(meta.url),
  'workspace: ' + JSON.stringify(meta.workspace),
  'source_type: ' + meta.source_type,
  'must_read: ' + meta.must_read,
  'created: ' + new Date().toISOString(),
  'tags: [' + (meta._tags || []).map(t => JSON.stringify(t)).join(', ') + ']',
  '---'
].join('\\n');

const keyPointsMd = (meta._key_points || []).map(p => '- ' + p).join('\\n');

const md = [
  fm,
  '',
  '# ' + meta.title,
  '',
  '> ' + meta.url,
  '',
  '## Summary',
  '',
  meta.summary,
  '',
  '## Key points',
  '',
  keyPointsMd || '- (none extracted)',
  '',
  '## Workspace',
  '',
  '[[' + meta.workspace + ']]',
  ''
].join('\\n');

return {
  json: {
    supabase_id: supabaseId,
    obsidian_path: meta.obsidian_path,
    filename: meta._filename,
    workspace: meta.workspace,
    title: meta.title,
    summary: meta.summary,
    chat_id: meta.chat_id,
    markdown: md
  },
  binary: {
    data: {
      data: Buffer.from(md, 'utf8').toString('base64'),
      mimeType: 'text/markdown',
      fileName: meta._filename
    }
  }
};`
    },
    position: [2120, 280]
  },
  output: [{ supabase_id: 1, obsidian_path: '/files/obsidian-vault/AI & LLMs/2026-05-20-x.md' }]
});

// ---------------------------------------------------------------------------
// 9. Write markdown to the Obsidian vault (inside container).
// ---------------------------------------------------------------------------
const writeFile = node({
  type: 'n8n-nodes-base.readWriteFile',
  version: 1.1,
  config: {
    name: 'Write Vault File',
    // IMPORTANT: requires `N8N_RESTRICT_FILE_ACCESS_TO=/files` env on the n8n
    // container, otherwise n8n blocks writes outside `~/.n8n-files` with a
    // misleading "is not writable" error. Workspace subdirs must also exist
    // beforehand — n8n's writeFile does NOT create parents.
    parameters: {
      operation: 'write',
      fileName: expr('={{ $json.obsidian_path }}'),
      dataPropertyName: 'data',
      options: { append: false }
    },
    position: [2360, 280]
  },
  output: [{ fileName: '/files/obsidian-vault/AI & LLMs/2026-05-20-x.md' }]
});

// ---------------------------------------------------------------------------
// 10. Telegram reply: ✅ Saved to <workspace> + 2 sentences + /star hint.
// ---------------------------------------------------------------------------
const telegramOk = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Reply: Saved',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr("={{ $('Render Markdown').item.json.chat_id }}"),
      text: expr(`={{ '✅ Saved to ' + $('Render Markdown').item.json.workspace + '\\n' + (($('Render Markdown').item.json.summary || '').split(/(?<=[.!?])\\s+/).slice(0, 2).join(' ')) + '\\n/star ' + $('Render Markdown').item.json.supabase_id + ' to mark must-read' }}`),
      additionalFields: { appendAttribution: false }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [2600, 280]
  },
  output: [{ ok: true }]
});

// ---------------------------------------------------------------------------
// Wire it up.
// telegramTrigger → extractUrl → urlGate
//   FALSE → telegramNoUrl (terminal)
//   TRUE  → routeSource (Switch)
//             web       → jinaFetch    ─┐
//             youtube   → jinaYoutube  ─┤→ geminiClassify → parseClassification
//             x         → jinaX        ─┤   → supabaseInsert → renderMarkdown
//             instagram → jinaInstagram─┘      → writeFile → telegramOk
// ---------------------------------------------------------------------------
const shared = geminiClassify
  .to(parseClassification)
  .to(supabaseInsert)
  .to(renderMarkdown)
  .to(writeFile)
  .to(telegramOk);

export default workflow('hivemind-ingest', 'Hivemind — Ingest (web + YouTube + X + Instagram)')
  .add(telegramTrigger)
  .to(extractUrl)
  .to(urlGate
    .onTrue(routeSource
      .branch(0, jinaFetch.to(shared))
      .branch(1, jinaYoutube.to(shared))
      .branch(2, jinaX.to(shared))
      .branch(3, jinaInstagram.to(shared)))
    .onFalse(telegramNoUrl));
