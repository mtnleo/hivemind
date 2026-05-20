// Hivemind — Ingest (web articles)
// Sprint 2: Telegram URL → Jina Reader → Gemini classify+summarize → Supabase + Obsidian write → reply.
// YouTube / X / Instagram are deferred to Sprint 3. This file only handles web URLs.
//
// Source-of-truth for the live n8n workflow. Pass to mcp__n8n__create_workflow_from_code,
// then publish_workflow to activate.
//
// Credentials referenced by NAME must exist in n8n UI:
//   - telegram-hivemind   (Telegram API)
//   - supabase-hivemind   (Postgres → aws-1-us-east-2.pooler.supabase.com:5432, ssl=require)
//   - gemini-api-key      (HTTP Header Auth, header `x-goog-api-key`)
//
// Vault path inside the n8n container is `/files/obsidian-vault` (host bind mount on the Pi).

import { workflow, node, trigger, newCredential, expr } from '@n8n/workflow-sdk';

// ---------------------------------------------------------------------------
// Static config — single place to tweak per environment.
// Keep prompts inline so updating prompts/classify.md only needs a re-paste here.
// ---------------------------------------------------------------------------
const CLASSIFY_SYSTEM_PROMPT = `You are a personal knowledge curator. Your job is to read a piece of content the user just saved and produce a structured JSON record for their hive-mind library.

You will receive:
- \`url\`: the source URL
- \`source_type\`: one of \`youtube\`, \`x\`, \`web\`, \`instagram\`, \`note\`
- \`content\`: the extracted full text (transcript, tweet body, article body, or the user's own note)

You MUST return ONE JSON object, nothing else — no prose, no markdown fence, no comments. Schema:

{
  "workspace": "<one of the workspaces below>",
  "title": "<short, specific, ≤80 chars. Prefer the original title if good; otherwise rewrite>",
  "summary": "<3–6 sentences. Capture the actual insight, not just a description. Past tense, third-person>",
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
- tags: 3–7 entries, kebab-case. Specific over generic.
- suggested_must_read = true only for unusually high-signal items (primary sources, foundational papers, definitive guides). Default false.
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
// 2. Extract URL — regex first URL in message.text + normalize + sha256 hash.
//    Sets hasUrl flag; downstream If node gates the rest of the pipeline.
//    Normalization is the simplified inline version of scripts/normalize-url.js.
// ---------------------------------------------------------------------------
const extractUrl = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Extract URL',
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      jsCode: `
const crypto = require('crypto');

const msg = $json.message || {};
const text = (msg.text || msg.caption || '').trim();
const match = text.match(/https?:\\/\\/[^\\s]+/);

if (!match) {
  return {
    hasUrl: false,
    chatId: msg.chat ? msg.chat.id : null,
    telegramMsgId: msg.message_id || null,
    rawText: text
  };
}

const TRACKING = new Set([
  'utm_source','utm_medium','utm_campaign','utm_term','utm_content','utm_id',
  'gclid','fbclid','mc_cid','mc_eid','igshid','ref','ref_src','ref_url',
  's','si','feature','app'
]);

let normalized = match[0];
try {
  const u = new URL(match[0]);
  u.hostname = u.hostname.toLowerCase().replace(/^www\\./, '');
  u.hash = '';
  const kept = [...u.searchParams.entries()].filter(([k]) => !TRACKING.has(k.toLowerCase()));
  u.search = '';
  for (const [k, v] of kept) u.searchParams.append(k, v);
  if (u.pathname.length > 1 && u.pathname.endsWith('/')) u.pathname = u.pathname.slice(0, -1);
  normalized = u.toString();
} catch (_) { /* keep raw */ }

const urlHash = crypto.createHash('sha256').update(normalized).digest('hex');

return {
  hasUrl: true,
  url: match[0],
  normalizedUrl: normalized,
  urlHash,
  sourceType: 'web',
  chatId: msg.chat.id,
  telegramMsgId: msg.message_id
};
`.trim()
    },
    position: [440, 400]
  },
  output: [{
    hasUrl: true,
    url: 'https://www.anthropic.com/news/example-article',
    normalizedUrl: 'https://anthropic.com/news/example-article',
    urlHash: 'deadbeef…',
    sourceType: 'web',
    chatId: 8837789534,
    telegramMsgId: 42
  }]
});

// ---------------------------------------------------------------------------
// 3. URL gate — If node. True branch → fetch + classify + save. False branch → reply "no URL".
// ---------------------------------------------------------------------------
const urlGate = node({
  type: 'n8n-nodes-base.if',
  version: 2.2,
  config: {
    name: 'Has URL?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' },
        combinator: 'and',
        conditions: [{
          id: 'has-url',
          operator: { type: 'boolean', operation: 'true', singleValue: true },
          leftValue: expr('{{ $json.hasUrl }}'),
          rightValue: ''
        }]
      },
      options: {}
    },
    position: [680, 400]
  },
  output: [{ hasUrl: true }]
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
      chatId: expr('{{ $("Extract URL").item.json.chatId }}'),
      text: 'No URL detected in that message. Send me a web link to save.',
      additionalFields: { appendAttribution: false }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [920, 560]
  },
  output: [{ ok: true }]
});

// ---------------------------------------------------------------------------
// 4b. Fetch article via Jina Reader (free, no auth). Returns markdown-ish text.
//    On non-2xx Jina sets `continueOnFail` so the pipeline still hits Gemini with
//    empty content; classify.md handles empty → Inbox.
// ---------------------------------------------------------------------------
const jinaFetch = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.2,
  config: {
    name: 'Jina Reader',
    parameters: {
      method: 'GET',
      url: expr('https://r.jina.ai/{{ $json.url }}'),
      options: {
        response: { response: { responseFormat: 'text' } },
        timeout: 30000
      }
    },
    onError: 'continueErrorOutput',
    position: [920, 240]
  },
  output: [{ data: '# Example article\\n\\nBody text…' }]
});

// ---------------------------------------------------------------------------
// 5. Classify + summarize via Gemini 2.5 Flash. Single JSON object output.
// ---------------------------------------------------------------------------
const geminiClassify = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.2,
  config: {
    name: 'Gemini Classify',
    parameters: {
      method: 'POST',
      url: GEMINI_URL,
      authentication: 'predefinedCredentialType',
      nodeCredentialType: 'httpHeaderAuth',
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
        source_type: 'web',
        content: (($json.data || $json.body || '') + '').slice(0, ${RAW_CONTENT_TRUNCATE})
      })
    }]
  }],
  generationConfig: { responseMimeType: 'application/json', temperature: 0.2 }
}) }}`),
      options: { timeout: 60000 }
    },
    credentials: { httpHeaderAuth: newCredential('gemini-api-key') },
    position: [1160, 240]
  },
  output: [{
    candidates: [{
      content: { parts: [{ text: '{"workspace":"AI & LLMs","title":"…","summary":"…","key_points":["…"],"tags":["…"],"suggested_must_read":false}' }] }
    }]
  }]
});

// ---------------------------------------------------------------------------
// 6. Parse Gemini JSON + derive slug, date, obsidian_path. Truncate raw content.
// ---------------------------------------------------------------------------
const parseClassification = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Parse + Derive',
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      jsCode: `
const ex = $('Extract URL').item.json;
const jina = $('Jina Reader').item ? ($('Jina Reader').item.json.data || $('Jina Reader').item.json.body || '') : '';

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
    tags: ['web'],
    suggested_must_read: false
  };
}

function slugify(s) {
  return (s || '')
    .toLowerCase()
    .normalize('NFKD').replace(/[\\u0300-\\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60) || 'untitled';
}

const date = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
const slug = slugify(parsed.title);
const filename = date + '-' + slug + '.md';
const obsidianPath = '${VAULT_BASE}/' + parsed.workspace + '/' + filename;
const rawContent = (jina + '').slice(0, ${RAW_CONTENT_TRUNCATE});

return {
  url: ex.url,
  url_hash: ex.urlHash,
  source_type: 'web',
  workspace: parsed.workspace,
  title: parsed.title,
  summary: parsed.summary,
  key_points: JSON.stringify(parsed.key_points || []),
  tags: '{' + (parsed.tags || []).map(t => '"' + String(t).replace(/"/g, '\\\\"') + '"').join(',') + '}', // PG text[] literal
  must_read: !!parsed.suggested_must_read,
  raw_content: rawContent,
  telegram_msg_id: ex.telegramMsgId,
  obsidian_path: obsidianPath,
  chat_id: ex.chatId,
  // Carry forward for Obsidian render:
  _filename: filename,
  _date: date,
  _slug: slug,
  _key_points: parsed.key_points || [],
  _tags: parsed.tags || []
};
`.trim()
    },
    position: [1400, 240]
  },
  output: [{
    workspace: 'AI & LLMs',
    title: '…',
    obsidian_path: '/files/obsidian-vault/AI & LLMs/2026-05-20-example.md'
  }]
});

// ---------------------------------------------------------------------------
// 7. Postgres INSERT into hivemind.bookmarks with RETURNING id.
//    Parameterized via queryReplacement to avoid SQL injection on title/summary.
// ---------------------------------------------------------------------------
const supabaseInsert = node({
  type: 'n8n-nodes-base.postgres',
  version: 2.6,
  config: {
    name: 'Insert Bookmark',
    parameters: {
      operation: 'executeQuery',
      query: `insert into hivemind.bookmarks
  (url, url_hash, source_type, workspace, title, summary, key_points, tags, must_read, raw_content, telegram_msg_id, obsidian_path)
values
  ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::text[], $9, $10, $11, $12)
on conflict (url_hash) do update set
  url = excluded.url
returning id;`,
      options: {
        queryReplacement: expr('={{ $json.url }},{{ $json.url_hash }},{{ $json.source_type }},{{ $json.workspace }},{{ $json.title }},{{ $json.summary }},{{ $json.key_points }},{{ $json.tags }},{{ $json.must_read }},{{ $json.raw_content }},{{ $json.telegram_msg_id }},{{ $json.obsidian_path }}')
      }
    },
    credentials: { postgres: newCredential('supabase-hivemind') },
    position: [1640, 240]
  },
  output: [{ id: 1 }]
});

// ---------------------------------------------------------------------------
// 8. Render Obsidian markdown + prepare binary payload for the file write.
//    Frontmatter includes supabase_id (from RETURNING) so /star can patch the file later.
// ---------------------------------------------------------------------------
const renderMarkdown = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Render Markdown',
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      jsCode: `
const meta = $('Parse + Derive').item.json;
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
};
`.trim()
    },
    position: [1880, 240]
  },
  output: [{ supabase_id: 1, obsidian_path: '/files/obsidian-vault/AI & LLMs/2026-05-20-example.md' }]
});

// ---------------------------------------------------------------------------
// 9. Write the markdown file into the Obsidian vault (inside container).
// ---------------------------------------------------------------------------
const writeFile = node({
  type: 'n8n-nodes-base.readWriteFile',
  version: 1,
  config: {
    name: 'Write Vault File',
    parameters: {
      operation: 'write',
      fileName: expr('={{ $json.obsidian_path }}'),
      dataPropertyName: 'data',
      options: {}
    },
    position: [2120, 240]
  },
  output: [{ fileName: '/files/obsidian-vault/AI & LLMs/2026-05-20-example.md' }]
});

// ---------------------------------------------------------------------------
// 10. Telegram reply: ✅ Saved to <workspace> + first 2 sentences of summary + /star hint.
// ---------------------------------------------------------------------------
const telegramOk = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Reply: Saved',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('{{ $("Render Markdown").item.json.chat_id }}'),
      text: expr(`={{
        '✅ Saved to ' + $('Render Markdown').item.json.workspace + '\\n' +
        (($('Render Markdown').item.json.summary || '').split(/(?<=[.!?])\\s+/).slice(0, 2).join(' ')) + '\\n' +
        '/star ' + $('Render Markdown').item.json.supabase_id + ' to mark must-read'
      }}`),
      additionalFields: { appendAttribution: false }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [2360, 240]
  },
  output: [{ ok: true }]
});

// ---------------------------------------------------------------------------
// Wire it up. Linear happy path with one If branch for "no URL".
// ---------------------------------------------------------------------------
export default workflow('hivemind-ingest', 'Hivemind — Ingest (web)')
  .add(telegramTrigger)
  .to(extractUrl)
  .to(urlGate)
  .branch('true', flow => flow
    .to(jinaFetch)
    .to(geminiClassify)
    .to(parseClassification)
    .to(supabaseInsert)
    .to(renderMarkdown)
    .to(writeFile)
    .to(telegramOk))
  .branch('false', flow => flow
    .to(telegramNoUrl));
