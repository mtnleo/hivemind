// Hivemind — Ingest (web + YouTube + X + Instagram) + /star command
// Sprint 4: Telegram message → Extract URL (detects /star | url | none)
//           → Route Message (switch) branches:
//             - star          → Mark Must-Read (PG) → Found? → vault read+rewrite+write → ⭐ reply
//             - star_invalid  → Reply: Star Usage
//             - url           → Route Source (per-source Jina fetch + Gemini + save) → ✅ reply
//             - none          → Reply: No URL
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
const GEMINI_EMBED_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent';
const EMBED_INPUT_TRUNCATE = 8000;
const EMBED_DIM = 768; // Matryoshka truncation — matches `embedding vector(768)` schema.
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
      // Emits one of six messageType values:
      //   - 'star'            → /star <int>             (also sets starId)
      //   - 'star_invalid'    → /star with bad/no arg
      //   - 'search'          → /search <query>         (also sets searchQuery)
      //   - 'search_invalid'  → bare /search with no query
      //   - 'url'             → first https?://… in message  (sets url, sourceType, urlHash)
      //   - 'none'            → no command, no URL
      jsCode: `const msg = $json.message || {};
const text = (msg.text || msg.caption || '').trim();
const chatId = msg.chat ? msg.chat.id : null;
const telegramMsgId = msg.message_id || null;
const base = { chatId, telegramMsgId, rawText: text };

// /star command detection (e.g. "/star 7" or "/star@BotName 7").
const starMatch = text.match(/^\\/star(?:@[A-Za-z0-9_]+)?(?:\\s+(.+))?\\s*$/);
if (starMatch) {
  const arg = (starMatch[1] || '').trim();
  if (/^\\d+$/.test(arg)) {
    return { json: { ...base, messageType: 'star', starId: parseInt(arg, 10), hasUrl: false } };
  }
  return { json: { ...base, messageType: 'star_invalid', hasUrl: false } };
}

// /search command detection (e.g. "/search agentic engineering").
const searchMatch = text.match(/^\\/search(?:@[A-Za-z0-9_]+)?(?:\\s+([\\s\\S]+))?\\s*$/);
if (searchMatch) {
  const q = (searchMatch[1] || '').trim();
  if (q.length > 0) {
    return { json: { ...base, messageType: 'search', searchQuery: q, hasUrl: false } };
  }
  return { json: { ...base, messageType: 'search_invalid', hasUrl: false } };
}

// URL extraction.
const m = text.match(/https?:\\/\\/[^\\s]+/);
if (!m) {
  return { json: { ...base, messageType: 'none', hasUrl: false } };
}
const rawUrl = m[0];
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
    ...base,
    messageType: 'url',
    hasUrl: true,
    url: rawUrl,
    normalizedUrl: normalized,
    urlHash: normalized,
    host,
    sourceType
  }
};`
    },
    position: [440, 400]
  },
  output: [{
    messageType: 'url',
    hasUrl: true,
    url: 'https://www.anthropic.com/news/example-article',
    urlHash: 'deadbeef',
    sourceType: 'web',
    chatId: 8837789534,
    telegramMsgId: 42
  }]
});

// ---------------------------------------------------------------------------
// 3. Route Message — single Switch on `messageType`. Replaces the S3 "Has URL?"
//    If node. Six explicit outputs (no fallback). Output index = port number;
//    appending new outputs preserves existing branch wiring.
// ---------------------------------------------------------------------------
function messageTypeRule(value: string, key: string) {
  return {
    conditions: {
      options: { caseSensitive: true, leftValue: '', typeValidation: 'loose' },
      conditions: [{
        leftValue: expr('={{ $json.messageType }}'),
        rightValue: value,
        operator: { type: 'string', operation: 'equals' }
      }],
      combinator: 'and'
    },
    renameOutput: true,
    outputKey: key
  };
}

const routeMessage = node({
  type: 'n8n-nodes-base.switch',
  version: 3.2,
  config: {
    name: 'Route Message',
    parameters: {
      rules: {
        values: [
          messageTypeRule('star', 'star'),
          messageTypeRule('star_invalid', 'star_invalid'),
          messageTypeRule('url', 'url'),
          messageTypeRule('none', 'none'),
          messageTypeRule('search', 'search'),
          messageTypeRule('search_invalid', 'search_invalid')
        ]
      },
      options: {}
    },
    position: [680, 400]
  }
});

// ---------------------------------------------------------------------------
// 4a. /star branch — Postgres UPDATE → vault rewrite → ⭐ Telegram reply.
//
// `Mark Must-Read` always returns exactly one row (COALESCE/EXISTS trick) so a
// missing id still surfaces a `found=false` row downstream instead of zero
// items (which would silently drop the Telegram reply).
// ---------------------------------------------------------------------------
const markMustRead = node({
  type: 'n8n-nodes-base.postgres',
  version: 2.6,
  config: {
    name: 'Mark Must-Read',
    parameters: {
      resource: 'database',
      operation: 'executeQuery',
      // Single $1::jsonb arg avoids n8n's naive comma-split queryReplacement.
      query: `with upd as (
  update hivemind.bookmarks
    set must_read = true
  where id = (($1::jsonb)->>'starId')::int
  returning id, title, obsidian_path
)
select
  coalesce((select id from upd), 0)::int as id,
  coalesce((select title from upd), '')::text as title,
  coalesce((select obsidian_path from upd), '')::text as obsidian_path,
  exists(select 1 from upd) as found,
  (($1::jsonb)->>'starId')::int as requested_id,
  (($1::jsonb)->>'chatId')::bigint as chat_id;`,
      options: {
        queryReplacement: expr('={{ JSON.stringify({ starId: $json.starId, chatId: $json.chatId }) }}')
      }
    },
    credentials: { postgres: newCredential('supabase-hivemind') },
    position: [920, 80]
  },
  output: [{ id: 7, title: 'Example', obsidian_path: '/files/obsidian-vault/Inbox/2026-05-20-x.md', found: true, requested_id: 7, chat_id: 8837789534 }]
});

// Route on numeric `id` instead of the boolean `found` — the n8n If v2.3
// `boolean: true` operator misroutes Postgres boolean values to the TRUE branch
// even when they arrive as JS false. Number-gt-0 sidesteps the coercion entirely.
const foundGate = ifElse({
  type: 'n8n-nodes-base.if',
  version: 2.3,
  config: {
    name: 'Found?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' },
        conditions: [{
          leftValue: expr('={{ $json.id }}'),
          rightValue: 0,
          operator: { type: 'number', operation: 'gt' }
        }],
        combinator: 'and'
      }
    },
    position: [1160, 80]
  }
});

const readVaultFile = node({
  type: 'n8n-nodes-base.readWriteFile',
  version: 1.1,
  config: {
    name: 'Read Vault File',
    parameters: {
      operation: 'read',
      fileSelector: expr('={{ $json.obsidian_path }}'),
      options: { dataPropertyName: 'data' }
    },
    position: [1400, 0]
  },
  output: [{ data: '---\nmust_read: false\n---\n# x' }]
});

const updateFrontmatter = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Update Frontmatter',
    // Decode the read binary, swap `must_read: false` → `must_read: true` on
    // the first matching line (frontmatter sits at top), re-emit as binary.
    //
    // GOTCHA: with N8N_DEFAULT_BINARY_DATA_MODE=filesystem (current Pi env),
    // `binary.data.data` is a `filesystem-v2:…` REFERENCE string, NOT inline
    // base64. Decoding it as base64 silently corrupts the payload and clobbers
    // the file. Always resolve via `helpers.getBinaryDataBuffer` — works
    // regardless of storage mode.
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      jsCode: `const pgRow = $('Mark Must-Read').item.json;
const bin = $input.item.binary && $input.item.binary.data;
if (!bin) {
  throw new Error('No vault file binary for ' + (pgRow && pgRow.obsidian_path));
}
const buf = await this.helpers.getBinaryDataBuffer(0, 'data');
const text = buf.toString('utf8');
const updated = text.replace(/^must_read:\\s*false\\s*$/m, 'must_read: true');
return {
  json: { ...pgRow, vault_updated: updated !== text },
  binary: {
    data: {
      data: Buffer.from(updated, 'utf8').toString('base64'),
      mimeType: 'text/markdown',
      fileName: bin.fileName || 'note.md'
    }
  }
};`
    },
    position: [1640, 0]
  },
  output: [{ vault_updated: true }]
});

const writeStarVaultFile = node({
  type: 'n8n-nodes-base.readWriteFile',
  version: 1.1,
  config: {
    name: 'Write Star Vault File',
    parameters: {
      operation: 'write',
      fileName: expr("={{ $('Mark Must-Read').item.json.obsidian_path }}"),
      dataPropertyName: 'data',
      options: { append: false }
    },
    position: [1880, 0]
  }
});

const replyStarred = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Reply: Starred',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr("={{ $('Mark Must-Read').item.json.chat_id }}"),
      text: expr("={{ '⭐ Marked #' + $('Mark Must-Read').item.json.id + ' as must-read: ' + ($('Mark Must-Read').item.json.title || '(untitled)') }}"),
      additionalFields: { appendAttribution: false }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [2120, 0]
  },
  output: [{ ok: true }]
});

const replyStarMissing = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Reply: Star Missing',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr("={{ $('Mark Must-Read').item.json.chat_id }}"),
      text: expr("={{ 'No bookmark found with id ' + $('Mark Must-Read').item.json.requested_id + '.' }}"),
      additionalFields: { appendAttribution: false }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [1400, 200]
  },
  output: [{ ok: true }]
});

const replyStarUsage = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Reply: Star Usage',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('={{ $json.chatId }}'),
      text: 'Usage: /star <id> — e.g. /star 7',
      additionalFields: { appendAttribution: false }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [920, 700]
  },
  output: [{ ok: true }]
});

// ---------------------------------------------------------------------------
// 4b. Telegram reply on no-URL / no-command branch (terminal).
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
// 4c. Route Source — Switch v3 branches by sourceType set in Extract URL.
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
// 6b. Gemini Embed — gemini-embedding-001 truncated to 768 dims for pgvector
//     semantic search. text-embedding-004 was deprecated; the v1beta endpoint
//     now returns 404 for it. gemini-embedding-001 is Matryoshka: native 3072
//     dims, request `outputDimensionality: 768` to match our column. taskType
//     SEMANTIC_SIMILARITY is the correct choice for /search by query.
//     Reuses googlePalmApi cred via nodeCredentialType (same trick as classify).
// ---------------------------------------------------------------------------
const geminiEmbed = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.4,
  config: {
    name: 'Gemini Embed',
    parameters: {
      method: 'POST',
      url: GEMINI_EMBED_URL,
      authentication: 'predefinedCredentialType',
      nodeCredentialType: 'googlePalmApi',
      sendBody: true,
      contentType: 'json',
      specifyBody: 'json',
      jsonBody: expr(`={{ JSON.stringify({
  model: 'models/gemini-embedding-001',
  content: {
    parts: [{
      text: (($json.title || '') + '\\n' + ($json.summary || '') + '\\n' + (Array.isArray($json._key_points) ? $json._key_points.join('\\n') : '')).slice(0, ${EMBED_INPUT_TRUNCATE})
    }]
  },
  outputDimensionality: ${EMBED_DIM},
  taskType: 'SEMANTIC_SIMILARITY'
}) }}`),
      options: { timeout: 30000 }
    },
    credentials: { googlePalmApi: newCredential('Google Gemini(PaLM) Api account') },
    retryOnFail: true,
    maxTries: 4,
    waitBetweenTries: 3000,
    position: [1880, 280]
  },
  output: [{ embedding: { values: [0.01, 0.02, 0.03] } }]
});

// ---------------------------------------------------------------------------
// 6c. Attach Embedding — merge Parse+Derive payload with embedding values.
//     pgvector accepts text literal '[v1,v2,...]'. We stringify the array here
//     and let SQL cast via (p->>'embedding')::vector — keeps everything inside
//     the single $1::jsonb arg (avoids the comma-split queryReplacement bug).
// ---------------------------------------------------------------------------
const attachEmbedding = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Attach Embedding',
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      jsCode: `const parsed = $('Parse + Derive').item.json;
const values = ($json.embedding && $json.embedding.values) || [];
if (!Array.isArray(values) || values.length !== 768) {
  throw new Error('Gemini embed returned ' + (values && values.length) + ' dims, expected 768');
}
return { json: { ...parsed, embedding: '[' + values.join(',') + ']' } };`
    },
    position: [2120, 280]
  },
  output: [{ embedding: '[0.01,0.02,0.03]' }]
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
      // comma (titles, summaries, embedding vector) shatters the param list.
      // Collapse all values into a single `$1::jsonb` parameter and extract
      // columns inside SQL. Embedding arrives as text literal '[v1,v2,...]'
      // (see Attach Embedding) and is cast directly to pgvector.
      query: `with payload as (select $1::jsonb as p)
insert into hivemind.bookmarks
  (url, url_hash, source_type, workspace, title, summary, key_points, tags, must_read, raw_content, telegram_msg_id, obsidian_path, embedding)
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
  p->>'obsidian_path',
  (p->>'embedding')::vector
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
  obsidian_path = excluded.obsidian_path,
  embedding = excluded.embedding
returning id;`,
      options: {
        queryReplacement: expr('={{ JSON.stringify($json) }}')
      }
    },
    credentials: { postgres: newCredential('supabase-hivemind') },
    position: [2360, 280]
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
    position: [2600, 280]
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
    position: [2840, 280]
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
    position: [3080, 280]
  },
  output: [{ ok: true }]
});

// ---------------------------------------------------------------------------
// 11. /search branch — embed query → cosine-rank top 5 → reply.
//
// Query embedding uses the SAME model + task as ingest (gemini-embedding-001,
// 768 dims, taskType SEMANTIC_SIMILARITY) so the doc-side embeddings live in
// the same vector space. Postgres uses `<=>` (cosine distance) and rides the
// HNSW index `bookmarks_embedding_hnsw_idx` (vector_cosine_ops).
// ---------------------------------------------------------------------------
const embedQuery = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.4,
  config: {
    name: 'Embed Query',
    parameters: {
      method: 'POST',
      url: GEMINI_EMBED_URL,
      authentication: 'predefinedCredentialType',
      nodeCredentialType: 'googlePalmApi',
      sendBody: true,
      contentType: 'json',
      specifyBody: 'json',
      jsonBody: expr(`={{ JSON.stringify({
  model: 'models/gemini-embedding-001',
  content: { parts: [{ text: ($json.searchQuery || '').slice(0, ${EMBED_INPUT_TRUNCATE}) }] },
  outputDimensionality: ${EMBED_DIM},
  taskType: 'SEMANTIC_SIMILARITY'
}) }}`),
      options: { timeout: 30000 }
    },
    credentials: { googlePalmApi: newCredential('Google Gemini(PaLM) Api account') },
    retryOnFail: true,
    maxTries: 4,
    waitBetweenTries: 3000,
    position: [920, 840]
  },
  output: [{ embedding: { values: [0.01, 0.02] } }]
});

const vectorSearch = node({
  type: 'n8n-nodes-base.postgres',
  version: 2.6,
  config: {
    name: 'Vector Search',
    parameters: {
      resource: 'database',
      operation: 'executeQuery',
      // `<=>` is the cosine distance operator from pgvector. Lower = closer.
      // qvec arrives as text literal `[v1,v2,...]` in the $1 jsonb arg.
      // `embedding is not null` is defensive — backfill is complete but keeps
      // the index hot path stable if future rows ever land without an embed.
      query: `select id, title, workspace, url, must_read,
       (embedding <=> ($1::jsonb->>'qvec')::vector) as distance
from hivemind.bookmarks
where embedding is not null
order by embedding <=> ($1::jsonb->>'qvec')::vector
limit 5;`,
      options: {
        queryReplacement: expr(`={{ JSON.stringify({ qvec: '[' + $json.embedding.values.join(',') + ']' }) }}`)
      }
    },
    credentials: { postgres: newCredential('supabase-hivemind') },
    position: [1160, 840]
  },
  output: [{ id: 14, title: 'x', workspace: 'AI & LLMs', url: 'https://x', must_read: false, distance: 0.12 }]
});

// Aggregator: Postgres emits N items (one per row). runOnceForAllItems lets us
// fold the whole result set into a single Telegram message. Empty input
// (table-wide no embeddings) → "No bookmarks yet." reply with carried chatId.
const formatResults = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Format Results',
    parameters: {
      mode: 'runOnceForAllItems',
      language: 'javaScript',
      // Telegram is called with parse_mode=HTML on Reply: Search so URLs with
      // `_` (e.g. x.com/user_name) don't get interpreted as italic by the
      // default Markdown parser. HTML mode only requires escaping `<`, `>`, `&`.
      //
      // Relative gap cutoff: Postgres always returns top 5 by cosine distance
      // (cheap, rides HNSW). We then drop results that are noticeably farther
      // from #1 than MAX_GAP (cosine). On a tight cluster of related items,
      // gaps stay small and all 5 survive; on a lone-wolf query, only #1
      // survives. Keeps #1 always so the bot never reduces to "no results".
      jsCode: `const MAX_GAP = 0.10;
const ex = $('Extract URL').first().json;
const chatId = ex.chatId;
const query = ex.searchQuery || '';
const items = $input.all();
function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}
if (items.length === 0) {
  return [{ json: { chat_id: chatId, text: 'No bookmarks yet — send me a URL first.' } }];
}
const topD = Number(items[0].json.distance);
const kept = items.filter((it, i) => i === 0 || (Number(it.json.distance) - topD) <= MAX_GAP);
const lines = kept.map((it, i) => {
  const r = it.json;
  const star = r.must_read ? '⭐ ' : '';
  const ws = r.workspace ? ' — [' + esc(r.workspace) + ']' : '';
  return (i + 1) + '. ' + star + esc(r.title || '(untitled)') + ws + '\\n   ' + esc(r.url || '');
});
const header = '🔍 Top ' + kept.length + ' for \\'' + esc(query) + '\\':';
return [{ json: { chat_id: chatId, text: header + '\\n' + lines.join('\\n') } }];`
    },
    position: [1400, 840]
  },
  output: [{ chat_id: 8837789534, text: '🔍 Top 5 for ...' }]
});

const replySearch = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Reply: Search',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('={{ $json.chat_id }}'),
      text: expr('={{ $json.text }}'),
      // parse_mode MUST be set to HTML — n8n Telegram v1.2 defaults to
      // Markdown, which trips on `_` inside URLs (e.g. x.com/user_name) and
      // returns "Bad Request: can't parse entities". The n8n API rejects
      // parse_mode='' (empty string), so HTML is the safe escape hatch: only
      // `<`, `>`, `&` need escaping (done in Format Results' `esc()` helper).
      // disableWebPagePreview (camelCase, NOT snake_case as Telegram's raw
      // API uses) suppresses the unfurl card under each of the 5 URLs.
      additionalFields: {
        appendAttribution: false,
        parse_mode: 'HTML',
        disableWebPagePreview: true
      }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [1640, 840]
  },
  output: [{ ok: true }]
});

const replySearchUsage = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Reply: Search Usage',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('={{ $json.chatId }}'),
      text: 'Usage: /search <query> — e.g. /search agentic engineering',
      additionalFields: { appendAttribution: false }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [920, 980]
  },
  output: [{ ok: true }]
});

// ---------------------------------------------------------------------------
// Wire it up.
// telegramTrigger → extractUrl → routeMessage (Switch)
//   star            → markMustRead → foundGate
//                       TRUE  → readVaultFile → updateFrontmatter
//                                 → writeStarVaultFile → replyStarred
//                       FALSE → replyStarMissing
//   star_invalid    → replyStarUsage
//   url             → routeSource (Switch)
//                       web       → jinaFetch     ─┐
//                       youtube   → jinaYoutube   ─┤→ geminiClassify
//                       x         → jinaX         ─┤   → parseClassification
//                       instagram → jinaInstagram ─┘     → geminiEmbed
//                                                          → attachEmbedding
//                                                          → supabaseInsert
//                                                          → renderMarkdown
//                                                          → writeFile → telegramOk
//   none            → telegramNoUrl
//   search          → embedQuery → vectorSearch → formatResults → replySearch
//   search_invalid  → replySearchUsage
// ---------------------------------------------------------------------------
const shared = geminiClassify
  .to(parseClassification)
  .to(geminiEmbed)
  .to(attachEmbedding)
  .to(supabaseInsert)
  .to(renderMarkdown)
  .to(writeFile)
  .to(telegramOk);

const starBranch = markMustRead.to(foundGate
  .onTrue(readVaultFile.to(updateFrontmatter).to(writeStarVaultFile).to(replyStarred))
  .onFalse(replyStarMissing));

const searchBranch = embedQuery.to(vectorSearch).to(formatResults).to(replySearch);

export default workflow('hivemind-ingest', 'Hivemind — Ingest (web + YouTube + X + Instagram) + /star + /search')
  .add(telegramTrigger)
  .to(extractUrl)
  .to(routeMessage
    .branch(0, starBranch)
    .branch(1, replyStarUsage)
    .branch(2, routeSource
      .branch(0, jinaFetch.to(shared))
      .branch(1, jinaYoutube.to(shared))
      .branch(2, jinaX.to(shared))
      .branch(3, jinaInstagram.to(shared)))
    .branch(3, telegramNoUrl)
    .branch(4, searchBranch)
    .branch(5, replySearchUsage));
