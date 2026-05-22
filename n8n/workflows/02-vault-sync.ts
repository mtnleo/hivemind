// Hivemind — Vault Sync (Obsidian → Supabase reconciliation)
//
// Trigger architecture: Pi-side `inotifywait` daemon (scripts/vault-watch.sh +
// systemd unit) POSTs `{event, path}` to this workflow's webhook whenever an
// `.md` file is created, moved, or saved under the vault. We tried n8n's
// built-in localFileTrigger first; this n8n build refused to activate it
// ("Unrecognized node type: n8n-nodes-base.localFileTrigger") — likely a
// security policy for filesystem triggers. The webhook + Pi daemon path
// gives us full inotify semantics (move-to, close_write) without depending
// on chokidar's quirks.
//
// What the workflow does once the webhook fires:
//   1. Reads the .md file at the posted (container-side) path.
//   2. Parses frontmatter + body for supabase_id, workspace (= parent dir),
//      title, summary, key_points.
//   3. Fetches the current DB row, diffs against parsed values.
//   4. If nothing changed → no-op (short-circuits ingest-write echo).
//   5. If anything changed → re-embed via gemini-embedding-001 and UPDATE.
//
// Path translation: inotifywait on the Pi emits the host path under
// `/home/mtnleo/n8n-compose/local-files/obsidian-vault/...`. The daemon
// rewrites that to the n8n-container path `/files/obsidian-vault/...`
// (same path that the ingest workflow's Write Vault File node uses) before
// POSTing.
//
// Cred reuse (no new credentials):
//   - Postgres account     (postgres → Supabase, ssl=require)
//   - Google Gemini(PaLM)  (googlePalmApi → reused via nodeCredentialType)
//
// Deploy: raw n8n REST POST + activate. Webhook triggers always activate.
//   POST https://n8n.mtnleo-n8n.org/api/v1/workflows  (create — new id)
//   POST /api/v1/workflows/<id>/activate

import { workflow, node, trigger, ifElse, newCredential, expr } from '@n8n/workflow-sdk';

const GEMINI_EMBED_URL = 'https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent';
const EMBED_INPUT_TRUNCATE = 8000;
const EMBED_DIM = 768;
const VAULT_BASE = '/files/obsidian-vault';

// ---------------------------------------------------------------------------
// 1. Trigger — Webhook. Daemon on the Pi posts JSON
//    `{event: 'close_write'|'moved_to', path: '/files/obsidian-vault/.../foo.md'}`.
//    Path field already container-side; daemon does the rewrite.
// ---------------------------------------------------------------------------
const vaultTrigger = trigger({
  type: 'n8n-nodes-base.webhook',
  version: 2,
  config: {
    name: 'Vault Webhook',
    parameters: {
      httpMethod: 'POST',
      path: 'vault-sync',
      responseMode: 'onReceived',
      responseData: 'firstEntryJson',
      options: {}
    },
    position: [200, 400]
  },
  output: [{
    body: { event: 'moved_to', path: `${VAULT_BASE}/AI & LLMs/2026-05-20-x.md` }
  }]
});

// ---------------------------------------------------------------------------
// 2. Read Vault File — pull the .md bytes into a binary property `data`.
//    Both triggers feed this; we use the trigger's emitted `path`.
// ---------------------------------------------------------------------------
const readVault = node({
  type: 'n8n-nodes-base.readWriteFile',
  version: 1.1,
  config: {
    name: 'Read Vault File',
    parameters: {
      operation: 'read',
      // Webhook trigger nests payload under $json.body; daemon already
      // posts the container-side path.
      fileSelector: expr('={{ $json.body.path }}'),
      options: { dataPropertyName: 'data' }
    },
    position: [440, 400]
  },
  output: [{ data: '---\nsupabase_id: 1\n---\n# x' }]
});

// ---------------------------------------------------------------------------
// 3. Parse Vault File — pull frontmatter + body into structured fields.
//    Uses helpers.getBinaryDataBuffer (works with filesystem binary-data mode
//    on the Pi; see S4 gotcha).
// ---------------------------------------------------------------------------
const parseVault = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Parse Vault File',
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      jsCode: `// Read Vault File overwrites $json with file metadata, so the original
// webhook payload must be pulled explicitly from the trigger node.
const trig = $('Vault Webhook').first().json;
const path = (trig.body && trig.body.path) || trig.path;
if (!path) {
  return { json: { skip: true, reason: 'no path on trigger payload' } };
}
// Drop non-.md files (the daemon may emit them; e.g. .obsidian/ writes).
if (!/\\.md$/i.test(path)) {
  return { json: { skip: true, reason: 'not a .md file', path } };
}
const buf = await this.helpers.getBinaryDataBuffer(0, 'data');
const text = buf.toString('utf8');

// Frontmatter: --- ... --- at top.
const fmMatch = text.match(/^---\\n([\\s\\S]*?)\\n---/);
if (!fmMatch) {
  return { json: { skip: true, reason: 'no frontmatter', path } };
}
const fm = fmMatch[1];
const supaMatch = fm.match(/^supabase_id:\\s*(\\d+)\\s*$/m);
if (!supaMatch) {
  return { json: { skip: true, reason: 'no supabase_id frontmatter (user-created note?)', path } };
}
const supabaseId = parseInt(supaMatch[1], 10);

// Workspace = immediate parent directory name.
const VAULT_BASE = '${VAULT_BASE}';
const rel = path.startsWith(VAULT_BASE + '/') ? path.slice(VAULT_BASE.length + 1) : path;
const parts = rel.split('/');
const workspace = parts.length >= 2 ? parts[0] : 'Inbox';

// Body section parsing.
const body = text.slice(fmMatch[0].length);
const titleMatch = body.match(/\\n#\\s+(.+?)\\n/);
const title = titleMatch ? titleMatch[1].trim() : '';

function section(label) {
  const re = new RegExp('##\\\\s+' + label + '\\\\s*\\\\n([\\\\s\\\\S]*?)(?=\\\\n##\\\\s|$)');
  const m = body.match(re);
  return m ? m[1].trim() : '';
}
const summary = section('Summary');
const keyPointsRaw = section('Key points');
const key_points = keyPointsRaw
  .split('\\n')
  .map(l => l.replace(/^[-*]\\s+/, '').trim())
  .filter(l => l.length > 0 && l !== '(none extracted)');

return {
  json: {
    skip: false,
    supabase_id: supabaseId,
    obsidian_path: path,
    workspace,
    title,
    summary,
    key_points,
    fingerprint: title + '|' + summary + '|' + JSON.stringify(key_points)
  }
};`
    },
    position: [680, 400]
  },
  output: [{ skip: false, supabase_id: 1, workspace: 'AI & LLMs', title: 'x', summary: 'x', key_points: [], fingerprint: 'x' }]
});

// ---------------------------------------------------------------------------
// 4. Has supabase_id? — skip files that don't carry one (e.g. user-authored
//    notes). Route on numeric `supabase_id > 0` to dodge the n8n If v2.3
//    boolean coercion bug (S4 gotcha).
// ---------------------------------------------------------------------------
const skipGate = ifElse({
  type: 'n8n-nodes-base.if',
  version: 2.3,
  config: {
    name: 'Has supabase_id?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' },
        conditions: [{
          leftValue: expr('={{ $json.supabase_id || 0 }}'),
          rightValue: 0,
          operator: { type: 'number', operation: 'gt' }
        }],
        combinator: 'and'
      }
    },
    position: [920, 400]
  }
});

// ---------------------------------------------------------------------------
// 5. Fetch Current Row — pull the current DB state so we can diff.
//    Postgres node returns 0 rows if id missing → downstream goes silent.
//    Use COALESCE/EXISTS wrapper (S4 pattern) so a missing id surfaces.
// ---------------------------------------------------------------------------
const fetchCurrent = node({
  type: 'n8n-nodes-base.postgres',
  version: 2.6,
  config: {
    name: 'Fetch Current Row',
    parameters: {
      resource: 'database',
      operation: 'executeQuery',
      query: `with src as (select $1::jsonb as p)
select
  coalesce(b.id, 0)::int as db_id,
  coalesce(b.title, '')::text as db_title,
  coalesce(b.summary, '')::text as db_summary,
  coalesce(b.key_points::text, '[]')::text as db_key_points,
  coalesce(b.workspace, '')::text as db_workspace,
  coalesce(b.obsidian_path, '')::text as db_obsidian_path,
  (p->>'supabase_id')::int as supabase_id,
  p->>'obsidian_path' as obsidian_path,
  p->>'workspace' as workspace,
  p->>'title' as title,
  p->>'summary' as summary,
  p->'key_points' as key_points,
  p->>'fingerprint' as fingerprint
from src
left join hivemind.bookmarks b on b.id = (p->>'supabase_id')::int;`,
      options: {
        queryReplacement: expr('={{ JSON.stringify($json) }}')
      }
    },
    credentials: { postgres: newCredential('supabase-hivemind') },
    position: [1160, 320]
  },
  output: [{ db_id: 1, db_title: 'x', db_workspace: 'Inbox', supabase_id: 1, title: 'x', workspace: 'AI & LLMs' }]
});

// ---------------------------------------------------------------------------
// 6. Diff — flag if anything actually changed. Drops no-op cycles like the
//    ingest workflow writing a fresh file and this watcher firing 'add'
//    immediately after.
// ---------------------------------------------------------------------------
const diff = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Diff',
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      jsCode: `const r = $json;
if ((r.db_id || 0) === 0) {
  // Frontmatter points at an id that doesn't exist — orphan file. Skip.
  return { json: { ...r, changed: 0, skip: true, reason: 'db_id 0 (orphaned file)' } };
}
// Postgres jsonb::text adds whitespace (\"a\", \"b\") and may reorder keys —
// JSON.parse + re-stringify both sides so the comparison is structural,
// not formatting-sensitive. Otherwise key_points would flag as changed
// on every event and trigger a wasted re-embed.
function canon(s) {
  try { return JSON.stringify(JSON.parse(s || '[]')); } catch (e) { return s || '[]'; }
}
const kpFile = JSON.stringify(r.key_points || []);
const kpDB = canon(r.db_key_points);
const changedFields = [];
if (r.title !== r.db_title) changedFields.push('title');
if (r.summary !== r.db_summary) changedFields.push('summary');
if (kpFile !== kpDB) changedFields.push('key_points');
if (r.workspace !== r.db_workspace) changedFields.push('workspace');
if (r.obsidian_path !== r.db_obsidian_path) changedFields.push('obsidian_path');
return { json: { ...r, changed: changedFields.length, changed_fields: changedFields } };`
    },
    position: [1400, 320]
  },
  output: [{ changed: 2, changed_fields: ['workspace', 'obsidian_path'] }]
});

// ---------------------------------------------------------------------------
// 7. Anything Changed? — gate. 0 → terminate silently. >0 → continue.
// ---------------------------------------------------------------------------
const changeGate = ifElse({
  type: 'n8n-nodes-base.if',
  version: 2.3,
  config: {
    name: 'Anything Changed?',
    parameters: {
      conditions: {
        options: { caseSensitive: true, leftValue: '', typeValidation: 'strict' },
        conditions: [{
          leftValue: expr('={{ $json.changed }}'),
          rightValue: 0,
          operator: { type: 'number', operation: 'gt' }
        }],
        combinator: 'and'
      }
    },
    position: [1640, 320]
  }
});

// ---------------------------------------------------------------------------
// 8. Gemini Embed — re-embed body (title + summary + key_points). Same
//    contract as the ingest embed: model gemini-embedding-001, 768 dims,
//    SEMANTIC_SIMILARITY.
//
//    Always re-embed when ANY field changed (even if only workspace moved).
//    Slight overhead but keeps the path simple — free tier covers it.
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
      text: (($json.title || '') + '\\n' + ($json.summary || '') + '\\n' + (Array.isArray($json.key_points) ? $json.key_points.join('\\n') : '')).slice(0, ${EMBED_INPUT_TRUNCATE})
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
    position: [1880, 240]
  },
  output: [{ embedding: { values: [0.01, 0.02] } }]
});

// ---------------------------------------------------------------------------
// 9. Attach Embedding — merge parsed payload with embedding string. Same
//    pattern as the ingest path.
// ---------------------------------------------------------------------------
const attachEmbedding = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Attach Embedding',
    parameters: {
      mode: 'runOnceForEachItem',
      language: 'javaScript',
      jsCode: `const parsed = $('Diff').item.json;
const values = ($json.embedding && $json.embedding.values) || [];
if (!Array.isArray(values) || values.length !== ${EMBED_DIM}) {
  throw new Error('Gemini embed returned ' + (values && values.length) + ' dims, expected ${EMBED_DIM}');
}
return {
  json: {
    supabase_id: parsed.supabase_id,
    title: parsed.title,
    summary: parsed.summary,
    key_points: parsed.key_points,
    workspace: parsed.workspace,
    obsidian_path: parsed.obsidian_path,
    embedding: '[' + values.join(',') + ']',
    changed_fields: parsed.changed_fields
  }
};`
    },
    position: [2120, 240]
  },
  output: [{ supabase_id: 1, embedding: '[0.01,0.02]' }]
});

// ---------------------------------------------------------------------------
// 10. Update Bookmark — single $1::jsonb arg, cast embedding to vector
//     inline. Same comma-split workaround as ingest.
// ---------------------------------------------------------------------------
const updateBookmark = node({
  type: 'n8n-nodes-base.postgres',
  version: 2.6,
  config: {
    name: 'Update Bookmark',
    parameters: {
      resource: 'database',
      operation: 'executeQuery',
      query: `with payload as (select $1::jsonb as p)
update hivemind.bookmarks set
  title = p->>'title',
  summary = p->>'summary',
  key_points = p->'key_points',
  workspace = p->>'workspace',
  obsidian_path = p->>'obsidian_path',
  embedding = (p->>'embedding')::vector
from payload
where hivemind.bookmarks.id = (p->>'supabase_id')::int
returning hivemind.bookmarks.id;`,
      options: {
        queryReplacement: expr('={{ JSON.stringify($json) }}')
      }
    },
    credentials: { postgres: newCredential('supabase-hivemind') },
    position: [2360, 240]
  },
  output: [{ id: 1 }]
});

// ---------------------------------------------------------------------------
// Wire it up.
// Vault Trigger (add + change) → Read Vault File → Parse Vault File →
//   Has supabase_id?
//     FALSE → end (user notes / non-md files)
//     TRUE  → Fetch Current Row → Diff → Anything Changed?
//             FALSE → end (ingest-loop guard, save with no real edit)
//             TRUE  → Gemini Embed → Attach Embedding → Update Bookmark
// ---------------------------------------------------------------------------
const tail = skipGate
  .onTrue(fetchCurrent.to(diff).to(changeGate
    .onTrue(geminiEmbed.to(attachEmbedding).to(updateBookmark))));

export default workflow('hivemind-vault-sync', 'Hivemind — Vault Sync (Obsidian → Supabase)')
  .add(vaultTrigger)
  .to(readVault)
  .to(parseVault)
  .to(tail);
