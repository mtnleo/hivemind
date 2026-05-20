// Hivemind — Ping
// Sprint 1 sanity check: proves Telegram → Cloudflare Tunnel → n8n → Supabase → reply round-trip.
// Source-of-truth for the workflow created in n8n via MCP `create_workflow_from_code`.
//
// Workflow ID:  NAp209DrgQQHN5ec
// URL:          https://n8n.mtnleo-n8n.org/workflow/NAp209DrgQQHN5ec
//
// To re-create: pass this code to mcp__n8n__create_workflow_from_code.
// Credentials referenced by NAME must exist in n8n UI:
//   - telegram-hivemind (Telegram API)
//   - supabase-hivemind (Postgres → aws-1-us-east-2.pooler.supabase.com:5432, ssl=require)

import { workflow, node, trigger, newCredential, expr } from '@n8n/workflow-sdk';

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
    position: [240, 300]
  },
  output: [{
    update_id: 1,
    message: {
      message_id: 1,
      from: { id: 8837789534, first_name: 'Martin' },
      chat: { id: 8837789534, type: 'private' },
      date: 1736000000,
      text: '/ping'
    }
  }]
});

const bookmarksCount = node({
  type: 'n8n-nodes-base.postgres',
  version: 2.6,
  config: {
    name: 'Bookmarks Count',
    parameters: {
      operation: 'executeQuery',
      query: 'select count(*)::int as count from hivemind.bookmarks;',
      options: {}
    },
    credentials: { postgres: newCredential('supabase-hivemind') },
    position: [540, 300]
  },
  output: [{ count: 0 }]
});

const pongReply = node({
  type: 'n8n-nodes-base.telegram',
  version: 1.2,
  config: {
    name: 'Pong Reply',
    parameters: {
      resource: 'message',
      operation: 'sendMessage',
      chatId: expr('{{ $("Telegram Trigger").item.json.message.chat.id }}'),
      text: expr('pong — {{ $json.count }} bookmark(s) saved'),
      additionalFields: { appendAttribution: false }
    },
    credentials: { telegramApi: newCredential('telegram-hivemind') },
    position: [840, 300]
  },
  output: [{ ok: true, result: { message_id: 2, text: 'pong — 0 bookmark(s) saved' } }]
});

export default workflow('hivemind-ping', 'Hivemind — Ping')
  .add(telegramTrigger)
  .to(bookmarksCount)
  .to(pongReply);
