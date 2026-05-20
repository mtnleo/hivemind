-- n8n-bookmarks schema
-- Shares a Supabase project with other apps, so EVERYTHING lives in a dedicated
-- `hivemind` schema for hard namespace isolation. No tables in `public`.
-- Run once in the Supabase SQL editor.

create extension if not exists vector;

create schema if not exists hivemind;

-- ============================================================================
-- hivemind.bookmarks: one row per saved item
-- ============================================================================
create table if not exists hivemind.bookmarks (
  id              bigserial primary key,
  telegram_msg_id bigint,
  url             text not null,
  url_hash        text unique not null,           -- sha256(normalized_url)
  source_type     text not null check (source_type in ('youtube','x','web','instagram','note')),
  workspace       text not null,
  title           text,
  summary         text,
  key_points      jsonb,
  tags            text[],
  must_read       boolean not null default false,
  embedding       vector(768),                    -- text-embedding-004 output dim
  raw_content     text,                           -- truncated to 50k chars
  obsidian_path   text,
  created_at      timestamptz not null default now()
);

create index if not exists bookmarks_workspace_idx
  on hivemind.bookmarks (workspace, created_at desc);

create index if not exists bookmarks_must_read_idx
  on hivemind.bookmarks (must_read)
  where must_read;

create index if not exists bookmarks_created_at_idx
  on hivemind.bookmarks (created_at desc);

create index if not exists bookmarks_embedding_idx
  on hivemind.bookmarks using ivfflat (embedding vector_cosine_ops) with (lists = 100);

-- ============================================================================
-- hivemind.match_bookmarks: cosine-similarity search RPC
-- ============================================================================
create or replace function hivemind.match_bookmarks (
  query_embedding vector(768),
  match_threshold float default 0.5,
  match_count     int   default 8
)
returns table (
  id          bigint,
  url         text,
  title       text,
  summary     text,
  workspace   text,
  tags        text[],
  must_read   boolean,
  similarity  float
)
language sql stable
as $$
  select
    b.id,
    b.url,
    b.title,
    b.summary,
    b.workspace,
    b.tags,
    b.must_read,
    1 - (b.embedding <=> query_embedding) as similarity
  from hivemind.bookmarks b
  where b.embedding is not null
    and 1 - (b.embedding <=> query_embedding) > match_threshold
  order by b.embedding <=> query_embedding
  limit match_count;
$$;

-- ============================================================================
-- Isolation: do NOT expose hivemind schema via PostgREST (Supabase REST API)
-- The schema is only reachable via direct Postgres connection from n8n.
-- (Default Supabase exposes only `public` and `graphql_public` — we change nothing.)
-- ============================================================================
