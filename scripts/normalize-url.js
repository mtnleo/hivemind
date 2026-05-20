// URL normalizer for dedupe-by-hash.
//
// Used inside an n8n Code node OR called via Execute Command from a shell.
// The same URL pasted from different surfaces (mobile, copy-tracking-link, share-sheet)
// must produce the same hash, otherwise dedupe is useless.
//
// CLI usage:   node normalize-url.js <url>      → prints { normalized, hash }
// n8n usage:   require this file's body in a Code node and call normalize(input.url)

const crypto = require('crypto');

const TRACKING_PARAMS = new Set([
  'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'utm_id',
  'gclid', 'fbclid', 'mc_cid', 'mc_eid', 'igshid', 'ref', 'ref_src', 'ref_url',
  's', 'si', 'feature', 'app',
]);

function normalize(rawUrl) {
  let u;
  try {
    u = new URL(rawUrl.trim());
  } catch {
    return { normalized: rawUrl.trim(), hash: sha256(rawUrl.trim()) };
  }

  // Lowercase host, strip www
  u.hostname = u.hostname.toLowerCase().replace(/^www\./, '');

  // Drop fragment (keep YouTube #t= timestamps in path canonicalization below)
  u.hash = '';

  // Strip tracking params, preserve order of remaining
  const kept = [...u.searchParams.entries()].filter(([k]) => !TRACKING_PARAMS.has(k.toLowerCase()));
  u.search = '';
  for (const [k, v] of kept) u.searchParams.append(k, v);

  // YouTube canonicalization: collapse youtu.be/<id> and youtube.com/shorts/<id> → youtube.com/watch?v=<id>
  if (u.hostname === 'youtu.be') {
    const id = u.pathname.replace(/^\//, '').split('/')[0];
    if (id) {
      u.hostname = 'youtube.com';
      u.pathname = '/watch';
      u.searchParams.set('v', id);
    }
  } else if (u.hostname === 'youtube.com' && u.pathname.startsWith('/shorts/')) {
    const id = u.pathname.replace(/^\/shorts\//, '').split('/')[0];
    if (id) {
      u.pathname = '/watch';
      u.searchParams.set('v', id);
    }
  }

  // X/Twitter: collapse x.com & twitter.com & mobile.twitter.com → x.com
  if (['twitter.com', 'mobile.twitter.com', 'm.twitter.com'].includes(u.hostname)) {
    u.hostname = 'x.com';
  }

  // Trailing slash on non-root paths is noise
  if (u.pathname.length > 1 && u.pathname.endsWith('/')) {
    u.pathname = u.pathname.slice(0, -1);
  }

  const normalized = u.toString();
  return { normalized, hash: sha256(normalized) };
}

function sha256(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

module.exports = { normalize };

if (require.main === module) {
  const arg = process.argv[2];
  if (!arg) { console.error('usage: node normalize-url.js <url>'); process.exit(1); }
  console.log(JSON.stringify(normalize(arg), null, 2));
}
