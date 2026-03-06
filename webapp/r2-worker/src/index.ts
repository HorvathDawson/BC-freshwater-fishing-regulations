interface Env {
  BUCKET: R2Bucket;
}

const CORS_HEADERS: Record<string, string> = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
  'Access-Control-Allow-Headers': 'Range, If-None-Match, If-Modified-Since',
  'Access-Control-Expose-Headers': 'Content-Length, Content-Range, Content-Type, Content-Encoding, ETag',
  'Access-Control-Max-Age': '86400',
};

// ── Cache policy per file type ──────────────────────────────────────
// PMTiles: tile data changes rarely — cache aggressively.
// The pmtiles JS library uses Range requests; browsers cache each range
// independently.  7 days + stale-while-revalidate means virtually zero
// re-fetches during a session and minimal egress between deploys.
//
// JSON data: changes on every pipeline run.  Short max-age so the
// browser re-validates quickly, but ETag support means a 304 (no body)
// when the file hasn't changed — saving the full download.
const CACHE_PMTILES = 'public, max-age=604800, stale-while-revalidate=86400'; // 7d + 1d swr
const CACHE_JSON    = 'public, max-age=300, stale-while-revalidate=60';       // 5min + 1min swr

function getCacheControl(key: string): string {
  if (key.endsWith('.pmtiles')) return CACHE_PMTILES;
  if (key.endsWith('.json'))    return CACHE_JSON;
  return 'public, max-age=3600';  // 1h default
}

function getContentType(key: string): string | null {
  if (key.endsWith('.pmtiles')) return 'application/octet-stream';
  if (key.endsWith('.json'))    return 'application/json; charset=utf-8';
  return null;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // ── CORS preflight ──────────────────────────────────────────────
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    if (request.method !== 'GET' && request.method !== 'HEAD') {
      return new Response('Method Not Allowed', { status: 405, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);
    const key = url.pathname.slice(1); // strip leading /
    if (!key) {
      return new Response('Not Found', { status: 404, headers: CORS_HEADERS });
    }

    // ── ETag conditional: return 304 if client has current version ───
    // For HEAD requests AND for full GET requests (non-Range), check
    // If-None-Match first with a cheap head() call to avoid reading
    // the object body unnecessarily.
    const ifNoneMatch = request.headers.get('If-None-Match');
    const rangeHeader = request.headers.get('Range');

    if (ifNoneMatch && !rangeHeader) {
      const meta = await env.BUCKET.head(key);
      if (meta && meta.httpEtag === ifNoneMatch) {
        const headers = new Headers(CORS_HEADERS);
        headers.set('ETag', meta.httpEtag);
        headers.set('Cache-Control', getCacheControl(key));
        const ct = getContentType(key);
        if (ct) headers.set('Content-Type', ct);
        return new Response(null, { status: 304, headers });
      }
    }

    // ── HEAD-only: metadata without body ────────────────────────────
    if (request.method === 'HEAD') {
      const meta = await env.BUCKET.head(key);
      if (!meta) {
        return new Response('Not Found', { status: 404, headers: CORS_HEADERS });
      }
      const headers = new Headers(CORS_HEADERS);
      meta.writeHttpMetadata(headers);
      headers.set('ETag', meta.httpEtag);
      headers.set('Cache-Control', getCacheControl(key));
      headers.set('Content-Length', String(meta.size));
      const ct = getContentType(key);
      if (ct) headers.set('Content-Type', ct);
      return new Response(null, { status: 200, headers });
    }

    // ── GET: full body or Range ─────────────────────────────────────
    let object: R2ObjectBody | R2Object | null;

    if (rangeHeader) {
      const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
      if (match) {
        const start = parseInt(match[1]);
        const end = match[2] ? parseInt(match[2]) : undefined;
        object = await env.BUCKET.get(key, {
          range: { offset: start, length: end !== undefined ? end - start + 1 : undefined },
        });
      } else {
        object = await env.BUCKET.get(key);
      }
    } else {
      object = await env.BUCKET.get(key);
    }

    if (!object) {
      return new Response('Not Found', { status: 404, headers: CORS_HEADERS });
    }

    const headers = new Headers(CORS_HEADERS);
    object.writeHttpMetadata(headers);
    headers.set('ETag', object.httpEtag);
    headers.set('Cache-Control', getCacheControl(key));

    const ct = getContentType(key);
    if (ct) headers.set('Content-Type', ct);

    if ('body' in object && object.body) {
      if (rangeHeader) {
        const offset = (object as any).range?.offset ?? 0;
        const length = (object as any).range?.length ?? object.size;
        headers.set('Content-Range', `bytes ${offset}-${offset + length - 1}/${object.size}`);
        return new Response(object.body, { status: 206, headers });
      }
      return new Response(object.body, { status: 200, headers });
    }

    // No body (shouldn't happen for GET, but be safe)
    return new Response(null, { status: 200, headers });
  },
};
