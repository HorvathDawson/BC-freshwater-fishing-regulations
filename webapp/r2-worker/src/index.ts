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
  if (key === 'data_version.json') return 'no-store';  // always fresh — tiny file, busts PMTiles cache
  if (key.endsWith('.pmtiles')) return CACHE_PMTILES;
  if (key.endsWith('.json'))    return CACHE_JSON;
  return 'public, max-age=3600';  // 1h default
}

/** True for files that must always hit R2 (no edge caching). */
function isNoCacheFile(key: string): boolean {
  return key === 'data_version.json';
}

function getContentType(key: string): string | null {
  if (key.endsWith('.pmtiles')) return 'application/octet-stream';
  if (key.endsWith('.json'))    return 'application/json; charset=utf-8';
  return null;
}

/**
 * Build an UNCOMPRESSED Response from an R2 object (full GET or Range).
 * Attaches CORS, Cache-Control, ETag, Content-Type.
 * Compression is applied separately after caching — see `maybeCompress`.
 */
function buildR2Response(
  object: R2ObjectBody | R2Object,
  key: string,
  rangeHeader: string | null,
): Response {
  const headers = new Headers(CORS_HEADERS);
  object.writeHttpMetadata(headers);
  headers.set('ETag', object.httpEtag);
  headers.set('Cache-Control', getCacheControl(key));

  const ct = getContentType(key);
  if (ct) headers.set('Content-Type', ct);

  if (!('body' in object) || !object.body) {
    return new Response(null, { status: 200, headers });
  }

  if (rangeHeader) {
    const offset = (object as any).range?.offset ?? 0;
    const length = (object as any).range?.length ?? object.size;
    headers.set('Content-Range', `bytes ${offset}-${offset + length - 1}/${object.size}`);
    return new Response(object.body, { status: 206, headers });
  }

  return new Response(object.body, { status: 200, headers });
}

/**
 * Wrap a Response with gzip compression if the client supports it
 * and the file is a compressible JSON file.
 * Range (206) responses are never compressed.
 */
function maybeCompress(
  response: Response,
  key: string,
  acceptEncoding: string,
): Response {
  if (
    response.status !== 200 ||
    !response.body ||
    !key.endsWith('.json') ||
    !acceptEncoding.includes('gzip')
  ) {
    return response;
  }
  const compressed = response.body.pipeThrough(new CompressionStream('gzip'));
  const headers = new Headers(response.headers);
  headers.set('Content-Encoding', 'gzip');
  headers.delete('Content-Length');
  return new Response(compressed, { status: 200, headers });
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

    // ── Cloudflare edge cache ───────────────────────────────────────
    // For cacheable files (everything except data_version.json), check
    // the Cloudflare Cache API first.  This sits at the same edge POP
    // as the worker and avoids an R2 read on cache hits.  The cached
    // response's Cache-Control header controls edge TTL automatically.
    //
    // Use the *full* request URL (including Range header via cache key)
    // so each Range slice is cached independently — critical for PMTiles
    // whose JS library issues many small Range requests per tile load.
    const useEdgeCache = !isNoCacheFile(key);
    const cache = useEdgeCache ? caches.default : null;

    // Build a cache key that distinguishes Range requests.
    // Range requests for the same URL but different byte ranges must
    // map to different cache entries.
    let cacheKey = request;
    const rangeHeader = request.headers.get('Range');
    if (useEdgeCache && rangeHeader) {
      // Append range as a query param so the cache treats each slice
      // as a separate entry (CF cache keys on URL, not Vary: Range).
      const rkUrl = new URL(request.url);
      rkUrl.searchParams.set('_r', rangeHeader);
      cacheKey = new Request(rkUrl.toString(), { method: 'GET' });
    }

    if (cache) {
      const cached = await cache.match(cacheKey);
      if (cached) {
        // Edge cache hit — add CORS headers (cache may strip them)
        const resp = new Response(cached.body, cached);
        for (const [k, v] of Object.entries(CORS_HEADERS)) {
          resp.headers.set(k, v);
        }
        // Compress on the fly — cached response is always uncompressed
        const acceptEncoding = request.headers.get('Accept-Encoding') || '';
        return maybeCompress(resp, key, acceptEncoding);
      }
    }

    // ── ETag conditional: return 304 if client has current version ───
    // For non-Range requests, check If-None-Match with a cheap head()
    // call to avoid reading the object body unnecessarily.
    const ifNoneMatch = request.headers.get('If-None-Match');

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
    const acceptEncoding = request.headers.get('Accept-Encoding') || '';

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

    const response = buildR2Response(object, key, rangeHeader);

    // ── Store UNCOMPRESSED response in edge cache ───────────────────
    // Cache the clean R2 response so that compression doesn't interfere
    // with cache storage.  Gzip is applied on the way out to the client.
    if (cache) {
      const status = response.status;
      if (status === 200 || status === 206) {
        const cloned = response.clone();
        cache.put(cacheKey, cloned).catch(() => {});
      }
    }

    // Compress JSON for the client (after caching the uncompressed version)
    return maybeCompress(response, key, acceptEncoding);
  },
};
