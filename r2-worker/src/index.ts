interface Env {
  BUCKET: R2Bucket;
  SHARD_VERSION: string;
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

// Increment when edge-cache storage format changes to invalidate stale entries.
const EDGE_CACHE_VER = '3';

function getContentType(key: string): string | null {
  if (key.endsWith('.pmtiles')) return 'application/octet-stream';
  if (key.endsWith('.json'))    return 'application/json; charset=utf-8';
  return null;
}

/**
 * Build an uncompressed Response from an R2 object (full GET or Range).
 * Attaches CORS, Cache-Control, ETag, Content-Type.
 * Cloudflare's edge automatically compresses eligible responses (gzip/br)
 * based on Content-Type and Accept-Encoding — no manual compression needed.
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

// ── Shard resolution API ─────────────────────────────────────────────

const MAX_BATCH_FIDS = 20;
const MAX_BATCH_WBKS = 20;
const MAX_BATCH_RIDS = 5;
const SHARD_CACHE_TTL = 86400; // 24h — shards are versioned, safe to cache long
const FID_PATTERN = /^\d{1,12}$/;
const WBK_PATTERN = /^\d{1,12}$/;
const RID_PATTERN = /^[0-9a-f]{12}$/;

/**
 * Compute the 3-char hex shard prefix for an ID using SHA-256.
 * Returns lowercase hex (000–fff) → 4096 uniform buckets.
 */
async function shardPrefix(id: string): Promise<string> {
  const data = new TextEncoder().encode(id);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = new Uint8Array(hashBuffer);
  const hex = Array.from(hashArray.slice(0, 2))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
  return hex.slice(0, 3);
}

/**
 * Group IDs by their shard prefix to minimize R2 fetches.
 */
async function groupByPrefix(ids: string[]): Promise<Map<string, string[]>> {
  const groups = new Map<string, string[]>();
  for (const id of ids) {
    const prefix = await shardPrefix(id);
    const arr = groups.get(prefix);
    if (arr) arr.push(id);
    else groups.set(prefix, [id]);
  }
  return groups;
}

/**
 * Fetch an R2 JSON shard with edge caching keyed by R2 path (not API URL).
 */
async function fetchShard<T>(
  r2: R2Bucket,
  objectKey: string,
  requestUrl: string,
): Promise<T | null> {
  const origin = new URL(requestUrl).origin;
  const cacheKey = new Request(`${origin}/_r2_cache/${objectKey}`);
  const cache = caches.default;

  const cached = await cache.match(cacheKey);
  if (cached) return cached.json() as Promise<T>;

  const obj = await r2.get(objectKey);
  if (!obj) return null;

  const body = await obj.text();
  const cacheResponse = new Response(body, {
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': `public, max-age=${SHARD_CACHE_TTL}`,
    },
  });
  cache.put(cacheKey, cacheResponse.clone()).catch(() => {});

  return JSON.parse(body) as T;
}

function parseIds(param: string | null, pattern: RegExp, max: number): string[] | null {
  if (!param) return [];
  const ids = param.split(',').filter(Boolean);
  if (ids.length > max) return null;
  for (const id of ids) {
    if (!pattern.test(id)) return null;
  }
  return ids;
}

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=3600',
      ...CORS_HEADERS,
    },
  });
}

function errorResponse(error: string, status: number): Response {
  return new Response(JSON.stringify({ error }), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS_HEADERS },
  });
}

interface ReachData {
  display_name: string;
  name_variants: { name: string; source: 'direct' | 'tributary' | 'admin' }[];
  feature_type: string;
  reg_set_index: number;
  watershed_code: string;
  min_zoom: number;
  regions: string[];
  bbox: [number, number, number, number] | null;
  length_km: number;
  fids: string[];
  tributary_reg_ids?: string[];
}

interface ResolveResult {
  reach_id: string;
  reach: Omit<ReachData, 'fids'>;
  fids: string[];
  matched_fids: string[];
  matched_wbks: string[];
}

async function handleResolve(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const version = env.SHARD_VERSION || 'v7';

  const fids = parseIds(url.searchParams.get('fids'), FID_PATTERN, MAX_BATCH_FIDS);
  if (fids === null) {
    return errorResponse(`Invalid fids: must be numeric, max ${MAX_BATCH_FIDS} per request`, 400);
  }
  const wbks = parseIds(url.searchParams.get('wbks'), WBK_PATTERN, MAX_BATCH_WBKS);
  if (wbks === null) {
    return errorResponse(`Invalid wbks: must be numeric, max ${MAX_BATCH_WBKS} per request`, 400);
  }
  const rids = parseIds(url.searchParams.get('rids'), RID_PATTERN, MAX_BATCH_RIDS);
  if (rids === null) {
    return errorResponse(`Invalid rids: must be 12-char hex, max ${MAX_BATCH_RIDS} per request`, 400);
  }
  if (fids.length === 0 && wbks.length === 0 && rids.length === 0) {
    return errorResponse('Provide ?fids=, ?wbks=, and/or ?rids=', 400);
  }

  const results: ResolveResult[] = [];
  const seenReachIds = new Set<string>();
  const reachIdsToFetch = new Map<string, string>();
  const reachSources = new Map<string, { fids: string[]; wbks: string[] }>();

  if (fids.length > 0) {
    const fidGroups = await groupByPrefix(fids);
    for (const [prefix, groupFids] of fidGroups) {
      const shard = await fetchShard<Record<string, string>>(
        env.BUCKET, `shards/${version}/fids/${prefix}.json`, request.url,
      );
      if (!shard) continue;
      for (const fid of groupFids) {
        const reachId = shard[fid];
        if (reachId && !seenReachIds.has(reachId)) {
          seenReachIds.add(reachId);
          reachIdsToFetch.set(reachId, 'fid');
          reachSources.set(reachId, { fids: [fid], wbks: [] });
        } else if (reachId && reachSources.has(reachId)) {
          reachSources.get(reachId)!.fids.push(fid);
        }
      }
    }
  }

  if (wbks.length > 0) {
    const wbkGroups = await groupByPrefix(wbks);
    for (const [prefix, groupWbks] of wbkGroups) {
      const shard = await fetchShard<Record<string, string>>(
        env.BUCKET, `shards/${version}/polys/${prefix}.json`, request.url,
      );
      if (!shard) continue;
      for (const wbk of groupWbks) {
        const reachId = shard[wbk];
        if (reachId && !seenReachIds.has(reachId)) {
          seenReachIds.add(reachId);
          reachIdsToFetch.set(reachId, 'wbk');
          const src = reachSources.get(reachId) || { fids: [], wbks: [] };
          src.wbks.push(wbk);
          reachSources.set(reachId, src);
        } else if (reachId && reachSources.has(reachId)) {
          reachSources.get(reachId)!.wbks.push(wbk);
        }
      }
    }
  }

  // Direct reach_id resolution — skip fid/wbk shard lookups
  if (rids.length > 0) {
    for (const rid of rids) {
      if (!seenReachIds.has(rid)) {
        seenReachIds.add(rid);
        reachIdsToFetch.set(rid, 'rid');
        reachSources.set(rid, { fids: [], wbks: [] });
      }
    }
  }

  if (reachIdsToFetch.size > 0) {
    const reachGroups = await groupByPrefix([...reachIdsToFetch.keys()]);
    for (const [prefix, groupReachIds] of reachGroups) {
      const shard = await fetchShard<Record<string, ReachData>>(
        env.BUCKET, `shards/${version}/reaches/${prefix}.json`, request.url,
      );
      if (!shard) continue;
      for (const reachId of groupReachIds) {
        const data = shard[reachId];
        if (!data) continue;
        const { fids: reachFids, ...reachMeta } = data;
        const src = reachSources.get(reachId) || { fids: [], wbks: [] };
        results.push({ reach_id: reachId, reach: reachMeta, fids: reachFids || [], matched_fids: src.fids, matched_wbks: src.wbks });
      }
    }
  }

  if (results.length === 0) return jsonResponse({ results: [] }, 404);
  return jsonResponse({ results });
}

async function handleVersion(env: Env): Promise<Response> {
  return jsonResponse({ version: env.SHARD_VERSION || 'v7' });
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

    // ── API routes (checked BEFORE R2 file lookup) ──────────────────
    if (url.pathname === '/api/resolve' && request.method === 'GET') {
      return handleResolve(request, env);
    }
    if (url.pathname === '/api/version' && request.method === 'GET') {
      return handleVersion(env);
    }

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

    // Build a cache key that distinguishes Range requests and includes
    // a version param to invalidate stale entries when caching logic changes.
    let cacheKey: Request | undefined;
    const rangeHeader = request.headers.get('Range');
    if (useEdgeCache) {
      const rkUrl = new URL(request.url);
      rkUrl.searchParams.set('_cv', EDGE_CACHE_VER);
      if (rangeHeader) {
        rkUrl.searchParams.set('_r', rangeHeader);
      }
      cacheKey = new Request(rkUrl.toString(), { method: 'GET' });
    }

    if (cache && cacheKey) {
      const cached = await cache.match(cacheKey);
      if (cached) {
        // Edge cache hit — add CORS headers (cache may strip them)
        const resp = new Response(cached.body, cached);
        for (const [k, v] of Object.entries(CORS_HEADERS)) {
          resp.headers.set(k, v);
        }
        return resp;
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

    // ── Store response in edge cache ────────────────────────────────
    // Cloudflare's edge handles gzip/br compression automatically based
    // on Content-Type and Accept-Encoding — no manual compression needed.
    if (cache && cacheKey) {
      const status = response.status;
      if (status === 200 || status === 206) {
        const cloned = response.clone();
        cache.put(cacheKey, cloned).catch(() => {});
      }
    }

    return response;
  },
};
