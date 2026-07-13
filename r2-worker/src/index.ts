import { EmailMessage } from "cloudflare:email";

interface Env {
  BUCKET: R2Bucket;
  SHARD_VERSION: string;
  /** Cloudflare Email send binding (optional — enables feedback emails). */
  SEND_EMAIL?: { send(message: EmailMessage): Promise<void> };
  /** Sender address on your verified domain (e.g. feedback@canifishthis.ca). */
  FEEDBACK_FROM?: string;
  /** Verified destination address that receives feedback emails. */
  FEEDBACK_TO?: string;
}

const CORS_HEADERS: Record<string, string> = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Range, If-None-Match, If-Modified-Since, Content-Type',
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
  // Mobile bundle: versioned under mobile/v{N}/ → immutable, cache aggressively.
  if (key.startsWith('mobile/')) return CACHE_PMTILES;
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
  if (key.endsWith('.sqlite'))  return 'application/vnd.sqlite3';
  if (key.endsWith('.gz'))      return 'application/gzip';
  if (key.endsWith('.json'))    return 'application/json; charset=utf-8';
  if (key.endsWith('.png'))     return 'image/png';
  if (key.endsWith('.pdf'))     return 'application/pdf';
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
  if (cached) {
    // Miniflare's caches.default can occasionally return an empty/truncated
    // body, which makes JSON.parse throw "Unexpected end of JSON input" and
    // 500s the whole resolve. Guard the read and fall through to a fresh R2
    // fetch on any cache-read/parse failure.
    try {
      const cachedText = await cached.text();
      if (cachedText) return JSON.parse(cachedText) as T;
    } catch { /* corrupt cache entry — fall through to R2 */ }
  }

  const obj = await r2.get(objectKey);
  if (!obj) return null;

  const body = await obj.text();
  if (!body) return null;

  const cacheResponse = new Response(body, {
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': `public, max-age=${SHARD_CACHE_TTL}`,
    },
  });
  cache.put(cacheKey, cacheResponse.clone()).catch(() => {});

  try {
    return JSON.parse(body) as T;
  } catch {
    return null;
  }
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

function jsonResponse(
  data: unknown,
  status = 200,
  // Dynamic API responses (resolve/version) depend on the current data
  // deploy, so the default is "always revalidate" — otherwise a browser
  // that cached a pre-deploy response keeps serving stale data (e.g. a
  // reach with no bathymetry) until the entry expires.
  cacheControl = 'no-cache',
): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': cacheControl,
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
  name_variants: { name: string; source: 'direct' | 'tributary' | 'admin' | 'bathymetry' }[];
  feature_type: string;
  reg_set_index: number;
  watershed_code: string;
  min_zoom: number;
  regions: string[];
  bbox: [number, number, number, number] | null;
  length_km: number;
  fids: string[];
  tributary_reg_ids?: string[];
  /** Bathymetry survey depth-map PDFs (polygon reaches only; served at bathymetry/<pdf>). */
  bathymetry?: { pdf: string; title: string }[];
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
    const fidShards = await Promise.all(
      [...fidGroups.entries()].map(async ([prefix, groupFids]) => ({
        groupFids,
        shard: await fetchShard<Record<string, string>>(
          env.BUCKET, `shards/${version}/fids/${prefix}.json`, request.url,
        ),
      })),
    );
    for (const { groupFids, shard } of fidShards) {
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
    const wbkShards = await Promise.all(
      [...wbkGroups.entries()].map(async ([prefix, groupWbks]) => ({
        groupWbks,
        shard: await fetchShard<Record<string, string>>(
          env.BUCKET, `shards/${version}/polys/${prefix}.json`, request.url,
        ),
      })),
    );
    for (const { groupWbks, shard } of wbkShards) {
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
    const reachShards = await Promise.all(
      [...reachGroups.entries()].map(async ([prefix, groupReachIds]) => ({
        groupReachIds,
        shard: await fetchShard<Record<string, ReachData>>(
          env.BUCKET, `shards/${version}/reaches/${prefix}.json`, request.url,
        ),
      })),
    );
    for (const { groupReachIds, shard } of reachShards) {
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
  // Source of truth for the active data version — must never be cached so
  // clients notice a new deploy immediately.
  return jsonResponse({ version: env.SHARD_VERSION || 'v7' }, 200, 'no-store');
}

// ── Feedback submission API ──────────────────────────────────────────

const MAX_FEEDBACK_TITLE = 300;
const MAX_FEEDBACK_BODY = 8000;
const MAX_FEEDBACK_EMAIL = 254; // RFC 5321 max address length

/** Pragmatic email shape check (mirrors the webapp's client-side validation). */
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

interface FeedbackRecord {
  receivedAt: string;
  title: string;
  body: string;
  /** Optional contact address supplied by the submitter (validated). */
  email: string | null;
  userAgent: string | null;
  country: string | null;
}

/**
 * Accept a feedback report (no account required) via POST /api/feedback.
 *
 * The report is ALWAYS stored durably in R2 under feedback/<date>/… — those
 * keys are never served over GET (see the guard in fetch()).  If Cloudflare
 * Email Routing is configured (SEND_EMAIL binding + FEEDBACK_FROM/FEEDBACK_TO),
 * a notification email is also sent.  When the submitter leaves a valid email, a
 * best-effort "we got your feedback" confirmation is sent to them as well.
 * Email is best-effort: a failure there never fails the request, because the
 * report is already saved.
 *
 * Body: { title?: string, body: string, email?: string, hp?: string }
 *   hp — honeypot; bots fill it, humans never see it → silently dropped.
 */
async function handleFeedback(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  let payload: { title?: unknown; body?: unknown; email?: unknown; hp?: unknown };
  try {
    const raw = await request.text();
    if (raw.length > MAX_FEEDBACK_BODY + 2000) {
      return errorResponse('Report too large', 413);
    }
    payload = JSON.parse(raw);
  } catch {
    return errorResponse('Invalid JSON body', 400);
  }

  // Honeypot: pretend success so bots move on, but store nothing.
  if (typeof payload.hp === 'string' && payload.hp.trim()) {
    return jsonResponse({ ok: true });
  }

  const title = String(payload.title ?? '').slice(0, MAX_FEEDBACK_TITLE).trim();
  const body = String(payload.body ?? '').slice(0, MAX_FEEDBACK_BODY).trim();
  if (!body) {
    return errorResponse('Report body is required', 400);
  }

  // Optional contact email — must be well-formed if present.  Enforced here too
  // (not just in the browser), since anyone can POST to this endpoint.
  const email = String(payload.email ?? '').trim().slice(0, MAX_FEEDBACK_EMAIL);
  if (email && !EMAIL_RE.test(email)) {
    return errorResponse('Invalid email address', 400);
  }

  const receivedAt = new Date().toISOString();
  const record: FeedbackRecord = {
    receivedAt,
    title: title || '[Report]',
    body,
    email: email || null,
    userAgent: request.headers.get('User-Agent'),
    country: request.headers.get('CF-IPCountry'),
  };

  // 1. Durable storage — the source of truth, independent of email setup.
  const key = `feedback/${receivedAt.slice(0, 10)}/${receivedAt}-${crypto.randomUUID()}.json`;
  try {
    await env.BUCKET.put(key, JSON.stringify(record, null, 2), {
      httpMetadata: { contentType: 'application/json' },
    });
  } catch (err) {
    // Storage is required — surface the failure rather than hiding it.
    console.error('Feedback storage failed:', err);
    return errorResponse('Failed to store report', 500);
  }

  // 2. Optional emails (best-effort — a failure here never fails the request,
  //    because the report is already stored above).
  if (env.SEND_EMAIL && env.FEEDBACK_FROM) {
    // 2a. Notify the maintainer.  FEEDBACK_TO is a verified destination, so this
    //     works even with only Email Routing configured.
    if (env.FEEDBACK_TO) {
      ctx.waitUntil(
        sendFeedbackNotification(env, record).catch(err => {
          console.error('Feedback notification email failed (report is stored in R2):', err);
        }),
      );
    }
    // 2b. Confirm receipt to the submitter.  This targets an arbitrary address,
    //     so it requires Email Sending to be onboarded on the domain; with only
    //     Email Routing it is rejected — hence best-effort.
    if (record.email) {
      ctx.waitUntil(
        sendFeedbackConfirmation(env, record).catch(err => {
          console.error('Feedback confirmation email failed (report is stored in R2):', err);
        }),
      );
    }
  }

  return jsonResponse({ ok: true });
}

/** Base64-encode a UTF-8 string (Workers-safe, no Buffer). */
function base64Utf8(str: string): string {
  const bytes = new TextEncoder().encode(str);
  let bin = '';
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
}

/** RFC 2047 encode a header value only when it contains non-ASCII characters. */
function encodeHeaderValue(value: string): string {
  return /[^\x20-\x7E]/.test(value) ? `=?utf-8?B?${base64Utf8(value)}?=` : value;
}

/**
 * Build a minimal RFC 5322 message (CRLF line endings, base64 text body) for
 * the Cloudflare Email send binding. A Message-ID whose domain matches the
 * sender is required by Cloudflare Email Routing.
 */
function buildMimeMessage(opts: {
  from: string;
  to: string;
  subject: string;
  text: string;
  replyTo?: string;
}): string {
  const domain = opts.from.split('@')[1] || 'localhost';
  const bodyB64 = base64Utf8(opts.text).replace(/(.{76})/g, '$1\r\n');
  const headers = [`From: ${opts.from}`, `To: ${opts.to}`];
  if (opts.replyTo) headers.push(`Reply-To: ${opts.replyTo}`);
  headers.push(
    `Subject: ${encodeHeaderValue(opts.subject)}`,
    `Message-ID: <${crypto.randomUUID()}@${domain}>`,
    `Date: ${new Date().toUTCString()}`,
    `MIME-Version: 1.0`,
    `Content-Type: text/plain; charset=utf-8`,
    `Content-Transfer-Encoding: base64`,
    ``,
    bodyB64,
  );
  return headers.join('\r\n');
}

/**
 * Notify the maintainer of a new feedback report (delivered to FEEDBACK_TO).
 * When the submitter left an email, it is set as Reply-To so a reply reaches
 * them directly.
 */
async function sendFeedbackNotification(env: Env, record: FeedbackRecord): Promise<void> {
  const from = env.FEEDBACK_FROM!;
  const to = env.FEEDBACK_TO!;
  const subject = record.title.replace(/[\r\n]+/g, ' ').slice(0, 200) || '[Report]';
  const text =
    `${record.body}\n\n` +
    `— — —\n` +
    `Contact email: ${record.email ?? '(none provided)'}\n` +
    `Received: ${record.receivedAt}\n` +
    `User-Agent: ${record.userAgent ?? 'n/a'}\n` +
    `Country: ${record.country ?? 'n/a'}`;
  const raw = buildMimeMessage({ from, to, subject, text, replyTo: record.email ?? undefined });
  await env.SEND_EMAIL!.send(new EmailMessage(from, to, raw));
}

/**
 * Send an automated "we got your feedback" confirmation to the submitter.
 * Sent from FEEDBACK_FROM as an unmonitored, no-reply address.
 */
async function sendFeedbackConfirmation(env: Env, record: FeedbackRecord): Promise<void> {
  const from = env.FEEDBACK_FROM!;
  const to = record.email!;
  const subject = 'We got your feedback — Can I Fish This';
  const text =
    `Hi,\n\n` +
    `Thanks for sending feedback on Can I Fish This. We received your report and a ` +
    `maintainer will take a look.\n\n` +
    `For your reference, here's what you submitted:\n\n` +
    `${record.title}\n${record.body}\n\n` +
    `— — —\n` +
    `This is an automated confirmation from an unmonitored address — please don't ` +
    `reply. Spotted something else? Just send another report from the app.\n\n` +
    `Can I Fish This\nhttps://canifishthis.ca`;
  const raw = buildMimeMessage({ from, to, subject, text });
  await env.SEND_EMAIL!.send(new EmailMessage(from, to, raw));
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    // ── CORS preflight ──────────────────────────────────────────────
    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);

    // ── Feedback submission (POST) — handled before the GET/HEAD guard ─
    if (url.pathname === '/api/feedback' && request.method === 'POST') {
      return handleFeedback(request, env, ctx);
    }

    if (request.method !== 'GET' && request.method !== 'HEAD') {
      return new Response('Method Not Allowed', { status: 405, headers: CORS_HEADERS });
    }

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

    // Stored feedback reports are private — never serve them over GET.
    if (key.startsWith('feedback/')) {
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
