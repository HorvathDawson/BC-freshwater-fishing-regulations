var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/index.ts
var CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
  "Access-Control-Allow-Headers": "Range, If-None-Match, If-Modified-Since",
  "Access-Control-Expose-Headers": "Content-Length, Content-Range, Content-Type, Content-Encoding, ETag",
  "Access-Control-Max-Age": "86400"
};
var CACHE_PMTILES = "public, max-age=604800, stale-while-revalidate=86400";
var CACHE_JSON = "public, max-age=300, stale-while-revalidate=60";
function getCacheControl(key) {
  if (key === "data_version.json") return "no-store";
  if (key.endsWith(".pmtiles")) return CACHE_PMTILES;
  if (key.endsWith(".json")) return CACHE_JSON;
  return "public, max-age=3600";
}
__name(getCacheControl, "getCacheControl");
function isNoCacheFile(key) {
  return key === "data_version.json";
}
__name(isNoCacheFile, "isNoCacheFile");
var EDGE_CACHE_VER = "3";
function getContentType(key) {
  if (key.endsWith(".pmtiles")) return "application/octet-stream";
  if (key.endsWith(".json")) return "application/json; charset=utf-8";
  return null;
}
__name(getContentType, "getContentType");
function buildR2Response(object, key, rangeHeader) {
  const headers = new Headers(CORS_HEADERS);
  object.writeHttpMetadata(headers);
  headers.set("ETag", object.httpEtag);
  headers.set("Cache-Control", getCacheControl(key));
  const ct = getContentType(key);
  if (ct) headers.set("Content-Type", ct);
  if (!("body" in object) || !object.body) {
    return new Response(null, { status: 200, headers });
  }
  if (rangeHeader) {
    const offset = object.range?.offset ?? 0;
    const length = object.range?.length ?? object.size;
    headers.set("Content-Range", `bytes ${offset}-${offset + length - 1}/${object.size}`);
    return new Response(object.body, { status: 206, headers });
  }
  return new Response(object.body, { status: 200, headers });
}
__name(buildR2Response, "buildR2Response");
var MAX_BATCH_FIDS = 20;
var MAX_BATCH_WBKS = 20;
var SHARD_CACHE_TTL = 86400;
var FID_PATTERN = /^\d{1,12}$/;
var WBK_PATTERN = /^\d{1,12}$/;
async function shardPrefix(id) {
  const data = new TextEncoder().encode(id);
  const hashBuffer = await crypto.subtle.digest("SHA-256", data);
  const hashArray = new Uint8Array(hashBuffer);
  const hex = Array.from(hashArray.slice(0, 2)).map((b) => b.toString(16).padStart(2, "0")).join("");
  return hex.slice(0, 3);
}
__name(shardPrefix, "shardPrefix");
async function groupByPrefix(ids) {
  const groups = /* @__PURE__ */ new Map();
  for (const id of ids) {
    const prefix = await shardPrefix(id);
    const arr = groups.get(prefix);
    if (arr) arr.push(id);
    else groups.set(prefix, [id]);
  }
  return groups;
}
__name(groupByPrefix, "groupByPrefix");
async function fetchShard(r2, objectKey, requestUrl) {
  const origin = new URL(requestUrl).origin;
  const cacheKey = new Request(`${origin}/_r2_cache/${objectKey}`);
  const cache = caches.default;
  const cached = await cache.match(cacheKey);
  if (cached) return cached.json();
  const obj = await r2.get(objectKey);
  if (!obj) return null;
  const body = await obj.text();
  const cacheResponse = new Response(body, {
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": `public, max-age=${SHARD_CACHE_TTL}`
    }
  });
  cache.put(cacheKey, cacheResponse.clone()).catch(() => {
  });
  return JSON.parse(body);
}
__name(fetchShard, "fetchShard");
function parseIds(param, pattern, max) {
  if (!param) return [];
  const ids = param.split(",").filter(Boolean);
  if (ids.length > max) return null;
  for (const id of ids) {
    if (!pattern.test(id)) return null;
  }
  return ids;
}
__name(parseIds, "parseIds");
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "public, max-age=3600",
      ...CORS_HEADERS
    }
  });
}
__name(jsonResponse, "jsonResponse");
function errorResponse(error, status) {
  return new Response(JSON.stringify({ error }), {
    status,
    headers: { "Content-Type": "application/json", ...CORS_HEADERS }
  });
}
__name(errorResponse, "errorResponse");
async function handleResolve(request, env) {
  const url = new URL(request.url);
  const version = env.SHARD_VERSION || "v7";
  const fids = parseIds(url.searchParams.get("fids"), FID_PATTERN, MAX_BATCH_FIDS);
  if (fids === null) {
    return errorResponse(`Invalid fids: must be numeric, max ${MAX_BATCH_FIDS} per request`, 400);
  }
  const wbks = parseIds(url.searchParams.get("wbks"), WBK_PATTERN, MAX_BATCH_WBKS);
  if (wbks === null) {
    return errorResponse(`Invalid wbks: must be numeric, max ${MAX_BATCH_WBKS} per request`, 400);
  }
  if (fids.length === 0 && wbks.length === 0) {
    return errorResponse("Provide ?fids= and/or ?wbks=", 400);
  }
  const results = [];
  const seenReachIds = /* @__PURE__ */ new Set();
  const reachIdsToFetch = /* @__PURE__ */ new Map();
  const reachSources = /* @__PURE__ */ new Map();
  if (fids.length > 0) {
    const fidGroups = await groupByPrefix(fids);
    for (const [prefix, groupFids] of fidGroups) {
      const shard = await fetchShard(
        env.BUCKET,
        `shards/${version}/fids/${prefix}.json`,
        request.url
      );
      if (!shard) continue;
      for (const fid of groupFids) {
        const reachId = shard[fid];
        if (reachId && !seenReachIds.has(reachId)) {
          seenReachIds.add(reachId);
          reachIdsToFetch.set(reachId, "fid");
          reachSources.set(reachId, { fids: [fid], wbks: [] });
        } else if (reachId && reachSources.has(reachId)) {
          reachSources.get(reachId).fids.push(fid);
        }
      }
    }
  }
  if (wbks.length > 0) {
    const wbkGroups = await groupByPrefix(wbks);
    for (const [prefix, groupWbks] of wbkGroups) {
      const shard = await fetchShard(
        env.BUCKET,
        `shards/${version}/polys/${prefix}.json`,
        request.url
      );
      if (!shard) continue;
      for (const wbk of groupWbks) {
        const reachId = shard[wbk];
        if (reachId && !seenReachIds.has(reachId)) {
          seenReachIds.add(reachId);
          reachIdsToFetch.set(reachId, "wbk");
          const src = reachSources.get(reachId) || { fids: [], wbks: [] };
          src.wbks.push(wbk);
          reachSources.set(reachId, src);
        } else if (reachId && reachSources.has(reachId)) {
          reachSources.get(reachId).wbks.push(wbk);
        }
      }
    }
  }
  if (reachIdsToFetch.size > 0) {
    const reachGroups = await groupByPrefix([...reachIdsToFetch.keys()]);
    for (const [prefix, groupReachIds] of reachGroups) {
      const shard = await fetchShard(
        env.BUCKET,
        `shards/${version}/reaches/${prefix}.json`,
        request.url
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
__name(handleResolve, "handleResolve");
async function handleVersion(env) {
  return jsonResponse({ version: env.SHARD_VERSION || "v7" });
}
__name(handleVersion, "handleVersion");
var src_default = {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("Method Not Allowed", { status: 405, headers: CORS_HEADERS });
    }
    const url = new URL(request.url);
    if (url.pathname === "/api/resolve" && request.method === "GET") {
      return handleResolve(request, env);
    }
    if (url.pathname === "/api/version" && request.method === "GET") {
      return handleVersion(env);
    }
    const key = url.pathname.slice(1);
    if (!key) {
      return new Response("Not Found", { status: 404, headers: CORS_HEADERS });
    }
    const useEdgeCache = !isNoCacheFile(key);
    const cache = useEdgeCache ? caches.default : null;
    let cacheKey;
    const rangeHeader = request.headers.get("Range");
    if (useEdgeCache) {
      const rkUrl = new URL(request.url);
      rkUrl.searchParams.set("_cv", EDGE_CACHE_VER);
      if (rangeHeader) {
        rkUrl.searchParams.set("_r", rangeHeader);
      }
      cacheKey = new Request(rkUrl.toString(), { method: "GET" });
    }
    if (cache) {
      const cached = await cache.match(cacheKey);
      if (cached) {
        const resp = new Response(cached.body, cached);
        for (const [k, v] of Object.entries(CORS_HEADERS)) {
          resp.headers.set(k, v);
        }
        return resp;
      }
    }
    const ifNoneMatch = request.headers.get("If-None-Match");
    if (ifNoneMatch && !rangeHeader) {
      const meta = await env.BUCKET.head(key);
      if (meta && meta.httpEtag === ifNoneMatch) {
        const headers = new Headers(CORS_HEADERS);
        headers.set("ETag", meta.httpEtag);
        headers.set("Cache-Control", getCacheControl(key));
        const ct = getContentType(key);
        if (ct) headers.set("Content-Type", ct);
        return new Response(null, { status: 304, headers });
      }
    }
    if (request.method === "HEAD") {
      const meta = await env.BUCKET.head(key);
      if (!meta) {
        return new Response("Not Found", { status: 404, headers: CORS_HEADERS });
      }
      const headers = new Headers(CORS_HEADERS);
      meta.writeHttpMetadata(headers);
      headers.set("ETag", meta.httpEtag);
      headers.set("Cache-Control", getCacheControl(key));
      headers.set("Content-Length", String(meta.size));
      const ct = getContentType(key);
      if (ct) headers.set("Content-Type", ct);
      return new Response(null, { status: 200, headers });
    }
    let object;
    if (rangeHeader) {
      const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
      if (match) {
        const start = parseInt(match[1]);
        const end = match[2] ? parseInt(match[2]) : void 0;
        object = await env.BUCKET.get(key, {
          range: { offset: start, length: end !== void 0 ? end - start + 1 : void 0 }
        });
      } else {
        object = await env.BUCKET.get(key);
      }
    } else {
      object = await env.BUCKET.get(key);
    }
    if (!object) {
      return new Response("Not Found", { status: 404, headers: CORS_HEADERS });
    }
    const response = buildR2Response(object, key, rangeHeader);
    if (cache) {
      const status = response.status;
      if (status === 200 || status === 206) {
        const cloned = response.clone();
        cache.put(cacheKey, cloned).catch(() => {
        });
      }
    }
    return response;
  }
};

// node_modules/wrangler/templates/middleware/middleware-ensure-req-body-drained.ts
var drainBody = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } finally {
    try {
      if (request.body !== null && !request.bodyUsed) {
        const reader = request.body.getReader();
        while (!(await reader.read()).done) {
        }
      }
    } catch (e) {
      console.error("Failed to drain the unused request body.", e);
    }
  }
}, "drainBody");
var middleware_ensure_req_body_drained_default = drainBody;

// node_modules/wrangler/templates/middleware/middleware-miniflare3-json-error.ts
function reduceError(e) {
  return {
    name: e?.name,
    message: e?.message ?? String(e),
    stack: e?.stack,
    cause: e?.cause === void 0 ? void 0 : reduceError(e.cause)
  };
}
__name(reduceError, "reduceError");
var jsonError = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } catch (e) {
    const error = reduceError(e);
    return Response.json(error, {
      status: 500,
      headers: { "MF-Experimental-Error-Stack": "true" }
    });
  }
}, "jsonError");
var middleware_miniflare3_json_error_default = jsonError;

// .wrangler/tmp/bundle-5Lrfd1/middleware-insertion-facade.js
var __INTERNAL_WRANGLER_MIDDLEWARE__ = [
  middleware_ensure_req_body_drained_default,
  middleware_miniflare3_json_error_default
];
var middleware_insertion_facade_default = src_default;

// node_modules/wrangler/templates/middleware/common.ts
var __facade_middleware__ = [];
function __facade_register__(...args) {
  __facade_middleware__.push(...args.flat());
}
__name(__facade_register__, "__facade_register__");
function __facade_invokeChain__(request, env, ctx, dispatch, middlewareChain) {
  const [head, ...tail] = middlewareChain;
  const middlewareCtx = {
    dispatch,
    next(newRequest, newEnv) {
      return __facade_invokeChain__(newRequest, newEnv, ctx, dispatch, tail);
    }
  };
  return head(request, env, ctx, middlewareCtx);
}
__name(__facade_invokeChain__, "__facade_invokeChain__");
function __facade_invoke__(request, env, ctx, dispatch, finalMiddleware) {
  return __facade_invokeChain__(request, env, ctx, dispatch, [
    ...__facade_middleware__,
    finalMiddleware
  ]);
}
__name(__facade_invoke__, "__facade_invoke__");

// .wrangler/tmp/bundle-5Lrfd1/middleware-loader.entry.ts
var __Facade_ScheduledController__ = class ___Facade_ScheduledController__ {
  constructor(scheduledTime, cron, noRetry) {
    this.scheduledTime = scheduledTime;
    this.cron = cron;
    this.#noRetry = noRetry;
  }
  static {
    __name(this, "__Facade_ScheduledController__");
  }
  #noRetry;
  noRetry() {
    if (!(this instanceof ___Facade_ScheduledController__)) {
      throw new TypeError("Illegal invocation");
    }
    this.#noRetry();
  }
};
function wrapExportedHandler(worker) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return worker;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  const fetchDispatcher = /* @__PURE__ */ __name(function(request, env, ctx) {
    if (worker.fetch === void 0) {
      throw new Error("Handler does not export a fetch() function.");
    }
    return worker.fetch(request, env, ctx);
  }, "fetchDispatcher");
  return {
    ...worker,
    fetch(request, env, ctx) {
      const dispatcher = /* @__PURE__ */ __name(function(type, init) {
        if (type === "scheduled" && worker.scheduled !== void 0) {
          const controller = new __Facade_ScheduledController__(
            Date.now(),
            init.cron ?? "",
            () => {
            }
          );
          return worker.scheduled(controller, env, ctx);
        }
      }, "dispatcher");
      return __facade_invoke__(request, env, ctx, dispatcher, fetchDispatcher);
    }
  };
}
__name(wrapExportedHandler, "wrapExportedHandler");
function wrapWorkerEntrypoint(klass) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return klass;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  return class extends klass {
    #fetchDispatcher = /* @__PURE__ */ __name((request, env, ctx) => {
      this.env = env;
      this.ctx = ctx;
      if (super.fetch === void 0) {
        throw new Error("Entrypoint class does not define a fetch() function.");
      }
      return super.fetch(request);
    }, "#fetchDispatcher");
    #dispatcher = /* @__PURE__ */ __name((type, init) => {
      if (type === "scheduled" && super.scheduled !== void 0) {
        const controller = new __Facade_ScheduledController__(
          Date.now(),
          init.cron ?? "",
          () => {
          }
        );
        return super.scheduled(controller);
      }
    }, "#dispatcher");
    fetch(request) {
      return __facade_invoke__(
        request,
        this.env,
        this.ctx,
        this.#dispatcher,
        this.#fetchDispatcher
      );
    }
  };
}
__name(wrapWorkerEntrypoint, "wrapWorkerEntrypoint");
var WRAPPED_ENTRY;
if (typeof middleware_insertion_facade_default === "object") {
  WRAPPED_ENTRY = wrapExportedHandler(middleware_insertion_facade_default);
} else if (typeof middleware_insertion_facade_default === "function") {
  WRAPPED_ENTRY = wrapWorkerEntrypoint(middleware_insertion_facade_default);
}
var middleware_loader_entry_default = WRAPPED_ENTRY;
export {
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=index.js.map
