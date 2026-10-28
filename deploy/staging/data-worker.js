// STAGING data worker (bc-fishing-r2-staging -> data-staging.canifishthis.ca). Serves the staging bucket as stored:
// GET/HEAD/OPTIONS, Range, CORS, and each object's own Content-Type / Content-Encoding / Cache-Control (publish.py sets
// them). A gzip-stored release file is passed through with encodeBody "manual": the v1 worker (archive/r2-worker) let the
// runtime gzip it a second time, so the app's sha256 check of the decoded bytes failed (seen 2026-10-28). Production's
// data worker has the same defect: fix it the same way before a release goes to prod (T86).
const CORS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
  "Access-Control-Allow-Headers": "Range, If-None-Match, If-Modified-Since, Content-Type",
  "Access-Control-Expose-Headers": "Content-Length, Content-Range, Content-Type, Content-Encoding, ETag",
  "Access-Control-Max-Age": "86400",
};
export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    if (request.method !== "GET" && request.method !== "HEAD") return new Response("Method Not Allowed", { status: 405, headers: CORS });
    const key = decodeURIComponent(new URL(request.url).pathname.slice(1));
    if (!key || key.split("/").includes("..")) return new Response("Not Found", { status: 404, headers: CORS });
    // T225: the feeds worker keeps its state (notified keys, runs archive, roster, DFO entries + page corpus) under
    // private/ in the same bucket; never serve it.
    if (key.startsWith("private/")) return new Response("Not Found", { status: 404, headers: CORS });
    const range = request.headers.get("Range");
    let opts = {};
    const m = range && /^bytes=(\d+)-(\d*)$/.exec(range);
    if (m) opts = { range: { offset: Number(m[1]), ...(m[2] ? { length: Number(m[2]) - Number(m[1]) + 1 } : {}) } };
    const obj = request.method === "HEAD" ? await env.BUCKET.head(key) : await env.BUCKET.get(key, opts);
    if (!obj) return new Response("Not Found", { status: 404, headers: CORS });
    const headers = new Headers(CORS);
    obj.writeHttpMetadata(headers);
    headers.set("ETag", obj.httpEtag);
    if (!headers.has("Cache-Control")) headers.set("Cache-Control", "public, max-age=300");
    if (key.endsWith(".wasm")) headers.set("Content-Type", "application/wasm");
    const inm = request.headers.get("If-None-Match");
    if (inm && !range && inm === obj.httpEtag) return new Response(null, { status: 304, headers });
    if (request.method === "HEAD") { headers.set("Content-Length", String(obj.size)); return new Response(null, { status: 200, headers }); }
    let status = 200;
    if (m && obj.range) {
      const off = obj.range.offset ?? 0, len = obj.range.length ?? obj.size - off;
      headers.set("Content-Range", `bytes ${off}-${off + len - 1}/${obj.size}`);
      status = 206;
    }
    headers.set("Accept-Ranges", "bytes");
    return new Response(obj.body, { status, headers, encodeBody: headers.has("Content-Encoding") ? "manual" : "automatic" });
  },
};
