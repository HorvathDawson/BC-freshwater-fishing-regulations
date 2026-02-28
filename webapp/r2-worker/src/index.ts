interface Env {
  BUCKET: R2Bucket;
}

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
  'Access-Control-Allow-Headers': 'Range, If-None-Match',
  'Access-Control-Expose-Headers': 'Content-Length, Content-Range, Content-Type, ETag',
  'Access-Control-Max-Age': '86400',
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Handle CORS preflight
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

    // Support Range requests (critical for PMTiles)
    const rangeHeader = request.headers.get('Range');
    let object: R2ObjectBody | R2Object | null;

    if (rangeHeader) {
      // Parse Range header: "bytes=start-end"
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
    headers.set('Cache-Control', 'public, max-age=86400');

    // Set Content-Type for known extensions
    if (key.endsWith('.pmtiles')) headers.set('Content-Type', 'application/octet-stream');
    else if (key.endsWith('.json')) headers.set('Content-Type', 'application/json');

    if ('body' in object && object.body) {
      if (rangeHeader) {
        headers.set('Content-Range', `bytes ${(object as any).range?.offset ?? 0}-${((object as any).range?.offset ?? 0) + ((object as any).range?.length ?? object.size) - 1}/${object.size}`);
        return new Response(object.body, { status: 206, headers });
      }
      return new Response(object.body, { headers });
    }

    return new Response(null, { status: 304, headers });
  },
};
