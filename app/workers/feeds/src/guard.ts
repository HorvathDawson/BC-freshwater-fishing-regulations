// T249: the feeds Worker's front door — a per-IP rate limit on every route (Workers Rate Limiting bindings,
// [[ratelimits]] in wrangler.toml) and a constant-time check of the run token.
//
//   RL_PUBLIC   GET /status, /status.json and anything unknown (404s): 60 a minute per IP
//   RL_AUTH     /run/*, /notify/*, /reports (the token routes): 10 a minute per IP — checked BEFORE the token, so guessing
//               it is 10 tries a minute per address
//   /report     (T237) has its own, stricter REPORT_LIMITER + a per-IP daily cap in src/reports.ts; RL_PUBLIC still applies
//
// The bindings are per Cloudflare location and eventually consistent (Cloudflare's design: a flood gets 429s, an exact
// count is not promised). A binding that is absent (tests, `wrangler dev` without it) or that throws lets the request
// through: the limiter must never take the feeds down.

export interface RateLimiter {
  limit(o: { key: string }): Promise<{ success: boolean }>;
}

export type RouteClass = "public" | "auth";

/** which limiter a path answers to */
export function routeClass(pathname: string): RouteClass {
  return /^\/(run|notify)\//.test(pathname) || pathname === "/reports" ? "auth" : "public";
}

/** a 429 when `rl` says this IP is over its limit for `cls`, else null (go on) */
export async function limited(rl: RateLimiter | undefined, req: Request, cls: RouteClass): Promise<Response | null> {
  if (!rl) return null;
  const ip = req.headers.get("CF-Connecting-IP") ?? req.headers.get("X-Forwarded-For")?.split(",")[0]?.trim() ?? "unknown";
  try {
    const { success } = await rl.limit({ key: `${cls}:${ip}` });
    if (success) return null;
  } catch {
    return null; // fail open
  }
  return new Response("Too Many Requests\n", {
    status: 429,
    headers: { "Retry-After": "60", "Cache-Control": "no-store", "Content-Type": "text/plain; charset=utf-8", "Access-Control-Allow-Origin": "*" },
  });
}

/** compare two secrets in time that does not depend on where they first differ */
export function sameSecret(a: string, b: string): boolean {
  const x = new TextEncoder().encode(a);
  const y = new TextEncoder().encode(b);
  let d = x.length ^ y.length;
  for (let i = 0; i < Math.max(x.length, y.length); i++) d |= (x[i] ?? 0) ^ (y[i] ?? 0);
  return d === 0;
}

/** `Authorization: Bearer <RUN_TOKEN>`; never true while RUN_TOKEN is unset (deploy/staging/rotate-run-token.sh sets a
 *  48-hex-character one) */
export function bearerOk(req: Request, token: string | undefined): boolean {
  if (!token) return false;
  const h = req.headers.get("Authorization") ?? "";
  return h.startsWith("Bearer ") && sameSecret(h.slice(7), token);
}
