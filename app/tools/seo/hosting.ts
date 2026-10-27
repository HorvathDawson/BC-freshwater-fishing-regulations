/**
 * T198 (code half) — HOSTING CONFIG, generated with the pages so it cannot drift from them:
 *
 *   - canifishthis.com, www.canifishthis.com and www.canifishthis.ca -> 301 to https://canifishthis.ca, path and query kept;
 *     http -> https; one trailing-slash form (none);
 *   - a removed water's slug -> 301 to its region page (never a soft 404); an unknown path gets the real 404.html (a 404);
 *   - HSTS on every response, nosniff, a referrer policy; hashed assets cached for a year, HTML for five minutes with
 *     revalidation (a release is a new set of pages).
 *
 * The host rules cannot live in a path-based `_redirects` file, so they are emitted as `redirects.json` for the edge
 * (Cloudflare Redirect Rules / a Worker / the registrar's forwarding) and mirrored by `redirectFor`, the pure function the
 * test (and a `curl -I` check after deploy) holds the edge to. Owner steps (registrar/DNS, accounts) stay in OWNER-TODO.
 */
import type { Site } from "./html";

/** one year; `preload` is left out on purpose (a preload-list submission is hard to undo: the owner's call) */
export const HSTS = "max-age=31536000; includeSubDomains";

export interface Removed { readonly slug: string; readonly region: number }

/** The URL a request should be redirected to, or null when it is served as is. */
export function redirectFor(url: string, site: Site, removed: readonly Removed[]): { status: 301; to: string } | null {
  const u = new URL(url);
  const apex = new URL(site.origin).hostname;
  const canonicalHost = u.hostname === apex;
  let path = u.pathname;
  if (path.length > 1 && path.endsWith("/")) path = path.slice(0, -1);
  const gone = /^\/water\/([^/]+)$/.exec(path);
  const away = gone ? removed.find((r) => r.slug === gone[1]) : undefined;
  if (away) path = `/region/${away.region}`;
  const to = `${site.origin}${path}${u.search}`;
  const same = canonicalHost && u.protocol === "https:" && path === u.pathname;
  return same ? null : { status: 301, to };
}

/** Host rules for the edge: every other name for the site goes to the canonical one. */
export function hostRules(site: Site, alsoHosts: readonly string[]): object {
  const apex = new URL(site.origin).hostname;
  const hosts = [...alsoHosts.flatMap((h) => [h, `www.${h}`]), `www.${apex}`];
  return {
    canonical: site.origin, hsts: HSTS,
    redirects: hosts.map((from) => ({ from_host: from, to: `${site.origin}{path}{query}`, status: 301 })),
    httpToHttps: { status: 301 },
    notes: "Path and query are kept. http://canifishthis.ca -> https. Trailing slashes are removed by the host (one form).",
  };
}

const HTML_CACHE = "Cache-Control: public, max-age=300, must-revalidate";

/** `_headers` (Cloudflare Pages / Netlify syntax). Specific groups, so a rule never merges with a broader one. */
export function headersFile(): string {
  const all = [`Strict-Transport-Security: ${HSTS}`, "X-Content-Type-Options: nosniff", "Referrer-Policy: strict-origin-when-cross-origin"];
  const block = (path: string, ...lines: string[]) => `${path}\n${lines.map((l) => `  ${l}`).join("\n")}\n`;
  return [
    block("/*", ...all),
    block("/assets/*", "Cache-Control: public, max-age=31536000, immutable"),
    block("/share/*", "Cache-Control: public, max-age=86400"),
    ...["/water/*", "/region/*", "/about", "/changes", "/sitemap.xml", "/sitemap-*.xml", "/robots.txt"].map((p) => block(p, HTML_CACHE)),
  ].join("\n");
}

/** `_redirects` for the removed waters: a 301 to the region page. */
export function redirectsFile(removed: readonly (Removed & { readonly region: number })[]): string {
  return removed.map((r) => `/water/${r.slug} /region/${r.region} 301`).join("\n") + (removed.length ? "\n" : "");
}
