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
import { APP_PACKAGE, PLAY_SIGNING_SHA256 } from "@app/core";
import type { Site } from "./html";

/** one year; `preload` is left out on purpose (a preload-list submission is hard to undo: the owner's call) */
export const HSTS = "max-age=31536000; includeSubDomains";

export interface Removed { readonly slug: string; readonly region: number }

/**
 * (T605, owner 2026-11-10) THE PAGES THAT WERE ABOUT "TODAY" ARE GONE: "what is open this weekend" and "waters opening and
 * closing soon" per region said open / closed words that were right only on the day the site was built. Their addresses
 * (still indexed) go with one 301 to the region page, never a 404, and they are in no sitemap.
 */
export const REMOVED_PAGES = ["open-this-weekend", "season-changes"] as const;
/** "/region/3/season-changes" -> "/region/3", for each region the site has */
export function removedPageMap(regions: readonly number[]): Record<string, string> {
  return Object.fromEntries(regions.flatMap((r) => REMOVED_PAGES.map((p) => [`/region/${r}/${p}`, `/region/${r}`] as const)));
}

/** The URL a request should be redirected to, or null when it is served as is. */
export function redirectFor(url: string, site: Site, removed: readonly Removed[], regions: readonly number[] = []): { status: 301; to: string } | null {
  const u = new URL(url);
  const apex = new URL(site.origin).hostname;
  const canonicalHost = u.hostname === apex;
  let path = u.pathname;
  if (path.length > 1 && path.endsWith("/")) path = path.slice(0, -1);
  const gone = /^\/water\/([^/]+)$/.exec(path);
  const away = gone ? removed.find((r) => r.slug === gone[1]) : undefined;
  if (away) path = `/region/${away.region}`;
  const page = removedPageMap(regions)[path];
  if (page !== undefined) path = page;
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

/**
 * (T625) `/.well-known/assetlinks.json`: the Digital Asset Links statement that lets Android open the installed app on this
 * site's `/water/…` links (the app's intent filter, `apps/mobile/app.json`, is `autoVerify`). It names the app's package and
 * the SHA-256 of the key Play signs an install with (`@app/core` `PLAY_SIGNING_SHA256`). Staging and production are the same
 * build, so both serve this same text from their own host: Android fetches it from each host the filter names.
 */
export function assetLinksFile(): string {
  return `${JSON.stringify([{
    relation: ["delegate_permission/common.handle_all_urls"],
    target: { namespace: "android_app", package_name: APP_PACKAGE, sha256_cert_fingerprints: [PLAY_SIGNING_SHA256] },
  }], null, 2)}\n`;
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
    block("/.well-known/assetlinks.json", "Content-Type: application/json", "Cache-Control: public, max-age=3600"),
    ...["/water/*", "/region/*", "/gauge/*", "/gauges", "/about", "/privacy", "/changes", "/sitemap.xml", "/sitemap-*.xml", "/robots.txt"].map((p) => block(p, HTML_CACHE)),
  ].join("\n");
}

/** `_redirects` for the removed waters: a 301 to the region page. */
export function redirectsFile(removed: readonly (Removed & { readonly region: number })[], regions: readonly number[] = []): string {
  const lines = [...removed.map((r) => `/water/${r.slug} /region/${r.region} 301`),
                 ...Object.entries(removedPageMap(regions)).map(([from, to]) => `${from} ${to} 301`)];
  return lines.join("\n") + (lines.length ? "\n" : "");
}

/**
 * The worker for every OTHER name of the site (`hostRules`: canifishthis.com, www.canifishthis.com, www.canifishthis.ca):
 * ONE 301 to the canonical origin with the path (no trailing slash) and query kept — a removed water straight to its region,
 * so no request takes two hops — and HSTS on the answer. It answers exactly as `redirectFor` (a test holds it to that).
 * The owner deploys it on those hosts' routes (deploy/prod/host.wrangler.toml; DNS is the owner's).
 */
export function hostWorker(site: Site, removed: readonly Removed[], regions: readonly number[] = []): string {
  const gone = { ...Object.fromEntries(removed.map((r) => [`/water/${r.slug}`, `/region/${r.region}`])), ...removedPageMap(regions) };
  return `// generated by tools/seo (hosting.ts): every other host name -> one 301 to ${site.origin}, path and query kept. Do not edit.
const ORIGIN = ${JSON.stringify(site.origin)};
const GONE = ${JSON.stringify(gone)};
export default {
  fetch(request) {
    const u = new URL(request.url);
    let path = u.pathname.length > 1 && u.pathname.endsWith("/") ? u.pathname.slice(0, -1) : u.pathname;
    if (Object.prototype.hasOwnProperty.call(GONE, path)) path = GONE[path];
    return new Response(null, { status: 301, headers: { location: ORIGIN + path + u.search,
      "strict-transport-security": ${JSON.stringify(HSTS)}, "cache-control": "public, max-age=86400" } });
  },
};
`;
}
