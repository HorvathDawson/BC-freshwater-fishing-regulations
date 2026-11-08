/**
 * (T404) ONE SWITCH: no staging or production host is spelt in app code outside the modules that own them, so moving the
 * app to production is `EXPO_PUBLIC_APP_ENV=production` and nothing else.
 *
 * A host is any `*.canifishthis.ca` / `canifishthis.com` name, a `*.workers.dev` name, or a `bc-fishing-*` worker or
 * bucket name. An email address (`support@canifishthis.ca`) is not a host. The owners:
 *   apps/mobile/src/env.ts    every environment's data origin and report/crash endpoint (`ORIGINS`, `REPORT_URLS`)
 *   packages/core/src/share.ts  the public site (`SITE_ORIGIN`): share links and the privacy page, one for every environment
 * Walked: app/apps and app/packages, .ts/.tsx/.js/.mjs/.cjs. Excepted, exactly: test files (`*.test.*`, `test/` and
 * `__tests__/` folders, `*.d.ts` is NOT excepted), docs (not walked: .md), generated native projects (`android/`, `ios/`),
 * `node_modules/`, `dist/`, and dot-folders. Tools (app/tools), the workers (app/workers) and deploy/ are not app code;
 * their hosts are listed in the T404 ticket's audit.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const OWNERS = new Set(["apps/mobile/src/env.ts", "packages/core/src/share.ts"]);
const SKIP_DIRS = new Set(["node_modules", "dist", "android", "ios", "test", "__tests__"]);

/** a host spelt in the text, or null (an email address is not one) */
function envHost(text: string): string | null {
  const m = /(?<![@\w.-])(?:[a-z0-9-]+\.)*canifishthis\.(?:ca|com)\b|(?:[a-z0-9-]+\.)+workers\.dev\b|\bbc-fishing-[a-z0-9-]+/i.exec(text);
  return m ? m[0] : null;
}

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (SKIP_DIRS.has(f) || f.startsWith(".")) continue;
    const p = join(dir, f);
    let isDir: boolean;
    // a file another worker's test deleted mid-walk is not a source (no-localhost.test.ts, T80 7); any other error throws
    try { isDir = statSync(p).isDirectory(); } catch (e) { if ((e as { code?: string }).code === "ENOENT") continue; throw e; }
    if (isDir) sources(p, out);
    else if (/\.(ts|tsx|js|mjs|cjs)$/.test(f) && !/\.test\.[cm]?[jt]sx?$/.test(f)) out.push(p);
  }
  return out;
}

describe("one switch (T404): environment hosts live only in their owning modules", () => {
  it("no app source outside env.ts / share.ts spells a staging or production host", () => {
    const hits = [...sources(join(APP, "apps")), ...sources(join(APP, "packages"))]
      .map((p) => [relative(APP, p), p] as const)
      .filter(([r]) => !OWNERS.has(r))
      .map(([r, p]) => { const h = envHost(readFileSync(p, "utf8")); return h ? `${r}: ${h}` : null; })
      .filter(Boolean);
    expect(hits).toEqual([]);
  });

  it("the owners do spell them (the walk reads the right tree)", () => {
    expect([...OWNERS].map((r) => envHost(readFileSync(join(APP, r), "utf8")) !== null)).toEqual([true, true]);
  });

  it("knows a host from an email address", () => {
    expect(["https://data-staging.canifishthis.ca", "data.canifishthis.ca/x", "www.canifishthis.com",
            "feeds.canifishthis.ca", "bucket bc-fishing-regulations-staging"].map(envHost))
      .toEqual(["data-staging.canifishthis.ca", "data.canifishthis.ca", "www.canifishthis.com",
                "feeds.canifishthis.ca", "bc-fishing-regulations-staging"]);
    expect(["support@canifishthis.ca", "privacy@canifishthis.ca", "canifishthis://water/1"].map(envHost))
      .toEqual([null, null, null]);
  });
});
