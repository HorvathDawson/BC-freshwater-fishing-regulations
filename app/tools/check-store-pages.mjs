#!/usr/bin/env node
/**
 * T901 — the store buttons' switches match the public pages. Run by hand (it needs the network; not in check:fast):
 *   pnpm run store-pages
 * Curls the Play and App Store pages (a browser User-Agent) and compares each with its flag in packages/core/src/storeLinks.ts.
 * FAILS (exit 1) when a flag is true while its page is not 200; REPORTS (exit 0) a page that went live while its flag is
 * still false: flip that flag. At launch both flags go true.
 */
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const SRC = new URL("../packages/core/src/storeLinks.ts", import.meta.url);

/** pure: `rows` = [{ name, flag, status, url }] -> { failures, flips } */
export function judge(rows) {
  const failures = rows.filter((r) => r.flag && r.status !== 200)
    .map((r) => `${r.name}: ${r.flag ? "flag is true" : ""} but ${r.url} answers ${r.status}`);
  const flips = rows.filter((r) => !r.flag && r.status === 200)
    .map((r) => `${r.name}: ${r.url} is live (200); set its flag to true in storeLinks.ts`);
  return { failures, flips };
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  const src = readFileSync(SRC, "utf8");
  const val = (n) => src.match(new RegExp(`export const ${n} = ([^;]+);`))?.[1].trim();
  const rows = [];
  for (const [name, url, flag] of [["Google Play", "PLAY_STORE_URL", "PLAY_PUBLIC"], ["App Store", "APP_STORE_URL", "APP_STORE_PUBLIC"]]) {
    const u = val(url)?.replace(/^"|"$/g, "");
    if (!u || !["true", "false"].includes(val(flag))) { console.error(`cannot read ${url}/${flag} from storeLinks.ts`); process.exit(2); }
    let status = 0;
    try { status = (await fetch(u, { redirect: "follow", headers: { "User-Agent": "Mozilla/5.0 (Macintosh) AppleWebKit/537.36 Chrome/124 Safari/537.36" } })).status; } catch { /* unreachable: 0 */ }
    if (status === 0) { console.error(`FAIL cannot reach ${u} (offline?): ${name} not checked`); process.exit(1); }
    rows.push({ name, flag: val(flag) === "true", status, url: u });
    console.log(`${name}: ${u} -> ${status}; flag ${val(flag)}`);
  }
  const { failures, flips } = judge(rows);
  for (const f of flips) console.log(`REPORT ${f}`);
  for (const f of failures) console.error(`FAIL ${f}`);
  process.exit(failures.length ? 1 : 0);
}
