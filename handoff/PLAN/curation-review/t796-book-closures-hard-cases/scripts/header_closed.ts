/** T796: a header-closed (region-wide "no fishing for salmon") chain that gains a book place — the card before (live
 *  release) and after (candidate), offline and under the live feed. Run: TZ=America/Vancouver vite-node header_closed.ts [sid ...] */
import { gunzipSync } from "node:zlib";
import { readFileSync } from "node:fs";
import { join } from "node:path";
const WT = "/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/twins-wt/app/packages/core/src";
const S = await import(`${WT}/salmon.ts`), G = await import(`${WT}/glance.ts`), FEED = await import(`${WT}/salmonFeed.ts`);
const HERE = "/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/t796-break";
function load(rel: string) {
  const m = JSON.parse(readFileSync(join(rel, "manifest.json"), "utf8")).files; const base = join(rel, "..");
  const h = m.salmon.section_handles;
  const shards = Object.entries(m).filter(([k]) => k.startsWith("salmon-")).map(([, v]: any) => S.decodeSalmonShard(new Uint8Array(gunzipSync(readFileSync(join(base, v.url)))), h));
  return S.salmonFacts(S.decodeSalmonHead(new Uint8Array(gunzipSync(readFileSync(join(base, m.salmon.url)))), h), h, shards);
}
const C = load("/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/t796/candidate-release/r-685b700b2802");
const L = load("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/release/r-46e4355a7277");
const feed = FEED.parseSalmonFeed(JSON.parse(readFileSync(join(HERE, "feed/data-staging-index.json"), "utf8")))!;
const sids = process.argv.slice(2).map(Number);
const days = [new Date(2028, 4, 1), new Date(2028, 6, 1)];
const show = (F: any, sid: number, d: Date) => {
  const c = S.salmonCard(F, { sids: [sid], item: null }, d); const st = S.salmonStripOf(c, { runs: [], line: [], todayAt: 0 });
  return { head: [c.badge, c.headline], rows: c.rows.map((r: any) => [r.label, r.statusLabel, r.notes, r.from ?? null]), closedOnly: c.closedOnly,
           bar: st ? [st.title, st.word] : null, glance: G.salmonGlance(c), regionRules: c.regionRules.map((g: any) => g.label) };
};
for (const sid of sids) for (const d of days) {
  console.log(`\n=== sid ${sid} chain live ${L.chainOf(sid)} cand ${C.chainOf(sid)} on ${d.toDateString()}`);
  for (const [k, F] of [["live release", L], ["candidate", C], ["candidate + feed", FEED.withSalmonFeed(C, feed)]] as const) console.log(k, JSON.stringify(show(F, sid, d)));
}
