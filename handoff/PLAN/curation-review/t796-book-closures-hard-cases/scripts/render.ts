/**
 * T796 adversarial check: render, with the app's OWN core code, what each of the 71 book-closure rows says for salmon,
 * on edge days, offline and under the live DFO feed, and check it against the book and against the live release.
 *
 * Run (from this folder):
 *   TZ=America/Vancouver <twins-wt>/app/node_modules/.bin/vite-node render.ts
 * Inputs: out/rows.json (python3 -I rows_prep.py), feed/data-staging-index.json (the live feed, read-only GET of
 * https://data-staging.canifishthis.ca/feeds/dfo/index.json). Writes out/render.json (every rendered surface) and
 * out/render-fail.json (every failed check), prints a summary per row.
 */
import { gunzipSync } from "node:zlib";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
const WT = "/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/twins-wt";
const core = `${WT}/app/packages/core/src`;
const S = await import(`${core}/salmon.ts`);
const G = await import(`${core}/glance.ts`);
const FEED = await import(`${core}/salmonFeed.ts`);
type Facts = import("/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/twins-wt/app/packages/core/src/salmon").SalmonFacts;

const HERE = "/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/699dcd1b-ff73-4ed1-a69c-69feb621d839/scratchpad/t796-break";
const CAND = "/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/t796/candidate-release/r-685b700b2802";
const LIVE = "/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/release/r-46e4355a7277";

function load(rel: string): { F: Facts; shards: { region: string; starts: Uint32Array; lens: Uint16Array; chain: Uint16Array }[] } {
  const m = JSON.parse(readFileSync(join(rel, "manifest.json"), "utf8")).files as Record<string, { url: string; section_handles: string }>;
  const base = join(rel, "..");
  const handles = m.salmon!.section_handles;
  const head = gunzipSync(readFileSync(join(base, m.salmon!.url)));
  const shardBytes = Object.entries(m).filter(([k]) => k.startsWith("salmon-")).map(([, v]) => gunzipSync(readFileSync(join(base, v.url))));
  const shards = shardBytes.map((b) => S.decodeSalmonShard(new Uint8Array(b), handles));
  return { F: S.salmonFacts(S.decodeSalmonHead(new Uint8Array(head), handles), handles, shards), shards };
}
const cand = load(CAND), live = load(LIVE);
const feedRaw = JSON.parse(readFileSync(join(HERE, "feed/data-staging-index.json"), "utf8"));
const feed = FEED.parseSalmonFeed(feedRaw);
if (!feed) throw new Error("the live feed did not parse");
const candLive = FEED.withSalmonFeed(cand.F, feed), liveLive = FEED.withSalmonFeed(live.F, feed);

const rows = JSON.parse(readFileSync(join(HERE, "out/rows.json"), "utf8")) as any[];
const at = (runs: readonly number[], day: number) => { let v = runs[1] ?? -1; for (let k = 0; k < runs.length; k += 2) { if (runs[k]! > day) break; v = runs[k + 1]!; } return v; };
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const DBM = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];
/** the file's day (Feb 29 = 59) as a date in 2028 (a leap year) */
const dateOf = (d: number) => { let m = 11; while (DBM[m]! > d) m--; return new Date(2028, m, d - DBM[m]! + 1); };
const dayWords = (d: number) => { const x = dateOf(d); return `${MONTHS[x.getMonth()]} ${x.getDate()}`; };
const SP = ["CH", "CO", "SK", "PK", "CM"];

function surfaces(F: Facts, sids: number[], d: number) {
  const day = dateOf(d);
  const card = S.salmonCard(F, { sids, item: null }, day);
  const strip = S.salmonStripOf(card, { runs: [], line: [], todayAt: 0 });
  const chains = [...new Set(sids.map((s) => F.chainOf(s)))];
  return {
    head: [card.title, card.badge, card.headline, card.headlineStatus],
    rows: card.rows.map((r: any) => ({ label: r.label, species: r.species, status: r.status, notes: r.notes, from: r.from ?? null, fromBy: r.fromBy ?? null })),
    fold: { title: card.sourceTitle, book: card.book, note: card.note },
    bar: strip ? [strip.title, strip.word, card.year.bars[0]?.today] : null,
    glance: (({ value, fold, book }: any) => ({ value, fold: fold ?? true, book: book ?? null }))(G.salmonGlance(card)),
    closedOnly: card.closedOnly,
    map: chains.map((c) => S.salmonChainStatus(F, c, d)),
    finfish: card.finfish,
  };
}

const fails: any[] = [], out: any[] = [];
const fail = (row: string, what: string, detail: any) => fails.push({ row, what, detail });

for (const r of rows) {
  const rowOut: any = { id: r.id, rule: r.rule_id, water: r.water, chains: [] };
  for (const c of r.chains) {
    const P = cand.F.payload, ch = P.chains[c.chain]!;
    const lidIdx = ch.l.findIndex((l: string) => (P.p![l] as any)?.book?.rule === r.rule_id);
    if (lidIdx < 0) { fail(r.id, "chain has no book place for the row's rule", { chain: c.chain, l: ch.l }); continue; }
    const lid = ch.l[lidIdx]!;
    const st = P.static![lid]!;
    const D = new Set<number>(); for (let d = 0; d < 366; d++) if (st.sp.some((runs: number[]) => at(runs, d) >= 0)) D.add(d);
    const Dans = new Set<number>(); for (let d = 0; d < 366; d++) if (ch.a!.some((runs: number[]) => at(runs, d) === lidIdx)) Dans.add(d);
    // the windows of D, as runs (wrapping kept as two runs)
    const runs: [number, number][] = [];
    for (let d = 0; d < 366; d++) if (D.has(d)) { if (runs.length && runs[runs.length - 1]![1] === d - 1) runs[runs.length - 1]![1] = d; else runs.push([d, d]); }
    const days = new Set<number>([0, 59, 365]);
    for (const [a, b] of runs) { for (const x of [a - 1, a, b, b + 1, Math.floor((a + b) / 2)]) days.add((x + 366) % 366); }
    const outside = [...Array(366).keys()].find((d) => !D.has(d)); if (outside !== undefined) days.add(outside);
    const sids: number[] = [c.sid];
    const chOut: any = { chain: c.chain, sid: c.sid, n: c.n, lid, staticDays: D.size, answeredDays: Dans.size,
                         windows: runs.map(([a, b]) => `${dayWords(a)}–${dayWords(b)}`), live_chain: c.live_chain, days: [] };
    for (let d = 0; d < 366; d++) {
      const off = surfaces(cand.F, sids, d), lv = surfaces(candLive, sids, d);
      const oldOff = surfaces(live.F, sids, d), oldLv = surfaces(liveLive, sids, d);
      const bookSp = SP.map((_, k) => at(ch.a![k]!, d)).map((i) => i >= 0 && !!(P.p![ch.l[i]!] as any).static);
      const rowSp = SP.map((_, k) => at(ch.a![k]!, d) === lidIdx);
      if (days.has(d)) chOut.days.push({ day: dayWords(d), d, inStatic: D.has(d), rowAnswers: rowSp, bookAnswers: bookSp, off, live: lv, oldOff: { head: oldOff.head, rows: oldOff.rows.map((x: any) => [x.label, x.status]) } });
      for (const [how, s, old] of [["offline", off, oldOff], ["live feed", lv, oldLv]] as const) {
        const tag = { chain: c.chain, sid: c.sid, day: dayWords(d), how };
        // map == card == bar
        const map = s.map[0];
        if (map !== s.head[3]) fail(r.id, "map status disagrees with the card's headline", { ...tag, map, card: s.head[3] });
        if (s.bar && s.bar[2] !== map) fail(r.id, "year bar's today disagrees with the map", { ...tag, map, bar: s.bar });
        const pages = (P.p![lid] as any).book.pages.join(", ");
        const prefix = `Closed to all fishing by the rule book (p.${pages}): ${r.bundle.verbatim}`;
        if (rowSp.some(Boolean)) {
          for (let k = 0; k < 5; k++) if (rowSp[k]) {
            const row = s.rows.find((x: any) => x.species.includes(SP[k]));
            if (!row || row.status !== "closed") fail(r.id, "book day: species not closed", { ...tag, sp: SP[k], row });
            else {
              const note = row.notes.find((n: string) => n.startsWith(prefix));
              if (!note) fail(r.id, "book day: the row's words are not the book's line", { ...tag, sp: SP[k], notes: row.notes, want: prefix });
              else {
                const tail = note.slice(prefix.length);
                if (!/^ \(.+\)$/.test(tail)) fail(r.id, "book words carry no dates", { ...tag, note });
                if (tail === " (all year)" && D.size !== 366) fail(r.id, "says all year but is not", { ...tag, note, days: D.size });
              }
              const from = s.rows.find((x: any) => x.species.includes(SP[k]))!.fromBy?.[SP[k]] ?? row.from;
              if (!from || !from.includes(`Rule book p.${pages}`)) fail(r.id, "book day: from does not name the rule book page", { ...tag, sp: SP[k], from });
            }
          }
          if (bookSp.every(Boolean)) {
            if (map !== "closed") fail(r.id, "all species book-closed but map not closed", { ...tag, map });
            if (s.head[1] !== "Rule book") fail(r.id, "all species book-closed but the badge is not Rule book", { ...tag, head: s.head });
            if (s.glance.value !== "closed") fail(r.id, "all species book-closed but the folded row is not closed", { ...tag, glance: s.glance });
            if (s.glance.fold === false && !s.glance.book) fail(r.id, "plain closed row names no book", { ...tag, glance: s.glance });
          }
          if (!s.fold.book.length) fail(r.id, "book day: source fold names no book", { ...tag });
          if (s.bar && !/rule book/.test(s.bar[0])) fail(r.id, "book day: the bar is not named for the rule book", { ...tag, bar: s.bar });
        }
        if (!bookSp.some(Boolean)) {
          // no book place answers: the card must be the live release's answer (same feed)
          const a = JSON.stringify(s.rows.map((x: any) => [x.label, x.status, x.notes])), b = JSON.stringify(old.rows.map((x: any) => [x.label, x.status, x.notes]));
          if (a !== b) fail(r.id, "non-book day: the answer differs from the live release", { ...tag, cand: s.rows.map((x: any) => [x.label, x.status, x.notes]), live: old.rows.map((x: any) => [x.label, x.status, x.notes]) });
          if (s.fold.book.length) fail(r.id, "non-book day: the fold names a book closure", { ...tag, book: s.fold.book });
          if (s.head[1] !== "DFO") fail(r.id, "non-book day: badge is not DFO", { ...tag, head: s.head });
        }
      }
    }
    rowOut.chains.push(chOut);
  }
  out.push(rowOut);
}
writeFileSync(join(HERE, "out/render.json"), JSON.stringify(out, null, 1));
writeFileSync(join(HERE, "out/render-fail.json"), JSON.stringify(fails, null, 1));
const byRow = new Map<string, Map<string, number>>();
for (const f of fails) { const m = byRow.get(f.row) ?? new Map(); m.set(f.what, (m.get(f.what) ?? 0) + 1); byRow.set(f.row, m); }
console.log("rows", rows.length, "failed checks", fails.length, "rows with a failure", byRow.size);
for (const [r, m] of byRow) console.log(r, JSON.stringify([...m]));
console.log("feed at", feed.at);
