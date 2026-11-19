/**
 * (T882, owner 2026-11-18 ~21:18: "To be determined I think we could change to actually say to be determined / However
 * they should still show green if any salmon there are open only yellow if closed other then the to be determined")
 * DFO'S OWN "TO BE DETERMINED" SAYS SO — the row, the headline, the folded row, the year bar and its key, an undrawn
 * place's in-part line — while a DFO row nobody could read keeps "Check DFO" and a held row its own words. And THE
 * COLOUR ORDER: open, then release, then check, then closed — green whenever any salmon is open (the live feed's
 * composition and the card's headline; the release's own `m` is `deliver.salmon.map_status`, pinned in Python).
 * Whole strings (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import { decodeSalmon, salmonCard, salmonDayIndex, salmonLegend, salmonStripOf, SALMON_WORDS } from "./salmon";
import { salmonGlance } from "./glance";
import { MAP_KEY } from "./mapKey";
import { HELP_WORDS } from "./help";
import { SALMON_FEED_GOLDEN } from "./salmonFeed.golden";
import { composeChain, parseSalmonFeed } from "./salmonFeed";

const HANDLES = "147b20dce7d8576c";
const NOV18 = new Date(2026, 10, 18), SEP1 = new Date(2026, 8, 1);
const AUG28 = salmonDayIndex(new Date(2026, 7, 28)), SEP23 = salmonDayIndex(new Date(2026, 8, 23));

const NOT_OPENED = 0, TBD = 1, CH_OPEN = 2, CH_SHUT = 3, UNREAD = 4, TBD_PRINTED = 5, MIXED = 6;
const closed = [0, NOT_OPENED];
/** South Thompson-like (the staging release r-fc137c4040fa, Region 3): chinook open Aug 28–Sep 22, sockeye DFO's "To be
 *  determined" all year, the rest not opened. `m` as the pipeline paints it (`map_status`): open while chinook is, else
 *  check. Chain 2 the same with sockeye a row nobody could read; chain 3 every species "To be determined" as printed;
 *  chain 4 a sockeye verdict mixing the two; chain 5 an undrawn place "to be determined" beside a closed page (chain 6). */
const PAYLOAD = {
  format: "salmon/4", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations", default: 0,
  regions: { "3": { name: "Thompson-Nicola", asof: "2026-11-06", url: "https://example/region3", shared: [], sizes: [] } },
  verdicts: [
    { s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
    { s: "check", t: "To be determined (Apr 1–Mar 31)" },
    { s: "open", t: "1 per day (Aug 28–Sep 22)" },
    { s: "closed", t: "No fishing for chinook (Sep 23–Mar 31)" },
    { s: "check", t: "Dates unparsed — check DFO: Sockeye 2 per day (as printed: Sept 1st to Oct 31th)" },
    { s: "check", t: "To be determined (as printed: To be determined)" },
    { s: "check", t: "To be determined (Apr 1–Mar 31); Dates unparsed — check DFO: 2 per day (as printed: Sept 1st)" },
  ],
  gears: [],
  chains: [
    { r: [], w: "", l: [], sp: [closed, closed, closed, closed, closed], g: [0, -1], m: [0, 3] },
    { r: ["3"], w: "South Thompson River", l: ["3:south-thompson"], sp: [[0, CH_SHUT, AUG28, CH_OPEN, SEP23, CH_SHUT], closed, [0, TBD], closed, closed],
      g: [0, -1], m: [0, 4, AUG28, 1, SEP23, 4] },
    { r: ["3"], w: "Unread River", l: ["3:unread"], sp: [closed, closed, [0, UNREAD], closed, closed], g: [0, -1], m: [0, 4] },
    { r: ["3"], w: "Kamloops Lake", l: ["3:kamloops"], sp: [[0, TBD_PRINTED], [0, TBD_PRINTED], [0, TBD_PRINTED], [0, TBD_PRINTED], [0, TBD_PRINTED]],
      g: [0, -1], m: [0, 4] },
    { r: ["3"], w: "Mixed River", l: ["3:mixed"], sp: [closed, closed, [0, MIXED], closed, closed], g: [0, -1], m: [0, 4] },
    { r: ["3"], w: "Little River — the lake part", l: ["3:little-part"], sp: [closed, closed, [0, TBD], closed, closed], g: [0, -1], m: [0, 4],
      n: [0, 1] },
    { r: ["3"], w: "Little River", l: ["3:little"], u: ["3:little-part"], sp: [closed, closed, closed, closed, closed], g: [0, -1], m: [0, 3] },
    // 7: Kamloops Lake read today (no held place): every species "To be determined"
    { r: ["3"], w: "Kamloops Lake (read)", l: ["3:kamloops-read"], sp: [[0, TBD_PRINTED], [0, TBD_PRINTED], [0, TBD_PRINTED], [0, TBD_PRINTED], [0, TBD_PRINTED]],
      g: [0, -1], m: [0, 4] },
    // 8: a held place on the chain, but the answer is another place's (`a` names place 1, the region's, read today)
    { r: ["3"], w: "Below Kamloops Lake", l: ["3:kamloops", "3:region"], sp: [closed, closed, [0, TBD], closed, closed], g: [0, -1], m: [0, 4],
      a: [[0, 1], [0, 1], [0, 1], [0, 1], [0, 1]] },
  ],
  p: { "3:kamloops": { r: "3", water: "Kamloops Lake" }, "3:region": { r: "3" } },
  unmapped: [{ items: ["gnis:little"], r: "3", w: "Little River — the lake part", chain: 5 }],
  held: ["3:kamloops"],
};
const hx = (h: string) => Array.from({ length: 8 }, (_, i) => parseInt(h.slice(2 * i, 2 * i + 2), 16));
function encode(payload: object): Uint8Array {
  const body = new TextEncoder().encode(JSON.stringify(payload));
  return new Uint8Array([0x42, 0x43, 0x53, 0x41, 4, ...hx(HANDLES),
    body.length & 255, (body.length >> 8) & 255, (body.length >> 16) & 255, (body.length >>> 24) & 255, ...body]);
}
function shard(region: string, runs: [number, number, number][]): Uint8Array {
  const out: number[] = [0x42, 0x43, 0x53, 0x52, 1, ...hx(HANDLES), region.length, ...new TextEncoder().encode(region)];
  const v = (n: number) => { while (n >= 0x80) { out.push((n & 0x7f) | 0x80); n = Math.floor(n / 128); } out.push(n); };
  v(runs.length);
  let end = 0;
  for (const [sid, n, chain] of runs) { v(sid - end); v(n); v(chain); end = sid + n; }
  return new Uint8Array(out);
}
/** sid 100 the South Thompson (chain 1), 200 Unread River (2), 300 Kamloops Lake (3), 400 Mixed River (4), 600 Little River (6) */
const facts = () => decodeSalmon(encode(PAYLOAD), HANDLES, [shard("3", [[100, 1, 1], [200, 1, 2], [300, 1, 3], [400, 1, 4], [600, 1, 6], [700, 1, 7], [800, 1, 8]])]);
const card = (sid: number, on: Date, item: string | null = null) => salmonCard(facts(), { sids: [sid], item }, on);
const rowsOf = (c: ReturnType<typeof card>) => c.rows.map((r) => `${r.label}: ${r.statusLabel}`);

describe("(T882) DFO's 'To be determined' says 'To be determined'", () => {
  it("the South Thompson today (Nov 18): sockeye 'To be determined', the headline says so, the check colour stays", () => {
    const c = card(100, NOV18);
    expect([c.headline, c.headlineStatus, rowsOf(c)]).toEqual(["To be determined by DFO", "check",
      ["Chinook: Closed", "Coho · pink · chum: Closed", "Sockeye: To be determined"]]);
    const bar = c.year.bars[0]!;
    expect([bar.today, bar.todayWord, bar.todayTbd, bar.keyTbd]).toEqual(["check", "to be determined", true, true]);
    const s = salmonStripOf(c, null)!;
    expect([s.status, s.mark + s.word, s.legend.map((l) => l.label)]).toEqual(["check", "To be determined", ["✓ Open", "To be determined"]]);
  });

  it("Sep 1, chinook open: green — 'Open today', sockeye still 'To be determined' on its row", () => {
    const c = card(100, SEP1);
    expect([c.headline, c.headlineStatus, rowsOf(c)]).toEqual(["Open today", "open",
      ["Chinook: Open", "Coho · pink · chum: Closed", "Sockeye: To be determined"]]);
    expect(salmonStripOf(c, null)!.word).toBe("Open");
  });

  it("every species 'To be determined' (as printed): the folded row says 'to be determined'", () => {
    const c = card(700, NOV18);
    expect([c.headline, rowsOf(c), salmonGlance(c).value]).toEqual(["To be determined by DFO", ["Chinook · coho · sockeye · pink · chum: To be determined"], "to be determined"]);
  });

  it("a HELD place whose carried answer was 'To be determined' keeps 'Check DFO' and the held line (DFO has changed that row)", () => {
    const c = card(300, NOV18);
    expect([c.headline, rowsOf(c), salmonGlance(c).value]).toEqual(["Check DFO today", ["Chinook · coho · sockeye · pink · chum: Check DFO"], "check DFO"]);
    const bar = c.year.bars[0]!;
    expect([bar.todayWord, bar.todayTbd, bar.keyTbd]).toEqual(["check DFO", undefined, undefined]);
    expect(c.waterRules).toContain("DFO changed a row here that the app could not read, so its earlier answer is shown. Check DFO's page.");
  });

  it("a held place on the chain that does not answer leaves the answering place's 'To be determined' as it is", () => {
    const c = card(800, NOV18);
    expect([c.headline, rowsOf(c)]).toEqual(["To be determined by DFO", ["Chinook · coho · pink · chum: Closed", "Sockeye: To be determined"]]);
  });

  it("a row nobody could read keeps 'Check DFO' — alone, or beside a 'To be determined' in one verdict", () => {
    for (const sid of [200, 400]) {
      const c = card(sid, NOV18);
      expect([c.headline, rowsOf(c)]).toEqual(["Check DFO today", ["Chinook · coho · pink · chum: Closed", "Sockeye: Check DFO"]]);
      const bar = c.year.bars[0]!;
      expect([bar.todayWord, bar.todayTbd, bar.keyTbd]).toEqual(["check DFO", undefined, undefined]);
      const s = salmonStripOf(c, null)!;
      expect([s.word, s.legend.map((l) => l.label)]).toEqual(["Check DFO", ["See Salmon below"]]);
    }
  });

  it("an undrawn place 'to be determined' beside a closed page: 'Closed today · to be determined in part'", () => {
    const c = card(600, NOV18, "gnis:little");
    expect([c.headline, c.headlineStatus, c.caveat!.title, c.caveat!.calm]).toEqual(["Closed today · to be determined in part", "closed",
      "Closed today · to be determined in part",
      "DFO has not set its salmon rules yet (“To be determined”) for a part of this water the map can’t draw yet: check DFO for where. Everywhere else, follow this page."]);
  });

  it("the map key, its short form and the Help sentence (one entry covers 'To be determined' and an unread row)", () => {
    expect(salmonLegend().map((l) => l.label)).toEqual(["Salmon may be kept", "Salmon: release only", "Closed to salmon",
      "To be determined by DFO, or a DFO row the app could not read"]);
    expect(MAP_KEY.salmon.check).toBe("TBD/unread");
    expect(HELP_WORDS.salmon.check).toBe("DFO has not set it yet (“To be determined”), or the app could not read DFO's row. Check DFO's notice for this water.");
    expect(SALMON_WORDS.status.check).toBe("Check DFO");
  });
});

describe("(T882) the colour order: open, then release, then check, then closed", () => {
  it("under a live feed: chinook open beside sockeye 'To be determined' paints open; release beats check; check beats closed", () => {
    const five = <T>(x: T) => [x, x, x, x, x];
    const feed = parseSalmonFeed({ ...(SALMON_FEED_GOLDEN as { feed: object }).feed,
      verdicts: [{ s: "closed", t: "No fishing for salmon" }, { s: "check", t: "To be determined (Apr 1–Mar 31)" },
                 { s: "open", t: "1 per day (Aug 28–Sep 22)" }, { s: "release", t: "Catch and release (Sep 1–Oct 31)" }],
      gears: [],
      locs: { open: { r: "3", sp: [[0, 2], [0, 0], [0, 1], [0, 0], [0, 0]], g: [0, -1] },
              release: { r: "3", sp: [[0, 3], [0, 0], [0, 1], [0, 0], [0, 0]], g: [0, -1] },
              tbd: { r: "3", sp: [[0, 0], [0, 0], [0, 1], [0, 0], [0, 0]], g: [0, -1] },
              shut: { r: "3", sp: five([0, 0]), g: [0, -1] } } })!;
    const CODES = ["none", "open", "release", "closed", "check"];
    expect(["open", "release", "tbd", "shut"].map((l) => CODES[composeChain([l], feed).m[1]!]))
      .toEqual(["open", "release", "check", "closed"]);
  });
});
