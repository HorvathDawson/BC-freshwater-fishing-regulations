/**
 * (T211) THE SALMON FILE AND THE SALMON CARD: `salmon/4` (a head + one slice per bundle region) decoded (the pipeline's encoder's layout, `pipeline/deliver/
 * salmon.py`), each section's chain (and the UNLISTED chain for a section in no run), the day's lookups, and the card's
 * words — whole strings (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import { decodeSalmon, decodeSalmonHead, decodeSalmonShard, salmonCard, salmonChainStatus, salmonDayIndex, salmonDayProfiles, salmonLabel,
         SALMON_WORDS, SalmonError, salmonTable, salmonTableTexts, salmonRowTexts, salmonStatusFromSpecies, gearTabsFor, salmonStripOf, salmonTimelineKey, quotaLine } from "./salmon";
import { tierWords } from "./regs/tree";
import { text } from "./regs/line";
import { salmonGlance } from "./glance";

const HANDLES = "147b20dce7d8576c";
const JUL20 = new Date(2026, 6, 20), SEP15 = new Date(2026, 8, 15);
const d = (date: Date) => salmonDayIndex(date);

/** The Kispiox, as the pipeline built it from the Region 6 page dated 2026-08-28 (abridged): coho 4/day Jul 15–Aug 23
 *  then closed (FN0897); chinook, sockeye, pink and chum closed; no natural bait all year. */
const PAYLOAD = {
  format: "salmon/4", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations",
  default: 0,
  regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/region6", shared: [], sizes: [] },
             "1": { name: "Vancouver Island", asof: "2026-08-25", url: "https://example/region1", shared: [], sizes: [] } },
  verdicts: [
    { s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },          // 0
    { s: "closed", t: "No fishing for chinook (Apr 1–Mar 31)" },                       // 1
    { s: "open", t: "4 per day, only 2 over 50 cm (Jul 15–Aug 23)" },                  // 2
    { s: "closed", t: "No fishing for coho — FN0897 (Aug 24–Mar 31)", n: "FN0897" },  // 3
    { s: "closed", t: "No fishing for sockeye (Apr 1–Mar 31)" },                       // 4
    { s: "closed", t: "No fishing for chum (Apr 1–Mar 31)" },                          // 5
    { s: "release", t: "Catch and release (Sep 1–Oct 31)" },                           // 6
  ],
  gears: [["No natural bait allowed (Jan 1–Dec 31)"]],
  chains: [
    // 0: the UNLISTED water — closed every day, every species
    { r: [], w: "", l: [], sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] },
    // 1: the Kispiox — coho open Jul 15 (196) to Aug 23, then closed
    // (T668 fix 1) its sections name the water's undrawn place (`u`), as the pipeline builds every section of its water
    { r: ["6"], w: "Kispiox River", l: ["6:kispiox-river:whole", "6:b(i):section", "6:a:region"], u: ["1:cowichan"],
      sp: [[0, 1], [0, 3, 196, 2, 236, 3], [0, 4], [0, 4], [0, 5]], g: [0, 0], m: [0, 3, 196, 1, 236, 3] },
    // 2: a water whose coho are release-only in the fall, the rest not opened
    { r: ["1"], w: "Cowichan River", l: ["1:cowichan"], sp: [[0, 0], [0, 0, 244, 6, 305, 0], [0, 0], [0, 0], [0, 0]],
      g: [0, -1], m: [0, 3, 244, 2, 305, 3], n: [0, 0, 244, 1, 305, 0] },
  ],
  unmapped: [{ items: ["gnis:4674"], r: "6", w: "Kispiox River — above the resort signs", chain: 2 }],
};

/** sid 100..104 the Kispiox (chain 1) in region 6's slice, 200..201 the Cowichan (chain 2) in region 1's; everything
 *  else unlisted. */
const hx = (handles: string) => Array.from({ length: 8 }, (_, i) => parseInt(handles.slice(2 * i, 2 * i + 2), 16));
function encode(payload: object, handles = HANDLES): Uint8Array {
  const body = new TextEncoder().encode(JSON.stringify(payload));
  return new Uint8Array([0x42, 0x43, 0x53, 0x41, 4, ...hx(handles),
    body.length & 255, (body.length >> 8) & 255, (body.length >> 16) & 255, (body.length >>> 24) & 255, ...body]);
}
function shard(region: string, runs: [number, number, number][], handles = HANDLES): Uint8Array {
  const out: number[] = [0x42, 0x43, 0x53, 0x52, 1, ...hx(handles), region.length, ...new TextEncoder().encode(region)];
  const v = (n: number) => { while (n >= 0x80) { out.push((n & 0x7f) | 0x80); n = Math.floor(n / 128); } out.push(n); };
  v(runs.length);
  let end = 0;
  for (const [sid, n, chain] of runs) { v(sid - end); v(n); v(chain); end = sid + n; }
  return new Uint8Array(out);
}
const FILE = encode(PAYLOAD);
const SHARDS = [shard("6", [[100, 5, 1]]), shard("1", [[200, 2, 2]])];
const facts = () => decodeSalmon(FILE, HANDLES, SHARDS);

describe("salmon/4", () => {
  it("decodes the runs: a placed section reads its chain; a section in no run reads the UNLISTED chain (closed)", () => {
    const f = facts();
    expect([f.chainOf(99), f.chainOf(100), f.chainOf(104), f.chainOf(105), f.chainOf(201), f.chainOf(5_000_000)])
      .toEqual([0, 1, 1, 0, 2, 0]);
    expect([salmonLabel(f, 102), salmonLabel(f, 7)]).toEqual(["s1", "s0"]);
    expect(f.loaded).toEqual(["6", "1"]);
  });

  it("one region's slice answers its own sections; another region's read chain 0 until their slice is read", () => {
    const r6 = decodeSalmon(FILE, HANDLES, [SHARDS[0]!]);
    expect([r6.chainOf(101), r6.chainOf(200)]).toEqual([1, 0]);
  });

  it("(T360) holds a slice at 8 bytes a run, and refuses a run or chain over 16 bits rather than wrap it", () => {
    const big = shard("6", [[4_000_000_000, 65_535, 65_535]]);
    const s = decodeSalmonShard(big, HANDLES);
    expect([s.starts[0], s.lens[0], s.chain[0]]).toEqual([4_000_000_000, 65_535, 65_535]);
    // (T549 item 11) a slice naming a chain the head does not hold is refused when the two are put together
    expect(() => decodeSalmon(FILE, HANDLES, [big])).toThrow("salmon: slice 6 run 0 names chain 65535, the head holds 3");
    expect(() => decodeSalmon(FILE, HANDLES, [shard("6", [[100, 65_536, 1]])])).toThrow(/over 32\/16 bits/);
    expect(() => decodeSalmon(FILE, HANDLES, [shard("6", [[100, 1, 65_536]])])).toThrow(/over 32\/16 bits/);
  });

  it("refuses another file, another atlas's handles, and trailing bytes", () => {
    expect(() => decodeSalmon(new Uint8Array([1, 2, 3]), HANDLES)).toThrow(SalmonError);
    expect(() => decodeSalmon(FILE, "0".repeat(16))).toThrow(/built for handles 147b20dce7d8576c/);
    expect(() => decodeSalmonHead(new Uint8Array([...FILE, 0]), HANDLES)).toThrow(/trailing/);
    expect(() => decodeSalmon(FILE, HANDLES, [new Uint8Array([...SHARDS[0]!, 0])])).toThrow(/trailing/);
    expect(() => decodeSalmon(FILE, HANDLES, [shard("6", [], "0".repeat(16))])).toThrow(/slice built for handles/);
  });

  it("(T356) checks the file on decode: a bad index, a missing region field or a stray closure is an error, never a quiet answer", () => {
    const bad = (p: object) => () => decodeSalmonHead(encode({ ...PAYLOAD, ...p }), HANDLES);
    const chains = PAYLOAD.chains;
    expect(bad({ chains: [chains[0], { ...chains[1]!, sp: [[0, 99], ...chains[1]!.sp.slice(1)] }, chains[2]] }))
      .toThrow(/chain 1 species 0: index 99 of 7/);
    expect(bad({ chains: [{ ...chains[0]!, g: [0, 5] }, chains[1], chains[2]] })).toThrow(/chain 0 gear: index 5 of 1/);
    expect(bad({ default: 9 })).toThrow(/default 9 is no chain/);
    expect(bad({ unmapped: [{ ...PAYLOAD.unmapped[0]!, chain: 9 }] })).toThrow(/unmapped .*chain 9/);
    expect(bad({ regions: { "6": { name: "Skeena", asof: null, url: "" } } })).toThrow(/region 6 has no shared \/ sizes/);
    expect(bad({ chains: [chains[0], { ...chains[1]!, c: ["6:closed:x"] }, chains[2]] })).toThrow(/closure 6:closed:x is no note/);
    expect(bad({ format: "salmon/3" })).toThrow(/payload format salmon\/3/);
  });

  it("the day index is the calendar day with Feb 29 kept: Jul 15 is 196 in every year", () => {
    expect([d(new Date(2026, 0, 1)), d(new Date(2026, 6, 15)), d(new Date(2025, 6, 15)), d(new Date(2026, 11, 31))])
      .toEqual([0, 196, 196, 365]);
  });
});

describe("the Salmon colouring's day", () => {
  it("chain labels by status for the day: the unlisted water is CLOSED, an answer — never `none`", () => {
    expect(salmonDayProfiles(facts(), JUL20)).toEqual({ open: ["s1"], release: [], closed: ["s0", "s2"], check: [], none: [] });
    expect(salmonDayProfiles(facts(), SEP15)).toEqual({ open: [], release: ["s2"], closed: ["s0", "s1"], check: [], none: [] });
  });

  it("a section paints its chain's answer: an unlisted section paints closed", () => {
    const f = facts(), day = salmonDayProfiles(f, JUL20);
    expect([day.open.includes(salmonLabel(f, 101)), day.closed.includes(salmonLabel(f, 7))]).toEqual([true, true]);
  });
});

describe("the Salmon card", () => {
  it("species that answer alike share a row; the open one stands alone (Jul 20 on the Kispiox)", () => {
    const c = salmonCard(facts(), { sids: [101], item: null }, JUL20);
    expect([c.allClosed, c.headline, c.headlineStatus]).toEqual([false, "Open today", "open"]);
    expect(c.rows.map((r) => [r.label, r.statusLabel, r.notes])).toEqual([
      ["Chinook", "Closed", ["No fishing for chinook (Apr 1–Mar 31)"]],
      ["Coho", "Open", ["4 per day, only 2 over 50 cm (Jul 15–Aug 23)"]],
      ["Sockeye · pink", "Closed", ["No fishing for sockeye (Apr 1–Mar 31)"]],
      ["Chum", "Closed", ["No fishing for chum (Apr 1–Mar 31)"]],
    ]);
    expect(c.gear).toEqual(["No natural bait allowed (Jan 1–Dec 31)"]);
    expect([c.source, c.asOf, c.notListed]).toEqual(["Fisheries and Oceans Canada · Region 6 Skeena",
                                                      "DFO page dated Aug 28, 2026", null]);
    expect(c.credit).toBe("Source: Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations. "
      + "Check DFO's page before you fish.");
  });

  it("EVERY species closed collapses to the one 'Salmon — closed' row (Sep 15 on the Kispiox)", () => {
    const c = salmonCard(facts(), { sids: [101], item: null }, SEP15);
    expect([c.allClosed, c.headline, c.rows.length, c.rows[0]!.label, c.rows[0]!.status]).toEqual(
      [true, "Closed today", 1, "Salmon", "closed"]);
    expect(c.rows[0]!.notes).toContain("No fishing for coho — FN0897 (Aug 24–Mar 31)");
  });

  it("A WATER DFO DOES NOT LIST READS CLOSED (owner 2026-10-27): 'Salmon — closed', and says why", () => {
    const c = salmonCard(facts(), { sids: [7, 8], item: "gnis:1" }, JUL20);
    expect([c.allClosed, c.headline, c.rows.map((r) => [r.label, r.status, r.notes])]).toEqual(
      [true, "Closed today", [["Salmon", "closed", ["Not opened by DFO: closed to salmon fishing"]]]]);
    expect(c.notListed).toBe("DFO does not list this water for salmon. It is closed to salmon fishing.");
    // and with no section at all (a water with no placed section) the same: the unlisted chain, not nothing
    expect(salmonCard(facts(), { sids: [], item: null }, JUL20).allClosed).toBe(true);
  });

  it("a page across two chains says where they differ, never one chain's answer for both", () => {
    const c = salmonCard(facts(), { sids: [101, 200], item: null }, SEP15);
    const coho = c.rows.find((r) => r.label.includes("Coho"))!;
    expect([coho.label, coho.status, coho.notes]).toEqual(
      ["Coho", "varies", ["Differs along this water. Tap a stretch on the map for its own answer."]]);
    expect(c.source).toBe("Fisheries and Oceans Canada · Region 1 Vancouver Island · Region 6 Skeena");
  });

  it("(T372, the trout way) a DFO place on this water not drawn yet is the trout card's in-part caveat, its day and DFO's words", () => {
    const c = salmonCard(facts(), { sids: [101], item: "gnis:4674" }, SEP15);
    // DFO speaks for the place today (its `n`): the hot block, the place in force first
    expect(c.caveat).toEqual({ kind: "inpart", hot: true, title: "Fishing is different in some spots today",
      calm: "Everywhere else, follow this page.", later: null, items: [{
        what: [{ t: "text", s: "Chinook · sockeye · pink · chum closed · Coho release only" }],
        where: "Where: Kispiox River — above the resort signs", rule: null, fish: null, book: [],
        detail: [   // (T502) the closed species: the "what" line says it, not repeated here
                 "Coho: Catch and release (Sep 1–Oct 31)"] }] });
    // a day DFO says nothing for it: the same block, not in force today
    const jul = salmonCard(facts(), { sids: [101], item: "gnis:4674" }, JUL20).caveat!;
    expect([jul.hot, jul.title, jul.calm, jul.items.length]).toEqual([false, "Some spots have their own rules",
      "Not in force today. The map can’t draw these places yet, so check where you are.", 1]);
    // (T668 fix 1) a section whose chain names no undrawn place: no block, though its water (item) has one elsewhere
    expect(salmonCard(facts(), { sids: [5], item: "gnis:4674" }, SEP15).caveat).toBeNull();
  });
});

// ── (T227, owner 2026-10-28) the salmon year bar, a section's timeline key, species notes, the region-wide closure ──────
describe("(T227) the salmon year at the top of the Keep tab", () => {
  it("the bar's day is the pipeline's map status, read from the species — every chain, every day", () => {
    const f = facts();
    for (let c = 0; c < f.chains; c++)
      for (let day = 0; day < 366; day++) expect([c, day, salmonStatusFromSpecies(f, c, day)]).toEqual([c, day, salmonChainStatus(f, c, day)]);
  });

  it("every place mapped: ONE bar for the page, no label; a day no rule opens is closed", () => {
    const c = salmonCard(facts(), { sids: [101], item: null }, JUL20);
    expect(c.year).toEqual({ bars: [{ label: null, unmapped: false, today: "open", todayWord: "open",
      runs: [{ status: "closed", days: 195 }, { status: "open", days: 40 }, { status: "closed", days: 130 }] }] });
  });

  it("a page across places that differ: still one bar, `varies` on the days they differ", () => {
    const c = salmonCard(facts(), { sids: [101, 200], item: null }, SEP15);
    expect(c.year.bars.length).toBe(1);
    expect([c.year.bars[0]!.today, c.year.bars[0]!.todayWord]).toEqual(["varies", "differs along this part"]);
    const s = salmonStripOf(c, null)!;
    // drawn like the game-fish bar: a day the sections differ is the neutral "unknown" fill; the details say why
    expect([s.status, s.mark + s.word, s.legend.map((l) => l.label)]).toEqual(["varies", "Varies", ["See Salmon below", "✕ Closed"]]);
  });

  it("(T372, the trout way) PARTIAL COVERAGE: ONE bar of the DRAWN answer; the undrawn place never turns a day to check DFO", () => {
    // the Kispiox (coho open Jul 15–Aug 23, else closed) with an undrawn place (coho release Sep 1–Oct 31, else closed)
    const c = salmonCard(facts(), { sids: [101], item: "gnis:4674" }, SEP15);
    expect(c.year.bars.map((b) => [b.label, b.today, b.runs])).toEqual([[null, "closed", [
      { status: "closed", days: 195 }, { status: "open", days: 40 }, { status: "closed", days: 130 }]]]);
    // the rows keep the drawn answer too (the place is the caveat): coho closed here today, never "check DFO"
    expect(c.rows.map((r) => r.status)).toEqual(["closed"]);
    const s = salmonStripOf(c, { runs: [{ bits: 0 }], line: [{ kind: "status", text: "Open" }], todayAt: 257 / 365 })!;
    expect(s.status).toBe("closed");
  });

  it("no salmon here (DFO lists nothing): no bar — never an all-closed salmon bar; the glance row is a plain 'closed'", () => {
    const c = salmonCard(facts(), { sids: [7], item: "gnis:1" }, JUL20);
    expect([c.year.bars, salmonStripOf(c, null), c.closedOnly]).toEqual([[], null, true]);
    expect(salmonGlance(c)).toEqual({ source: "dfo", thumb: "CO", name: SALMON_WORDS.title, value: "closed", tone: "closed", fold: false });
    expect(salmonCard(facts(), { sids: [101], item: null }, JUL20).closedOnly).toBe(false);
  });

  it("the next change is said as the game-fish bar says it: closed → 'Opens …', open → 'Closed from …' within 21 days", () => {
    const g = (todayAt: number) => ({ runs: [{ bits: 0 }], line: [{ kind: "when", text: "today" }], todayAt });
    const shut = salmonStripOf(salmonCard(facts(), { sids: [101], item: null }, new Date(2026, 5, 1)), g(151 / 365))!;
    expect([shut.mark + shut.word, shut.soon]).toEqual(["✕Closed", { kind: "soonOpen", text: "Opens Jul 15" }]);
    const open = salmonCard(facts(), { sids: [101], item: null }, new Date(2026, 7, 10));
    const o = salmonStripOf(open, g(221 / 365))!;
    expect([o.mark + o.word, o.soon]).toEqual(["✓ Open", { kind: "soon", text: "Closed from Aug 24" }]);
  });

  it("salmon closed all year beside a provincial bar closed all year: no second bar (the two say the same)", () => {
    const card = { headlineStatus: "closed", year: {
      bars: [{ label: null, unmapped: false, today: "closed", todayWord: "closed", runs: [{ status: "closed", days: 365 }] }] } } as never;
    expect(salmonStripOf(card, { runs: [{ bits: 1 }], line: [] })).toBeNull();
    expect(salmonStripOf(card, { runs: [{ bits: 1 }, { bits: 0 }], line: [] })?.word).toBe("Closed");
  });

  it("(owner) the Gear / Methods tabs state no closure: shown only when game fish or salmon may be fished", () => {
    const open = { closed: null, closedMore: [], day: "Today, Oct 28", mode: null, lines: [], bait: null } as never;
    const shut = { closed: "No fishing here until Nov 16.", closedMore: [], day: null, mode: null, lines: [], bait: null } as never;
    const m = { closed: null, closedMore: [], ways: null, boats: null, reference: null } as never;
    const card = { gear: ["No natural bait allowed (Jan 1–Dec 31)"] } as never;
    const strip = (status: string) => ({ status }) as never;
    // game fish open: the page's tabs (DFO's gear too where salmon may be fished)
    expect(gearTabsFor({ status: "open", gear: open, methods: m }, null, strip("release"), card))
      .toEqual({ gear: open, methods: m, salmonGear: { lead: "For salmon (DFO)", lines: ["No natural bait allowed (Jan 1–Dec 31)"] } });
    // closed to game fish, salmon release only: the day's book gear as if open, and DFO's
    const any = { gear: open, methods: m };
    expect(gearTabsFor({ status: "closed", gear: shut, methods: m }, any, strip("release"), card)).toEqual({ gear: open, methods: m,
      salmonGear: { lead: "Gear for salmon fishing: the B.C. gear rules below, and DFO's conditions", lines: ["No natural bait allowed (Jan 1–Dec 31)"] } });
    // both closed (or no salmon bar): no tabs — never a closure sentence
    for (const s of [strip("closed"), null])
      expect(gearTabsFor({ status: "closed", gear: shut, methods: m }, any, s, card)).toEqual({ gear: null, methods: null, salmonGear: null });
  });

  it("a section's salmon timeline key: equal on one chain, different across chains", () => {
    const f = facts();
    expect([salmonTimelineKey(f, 100) === salmonTimelineKey(f, 104), salmonTimelineKey(f, 100) === salmonTimelineKey(f, 200),
            salmonTimelineKey(f, 7) === salmonTimelineKey(f, 8)]).toEqual([true, false, true]);
  });
});

describe("(T227) region notes about a species sit with that species", () => {
  const n = (id: string, k: string, t: string, extra: object = {}) => ({ id, k, t, ...extra });
  const withNotes = (): ReturnType<typeof facts> => {
    const f = facts();
    return { ...f, payload: { ...f.payload, regions: { ...f.payload.regions, "6": { ...f.payload.regions["6"]!, notes: [
      n("6:daylight:a", "daylight", "Daylight only."),
      n("6:min_size:b", "min_size", "All retained coho must measure 25 cm or more.", { sp: ["CO"], lengths: [{ min_cm: 25 }], fold: "sizes" }),
      n("6:annual:c", "annual", "The annual coho limit is 10.", { sp: ["CO"], n: 10 }),
      n("6:adult:d", "adult", "An \"adult chinook\" is over 50 cm.", { sp: ["CH"], lengths: [{ min_cm: 50 }], fold: "sizes" }),
      n("6:facility:e", "facility", "No fishing within 100 m of a government facility.", { fold: "drop" }),
    ], sizes: [{ sp: ["CH"], adult: { min_cm: 50 } }, { sp: ["CO"], min: { min_cm: 25 } }] } } } } as never;
  };
  it("an open coho row carries its annual note (its size is the sizes line); (T393) a closed chinook's size is on its own row, not in the block", () => {
    const c = salmonCard(withNotes(), { sids: [101], item: null }, JUL20);
    const coho = c.rows.find((r) => r.species.includes("CO"))!;
    expect([coho.sizes, coho.speciesNotes]).toEqual(["Sizes (DFO): coho 25 cm or more to keep", ["The annual coho limit is 10."]]);
    const chinook = c.rows.find((r) => r.species.includes("CH"))!;
    expect([chinook.status, chinook.sizesBy]).toEqual(["closed", { CH: "Sizes (DFO): adult chinook over 50 cm" }]);
    expect(salmonTable(c).species.map((r) => [r.code, r.limit])).toEqual([
      ["CH", "Sizes (DFO): adult chinook over 50 cm"], ["CO", "Sizes (DFO): coho 25 cm or more to keep"],
      ["SK", null], ["PK", null], ["CM", null]]);
    expect(c.regionRules.map(({ label, notes }) => ({ label, notes }))).toEqual([{ label: "Region 6 Skeena", notes: ["Daylight only."] }]);
  });
  it("(T482, owner 2026-11-07) every species closed: each species' sizes are on its own row, none left in the region block", () => {
    const c = salmonCard(withNotes(), { sids: [101], item: null }, SEP15);           // the Kispiox: all five closed
    expect(c.allClosed).toBe(true);
    expect(salmonTable(c).species.map((r) => [r.code, r.status, r.limit])).toEqual([
      ["CH", "closed", "Sizes (DFO): adult chinook over 50 cm"], ["CO", "closed", "Sizes (DFO): coho 25 cm or more to keep"],
      ["SK", "closed", null], ["PK", "closed", null], ["CM", "closed", null]]);
    const loose = [...c.regionRules.flatMap((g) => g.notes), ...c.waterRules];
    expect(loose.filter((t) => /cm/.test(t))).toEqual([]);
  });
  it("(T343) a folded note is said by its structure: a dropped one never shows, a bucket one not beside its bucket", () => {
    const f = withNotes() as unknown as { payload: { regions: Record<string, { notes: unknown[] }> } };
    const r6 = f.payload.regions["6"]!;
    const bucket = { tree: { node: "tier", id: "t0", tier: "total", n: 4, says: "Up to 4 salmon a day in total", by: 0, children: [] },
                     t: "The daily limit … is 4.", ids: ["6:daily:f"] };
    const g = { ...f, payload: { ...f.payload, regions: { ...f.payload.regions, "6": { ...r6, bucket,
      notes: [...r6.notes, n("6:daily:f", "daily", "The daily limit … is 4.", { n: 4, fold: "bucket" })] } } } } as never;
    const words = (c: ReturnType<typeof salmonCard>) => [...c.regionRules.flatMap((x) => x.notes), ...c.rows.flatMap((r) => [...r.notes, ...(r.speciesNotes ?? [])])];
    expect(words(salmonCard(withNotes(), { sids: [101], item: null }, JUL20))).not.toContain("No fishing within 100 m of a government facility.");
    const c = salmonCard(g, { sids: [101], item: null }, JUL20);
    expect([c.bucket?.ids, words(c).includes("The daily limit … is 4.")]).toEqual([["6:daily:f"], false]);
  });
  it("(T343, T482) the page's fishing hours are the card's `hours` line; the daylight words leave the region block", () => {
    const f = withNotes() as unknown as { payload: { regions: Record<string, { notes: unknown[] }> } };
    const r6 = f.payload.regions["6"]!;
    const g = { ...f, payload: { ...f.payload, regions: { ...f.payload.regions, "6": { ...r6,
      hours: { says: "Salmon fishing in daylight only: sunrise to sunset", id: "6:daylight:a" },
      notes: (r6.notes as { id: string }[]).map((x) => (x.id === "6:daylight:a" ? { ...x, fold: "hours" } : x)) } } } } as never;
    const c = salmonCard(g, { sids: [101], item: null }, JUL20);
    const loose = (x: ReturnType<typeof salmonCard>) => x.regionRules.flatMap((r) => r.notes).includes("Daylight only.");
    expect([c.hours, loose(c)]).toEqual(["Salmon fishing in daylight only: sunrise to sunset", false]);
    // a daylight note folded to hours its region does not ship (an older file) stays as words: never lost
    const bare = { ...f, payload: { ...f.payload, regions: { ...f.payload.regions, "6": { ...r6,
      notes: (r6.notes as { id: string }[]).map((x) => (x.id === "6:daylight:a" ? { ...x, fold: "hours" } : x)) } } } } as never;
    expect(loose(salmonCard(bare, { sids: [101], item: null }, JUL20))).toBe(true);
    const closed = salmonCard(g, { sids: [101], item: null }, SEP15);      // every species closed: no hours line
    expect([closed.allClosed, closed.hours]).toEqual([true, null]);
  });
});

describe("(T227, owner 2026-10-28) a region-wide closure is the answer", () => {
  const P4 = { ...PAYLOAD,
    regions: { ...PAYLOAD.regions, "4": { name: "Kootenays", asof: "2026-08-01", url: "https://example/region4",
      notes: [{ id: "4:closed:a", k: "closed", t: "There is no fishing for salmon in Region 4 - Kootenays until further notice." }],
      shared: [], sizes: [] } },
    // (T356) the pipeline closes the page's places with the header's words (`quota.header_closure`): the verdict carries
    // the note's id and the chain the ids closing it every day — the app reads them, never a location id
    verdicts: [...PAYLOAD.verdicts, { s: "closed", t: "There is no fishing for salmon in Region 4 - Kootenays until further notice.", c: "4:closed:a" }],
    chains: [...PAYLOAD.chains,
      { r: ["4"], w: "Kootenay River", l: ["4:kootenay-river:whole", "4:region:section"], sp: [[0, 7], [0, 7], [0, 7], [0, 7], [0, 7]], g: [0, -1], m: [0, 3],
        c: ["4:closed:a"] }] };
  const f4 = () => decodeSalmon(encode(P4), HANDLES, [...SHARDS, shard("4", [[300, 2, 3]])]);
  it("a chain the header closes (any tier — a water's own row too): one closed row in the header's words, no bar, no Details", () => {
    const c = salmonCard(f4(), { sids: [300], item: "gnis:14097" }, JUL20);
    expect([c.allClosed, c.headlineStatus, c.caveat, c.closedOnly, c.year.bars, c.regionRules])
      .toEqual([true, "closed", null, true, [], []]);
    expect(c.rows.map((r) => [r.label, r.status, r.notes])).toEqual([
      ["Salmon", "closed", ["There is no fishing for salmon in Region 4 - Kootenays until further notice."]]]);
    expect(salmonGlance(c).fold).toBe(false);
    expect(salmonChainStatus(f4(), 3, salmonDayIndex(JUL20))).toBe("closed");           // the map says the same
  });
});

const SALMON_ORDER = (c: ReturnType<typeof salmonCard>) => (["CH", "CO", "SK", "PK", "CM"] as const).map((k) => c.rows.find((r) => r.species.includes(k))!.status);
describe("(T318) the salmon table keeps every DFO word", () => {
  const withNotes = () => {
    const f = facts();
    return { ...f, payload: { ...f.payload, regions: { ...f.payload.regions, "6": { ...f.payload.regions["6"]!, notes: [
      { id: "6:daylight:a", k: "daylight", t: "Daylight only." },
      { id: "6:min_size:b", k: "min_size", t: "All retained coho must measure 25 cm or more.", sp: ["CO"], lengths: [{ min_cm: 25 }] },
      { id: "6:annual:c", k: "annual", t: "The annual coho limit is 10.", sp: ["CO"], n: 10 },
    ], sizes: [{ sp: ["CO"], min: { min_cm: 25 } }] } } } } as never as ReturnType<typeof facts>;
  };
  const cases: [string, Date, number[], string | null][] = [
    ["an open coho with a place not drawn", JUL20, [101], "gnis:4674"], ["the same, release only in the fall", SEP15, [101], "gnis:4674"],
    ["a water with no undrawn place", JUL20, [101], null], ["the Cowichan", SEP15, [200], null], ["an unlisted water", JUL20, [7], null]];
  for (const [what, day, sids, item] of cases) it(`${what}: every row word, size, species note and place word is in the table`, () => {
    const c = salmonCard(withNotes(), { sids, item }, day), t = salmonTable(c);
    const have = new Set(salmonTableTexts(t, c.gear));
    // (T502) a row's words its species rows already say are not repeated under the table: closed words on rows all
    // Closed, a read verdict's words on rows whose quota line says its numbers (its dates on the line's `window`)
    const said = (s: string) => c.rows.some((r) => r.repeats?.includes(s) && r.species.every((sp) => {
      const row = t.species.find((x) => x.code === sp)!;
      return r.status === "closed" ? row.status === "closed" : !!row.quota && (!r.window || row.window === r.window);
    }));
    for (const s of salmonRowTexts(c)) if (!said(s)) expect(have, s).toContain(s);
    // one species row each, in order, nothing repeated per picture
    expect(t.species.map((r) => r.code)).toEqual(["CH", "CO", "SK", "PK", "CM"]);
  });

  it("the species rows carry today's status; (T372) an undrawn place keeps its own words, in the caveat", () => {
    const c = salmonCard(withNotes(), { sids: [101], item: "gnis:4674" }, SEP15), t = salmonTable(c);
    expect(t.species.map((r) => r.name)).toEqual(["Chinook", "Coho", "Sockeye", "Pink", "Chum"]);
    expect(t.species.map((r) => r.status)).toEqual(SALMON_ORDER(c));
    expect(c.caveat!.items[0]!.where).toBe("Where: Kispiox River — above the resort signs");
    expect(c.caveat!.items[0]!.detail).toContain("Coho: Catch and release (Sep 1–Oct 31)");
  });
});

describe("(T320, T332) the salmon bucket and each species' numbers — the game-fish quota tree", () => {
  // T332: the store's nodes, as `pipeline/regs/dfo_salmon/quota.species_tree` writes them (TreeTier / TreeKind)
  const tier = (t: "total" | "count" | "size", n: number, says: string, kids: object[] = [], more: object = {}) =>
    ({ node: "tier", id: "t1", tier: t, n, says, by: 0, children: kids, ...more });
  const kind = (name: string, n: number, says: string, folded: boolean, mx: number | null = null) =>
    ({ node: "kind", species: ["CO"], name, n, says, detail: "", sizes: { cm: [[0, mx]], in: [[0, mx === null ? null : Math.round(mx / 2.54)]] },
       also_counts: [], duties: [], sentence: says, ...(folded ? { folded: true } : {}) });
  const size = (n: number, cm: number, says: string, period?: string) => tier("size", n, says, [], { over_cm: cm, ...(period ? { period } : {}) });
  const count = (d: number, says: string, kids: object[] = []) => tier("count", d, says, kids, { species: ["CO"] });
  const BUCKET = { tree: tier("total", 4, "Up to 4 salmon a day in total"), possession: 8,
                   t: "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4.",
                   tp: "The possession limit for all salmon from all waters is twice the daily limit." };
  const withQuota = (q: object) => {
    const P = PAYLOAD as never as { verdicts: object[]; regions: Record<string, object> };
    const verdicts = P.verdicts.map((v, i) => (i === 2 ? { ...v, ...q } : v));
    const regions = { ...P.regions, "6": { ...P.regions["6"]!, bucket: BUCKET } };
    return decodeSalmon(encode({ ...PAYLOAD, verdicts, regions }), HANDLES, SHARDS);
  };
  const co = (q: object) => salmonTable(salmonCard(withQuota(q), { sids: [101], item: null }, JUL20)).species.find((r) => r.code === "CO")!;

  it("an open coho says its daily and size sub-limit under the bucket total; possession is the bucket's only", () => {
    const t = salmonTable(salmonCard(withQuota({ tree: count(4, "4 a day", [size(2, 50, "only 2 over 50 cm")]) }), { sids: [101], item: null }, JUL20));
    expect(t.bucket?.line).toBe("Up to 4 salmon a day in total · 8 in possession");
    expect(t.species.find((r) => r.code === "CO")!.quota).toBe("4 a day · only 2 over 50 cm");
    expect(t.species.filter((r) => r.code !== "CO").map((r) => r.quota ?? null)).toEqual([null, null, null, null]);
  });

  it("(T671 D3) the bucket's period limits are lines under the total, each while a species it names may be kept", () => {
    // DFO's header: "The total annual limit for chinook salmon over 50 cm is 10 …" / Region 8's monthly 4 — the release
    // holds them as the bucket tree's child tiers; no card drew them
    const annual = (sp: string) => ({ ...size(10, 50, `The total annual limit for ${sp} salmon over 50 cm is 10 fish from all fresh waters combined.`, "annual"), id: `a${sp}`, species: [sp === "coho" ? "CO" : "CH"] });
    const monthly = { ...size(4, 50, "There is a monthly limit of 4 coho over 50 cm in Region 8.", "monthly"), id: "m", species: ["CO"] };
    const any = { ...size(2, 80, "Only 2 salmon over 80 cm a year.", "annual"), id: "x" };
    const tree = { ...BUCKET.tree, children: [monthly, annual("chinook"), annual("coho"), any] };
    const P = PAYLOAD as never as { verdicts: object[]; regions: Record<string, object> };
    const f = decodeSalmon(encode({ ...PAYLOAD, regions: { ...P.regions, "6": { ...P.regions["6"]!, bucket: { ...BUCKET, tree } } } }), HANDLES, SHARDS);
    const t = salmonTable(salmonCard(f, { sids: [101], item: null }, JUL20));
    expect(t.species.filter((r) => r.status === "open").map((r) => r.code)).toEqual(["CO"]);   // coho may be kept; chinook may not
    expect(t.bucket!.line).toBe("Up to 4 salmon a day in total · 8 in possession");
    const plain = (x: string) => x.replace(/\u00a0/g, " ");                    // the presenter ties a number to its noun
    expect(t.bucket!.tiers!.map((x) => plain(x.line))).toEqual([
      "There is a monthly limit of 4 coho over 50 cm in Region 8.",
      "The total annual limit for coho salmon over 50 cm is 10 fish from all fresh waters combined.",
      "Only 2 salmon over 80 cm a year."]);
    expect(t.bucket!.tiers![0]!.runs).toEqual(tierWords(monthly as never));
    expect(salmonTableTexts(t).map(plain)).toContain("The total annual limit for coho salmon over 50 cm is 10 fish from all fresh waters combined.");
    // no child tier: no `tiers` key (the card as it was)
    expect("tiers" in salmonTable(salmonCard(withQuota({}), { sids: [101], item: null }, JUL20)).bucket!).toBe(false);
  });

  it("THE TROUT PRESENTER draws it: each tier by tierWords (its number big), the line's words the store's", () => {
    const q = { tree: count(4, "4 a day", [size(2, 50, "only 2 over 50 cm")]) };
    const r = co(q);
    expect(r.quotaRuns).toEqual([...tierWords(q.tree as never), text(" · "), ...tierWords(q.tree.children[0] as never)]);
    expect(r.quotaRuns!.filter((x) => x.t === "num").map((x) => x.s)).toEqual(["4", "2"]);
    expect(quotaLine(q as never)).toEqual(r.quotaRuns);
    const b = salmonTable(salmonCard(withQuota(q), { sids: [101], item: null }, JUL20)).bucket!;
    expect(b.runs!.slice(0, 3)).toEqual(tierWords(BUCKET.tree as never));
  });

  it("hatchery-marked only and a maximum size are said; an unread row shows no number, flagged", () => {
    const h = count(1, "1 a day, hatchery-marked only", [kind("hatchery-marked coho", 1, "1 a day, hatchery-marked only", true, 35),
                                                         size(0, 35, "none over 35 cm")]);
    expect(co({ tree: h }).quota).toBe("1 a day, hatchery-marked only · none over 35 cm");      // the folded kind: not said twice
    const u = co({ u: 1 });
    expect([u.quota, u.quotaUnread]).toEqual([null, true]);
  });

  it("a wild allowance and a monthly quota are said; 'N of N unmarked' is marked or unmarked", () => {
    expect(co({ tree: count(2, "2 a day", [kind("coho", 2, "2 a day", true), kind("unmarked (wild) coho", 1, "up to 1 unmarked (wild), the rest hatchery-marked", false)]) }).quota)
      .toBe("2 a day · up to 1 unmarked (wild), the rest hatchery-marked");
    expect(co({ tree: count(2, "2 a day", [kind("coho", 2, "2 a day", true), kind("unmarked (wild) coho", 2, "marked or unmarked", false)]) }).quota)
      .toBe("2 a day · marked or unmarked");
    expect(co({ tree: count(4, "4 a day", [size(1, 65, "only 1 over 65 cm"), size(1, 65, "1 over 65 cm a month", "monthly")]) }).quota)
      .toBe("4 a day · only 1 over 65 cm · 1 over 65 cm a month");
  });

  // (T349, T356) a water system's combined limit (Squamish) is the region's `shared` — the store's aggregate tier, with or
  // without a bucket — shown only while its species is open here; an aggregate note with no shared limit is an error
  const squamish = (q: object, day: Date, shared: boolean) => {
    const f = withQuota(q);
    const P = f.payload as never as { regions: Record<string, { notes?: object[]; bucket?: object }> };
    const notes = [{ id: "6:aggregate:s", k: "aggregate", sp: ["CO"], n: 1, hatchery: true, t: "Squamish watershed: …",
                     w: [{ n: "Squamish River", ws: true, key: "squamish river+ws" }] }];
    const tree = { node: "tier", id: "t0", tier: "total", n: 1, by: 0, children: [], species: ["CO"], whose: "the Squamish River watershed",
                   says: "Up to 1 hatchery-marked coho a day in total across the Squamish River watershed", origin: "hatchery" };
    const { bucket: _b, ...r6 } = P.regions["6"]!;            // no combined daily limit on this page
    const payload = { ...f.payload, scopes: { "6": { "squamish river+ws": ["gnis:sq"] } },
                      regions: { ...P.regions, "6": { ...r6, notes, shared: shared ? [{ tree, t: notes[0]!.t, id: "6:aggregate:s", w: ["squamish river+ws"] }] : [] } } };
    return () => salmonTable(salmonCard({ ...f, payload } as never, { sids: [101], item: "gnis:sq" }, day));
  };
  const hatchery = { tree: count(4, "4 a day, hatchery-marked only", [kind("hatchery-marked coho", 4, "4 a day, hatchery-marked only", true)]) };
  it("(T349) draws the store's aggregate tier over the Squamish watershed — the region's own, with no bucket", () => {
    const w = squamish(hatchery, JUL20, true)().waterBuckets!;
    expect(w.map((x) => x.line)).toEqual(["Up to 1 hatchery-marked coho a day in total across the Squamish River watershed"]);
    expect(w[0]!.runs!.find((x) => x.t === "num")).toEqual({ t: "num", s: "1", size: "big" });
    expect(squamish(hatchery, SEP15, true)().waterBuckets ?? []).toEqual([]);
  });
  it("(T356) an aggregate note the file gives no shared limit is an error, never a tier made up here", () => {
    expect(squamish(hatchery, JUL20, false)).toThrow(/aggregate note 6:aggregate:s has no shared limit/);
  });

  it("no bucket line when nothing may be kept today", () => {
    const t = salmonTable(salmonCard(withQuota({ tree: count(4, "4 a day") }), { sids: [101], item: null }, SEP15));
    expect(t.bucket ?? null).toBeNull();
  });
});

describe("(T321) DFO's rows in DFO's words", () => {
  const X = {
    "6:a:region": [{ k: "new", sp: "Coho", d: "Jul 15 to Aug 23", t: "4 per day, fish from shore on Sundays" }],
    "6:kispiox-river:whole": [{ k: "note", sp: "Chinook", d: "", t: "4 per day, only 1 over 65 cm (See exception below)." },
                              { k: "new", sp: "Coho", d: "Jul 15 to Aug 23", t: "4 per day, fish from shore on Sundays" }],
  } as const;
  const withX = (x: object) => {
    const f = facts();
    return { handles: f.handles, chains: f.chains, chainOf: (s: number) => f.chainOf(s), loaded: f.loaded,
             payload: { ...f.payload, x } } as typeof f;
  };

  it("a water whose rows include an unparsed one: the 'wording changed' block, species, dates, DFO's words, once each", () => {
    const c = salmonCard(withX(X), { sids: [101], item: null }, JUL20);
    expect(c.dfoWords).toEqual({
      changedHead: "DFO changed the wording here — read their text",
      changed: [{ species: "Coho", dates: "Jul 15 to Aug 23", words: "4 per day, fish from shore on Sundays" }],
      notesHead: SALMON_WORDS.wordingNotes,
      notes: [{ species: "Chinook", dates: "All year", words: "4 per day, only 1 over 65 cm (See exception below)." }],
    });
  });

  it("none on another water, and none at all without rows", () => {
    expect(salmonCard(withX(X), { sids: [200], item: null }, JUL20).dfoWords).toBeNull();
    expect(salmonCard(facts(), { sids: [101], item: null }, JUL20).dfoWords).toBeNull();
  });
});
