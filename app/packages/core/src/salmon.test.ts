/**
 * (T211) THE SALMON FILE AND THE SALMON CARD: `salmon/2` (a head + one slice per bundle region) decoded (the pipeline's encoder's layout, `pipeline/deliver/
 * salmon.py`), each section's chain (and the UNLISTED chain for a section in no run), the day's lookups, and the card's
 * words — whole strings (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import { decodeSalmon, decodeSalmonHead, salmonCard, salmonChainStatus, salmonDayIndex, salmonDayProfiles, salmonLabel, salmonLegendSource,
         SALMON_WORDS, SalmonError, salmonStatusFromSpecies, gearTabsFor, salmonStripOf, salmonTimelineKey } from "./salmon";
import { salmonGlance } from "./glance";

const HANDLES = "147b20dce7d8576c";
const JUL20 = new Date(2026, 6, 20), SEP15 = new Date(2026, 8, 15);
const d = (date: Date) => salmonDayIndex(date);

/** The Kispiox, as the pipeline built it from the Region 6 page dated 2026-08-28 (abridged): coho 4/day Jul 15–Aug 23
 *  then closed (FN0897); chinook, sockeye, pink and chum closed; no natural bait all year. */
const PAYLOAD = {
  format: "salmon/2", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations",
  default: 0,
  regions: { "6": { name: "Skeena", asof: "2026-08-28", url: "https://example/region6" },
             "1": { name: "Vancouver Island", asof: "2026-08-25", url: "https://example/region1" } },
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
    { r: ["6"], w: "Kispiox River", l: ["6:kispiox-river:whole", "6:b(i):section", "6:a:region"],
      sp: [[0, 1], [0, 3, 196, 2, 236, 3], [0, 4], [0, 4], [0, 5]], g: [0, 0], m: [0, 3, 196, 1, 236, 3] },
    // 2: a water whose coho are release-only in the fall, the rest not opened
    { r: ["1"], w: "Cowichan River", l: ["1:cowichan"], sp: [[0, 0], [0, 0, 244, 6, 305, 0], [0, 0], [0, 0], [0, 0]],
      g: [0, -1], m: [0, 3, 244, 2, 305, 3] },
  ],
  unmapped: [{ items: ["gnis:4674"], r: "6", w: "Kispiox River — above the resort signs", chain: 2 }],
};

/** sid 100..104 the Kispiox (chain 1) in region 6's slice, 200..201 the Cowichan (chain 2) in region 1's; everything
 *  else unlisted. */
const hx = (handles: string) => Array.from({ length: 8 }, (_, i) => parseInt(handles.slice(2 * i, 2 * i + 2), 16));
function encode(payload: object, handles = HANDLES): Uint8Array {
  const body = new TextEncoder().encode(JSON.stringify(payload));
  return new Uint8Array([0x42, 0x43, 0x53, 0x41, 2, ...hx(handles),
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

describe("salmon/2", () => {
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

  it("refuses another file, another atlas's handles, and trailing bytes", () => {
    expect(() => decodeSalmon(new Uint8Array([1, 2, 3]), HANDLES)).toThrow(SalmonError);
    expect(() => decodeSalmon(FILE, "0".repeat(16))).toThrow(/built for handles 147b20dce7d8576c/);
    expect(() => decodeSalmonHead(new Uint8Array([...FILE, 0]), HANDLES)).toThrow(/trailing/);
    expect(() => decodeSalmon(FILE, HANDLES, [new Uint8Array([...SHARDS[0]!, 0])])).toThrow(/trailing/);
    expect(() => decodeSalmon(FILE, HANDLES, [shard("6", [], "0".repeat(16))])).toThrow(/slice built for handles/);
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

  it("the legend says whose rules, as of when, and the unlisted rule", () => {
    expect(salmonLegendSource(facts())).toBe("Salmon rules from Fisheries and Oceans Canada (DFO), pages dated up to "
      + "Aug 28, 2026. Waters DFO does not list are closed to salmon.");
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

  it("a DFO place on this water not drawn yet is listed under 'Location not yet mapped' with its own rules", () => {
    const c = salmonCard(facts(), { sids: [101], item: "gnis:4674" }, SEP15);
    expect(c.unmapped.map((u) => [u.label, u.tag, u.block.rows.map((r) => [r.label, r.statusLabel])])).toEqual([
      ["Kispiox River — above the resort signs", "Location not yet mapped",
       [["Chinook · sockeye · pink · chum", "Closed"], ["Coho", "Release only"]]]]);
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
    expect(c.year).toEqual({ partial: false, unplaced: [], bars: [{ label: null, unmapped: false, today: "open", todayWord: "open",
      runs: [{ status: "closed", days: 195 }, { status: "open", days: 40 }, { status: "closed", days: 130 }] }] });
  });

  it("a page across places that differ: still one bar, `varies` on the days they differ", () => {
    const c = salmonCard(facts(), { sids: [101, 200], item: null }, SEP15);
    expect(c.year.bars.length).toBe(1);
    expect([c.year.bars[0]!.today, c.year.bars[0]!.todayWord]).toEqual(["varies", "differs along this part"]);
    const s = salmonStripOf(c, null)!;
    // drawn like the game-fish bar: a day the sections differ is the neutral "unknown" fill; the details say why
    expect([s.status, s.mark + s.word, s.legend.map((l) => l.label)]).toEqual(["varies", "Varies", ["see Salmon below", "✕ closed"]]);
  });

  it("PARTIAL COVERAGE (owner 2026-10-28): ONE bar; a day the undrawn place answers otherwise is check DFO; one line lists the places", () => {
    // the Kispiox (coho open Jul 15–Aug 23, else closed) with an undrawn place (coho release Sep 1–Oct 31, else closed)
    const c = salmonCard(facts(), { sids: [101], item: "gnis:4674" }, SEP15);
    expect(c.year.bars.map((b) => [b.label, b.today, b.runs])).toEqual([[null, "check", [
      { status: "closed", days: 195 }, { status: "check", days: 40 }, { status: "closed", days: 8 },
      { status: "check", days: 61 }, { status: "closed", days: 61 }]]]);
    expect(c.year.unplaced).toEqual([{ label: "Kispiox River — above the resort signs", today: "release", todayWord: "release only" }]);
    const s = salmonStripOf(c, { runs: [{ bits: 0 }], line: [{ kind: "status", text: "Open" }, { kind: "when", text: "on Sep 15" }],
                                 todayAt: 257 / 365 })!;
    expect([s.title, s.status, s.mark + s.word, s.when, s.soon, s.generalTitle, s.runs])
      .toEqual(["Salmon (DFO)", "check", "Check DFO", "on Sep 15", null, "Game fish (B.C. rules)", [
        { drawn: "closed", days: 195 }, { drawn: "unknown", days: 40 }, { drawn: "closed", days: 8 },
        { drawn: "unknown", days: 61 }, { drawn: "closed", days: 61 }]]);
    // a day both say closed: the bar says closed, and so does the card's row
    const may = salmonCard(facts(), { sids: [101], item: "gnis:4674" }, new Date(2026, 4, 1));
    expect([may.year.bars[0]!.today, may.rows.map((r) => r.status)]).toEqual(["closed", ["closed"]]);
  });

  it("no salmon here (DFO lists nothing): no bar — never an all-closed salmon bar; the glance row is a plain 'closed'", () => {
    const c = salmonCard(facts(), { sids: [7], item: "gnis:1" }, JUL20);
    expect([c.year.bars, salmonStripOf(c, null), c.closedOnly]).toEqual([[], null, true]);
    expect(salmonGlance(c)).toEqual({ thumb: "CO", name: SALMON_WORDS.title, value: "closed", tone: "closed", fold: false });
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
    const card = { headlineStatus: "closed", year: { partial: false, unplaced: [],
      bars: [{ label: null, unmapped: false, today: "closed", todayWord: "closed", runs: [{ status: "closed", days: 365 }] }] } } as never;
    expect(salmonStripOf(card, { runs: [{ bits: 1 }], line: [] })).toBeNull();
    expect(salmonStripOf(card, { runs: [{ bits: 1 }, { bits: 0 }], line: [] })?.word).toBe("Closed");
  });

  it("(owner) the Gear / Methods tabs state no closure: shown only when game fish or salmon may be fished", () => {
    const open = { closed: null, closedMore: [], day: "Today, Oct 28", mode: null, lines: [], bait: null } as never;
    const shut = { closed: "No fishing here until Nov 16.", closedMore: [], day: null, mode: null, lines: [], bait: null } as never;
    const m = { closed: null, closedMore: [], ways: null, boats: null, reference: null } as never;
    const card = { gear: ["No natural bait allowed (Jan 1–Dec 31)"], unmapped: [] } as never;
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
      n("6:min_size:b", "min_size", "All retained coho must measure 25 cm or more.", { sp: ["CO"], cm: 25 }),
      n("6:annual:c", "annual", "The annual coho limit is 10.", { sp: ["CO"], n: 10 }),
      n("6:adult:d", "adult", "An \"adult chinook\" is over 50 cm.", { sp: ["CH"], cm: 50 }),
    ] } } } } as never;
  };
  it("an open coho row carries its annual note (its size is the sizes line); a closed chinook's note stays in the block", () => {
    const c = salmonCard(withNotes(), { sids: [101], item: null }, JUL20);
    const coho = c.rows.find((r) => r.species.includes("CO"))!;
    expect([coho.sizes, coho.speciesNotes]).toEqual(["Sizes (DFO): coho 25 cm or more to keep", ["The annual coho limit is 10."]]);
    expect(c.regionRules).toEqual([{ label: "Region 6 Skeena", notes: ["Daylight only.", "An \"adult chinook\" is over 50 cm."] }]);
  });
});

describe("(T227, owner 2026-10-28) a region-wide closure is the answer", () => {
  const P4 = { ...PAYLOAD,
    regions: { ...PAYLOAD.regions, "4": { name: "Kootenays", asof: "2026-08-01", url: "https://example/region4",
      notes: [{ id: "4:closed:a", k: "closed", t: "There is no fishing for salmon in Region 4 - Kootenays until further notice." }] } },
    verdicts: [...PAYLOAD.verdicts, { s: "check", t: "Non-retention (as printed: Apr 1 until further notice)" }],
    chains: [...PAYLOAD.chains,
      { r: ["4"], w: "All Region 4 waters", l: ["4:region:section"], sp: [[0, 7], [0, 7], [0, 7], [0, 7], [0, 7]], g: [0, -1], m: [0, 4] }] };
  const f4 = () => decodeSalmon(encode(P4), HANDLES, [...SHARDS, shard("4", [[300, 2, 3]])]);
  it("only the region's own rows and its header closes salmon: one closed row in the header's words, no bar, no Details", () => {
    const c = salmonCard(f4(), { sids: [300], item: "gnis:14097" }, JUL20);
    expect([c.allClosed, c.headlineStatus, c.partial, c.closedOnly, c.year.bars, c.regionRules])
      .toEqual([true, "closed", null, true, [], []]);
    expect(c.rows.map((r) => [r.label, r.status, r.notes])).toEqual([
      ["Salmon", "closed", ["There is no fishing for salmon in Region 4 - Kootenays until further notice."]]]);
    expect(salmonGlance(c).fold).toBe(false);
    expect(salmonChainStatus(f4(), 3, salmonDayIndex(JUL20))).toBe("closed");           // the map says the same
  });
});
