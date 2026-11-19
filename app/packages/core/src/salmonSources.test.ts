/**
 * (T869, owner 2026-11-18 wording A) THE ANNUAL CHINOOK LIMIT COUNTS ADULT CHINOOK, and what an adult is differs by
 * water: the bucket's annual line is "10 adult chinook a year from all fresh waters" (the pipeline's `quota.period_says`)
 * and, where an adult here is another length, "(here an adult is over 62 cm, Sep 1–Dec 31)" from DFO's scoped `adult`
 * sizes; the chinook row's sizes follow the same scoped length.
 * (T870, owner 2026-11-18) THE SOURCE (DFO) FOLD LISTS THE ROWS BEHIND THE ANSWERS (each place of the page's chains with
 * its rows as printed, `pr`; the page header's notes the card's lines were read from), and EACH BAIT-BAN LINE SAYS WHOSE
 * IT IS, the same ban said once. Whole composed strings (AGENTS 33).
 */
import { describe, expect, it } from "vitest";
import { salmonCard, salmonTable, type SalmonFacts } from "./salmon";

const all = (v: number) => [[0, v], [0, v], [0, v], [0, v], [0, v]];
const n = (id: string, k: string, t: string, extra: object = {}) => ({ id, k, t, ...extra });
const AREAS = "There is no fishing for salmon in waters of Region 2 flowing into Areas 28 and 29 except for the opportunities listed in the table below.";
const DAYLIGHT = "You can only fish for salmon in Region 2 during daylight hours.";
const DAILY = "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4.";
const ANNUAL = "The total annual limit for chinook salmon over 50 cm is 10 fish from all fresh waters combined.";
const ADULT = "An \"adult chinook\" in Region 2 is defined as being over 50 cm except in the following areas where an \"adult chinook\" is defined as being over 62 cm: the Fraser River from Sep 1 to Dec 31; the Vedder River.";
const FACILITY = "No fishing is allowed within 100 meters of any government facility operated for counting, passing or rearing fish.";

/** One Region 2 water (chain 1: its own place, then the areas 28-29 default), chinook open, the rest closed by the
 *  default; its places' rows as printed (`pr`) unless `noRows`; the region's own adult length `regionAdult`. */
function facts(o: { gear?: string; noRows?: boolean; regionAdult?: number } = {}): SalmonFacts {
  return {
    handles: "0".repeat(16), chains: 2, chainOf: () => 1,
    payload: {
      format: "salmon/4", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations", default: 0,
      regions: { "2": {
        name: "Lower Mainland", asof: "2026-11-06", url: "https://example/2", shared: [],
        hours: { says: "Salmon fishing in daylight only: one hour before sunrise to one hour after sunset", id: "2:daylight:a" },
        bucket: { t: DAILY, ids: ["2:daily:b", "2:annual:c"],
                  tree: { node: "tier", id: "t0", tier: "total", n: 4, says: "Up to 4 salmon a day in total", by: 0,
                          children: [{ node: "tier", id: "t1", tier: "size", n: 10, says: "10 adult chinook a year from all fresh waters",
                                       by: 0, children: [], over_cm: 50, period: "annual", species: ["CH"] }] } },
        sizes: [{ sp: ["CH"], min: { min_cm: 30 }, adult: { min_cm: 62 }, on: [[244, 365]], w: ["fraser river"] },
                { sp: ["CH"], min: { min_cm: 30 }, adult: { min_cm: 62 }, w: ["vedder river"] },
                { sp: ["CH"], min: { min_cm: 30 }, adult: { min_cm: o.regionAdult ?? 50 } }],
        notes: [n("2:daylight:a", "daylight", DAYLIGHT, { fold: "hours" }),
                n("2:facility:f", "facility", FACILITY, { fold: "drop" }),
                n("2:daily:b", "daily", DAILY, { n: 4, fold: "bucket" }),
                n("2:annual:c", "annual", ANNUAL, { sp: ["CH"], n: 10, lengths: [{ min_cm: 50 }], fold: "bucket" }),
                n("2:adult:d", "adult", ADULT, { sp: ["CH"], lengths: [{ min_cm: 50 }], fold: "sizes" }),
                n("2:adult:e", "adult", `${ADULT.split(": ")[0]}: the Vedder River.`,
                  { sp: ["CH"], lengths: [{ min_cm: 62 }], fold: "sizes", w: [{ n: "Vedder River", ws: false, key: "vedder river" }] })],
      } },
      scopes: { "2": { "fraser river": ["gnis:F"], "vedder river": ["gnis:V"] } },
      verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
                 { s: "open", t: "1 per day (Sep 19–Dec 31)",
                   tree: { node: "tier", id: "t1", tier: "count", n: 1, says: "1 a day", by: 0, children: [], species: ["CH"] } },
                 { s: "closed", t: AREAS }],
      gears: [[o.gear ?? "Bait ban (all year)"]],
      chains: [
        { r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
        { r: ["2"], w: "Mamquam River", l: ["2:mamquam-river:whole", "2:default:areas-28-29"],
          sp: [[0, 1], [0, 2], [0, 2], [0, 2], [0, 2]], g: [0, 0], m: [0, 1],
          a: [[0, 0], [0, 1], [0, 1], [0, 1], [0, 1]] },
      ],
      unmapped: [],
      p: { "2:mamquam-river:whole": { r: "2", water: "Mamquam River" },
           "2:default:areas-28-29": { r: "2", water: "Region 2 waters flowing into Areas 28 and 29" } },
      ...(o.noRows ? {} : { pr: {
        "2:mamquam-river:whole": ["All | Apr 1 to Mar 31 | Bait ban", "Chinook | Sep 19 to Dec 31 | 1 per day"],
        "2:default:areas-28-29": [AREAS] } }),
    } as unknown as SalmonFacts["payload"],
  };
}
const NOV19 = new Date(2026, 10, 19);
const JUL1 = new Date(2026, 6, 1);
const card = (f: SalmonFacts, item: string | null, gear: readonly ("bait" | "fly" | "none")[] = [], on = NOV19) =>
  salmonCard(f, { sids: [1], item, gear }, on);
const annualLine = (c: ReturnType<typeof card>) => salmonTable(c).bucket?.tiers?.map((t) => t.line);

describe("(T870) Source (DFO) lists every DFO row and header note behind the card's answers", () => {
  it("each place of the chain with its rows as printed, then the page header's notes the card's lines read (never a dropped one)", () => {
    expect(card(facts(), "gnis:M").sources).toEqual([
      { from: "Region 2 · Mamquam River", lines: ["All | Apr 1 to Mar 31 | Bait ban", "Chinook | Sep 19 to Dec 31 | 1 per day"] },
      { from: "Region 2 · Region 2 waters flowing into Areas 28 and 29", lines: [AREAS] },
      { from: "Region 2 Lower Mainland · page header", lines: [DAYLIGHT, DAILY, ANNUAL, ADULT] },
    ]);
  });
  it("a water's own header note (the Vedder's adult length) is listed on its water only", () => {
    expect(card(facts(), "gnis:V").sources!.at(-1)).toEqual(
      { from: "Region 2 Lower Mainland · page header", lines: [DAYLIGHT, DAILY, ANNUAL, ADULT, `${ADULT.split(": ")[0]}: the Vedder River.`] });
  });
  it("a release before T870 (no `pr`): the header notes only, no place rows", () => {
    expect(card(facts({ noRows: true }), "gnis:M").sources!.map((g) => g.from)).toEqual(["Region 2 Lower Mainland · page header"]);
  });
});

describe("(T870 items 2 and 3) each bait-ban line says whose it is, the same ban once", () => {
  it("DFO's plain bait ban and the B.C. rules' bait ban on one day: one line naming both, no B.C. bait line", () => {
    const c = card(facts(), "gnis:M", ["bait"]);
    expect([c.gearLines, c.gearProvincial]).toEqual([["Bait ban: no bait of any kind — DFO (all year) and B.C. rules"], null]);
    expect(c.gear).toEqual(["Bait ban (all year)"]);                      // the data line itself is DFO's, unchanged
  });
  it("the B.C. ban on part of the page only: the one line keeps where the B.C. rules hold", () => {
    const c = card(facts(), "gnis:M", ["bait", "none"]);
    expect([c.gearLines, c.gearProvincial]).toEqual([["Bait ban: no bait of any kind — DFO (all year) and B.C. rules on parts of this water"], null]);
  });
  it("a place with rows missing from the file's places is a bad file: an error, never named another way", () => {
    const f = facts();
    // (a chain naming its answering places, `a`, is refused on decode without them; one without `a` reaches the fold)
    const { a: _a, ...chain } = f.payload.chains[1]!;
    const bad = { ...f, payload: { ...f.payload, chains: [f.payload.chains[0]!, chain],
                                   p: { "2:default:areas-28-29": f.payload.p!["2:default:areas-28-29"]! } } } as SalmonFacts;
    expect(() => card(bad, "gnis:M")).toThrow("salmon: place 2:mamquam-river:whole has rows (pr) and is not in p");
  });
  it("DFO's alone: its line says DFO; the B.C. rules' alone: their line as before", () => {
    expect(card(facts(), "gnis:M").gearLines).toEqual(["Bait ban (all year) — DFO"]);
    const bc = card(facts({ gear: "Single barbless hook (all year)" }), "gnis:M", ["bait"]);
    expect([bc.gearLines, bc.gearProvincial]).toEqual([["Single barbless hook (all year) — DFO"],
                                                       "B.C. rules, also for salmon: Bait ban: no bait of any kind."]);
  });
  it("a fly-only day keeps the B.C. fly line, the bait ban said once with DFO's", () => {
    const c = card(facts({ gear: "Bait ban (Aug 1–Oct 31)" }), "gnis:M", ["fly", "bait"]);
    expect([c.gearLines, c.gearProvincial]).toEqual([["Bait ban: no bait of any kind — DFO (Aug 1–Oct 31) and B.C. rules"],
                                                     "B.C. rules, also for salmon: Fly fishing only or artificial flies only — no bait."]);
  });
});

describe("(T869) the annual chinook limit counts adult chinook, and says where an adult is another length here", () => {
  it("a water where an adult is the limit's own length: the line alone; the chinook sizes say over 50 cm", () => {
    const c = card(facts(), "gnis:M");
    expect([c.adultHere, annualLine(c), salmonTable(c).species[0]!.limit]).toEqual([
      null, ["10 adult chinook a year from all fresh waters"], "Sizes (DFO): chinook 30 cm or more to keep, adult chinook over 50 cm"]);
  });
  it("the Vedder (over 62 cm, no dates): the line says so; its chinook sizes say over 62 cm", () => {
    const c = card(facts(), "gnis:V");
    expect([annualLine(c), salmonTable(c).species[0]!.limit]).toEqual([
      ["10 adult chinook a year from all fresh waters (here an adult is over 62 cm)"],
      "Sizes (DFO): chinook 30 cm or more to keep, adult chinook over 62 cm"]);
  });
  it("the Fraser (over 62 cm Sep 1–Dec 31): the line says the window; the sizes follow the day", () => {
    const c = card(facts(), "gnis:F");
    expect(annualLine(c)).toEqual(["10 adult chinook a year from all fresh waters (here an adult is over 62 cm, Sep 1–Dec 31)"]);
    expect(salmonTable(c).species[0]!.limit).toBe("Sizes (DFO): chinook 30 cm or more to keep, adult chinook over 62 cm");
    expect(card(facts(), "gnis:F", [], JUL1).adultHere).toBe("(here an adult is over 62 cm, Sep 1–Dec 31)");
  });
  it("a region whose own adult is longer (Region 6: over 65 cm): said on its waters; with a dated scoped length, the rest of the year", () => {
    expect(card(facts({ regionAdult: 65 }), "gnis:M").adultHere).toBe("(here an adult is over 65 cm)");
    expect(card(facts({ regionAdult: 65 }), "gnis:F").adultHere)
      .toBe("(here an adult is over 62 cm, Sep 1–Dec 31; over 65 cm the rest of the year)");
  });
});
