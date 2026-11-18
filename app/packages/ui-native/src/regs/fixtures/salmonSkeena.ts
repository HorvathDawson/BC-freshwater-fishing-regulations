/**
 * (T829) A salmon file the tests build themselves: the Skeena below Terrace on Aug 15 as the owner's mockup showed it
 * (scratch t829/sheet-B), cut down — chinook and chum closed, coho 4 a day only 2 over 50 cm (Jul 15–Aug 31), sockeye
 * open with no number of its own (an in-season notice, FN0787), pink 2 a day; Region 6's day total of 4 and 8 in
 * possession; DFO's eulachon closure; the provincial bait ban that day (`gear: ["bait"]`). Shapes as core reads them
 * (`SalmonFacts`, salmon/4); every word is DFO's as the release carries it.
 */
import { salmonCard, type SalmonCardVM, type SalmonFacts } from "@app/core";

export const EULACHON = "Eulachon closed all year: No fishing for eulachon";
export const BAIT = "B.C. rules, also for salmon: Bait ban: no bait of any kind.";

const tier = (t: string, n: number, says: string, more: object = {}) =>
  ({ node: "tier", id: `${t}${n}`, tier: t, n, says, by: 0, children: [], ...more });

export const SKEENA_FACTS: SalmonFacts = {
  handles: "0".repeat(16), chains: 2, chainOf: (sid) => (sid === 1 ? 1 : 0), loaded: [],
  payload: {
    format: "salmon/4", credit: "Fisheries and Oceans Canada (DFO), freshwater salmon sport fishing regulations", default: 0,
    regions: { "6": { name: "Skeena", asof: "2026-09-02", url: "https://example/6", shared: [],
                      sizes: [{ sp: ["CO"], min: { min_cm: 30 } }],
                      bucket: { tree: tier("total", 4, "Up to 4 salmon a day in total, fresh and tidal waters combined"), possession: 8,
                                t: "The daily limit for all species of Pacific salmon from tidal and fresh waters combined is 4." } } },
    verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
               { s: "closed", t: "No fishing for chinook (Apr 1–Mar 31)" },
               { s: "open", t: "4 per day, only 2 over 50 cm (Jul 15–Aug 31)",
                 tree: tier("count", 4, "4 a day", { species: ["CO"], children: [tier("size", 2, "only 2 over 50 cm", { over_cm: 50 })] }) },
               { s: "open", t: "4 per day (Jul 29–Aug 16)", n: "FN0787" },
               { s: "open", t: "2 per day (Apr 1–Mar 31)", tree: tier("count", 2, "2 a day", { species: ["PK"] }) },
               { s: "closed", t: "No fishing for chum (Apr 1–Mar 31)" }],
    gears: [[EULACHON]],
    chains: [
      { r: [], w: "", l: [], sp: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]], g: [0, -1], m: [0, 3] },
      { r: ["6"], w: "Skeena River", l: ["6:skeena"], sp: [[0, 1], [0, 2], [0, 3], [0, 4], [0, 5]], g: [0, -1], o: [0, 0], m: [0, 1],
        a: [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]] },
    ],
    p: { "6:skeena": { r: "6", water: "Skeena River", sec: "B(ii)" } },
    unmapped: [], scopes: {},
  } as never,
};

/** the card on Aug 15, the provincial bait ban in force */
export const skeenaCard = (): SalmonCardVM => salmonCard(SKEENA_FACTS, { sids: [1], item: null, gear: ["bait"] }, new Date(2026, 7, 15));
