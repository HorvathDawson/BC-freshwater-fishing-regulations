/**
 * (T759, `@app/core/regs/testing`) A release and a live feed carrying `issues`, built by hand in the shape of the T759 ticket's example
 * (`handoff/PLAN/app-integration/tickets/T759-…md`, "For the worker and app"): no fixture of a feed with issues exists
 * yet (the worker lane's vectors will give real ones). Shared by the core and render tests.
 *
 * Sections: 10, 11 the Campbell River (chains 1, 2; item gnis:100); 20 Babine Lake (chain 3, whose `u` lists the
 * undrawn place 6:babine:undrawn; item gnis:600); 30 the Held River (chain 4, Region 3; item gnis:300); 40 another
 * river (chain 5; item gnis:999).
 */
import type { SalmonFacts } from "../../salmon";
import { parseSalmonFeed, SALMON_FEED_FORMAT, type SalmonFeed } from "../../salmonFeed";

const all = (v: number) => [[0, v], [0, v], [0, v], [0, v], [0, v]];
const COHO_OPEN = [[0, 0], [0, 1], [0, 0], [0, 0], [0, 0]];
const VERDICTS = [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },
                  { s: "open", t: "4 hatchery-marked coho per day (Jul 1–Oct 31)" },
                  { s: "closed", t: "No fishing (Jan 1–Dec 31)" }];

export const ISSUE_SIDS = { campbell: 10, campbell2: 11, babine: 20, held: 30, other: 40 } as const;
export const ISSUE_ITEMS = { campbell: "gnis:100", babine: "gnis:600", held: "gnis:300", other: "gnis:999" } as const;

export function issueRelease(): SalmonFacts {
  const chainOf: Record<number, number> = { 10: 1, 11: 2, 20: 3, 30: 4, 40: 5 };
  const payload = {
    format: "salmon/4", credit: "DFO", default: 0,
    regions: Object.fromEntries([["1", "Vancouver Island"], ["3", "Lower Fraser"], ["6", "Skeena"]].map(([r, name]) =>
      [r, { name, asof: "2026-11-01", url: `https://example/region${r}`, shared: [], sizes: [] }])),
    verdicts: VERDICTS, gears: [],
    chains: [
      { r: [], w: "", l: [], sp: all(0), g: [0, -1], m: [0, 3] },
      { r: ["1"], w: "Campbell River", l: ["1:campbell:lower"], sp: COHO_OPEN, g: [0, -1], m: [0, 1] },
      { r: ["1"], w: "Campbell River", l: ["1:campbell:upper"], sp: all(2), g: [0, -1], m: [0, 3] },
      { r: ["6"], w: "Babine Lake", l: ["6:babine:whole"], u: ["6:babine:undrawn"], sp: all(2), g: [0, -1], m: [0, 3] },
      { r: ["3"], w: "Held River", l: ["3:held:whole"], sp: COHO_OPEN, g: [0, -1], m: [0, 1] },
      { r: ["1"], w: "Other River", l: ["1:other:whole"], sp: COHO_OPEN, g: [0, -1], m: [0, 1] },
      { r: ["6"], w: "Babine Lake — the 400 m mouths", l: ["6:babine:undrawn"], sp: all(2), g: [0, -1], m: [0, 3], n: [0, 1] },
    ],
    unmapped: [{ items: [ISSUE_ITEMS.babine], r: "6", w: "Babine Lake — the 400 m mouths", chain: 6 }],
  } as unknown as SalmonFacts["payload"];
  return { handles: "147b20dce7d8576c", chains: payload.chains.length, payload, loaded: [], chainOf: (sid) => chainOf[sid] ?? 0 };
}

/** The Campbell's three issues, listed out of `key` order (the app sorts them). */
export const CAMPBELL_ISSUES = [
  { kind: "unmatched", key: "unmatched:fp2", fingerprint: "fp2", why: "near_place_taken", place: "1:campbell:lower",
    water: "Campbell River", area: "from the end of Maple Street downstream to the confluence with the Quinsam River.",
    rows: ["All | Aug 1 to Oct 31 | No fishing"] },
  { kind: "drift", key: "drift:fp1", fingerprint: "fp1", place: "1:campbell:upper", water: "Campbell River",
    area: "from a fishing boundary sign at the  cement block upstream.", rows: ["Coho | Jul 1 to Oct 31 | 2 per day", "Chinook | All year | 0"] },
  { kind: "closed_area_unbound", key: "closed_area_unbound:1:campbell:upper#closed:a1b2c3d4e5f6", fingerprint: "fp3",
    piece: "1:campbell:upper#closed:a1b2c3d4e5f6", water: "Campbell River", area: "Pierre Ck.",
    rows: ["No fishing for salmon — within a 400 m radius of the mouth of Pierre Ck."] },
];

export const ISSUES = {
  waters: {
    "1:campbell-river": { r: "1", name: "Campbell River", items: [ISSUE_ITEMS.campbell], list: CAMPBELL_ISSUES },
    "6:babine-lake": { r: "6", name: "Babine Lake", items: [ISSUE_ITEMS.babine], list: [
      { kind: "unmatched", key: "unmatched:fpb1", why: "undrawn", location_id: "6:babine:undrawn", water: "Babine Lake",
        area: "within 400 m of the mouths.", rows: ["All | All year | No fishing"] }] },
    "3:held-river": { r: "3", name: "Held River", items: [ISSUE_ITEMS.held], list: [
      { kind: "unmatched", key: "unmatched:fph", why: "new", water: "Held River", area: "below the falls.", rows: ["All | All year | No fishing"] }] },
  },
  no_water: [{ kind: "unmatched", why: "new_water", water: "Nowhere Creek", area: "all of it.", rows: ["All | All year | 2 per day"] }],
  unread: { "3": "held" },
};

export function issueFeedRaw(issues: unknown): Record<string, unknown> {
  const loc = (r: string, sp: number[][]) => ({ r, sp, g: [0, -1] });
  return {
    format: SALMON_FEED_FORMAT, at: "2026-11-15T18:00:00Z",
    regions: Object.fromEntries(["1", "3", "6"].map((r) => [r, { name: `Region ${r}`, url: `https://example/region${r}`, asof: "2026-11-14",
                                                                checked: "2026-11-15T18:00:00Z", stale: r === "3", held: null }])),
    verdicts: VERDICTS, gears: [],
    locs: { "1:campbell:lower": loc("1", COHO_OPEN), "1:campbell:upper": loc("1", all(2)), "6:babine:whole": loc("6", all(2)),
            "6:babine:undrawn": loc("6", all(2)), "3:held:whole": loc("3", COHO_OPEN), "1:other:whole": loc("1", COHO_OPEN) },
    ...(issues === undefined ? {} : { issues }),
  };
}

export const issueFeed = (issues: unknown = ISSUES): SalmonFeed => parseSalmonFeed(issueFeedRaw(issues))!;
