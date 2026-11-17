/**
 * (T757 item 6) THE MINIMUM FEED SHAPE: a release that binds a closed-area piece (`p[l].piece` and `bound`, T573) needs
 * a feed that states `dfo.pieces >= 1`; without it the live feed is not used for salmon at all — the release's own file
 * for every chain, said on the card's feed line, reported once per feed read. A piece a feed WITH the shape lacks falls
 * through to the next place, as any missing place does (owner, T759: DFO removed the words, the remaining rule answers).
 * Hand-built payload in `pipeline/deliver/salmon.py` `build`'s shape: no app fixture carries the marker yet.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { setFailureSink } from "./failure";
import { salmonChainStatus, type SalmonFacts } from "./salmon";
import { composeChain, feedMeets, parseSalmonFeed, pickSalmon, requiredFeedShape, salmonFeedLine, salmonFeedOf, SALMON_FEED_FORMAT,
         type SalmonFeed } from "./salmonFeed";

const PIECE = "5:thompson#closed:deadman", PLACE = "5:thompson:whole", A = "5:a", B = "5:b", CARRIED = "5:babine#closed:circle";
const CLOSED_YEAR = [[0, 2], [0, 2], [0, 2], [0, 2], [0, 2]];
const OPEN_COHO = [[0, 0], [0, 1], [0, 0], [0, 0], [0, 0]];

const payload = (p: Record<string, object>) => ({
  format: "salmon/4", credit: "DFO", default: 0,
  regions: { "5": { name: "Thompson", asof: "2026-11-01", url: "https://example/region5", shared: [], sizes: [] } },
  verdicts: [
    { s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },   // 0
    { s: "open", t: "4 coho per day (Jan 1–Dec 31)" },                         // 1
    { s: "closed", t: "No fishing (Jan 1–Dec 31)" },                           // 2
  ],
  gears: [],
  chains: [
    { r: [], w: "", l: [], sp: CLOSED_YEAR.map(() => [0, 0]), g: [0, -1], m: [0, 3] },
    // 1: a section of the Thompson inside the closed area — the release's own calendar closes it
    { r: ["5"], w: "Thompson River", l: [PIECE, PLACE], sp: CLOSED_YEAR, g: [0, -1], m: [0, 3] },
    // 2: two ordinary places, narrowest first; the release closes it, the feed opens B
    { r: ["5"], w: "Other River", l: [A, B], sp: CLOSED_YEAR, g: [0, -1], m: [0, 3] },
    // 3: the carried piece's own unmapped chain
    { r: ["5"], w: "Babine River", l: [CARRIED], sp: CLOSED_YEAR, g: [0, -1], m: [0, 3], n: [0, 1] },
  ],
  unmapped: [{ items: ["gnis:1"], r: "5", w: "Babine River — the circle", chain: 3 }],
  p,
});
const MARKED = { [PIECE]: { r: "5", water: "Thompson River", piece: 1, bound: 1 }, [PLACE]: { r: "5", water: "Thompson River" },
                 [A]: { r: "5" }, [B]: { r: "5" }, [CARRIED]: { r: "5", water: "Babine River", piece: 1 } };
const NO_BOUND = { ...MARKED, [PIECE]: { r: "5", water: "Thompson River", piece: 1 } };       // every piece carried only
const UNMARKED = { ...MARKED, [PIECE]: { r: "5", water: "Thompson River" }, [CARRIED]: { r: "5", water: "Babine River" } };
const release = (p: Record<string, object> = MARKED): SalmonFacts => {
  const P = payload(p) as unknown as SalmonFacts["payload"];
  return { handles: "147b20dce7d8576c", chains: P.chains.length, payload: P, loaded: [],
           chainOf: (sid) => ({ 10: 1, 20: 2 } as Record<number, number>)[sid] ?? 0 };
};

const loc = (sp: number[][]) => ({ r: "5", sp, g: [0, -1] });
const raw = (locs: Record<string, object>, dfo?: unknown) => ({
  format: SALMON_FEED_FORMAT, at: "2026-11-15T18:00:00Z", ...(dfo === undefined ? {} : { dfo }),
  regions: { "5": { name: "Thompson", url: "https://example/region5", asof: "2026-11-14", checked: "2026-11-15T18:00:00Z", stale: false, held: null } },
  verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "open", t: "4 coho per day (Jan 1–Dec 31)" },
             { s: "closed", t: "No fishing (Jan 1–Dec 31)" }],
  gears: [], locs,
});
const feedOf = (locs: Record<string, object>, dfo?: unknown): SalmonFeed => parseSalmonFeed(raw(locs, dfo))!;
const ALL = { [PIECE]: loc(CLOSED_YEAR), [PLACE]: loc(OPEN_COHO), [B]: loc(OPEN_COHO) };

const DAYS = [0, 100, 200, 365];
const statusOn = (f: SalmonFacts, chain: number) => DAYS.map((d) => salmonChainStatus(f, chain, d));
const SAID = "The latest DFO check could not be used with this app’s data, so the DFO rules this data was built with are shown. Check DFO’s page before you fish.";

let reports: string[] = [];
let undo: () => void = () => {};
beforeEach(() => { reports = []; undo = setFailureSink((e) => reports.push((e as Error).message)); });
afterEach(() => undo());

describe("the minimum feed shape a release requires (T757 item 6)", () => {
  it("a release binding a piece, read with a feed that states no shape: the release's own file for every chain, said, reported once per feed read", () => {
    const r = release(), f = feedOf(ALL);
    const got = pickSalmon(r, f);
    expect(got.payload.chains).toEqual(r.payload.chains);          // every chain, not only the piece's
    expect([statusOn(got, 1), statusOn(got, 2)]).toEqual([["closed", "closed", "closed", "closed"], ["closed", "closed", "closed", "closed"]]);
    expect(salmonFeedOf(got)).toBeNull();
    expect(salmonFeedLine(got)).toBe(SAID);
    const said = "salmon feed: the release requires feed shape {\"pieces\":1}, the feed of 2026-11-15T18:00:00Z states {}; the release's own salmon file is shown";
    pickSalmon(r, f);
    pickSalmon({ ...r, loaded: ["5"] }, f);                          // another set of slices, same head and feed
    expect(reports).toEqual([said]);
    pickSalmon(r, feedOf(ALL));                                       // the next feed read
    expect(reports).toEqual([said, said]);
    pickSalmon(r, feedOf(ALL, { pieces: 0 }));
    expect(reports.length).toBe(3);
  });

  it("with the shape and the piece in the feed: from the feed", () => {
    const got = pickSalmon(release(), feedOf(ALL, { pieces: 1 }));
    expect(salmonFeedOf(got)?.at).toBe("2026-11-15T18:00:00Z");
    expect(statusOn(got, 1)).toEqual(["closed", "closed", "closed", "closed"]);
    expect(statusOn(got, 2)).toEqual(["open", "open", "open", "open"]);
    expect(salmonFeedLine(got)).toBe("Checked with DFO Nov 15, 2026, 11:00 am PT");
    expect(reports).toEqual([]);
  });

  it("with the shape and one piece missing from the feed: it falls through to the place, no report, no words", () => {
    const f = feedOf({ [PLACE]: loc(OPEN_COHO), [B]: loc(OPEN_COHO) }, { pieces: 1 });
    const got = pickSalmon(release(), f);
    const { l: _l, r: _r, w: _w, ...c } = got.payload.chains[1]!;
    expect(c).toEqual(composeChain([PIECE, PLACE], f));
    expect(statusOn(got, 1)).toEqual(["open", "open", "open", "open"]);
    expect(salmonFeedLine(got)).toBe("Checked with DFO Nov 15, 2026, 11:00 am PT");
    expect(reports).toEqual([]);
  });

  it("a release binding no piece ignores the field: old and new feeds both used", () => {
    for (const p of [NO_BOUND, UNMARKED]) for (const dfo of [undefined, { pieces: 1 }]) {
      const got = pickSalmon(release(p), feedOf(ALL, dfo));
      expect([salmonFeedOf(got)?.at, statusOn(got, 2)]).toEqual(["2026-11-15T18:00:00Z", ["open", "open", "open", "open"]]);
    }
    expect(reports).toEqual([]);
  });

  it("names the shape: a carried piece (no `bound`) and an old payload require nothing", () => {
    expect([requiredFeedShape(release().payload), requiredFeedShape(release(NO_BOUND).payload),
            requiredFeedShape(release(UNMARKED).payload)]).toEqual([{ pieces: 1 }, null, null]);
    expect([feedMeets(feedOf(ALL), null), feedMeets(feedOf(ALL), { pieces: 1 }), feedMeets(feedOf(ALL, { pieces: 1 }), { pieces: 1 }),
            feedMeets(feedOf(ALL, { pieces: 2 }), { pieces: 1 })]).toEqual([true, false, true, true]);
  });

  it("reads a feed with or without `dfo`; a malformed one is no feed", () => {
    expect(parseSalmonFeed(raw(ALL))?.dfo).toBeUndefined();
    expect(parseSalmonFeed(raw(ALL, { pieces: 1 }))?.dfo).toEqual({ pieces: 1 });
    expect([null, 1, "1", [1], { pieces: "1" }, { pieces: null }].map((d) => parseSalmonFeed(raw(ALL, d))))
      .toEqual([null, null, null, null, null, null]);
  });
});
