/**
 * (T812 / T885 item 2) THE CLOSED-AREA CIRCLES the Salmon map draws: the release's `ca` (`deliver.salmon.closed_area_circles`)
 * on the days the piece's own rule speaks, in its own chain's status; under a live feed only while today's page names the
 * piece (owner 2026-11-10: "if they remove it circles should go away"). Hand-built payload in `build`'s shape.
 */
import { describe, expect, it } from "vitest";
import { decodeSalmonHead, salmonCirclesOn, type SalmonFacts } from "./salmon";
import { parseSalmonFeed, SALMON_FEED_FORMAT, withSalmonFeed } from "./salmonFeed";

const CIRCLE = "6:babine-lake#closed:f4c347aad97f", DATED = "6:babine-lake#closed:00503bc8b63e", LAKE = "6:babine-lake:whole";
const RING: [number, number][] = [[-126.28, 55.15], [-126.27, 55.15], [-126.27, 55.16], [-126.28, 55.15]];
const ALL_YEAR = [[0, 2], [0, 2], [0, 2], [0, 2], [0, 2]];
// "No fishing" Aug 1 (day 213) to Aug 31 only; "not opened" (d) on every other day
const AUGUST = [[0, 0, 213, 2, 244, 0], [0, 0, 213, 2, 244, 0], [0, 0, 213, 2, 244, 0], [0, 0, 213, 2, 244, 0], [0, 0, 213, 2, 244, 0]];

const payload = () => ({
  format: "salmon/4", credit: "DFO", default: 0,
  regions: { "6": { name: "Skeena", asof: "2026-11-01", url: "https://example/region6", shared: [], sizes: [] } },
  verdicts: [
    { s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },   // 0
    { s: "open", t: "2 sockeye per day" },                                     // 1
    { s: "closed", t: "No fishing for salmon" },                               // 2
  ],
  gears: [],
  chains: [
    { r: [], w: "", l: [], sp: ALL_YEAR.map(() => [0, 0]), g: [0, -1], m: [0, 3] },
    { r: ["6"], w: "Babine Lake", l: [LAKE], sp: [[0, 0], [0, 0], [0, 1], [0, 0], [0, 0]], g: [0, -1], m: [0, 1], u: [CIRCLE] },
    { r: ["6"], w: "Babine Lake — Morrison Creek", l: [CIRCLE], sp: ALL_YEAR, g: [0, -1], m: [0, 3] },
    { r: ["6"], w: "Babine Lake — Twain Creek", l: [DATED], sp: AUGUST, g: [0, -1], m: [0, 3] },
  ],
  unmapped: [],
  p: { [LAKE]: { r: "6", water: "Babine Lake" }, [CIRCLE]: { r: "6", water: "Babine Lake", piece: 1 },
       [DATED]: { r: "6", water: "Babine Lake", piece: 1 } },
  ca: {
    [CIRCLE]: { c: [-126.275, 55.152], radius: 400, g: RING, chain: 2, r: "6", w: "Babine Lake — Morrison Creek", items: ["wbk:-24"] },
    [DATED]: { c: [-125.81, 54.61], radius: 400, g: RING, chain: 3, r: "6", w: "Babine Lake — Twain Creek", items: ["wbk:-24"] },
  },
});
const facts = (P: object): SalmonFacts =>
  ({ handles: "0011223344556677", chains: (P as { chains: unknown[] }).chains.length, payload: P as SalmonFacts["payload"],
     loaded: [], chainOf: () => 0 });

/** the head bytes `encode_files` writes, for `decodeSalmonHead`'s check */
const head = (P: object): Uint8Array => {
  const json = new TextEncoder().encode(JSON.stringify(P));
  const b = new Uint8Array(17 + json.length);
  b.set([0x42, 0x43, 0x53, 0x41, 4, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77]);
  new DataView(b.buffer).setUint32(13, json.length, true);
  b.set(json, 17);
  return b;
};

describe("the closed-area circles (T812)", () => {
  it("each circle on the days its own rule speaks, in its own chain's status — never the lake's", () => {
    const f = facts(payload());
    expect(salmonCirclesOn(f, 100)).toEqual([{ id: CIRCLE, ring: RING, status: "closed" }]);
    expect(salmonCirclesOn(f, 220).map((c) => [c.id, c.status])).toEqual([[DATED, "closed"], [CIRCLE, "closed"]]);
    expect(salmonCirclesOn(f, 244).map((c) => c.id)).toEqual([CIRCLE]);          // the day after its season: not drawn
  });

  it("a release with no circles draws none", () => {
    const { ca: _ca, ...P } = payload();
    expect(salmonCirclesOn(facts(P), 100)).toEqual([]);
  });

  it("the file is checked on decode: a ring that is not closed, or a chain the file does not hold, is refused", () => {
    expect(() => decodeSalmonHead(head(payload()), "0011223344556677")).not.toThrow();
    const open = payload();
    (open.ca[CIRCLE] as { g: unknown }).g = RING.slice(0, 3);
    expect(() => decodeSalmonHead(head(open), "0011223344556677")).toThrow(/ca 6:babine-lake#closed:f4c347aad97f/);
    // a ring of four points whose last is not its first: the closure check itself refuses it (not the length check)
    const unclosed = payload();
    (unclosed.ca[CIRCLE] as { g: unknown }).g = [...RING.slice(0, 3), [RING[0]![0] + 0.001, RING[0]![1]]];
    expect(() => decodeSalmonHead(head(unclosed), "0011223344556677")).toThrow(/ca 6:babine-lake#closed:f4c347aad97f/);
    const lost = payload();
    (lost.ca[CIRCLE] as { chain: number }).chain = 9;
    expect(() => decodeSalmonHead(head(lost), "0011223344556677")).toThrow(/ca /);
  });

  it("under a live feed, a circle shows only while today's page names its piece", () => {
    const feed = parseSalmonFeed({
      format: SALMON_FEED_FORMAT, at: "2026-11-15T18:00:00Z", dfo: { pieces: 1 },
      regions: { "6": { name: "Skeena", url: "https://example/region6", asof: "2026-11-14", checked: "2026-11-15T18:00:00Z", stale: false, held: null } },
      verdicts: [{ s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 }, { s: "open", t: "2 sockeye per day" },
                 { s: "closed", t: "No fishing for salmon" }],
      gears: [], locs: { [LAKE]: { r: "6", sp: [[0, 0], [0, 0], [0, 1], [0, 0], [0, 0]], g: [0, -1] },
                         [CIRCLE]: { r: "6", sp: ALL_YEAR, g: [0, -1] } },
    })!;
    const got = withSalmonFeed(facts(payload()), feed);
    expect(Object.keys(got.payload.ca ?? {})).toEqual([CIRCLE]);
    expect(salmonCirclesOn(got, 220).map((c) => c.id)).toEqual([CIRCLE]);
  });
});
