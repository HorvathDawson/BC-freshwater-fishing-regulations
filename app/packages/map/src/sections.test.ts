/**
 * N2: what both maps agree a section IS on screen — the feature id (the MVT id the tiles now carry), read the same way
 * from either renderer. Android hands ids back as STRINGS (N1 §3), a feature with no id comes back as "" there, and GL JS
 * gives numbers; every one of those has to come out as the same number or nothing.
 */
import { describe, expect, it } from "vitest";
import { STYLE_META } from "./style";
import { SECTION_LAYERS, TAP_GROUPS, TAP_RINGS, pickSection, VISIBLE_LAYERS, firstSection, isTapTarget, sectionIdOf, stateHandle,
         stateTarget, tapBox, visibleSections } from "./sections";

describe("a section id off a rendered feature", () => {
  it("is a number from a number, a numeric string (Android), and nothing from anything else", () => {
    expect([311274, "311274", "1566515", 0, "", " ", null, undefined, "12:3400", "abc", -1, 1.5, "NaN", Infinity]
      .map(sectionIdOf))
      .toEqual([311274, 311274, 1566515, 0, null, null, null, null, null, null, null, null, null, null]);
  });
  it("the first hit with an id wins; id-less hits are skipped", () => {
    expect([firstSection([{ id: "" }, { id: undefined }, { id: "302029" }, { id: 7 }]),
            firstSection([{ id: "" }, {}]), firstSection([])]).toEqual([302029, null, null]);
  });
  it("visible sections are de-duplicated numbers in first-seen order", () => {
    expect(visibleSections([{ id: "5" }, { id: 3 }, { id: 5 }, { id: "" }, { id: "3" }, {}, { id: 9 }]))
      .toEqual([5, 3, 9]);
  });
});

describe("the layers a section is read from", () => {
  it("the section layers are exactly the highlightable ones (section_id), and only they", () => {
    expect([...SECTION_LAYERS].sort()).toEqual(["lake", "lake_flowing", "stream", "wetland", "wetland_flowing"]);
    expect([...SECTION_LAYERS].sort()).toEqual(STYLE_META.highlightable.map((h) => h.id).sort());
  });
  it("a tap tries the stream group first (a stream's own polygons included), then still water", () => {
    expect(TAP_GROUPS).toEqual([
      { as: "stream", layers: ["stream", "lake_flowing", "wetland_flowing"] },
      { as: "lake", layers: ["lake", "wetland"] },
    ]);
    // every section layer is tappable exactly once: a new section layer with no place in the order fails here
    expect(TAP_GROUPS.flatMap((g) => g.layers).sort()).toEqual([...SECTION_LAYERS].sort());
  });
  it("the tap box is 24 px: 12 each way of the finger", () => {
    expect(tapBox(100, 200)).toEqual([[88, 188], [112, 212]]);
  });
  it("visible water is streams, lakes and the polygons a stream flows through — not marsh", () => {
    expect(VISIBLE_LAYERS).toEqual(["stream", "lake", "lake_flowing", "wetland_flowing"]);
  });
  it("the shared state handle: numbers for section keys, other keys as given, and a clear removes only the highlight's keys", () => {
    const log: string[] = [];
    const h = stateHandle({
      set: (t, id, s) => log.push(`set ${t.source}/${t.sourceLayer} ${typeof id}:${id} ${JSON.stringify(s)}`),
      remove: (t, id, k) => log.push(`remove ${t.source}/${t.sourceLayer} ${typeof id}:${id} ${k}`),
    }, () => ["7", 8]);
    h.setFeatureState("lake_flowing", "311274", { status: "open" });
    h.setFeatureState("basin", "08MF-a", { standing: 2 });
    h.setFeatureState("nope", 1, { x: 1 });
    h.clearFeatureStates("stream");
    expect(log).toEqual([
      'set atlas/lake number:311274 {"status":"open"}',
      'set basins/basin string:08MF-a {"standing":2}',
      // (T151) `part`: a water's Map view's part, cleared with the rest
      "remove atlas/stream number:7 selected",
      "remove atlas/stream number:7 part",
      "remove atlas/stream number:7 water",
      // (T459) `near`: a focused water map's receiving water
      "remove atlas/stream number:7 near",
      "remove atlas/stream number:7 inseason",
      "remove atlas/stream number:8 selected",
      "remove atlas/stream number:8 part",
      "remove atlas/stream number:8 water",
      "remove atlas/stream number:8 near",
      "remove atlas/stream number:8 inseason",
    ]);
  });
  it("feature-state goes to the layer's own source and SOURCE-layer", () => {
    expect(["stream", "lake", "lake_flowing", "wetland", "wetland_flowing", "basin", "nope"].map(stateTarget))
      .toEqual([
        { source: "atlas", sourceLayer: "stream" }, { source: "atlas", sourceLayer: "lake" },
        { source: "atlas", sourceLayer: "lake" }, { source: "atlas", sourceLayer: "wetland" },
        { source: "atlas", sourceLayer: "wetland" }, { source: "basins", sourceLayer: "basin" }, null,
      ]);
  });
});

describe("(T621, G4) the nearest water under a press wins, on both maps", () => {
  const half = (b: [[number, number], [number, number]]) => (b[1][0] - b[0][0]) / 2;
  it("one water in the box: one query, that water", async () => {
    const seen: number[] = [];
    const got = await pickSection(async (b, g) => { seen.push(half(b)); return g.as === "stream" ? [{ id: 4 }] : []; }, 100, 100);
    expect([got, seen]).toEqual([{ layer: "stream", id: 4 }, [12]]);
  });
  it("two waters: the smallest ring holding one decides, whatever order the engine lists the wide box in", async () => {
    const seen: number[] = [];
    const got = await pickSection(async (b, g) => {
      seen.push(half(b));
      if (g.as !== "stream") return [];
      return half(b) <= TAP_RINGS[0]! ? [{ id: 11 }] : [{ id: 99 }, { id: 11 }];     // the far creek listed first
    }, 100, 100);
    expect([got, seen]).toEqual([{ layer: "stream", id: 11 }, [12, 2]]);
  });
  it("two waters, none in the inner rings: the wide box's first hit, as before", async () => {
    const got = await pickSection(async (b, g) => g.as === "stream" && half(b) === 12 ? [{ id: 9 }, { id: 8 }] : [], 0, 0);
    expect(got).toEqual({ layer: "stream", id: 9 });
  });
  it("a stream in the box beats a lake; nothing, and a missing map, answer null", async () => {
    expect(await pickSection(async (b, g) => (g.as === "lake" ? [{ id: 5 }] : [{ id: "6" }]), 0, 0))
      .toEqual({ layer: "stream", id: 6 });
    expect(await pickSection(async () => [], 0, 0)).toBeNull();
    expect(await pickSection(async () => null, 0, 0)).toBeNull();
  });
});

describe("(T689) the test bed's tap target for the lit water (Map.native findTarget)", () => {
  const lake = new Set([501, 502]);       // a lake lit whole: two parts
  const river = new Set([7]);
  it("a lit lake answers on its own water: still water under the box that IS the lit lake is no reason to skip", () => {
    // before T689 any still water under the box skipped the point, so a lake (all still water) never had a target:
    // build 29's regs-open lake step, "Cultus", 9/10 twice
    expect(isTapTarget({ id: 501 }, lake, [{ id: "501" }])).toBe(true);
    expect(isTapTarget({ id: 502 }, lake, [{ id: 502 }, { id: "501" }])).toBe(true);
  });
  it("(T478) a lit river is never targeted where another water's still water is under the box", () => {
    expect(isTapTarget({ id: 7 }, river, [{ id: "88" }])).toBe(false);          // a lake's tip
    expect(isTapTarget({ id: 501 }, lake, [{ id: "501" }, { id: 88 }])).toBe(false);  // the lit lake beside another one
    expect(isTapTarget({ id: 501 }, lake, [{ id: "501" }, { id: "" }])).toBe(false);  // a still water with no id: not known lit
  });
  it("only a hit on the lit water counts; no still water at all is fine; a failed query is no target", () => {
    expect(isTapTarget({ id: 7 }, river, [])).toBe(true);
    expect(isTapTarget({ id: 8 }, river, [])).toBe(false);
    expect(isTapTarget(null, river, [])).toBe(false);
    expect(isTapTarget({ id: 7 }, river, null)).toBe(false);
  });
});
