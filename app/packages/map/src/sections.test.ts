/**
 * N2: what both maps agree a section IS on screen — the feature id (the MVT id the tiles now carry), read the same way
 * from either renderer. Android hands ids back as STRINGS (N1 §3), a feature with no id comes back as "" there, and GL JS
 * gives numbers; every one of those has to come out as the same number or nothing.
 */
import { describe, expect, it } from "vitest";
import { STYLE_META } from "./style";
import { SECTION_LAYERS, TAP_GROUPS, VISIBLE_LAYERS, firstSection, sectionIdOf, stateHandle, stateTarget, tapBox,
         visibleSections } from "./sections";

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
      "remove atlas/stream number:7 selected",
      "remove atlas/stream number:7 water",
      "remove atlas/stream number:8 selected",
      "remove atlas/stream number:8 water",
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
