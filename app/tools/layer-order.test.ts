/**
 * BOUNDARIES UNDER THE WATER'S STATUS, OVER THE FILLS (T83; T47 part 1 question 4, owner call 11 default).
 *
 * T47 made the region and unit boundaries neutral grey so a boundary never reads as a closure, and said they are drawn
 * "under the water's status colour". They were not: `region` and `mu` sat last of the geometry layers, so a grey
 * boundary crossed every status line it met — a closed river was cut into pieces by a dotted grey unit line laid on
 * top of it, and the colour that is the answer lost to the line that only says where a table changes.
 *
 * The order now, bottom to top, for the layers that meet at a boundary:
 *
 *   fills       lake, lake_flowing (and every area fill below them) — a boundary must stay traceable across a lake,
 *               so it is drawn OVER the fill (the reason the boundaries were moved up from first place, see `mu`)
 *   boundaries  region, region__edge, mu, mu__edge
 *   water lines lake__edge (a lake's status shoreline, `outline.drawAfter`), the selection ring, T175's water copy
 *               (its casing and line, under the water's own), the selection casing, stream,
 *               stream-nobaseline, stream-noaccess (T165's dots) — the answer, drawn last, so where a boundary crosses a
 *               water the water's colour wins
 *
 * `faults` is a pure function of a layer order, so the mutation pin runs it on the order before T83.
 */
import { describe, expect, it } from "vitest";
import { SELECTION_CASING, SELECTION_RINGS } from "../packages/map/src/paint";
import { runtimeStyle } from "../packages/map/src/runtime-style";
import { MAP_STYLE } from "../packages/map/src/style";
import { COPY_CASING, COPY_FILL, COPY_LINE } from "../packages/map/src/water-copy";

const BOUNDARIES = ["region", "region__edge", "mu", "mu__edge"];
/** The water's polygons: a boundary is drawn over these. */
const FILLS = ["lake", "lake_flowing", "not_fishing_water", "wetland", "wetland_flowing", COPY_FILL];
/** The lines that carry a water's colour (status, flow, stocking) — and what outlines a selected one. */
const WATER_LINES = ["lake__edge", SELECTION_RINGS.lake, COPY_CASING, COPY_LINE, SELECTION_CASING, "stream",
                     "stream-nobaseline", "stream-noaccess"];

/** Every way an order breaks the rule, one sentence each; empty = the order is right. Layers absent are skipped. */
export function faults(ids: readonly string[]): string[] {
  const at = (id: string) => ids.indexOf(id);
  const out: string[] = [];
  for (const b of BOUNDARIES.filter((x) => at(x) >= 0)) {
    for (const f of FILLS.filter((x) => at(x) >= 0))
      if (at(f) > at(b)) out.push(`${b} is under the ${f} fill`);
    for (const w of WATER_LINES.filter((x) => at(x) >= 0))
      if (at(w) < at(b)) out.push(`${b} is over the water line ${w}`);
  }
  return out;
}

const geometry = (layers: readonly { id: string; type: string }[]) =>
  layers.filter((l) => l.type !== "symbol").map((l) => l.id);

describe("boundaries sit under the water's status lines and over its fills (T83)", () => {
  it("the generated style, from the lakes to the streams, whole", () => {
    const ids = geometry(MAP_STYLE.layers as { id: string; type: string }[]);
    expect(ids.slice(ids.indexOf("lake"), ids.indexOf("stream") + 1)).toEqual([
      "lake", "lake_flowing", "not_fishing_water", "contour", "under_lake",
      "region", "region__edge", "mu", "mu__edge",
      "lake__edge", "stream"]);
    expect(faults(ids)).toEqual([]);
  });

  it("the style the map is actually given — with the selection outlines, the no-baseline dash and the no-access dots — in every theme", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const s = runtimeStyle({ atlas: "a.pmtiles", basemap: "b.pmtiles" } as never, theme) as unknown as
        { layers: { id: string; type: string }[] };
      const ids = geometry(s.layers);
      expect([theme, faults(ids)]).toEqual([theme, []]);
      expect([theme, ids.slice(ids.indexOf("mu__edge"), ids.indexOf("stream-noaccess") + 1)]).toEqual([theme,
        ["mu__edge", "lake__edge", SELECTION_RINGS.lake, COPY_CASING, COPY_LINE, SELECTION_CASING, "stream",
         "stream-nobaseline", "stream-noaccess"]]);
      // (T175) the water copy directly UNDER the water's own layers: the lake's under the lake, the line under the casing
      expect([theme, ids.indexOf(COPY_FILL) === ids.indexOf("lake") - 1]).toEqual([theme, true]);
    }
  });

  it("fails the order before T83, boundaries over every water line (mutation pin)", () => {
    const before = ["lake", "lake__edge", "lake_flowing", SELECTION_RINGS.lake, "contour", "under_lake",
                    SELECTION_CASING, "stream", "stream-nobaseline", "region", "region__edge", "mu", "mu__edge"];
    expect(faults(before)).toEqual(BOUNDARIES.flatMap((b) =>
      ["lake__edge", SELECTION_RINGS.lake, SELECTION_CASING, "stream", "stream-nobaseline"]
        .map((w) => `${b} is over the water line ${w}`)));
  });

  it("fails boundaries moved under the lakes, the order before v1's fix (mutation pin)", () => {
    expect(faults(["region", "region__edge", "lake", "lake_flowing", "lake__edge", "stream"])).toEqual([
      "region is under the lake fill", "region is under the lake_flowing fill",
      "region__edge is under the lake fill", "region__edge is under the lake_flowing fill"]);
  });
});
