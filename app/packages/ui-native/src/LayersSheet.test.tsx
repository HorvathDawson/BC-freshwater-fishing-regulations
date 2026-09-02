/**
 * Every Layers choice must name something the map actually has.
 *
 * The bug this exists for: "Depth" reads like a colouring but is a LAYER, and passing its
 * key through as a colour mode threw `layer "lake" has no colour mode "depth"` from inside
 * a render effect — which unmounts the tree. Tapping one option in a menu turned the whole
 * app into a blank screen.
 *
 * A menu is a list of promises about what the map can do. This checks them against the
 * generated style, which is the only thing that knows.
 */
import { describe, expect, it } from "vitest";
import { STYLE_META } from "@app/map";
import { lakeChoices, streamChoices } from "./LayersSheet";
import { LIGHT } from "./theme";

const modesFor = (layer: string) => Object.keys(STYLE_META.colorModes[layer] ?? {});
const groupIds = new Set(STYLE_META.groups.map((g) => g.id));

describe("Layers choices", () => {
  it("only offer colour modes the stream layer has", () => {
    for (const c of streamChoices(LIGHT))
      expect(modesFor("stream"), `stream choice "${c.k}" -> mode "${c.mode}"`).toContain(c.mode);
  });

  it("only offer colour modes the lake layer has", () => {
    for (const c of lakeChoices(LIGHT))
      expect(modesFor("lake"), `lake choice "${c.k}" -> mode "${c.mode}"`).toContain(c.mode);
  });

  it("only switch on layer groups that exist", () => {
    for (const c of [...streamChoices(LIGHT), ...lakeChoices(LIGHT)])
      if (c.group) expect(groupIds, `choice "${c.k}" -> group "${c.group}"`).toContain(c.group);
  });

  it("gives Depth a real layer to turn on AND a real colouring", () => {
    const depth = lakeChoices(LIGHT).find((c) => c.k === "depth");
    expect(depth, "the Depth choice is gone").toBeTruthy();
    // the contour layer
    expect(depth!.group, "Depth turns on no layer, so it would do nothing").toBe("depth");
    // and a colouring that answers "was this lake surveyed at all" — contours alone show a
    // near-empty map, because most of the 2,741 sheets were never traced into vectors
    expect(modesFor("lake"), `Depth -> mode "${depth!.mode}"`).toContain(depth!.mode);
    expect(depth!.mode).not.toBe("plain");
  });

  it("gives every choice three swatches, because the swatches are the label", () => {
    for (const c of [...streamChoices(LIGHT), ...lakeChoices(LIGHT)]) {
      expect(c.swatch, c.k).toHaveLength(3);
      for (const s of c.swatch) expect(s, `${c.k} swatch`).toMatch(/^#[0-9A-Fa-f]{6}$/);
    }
  });
});
