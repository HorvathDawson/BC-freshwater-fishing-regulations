/**
 * (T692, owner 2026-11-12: "Keep fill, no tap sheet") The tidal water fill stays drawn and labelled "Tidal water";
 * tapping it does nothing. No renderer asks the fill, no prop carries a tidal press, no core sheet words remain.
 */
import { describe, expect, it } from "vitest";
import * as sections from "./sections";
import { nativeRenderer } from "./adapters/native";
import { webRenderer } from "./adapters/web";
import { MAP_STYLE } from "./style";
import * as core from "@app/core";

describe("tidal water fill is inert (T692)", () => {
  it("is still drawn: the tidal_water layer is in the style", () => {
    expect(JSON.stringify(MAP_STYLE)).toContain("tidal_water");
  });
  it("asks nothing of it on a tap: no tidal layer list, no tidal query on either renderer", () => {
    expect("TIDAL_LAYERS" in sections).toBe(false);
    expect("anyTidal" in sections).toBe(false);
    const gl = { queryRenderedFeatures: () => [], getLayer: () => ({}) } as never;
    expect("pressTidal" in webRenderer(gl, 14)).toBe(false);
    expect("pressTidal" in nativeRenderer({} as never, () => {})).toBe(false);
  });
  it("core has no tap sheet for it, and the legend does not promise one", () => {
    expect("tidalSheet" in core).toBe(false);
    expect(core.HELP_WORDS.tidal).not.toMatch(/tap/i);
  });
});
