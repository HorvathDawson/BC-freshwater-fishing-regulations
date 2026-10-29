/**
 * (T235) The map's credits follow what it draws: the always-drawn sources carry theirs, and a mode's or an overlay's
 * credit is on exactly while the look draws it — on the web by the layer's visibility (MapLibre lists the sources a
 * visible layer reads), on a phone by the style itself (MapLibre Native lists every source's credit, so a hidden one is
 * dropped).
 */
import { describe, expect, it } from "vitest";
import { ATLAS_CREDIT, BASEMAP_CREDIT, creditId, MAP_CREDITS_DRAWN } from "./credits";
import { nativeStyle } from "./native-style";
import { runtimeStyle } from "./runtime-style";

type S = { layers: { id: string; layout?: Record<string, unknown>; paint?: Record<string, unknown> }[];
           sources: Record<string, { attribution?: string }> };
const AT = { atlas: "file:///a.pmtiles", basemap: "file:///b.pmtiles" };
const base = { at: AT, theme: "light", view: "plain", modes: {}, hide: [], groups: {}, basemap: "map" as const,
               gauges: '{"type":"FeatureCollection","features":[]}' };
const credited = (s: S) => Object.entries(s.sources).filter(([, v]) => v.attribution).map(([k]) => k).sort();

describe("the map's credits (T235)", () => {
  it("credits OpenStreetMap (ODbL wording, linked) and the Province on the always-drawn sources", () => {
    const s = runtimeStyle(AT, "light") as unknown as S;
    expect(s.sources.basemap!.attribution).toBe(BASEMAP_CREDIT);
    expect(BASEMAP_CREDIT).toContain("© OpenStreetMap contributors");
    expect(BASEMAP_CREDIT).toContain("https://www.openstreetmap.org/copyright");
    expect(s.sources.atlas!.attribution).toBe(ATLAS_CREDIT);
    expect(ATLAS_CREDIT).toContain("OGL-BC");
    expect(ATLAS_CREDIT).toContain("OpenStreetMap contributors");
  });

  it("gives each mode/overlay credit its own source and a layer that draws nothing, hidden by default", () => {
    const s = runtimeStyle(AT, "light") as unknown as S;
    for (const c of MAP_CREDITS_DRAWN) {
      expect(s.sources[creditId(c)]!.attribution).toBe(c.html);
      const l = s.layers.find((x) => x.id === creditId(c))!;
      expect(l.layout!.visibility).toBe("none");
      expect(l.paint!["circle-opacity"]).toBe(0);
    }
  });

  it("on a phone: a credit stays only while the look draws its data", () => {
    const plain = nativeStyle(base).style as unknown as S;
    expect(credited(plain)).toEqual(["atlas", "basemap", "gauges"].filter((k) => plain.sources[k]?.attribution).sort());
    expect(credited(plain).some((k) => k.startsWith("credit-"))).toBe(false);

    const salmon = nativeStyle({ ...base, modes: { stream: "salmon" } }).style as unknown as S;
    expect(credited(salmon)).toContain("credit-salmon");
    expect(credited(salmon)).not.toContain("credit-obstacles");

    const obstacles = nativeStyle({ ...base, groups: { obstacles: true } }).style as unknown as S;
    expect(credited(obstacles)).toContain("credit-obstacles");
    expect(obstacles.layers.find((l) => l.id === "credit-obstacles")!.layout!.visibility).toBe("visible");
    expect(credited(obstacles)).not.toContain("credit-salmon");
  });

  it("names the publisher and the licence the data comes under", () => {
    const by = Object.fromEntries(MAP_CREDITS_DRAWN.map((c) => [c.id, c.html]));
    expect(by.salmon).toContain("Fisheries and Oceans Canada");
    expect(by.runs).toContain("Pacific Salmon Foundation");
    expect(by.obstacles).toMatch(/FISS, WRIS, PSCIS.*OGL-BC/);
    expect(by.stocking).toContain("OGL-BC");
  });
});
