/**
 * (T940) THE KEY FOLLOWS THE GROUND: over satellite the regulations key's swatches are the satellite set (the same in every theme), and
 * the ones that would fall under 3:1 on the card carry an ink outline; on the drawn map the key is as it was (the theme's own colours,
 * no outline). `MapLegend` reads `col.mm.basemap`.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { MAP_KEY } from "@app/core";
import { resolveTheme } from "@app/map";
import { MapLegend } from "./shell/MapLegend";
import { CVD, DARK, LIGHT, type Palette } from "./theme";

afterEach(cleanup);

const col = (basemap: "map" | "satellite", regsLayer: "standard" | "salmon" = "standard", drawn = "regulations") => ({
  flowParam: "discharge", horizon: 0, mm: { onConditions: false, basemap }, cond: { marks: [] }, gearMarked: true, regsLayer,
  salmonOffered: true, statusOk: true, salmonFacts: null, setRegsLayer: () => {}, isolated: null, isolate: () => {},
  layers: { water: { drawn }, effective: { groups: { obstacles: false } } },
}) as never;
// (T1211) the Map tab's key floats behind its chip: drawn, then opened
const show = (basemap: "map" | "satellite", palette: Palette, theme: string, regsLayer: "standard" | "salmon" = "standard", drawn = "regulations") => {
  const r = render(<MapLegend col={col(basemap, regsLayer, drawn)} conditions={false} palette={palette} theme={theme} mapDay={null} />);
  const chip = screen.queryByTestId("map-key-chip"); if (chip) fireEvent.click(chip);
  return r;
};
const entry = (label: string) => screen.getAllByTestId("legend-entry").find((e) => e.textContent === label)!;
/** The colour of the status entry's swatch line: its first painted descendant. */
const swatchColour = (label: string): string => {
  const all = [entry(label), ...Array.from(entry(label).querySelectorAll("*"))] as HTMLElement[];
  const hit = all.map((e) => getComputedStyle(e).backgroundColor).find((c) => /^rgb\(/.test(c));
  return hit ?? "";
};
const rgb = (hex: string) => `rgb(${[1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16)).join(", ")})`;
const THEMES = [["light", LIGHT], ["dark", DARK], ["cvd", CVD]] as const;

describe("(T940) the satellite key", () => {
  it("over satellite the swatches are the satellite set in every theme; on the drawn map they are the theme's", () => {
    for (const [th, palette] of THEMES) {
      const t = resolveTheme(th);
      show("satellite", palette, th);
      expect([th, swatchColour(MAP_KEY.status.base)]).toEqual([th, rgb(String(t["color.imagery.status.base"]))]);
      expect([th, swatchColour(MAP_KEY.status.own)]).toEqual([th, rgb(String(t["color.imagery.status.own"]))]);
      cleanup();
      show("map", palette, th);
      expect([th, swatchColour(MAP_KEY.status.base)]).toEqual([th, rgb(String(t["color.status.base"]))]);
      expect(screen.queryAllByTestId("legend-outline")).toHaveLength(0);
      cleanup();
    }
  });

  it("over satellite the pale swatches wear an ink outline on the light card; dark's need none", () => {
    show("satellite", LIGHT, "light");
    expect(within(entry(MAP_KEY.status.base)).getAllByTestId("legend-outline")).toHaveLength(1);
    cleanup();
    show("satellite", DARK, "dark");
    expect(screen.queryAllByTestId("legend-outline")).toHaveLength(0);
  });

  it("the Salmon key follows the ground too: satellite colours, and an ink outline on the light card's pale ones", () => {
    const t = resolveTheme("light");
    show("satellite", LIGHT, "light", "salmon");
    const want = { [MAP_KEY.salmon.open]: "open", [MAP_KEY.salmon.release]: "restricted", [MAP_KEY.salmon.closed]: "closed" } as const;
    for (const [label, status] of Object.entries(want)) {
      expect([label, swatchColour(label)]).toEqual([label, rgb(String(t[`color.imagery.status.${status}`]))]);
      // closed (#FF3B6D) reaches 3:1 on the white card by itself: no outline; keep and release are pale: outlined
      expect([label, within(entry(label)).queryAllByTestId("legend-outline").length]).toEqual([label, status === "closed" ? 0 : 1]);
    }
    cleanup();
    show("satellite", DARK, "dark", "salmon");
    expect(screen.queryAllByTestId("legend-outline")).toHaveLength(0);
    cleanup();
    show("map", LIGHT, "light", "salmon");
    expect(screen.queryAllByTestId("legend-outline")).toHaveLength(0);
    expect([swatchColour(MAP_KEY.salmon.release)]).toEqual([rgb(String(t["color.salmon.release"]))]);
  });

  it("the other water keys (stocked, depth) are not status colours: no outline, whatever the ground", () => {
    for (const drawn of ["stocked", "depth"]) {
      show("satellite", LIGHT, "light", "standard", drawn);
      expect(screen.queryAllByTestId("legend-outline"), drawn).toHaveLength(0);
      expect(screen.getAllByTestId("legend-entry").length, drawn).toBeGreaterThan(0);
      cleanup();
    }
  });
});
