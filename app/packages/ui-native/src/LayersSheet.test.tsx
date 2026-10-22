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
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { STYLE_META } from "@app/map";
import { STOCK_RECENCY, decodeLakeFacts } from "@app/core";
import { lakeModesReady, type LakeModesReady } from "@app/ui";
import { LayersSheet, STOCK_BANDS, lakeChoices, type LayersState } from "./LayersSheet";
import { LIGHT } from "./theme";

const modesFor = (layer: string) => Object.keys(STYLE_META.colorModes[layer] ?? {});
const groupIds = new Set(STYLE_META.groups.map((g) => g.id));

describe("Layers choices", () => {
  it("only offer colour modes the lake layer has", () => {
    for (const c of lakeChoices(LIGHT))
      expect(modesFor("lake"), `lake choice "${c.k}" -> mode "${c.mode}"`).toContain(c.mode);
  });

  it("only switch on layer groups that exist", () => {
    for (const c of lakeChoices(LIGHT))
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

  it("the Stocked legend's bands are the ones the map colours by (@app/core STOCK_RECENCY), in the style's order", () => {
    expect(STOCK_BANDS.map((b) => b.days)).toEqual(STOCK_RECENCY.map((b) => b.days));
    expect(STOCK_RECENCY.map((b) => b.band)).toEqual(
      (STYLE_META as unknown as { enums: Record<string, string[]> }).enums.stock_recency!.filter((x) => x !== "never"));
  });

  it("gives every choice three swatches, because the swatches are the label", () => {
    for (const c of lakeChoices(LIGHT)) {
      expect(c.swatch, c.k).toHaveLength(3);
      for (const s of c.swatch) expect(s, `${c.k} swatch`).toMatch(/^#[0-9A-Fa-f]{6}$/);
    }
  });
});

/*
 * THE LAKE MODES' GATE (owner 2026-10-21: Stocked / Depth prepared, not fed). While the loaded release carries no lake
 * values the two modes are greyed, not pressable, said disabled to a screen reader (`aria-disabled`, AGENTS 34), and one
 * line says why; a release that carries values enables them with no app change.
 */
describe("Layers: the lake modes' gate", () => {
  afterEach(cleanup);
  const mount = (ready: LakeModesReady, lake = "plain") => {
    const set: LayersState[] = [];
    render(<LayersSheet open onClose={() => {}} palette={LIGHT} state={{ lake, basemap: "map" }}
                        onState={(s) => set.push(s)} theme="light" onTheme={() => {}} ready={ready} />);
    const radios = screen.getAllByRole("radio").filter((r) => ["Depth", "Stocked", "Plain"].includes(r.textContent ?? ""))
      .map((r) => `${r.textContent}${r.getAttribute("aria-disabled") === "true" ? " (off)" : ""}`
                  + `${r.getAttribute("aria-checked") === "true" ? " (chosen)" : ""}`);
    return { set, radios };
  };
  const note = () => screen.queryByTestId("lake-modes-note")?.textContent ?? null;
  /** A synthetic lakes/1 release file: sid 12 a sheet, last stocked on day 4 — no feed makes one yet. */
  const FED = lakeModesReady({ day: 46314, facts: decodeLakeFacts(Uint8Array.from(
    [0x42, 0x43, 0x4c, 0x4b, 1, 0xd7, 0x68, 0xd6, 0xe1, 0x35, 0xe6, 0x2b, 0x99, 0, 1, 12, 1, 5]), "d768d6e135e62b99") });

  it("no lake values in the release: Depth and Stocked greyed and not pressable, and the note says why", () => {
    const { set, radios } = mount(lakeModesReady(null));
    expect(radios).toEqual(["Depth (off)", "Stocked (off)", "Plain (chosen)"]);
    expect(note()).toBe("No data yet for Depth and Stocked");
    fireEvent.click(screen.getByRole("radio", { name: "Stocked" }));
    fireEvent.click(screen.getByRole("radio", { name: "Depth" }));
    expect(set).toEqual([]);
  });

  it("a stale choice with no values shows Plain chosen and no Stocked legend", () => {
    mount(lakeModesReady(null), "stocked");
    expect(screen.queryByText("Stocked this season")).toBeNull();
    expect(screen.getByRole("radio", { name: "Plain" }).getAttribute("aria-checked")).toBe("true");
  });

  it("a release carrying lake values: both enabled, no note, a press chooses the mode", () => {
    const { set, radios } = mount(FED);
    expect(radios).toEqual(["Depth", "Stocked", "Plain (chosen)"]);
    expect(note()).toBeNull();
    fireEvent.click(screen.getByRole("radio", { name: "Stocked" }));
    expect(set).toEqual([{ lake: "stocked", basemap: "map" }]);
  });

  it("only the mode without values waits: survey values alone enable Depth, not Stocked", () => {
    const { radios } = mount({ depth: true, stocked: false });
    expect(radios).toEqual(["Depth", "Stocked (off)", "Plain (chosen)"]);
    expect(note()).toBe("No data yet for Stocked");
  });
});
