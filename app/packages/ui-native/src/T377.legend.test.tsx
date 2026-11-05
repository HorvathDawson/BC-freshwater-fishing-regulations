/**
 * T377 — the regulations key is tappable: a tap on a class shows it alone on the map (`col.isolate`), the entry says so
 * (aria-pressed, the whole sentence), and the other classes' entries fade as their water does. Gear marks and not-fishable
 * are marks, not classes: never tappable.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { KEY_ISOLATE_WORDS, MAP_KEY } from "@app/core";
import { MapLegend } from "./shell/MapLegend";
import { LIGHT } from "./theme";

afterEach(cleanup);

const col = (regsLayer: "standard" | "salmon", isolated: string | null, isolate = vi.fn()) => ({
  flowParam: "discharge", horizon: 0, mm: { onConditions: false }, cond: { marks: [] }, gearMarked: true, regsLayer,
  salmonOffered: true, statusOk: true, salmonFacts: null, setRegsLayer: () => {}, isolated, isolate,
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } },
}) as never;
const show = (c: unknown) =>
  render(<MapLegend col={c as never} conditions={false} palette={LIGHT} theme="light" mapDay={null} />);
const entry = (label: string) => screen.getAllByTestId("legend-entry").find((e) => e.textContent === label)!;

describe("(T377) the tappable key", () => {
  it("a tap on a status asks the map for it alone; the marks are not tappable", () => {
    const isolate = vi.fn();
    show(col("standard", null, isolate));
    fireEvent.click(entry(MAP_KEY.status.own));
    expect(isolate).toHaveBeenCalledWith("own");
    fireEvent.click(entry("Closed"));
    expect(isolate).toHaveBeenLastCalledWith("closed");
    for (const m of [MAP_KEY.gear.fly, MAP_KEY.gear.bait, MAP_KEY.notFishing])
      expect(entry(m).getAttribute("role"), m).not.toBe("button");
    expect(entry(MAP_KEY.status.own).getAttribute("aria-label")).toBe(KEY_ISOLATE_WORDS.entry(MAP_KEY.status.own, false));
  });

  it("the class shown alone is pressed and says so whole; the others fade", () => {
    show(col("standard", "closed"));
    const closed = entry("Closed");
    expect(closed.getAttribute("aria-pressed")).toBe("true");
    expect(closed.getAttribute("aria-label")).toBe("Closed, shown alone on the map. Tap to show every class.");
    expect(getComputedStyle(closed).opacity).toBe("1");
    expect(getComputedStyle(entry(MAP_KEY.status.own)).opacity).toBe("0.4");
    expect(entry(MAP_KEY.status.own).getAttribute("aria-pressed")).toBe("false");
  });

  it("Salmon's classes the same way", () => {
    const isolate = vi.fn();
    show(col("salmon", "release", isolate));
    expect(entry(MAP_KEY.salmon.release).getAttribute("aria-pressed")).toBe("true");
    fireEvent.click(entry(MAP_KEY.salmon.open));
    expect(isolate).toHaveBeenCalledWith("open");
  });

  it("the key keeps its two rows and seven entries when tappable", () => {
    show(col("standard", "own"));
    expect(screen.getAllByTestId("legend-row")).toHaveLength(2);
    expect(screen.getAllByTestId("legend-entry")).toHaveLength(7);
  });
});

describe("(T377) the regulations bar folds", () => {
  it("the fold hides the key's rows and keeps the switch; again shows them; the same in both modes", () => {
    for (const mode of ["standard", "salmon"] as const) {
      show(col(mode, null));
      const fold = screen.getByTestId("legend-fold");
      expect(fold.getAttribute("aria-expanded")).toBe("true");
      expect(fold.getAttribute("aria-label")).toBe(MAP_KEY.fold.hide);
      expect(screen.getAllByTestId("legend-row")).toHaveLength(2);
      fireEvent.click(fold);
      expect(screen.queryByTestId("legend-rows")).toBeNull();
      expect(screen.getByTestId("regs-paint")).toBeTruthy();
      expect(fold.getAttribute("aria-expanded")).toBe("false");
      expect(fold.getAttribute("aria-label")).toBe(MAP_KEY.fold.show);
      fireEvent.click(fold);
      expect(screen.getAllByTestId("legend-row")).toHaveLength(2);
      cleanup();
    }
  });
});
