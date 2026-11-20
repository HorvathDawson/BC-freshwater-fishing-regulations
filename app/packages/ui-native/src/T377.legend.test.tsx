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

describe("(T896) the regulations key is always shown, with no fold", () => {
  it("no fold button in either mode, and the whole key is there", () => {
    for (const mode of ["standard", "salmon"] as const) {
      show(col(mode, null));
      expect(screen.queryByTestId("legend-fold")).toBeNull();
      expect(screen.getAllByTestId("legend-entry")).toHaveLength(7);
      cleanup();
    }
  });
});

describe("(T425) the legend bar holds the legend only", () => {
  it("the Trout / Salmon switch is on the map's rail, not in the bar, in both modes", () => {
    for (const mode of ["standard", "salmon"] as const) {
      show(col(mode, null));
      expect(screen.queryByTestId("regs-paint")).toBeNull();
      expect(screen.queryByTestId("regs-paint-salmon")).toBeNull();
      cleanup();
    }
  });
});

describe("(T896) the key is visible at a phone's width", () => {
  it("390: no fold button, T396's two rows and all seven entries (the one-row desktop key is pictured, trout-1440.png)", () => {
    show(col("standard", null));
    expect(screen.queryByTestId("legend-fold")).toBeNull();
    expect(screen.getAllByTestId("legend-row")).toHaveLength(2);
    expect(screen.getAllByTestId("legend-entry")).toHaveLength(7);
  });
});

describe("(T396, T896) both modes' bars are the same two rows on a phone, nothing reserved", () => {
  it("no head or reserve, and no checked date in the key (owner 2026-11-20)", () => {
    for (const mode of ["standard", "salmon"] as const) {
      show(col(mode, null));
      expect(screen.getAllByTestId("legend-row"), mode).toHaveLength(2);
      expect(screen.queryByTestId("legend-fishery-head"), mode).toBeNull();
      expect(screen.queryByTestId("legend-reserve"), mode).toBeNull();
      expect(screen.queryByTestId("salmon-legend-source"), mode).toBeNull();
      cleanup();
    }
  });
});
