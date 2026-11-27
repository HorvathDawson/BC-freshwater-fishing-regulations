/**
 * T1212 (owner 2026-11-26: "do tiday with the attribution moved to botton corner now") — OPTION A, "TIDY": the map's
 * controls on one 14 px / 44 px grid. On a phone-wide window the web map draws no + / − and the rail starts level with the
 * day (8 px under the bar, 14 px in); a wide window keeps the zoom stack and the rail under it. The Conditions horizons are
 * 44 px like the day chip and the rail. The © goes where `shell/mapCorner.ts` says: the map's bottom-right corner beside the
 * floating key chip, above any bar across the foot.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { horizonChips, LayoutWidthContext } from "@app/ui";
import { LIGHT } from "./theme";

const mapProps: { last: Record<string, unknown> | null } = { last: null };
vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: (p: Record<string, unknown>) => { mapProps.last = p; return null; },
}));
const { MapScreen } = await import("./MapScreen");
const { creditsInset, keyFloats } = await import("./shell/mapCorner");
const { MapLegend } = await import("./shell/MapLegend");

afterEach(cleanup);

const BASE = {
  at: { atlas: "a", basemap: "b" } as never, palette: LIGHT, theme: "light", view: "plain",
  camera: { lon: -122, lat: 49, zoom: 8 } as never, onLayers: () => {}, insetTop: 60,
};
const at = (width: number, el: React.ReactElement) => <LayoutWidthContext.Provider value={width}>{el}</LayoutWidthContext.Provider>;
const rail = () => { const s = (screen.getByTestId("map-rail-column") as HTMLElement).style; return [s.top, s.right]; };

describe("T1212 the rail on the grid, and + / − for wide windows only", () => {
  it("phone-wide (320, 412, 899): no + / −; the rail 8 px under the bar (level with the day), 14 px in", () => {
    for (const w of [320, 412, 899]) {
      render(at(w, <MapScreen {...BASE} />));
      expect([w, mapProps.last?.zoomButtons, ...rail()]).toEqual([w, false, "68px", "14px"]);
      cleanup();
    }
  });

  it("a tablet or desktop window keeps the web's zoom stack, and the rail stays under it (84 px, 10 px in)", () => {
    for (const w of [900, 1440]) {
      render(at(w, <MapScreen {...BASE} />));
      expect([w, mapProps.last?.zoomButtons, ...rail()]).toEqual([w, true, "144px", "10px"]);
      cleanup();
    }
  });

  it("the Conditions horizons are 44 px, the rail's height (42 inside the strip's 1 px edge)", () => {
    render(at(412, <MapScreen {...BASE} horizons={{ chips: horizonChips(0), onPick: () => {} }} />));
    const cells = [...screen.getByTestId("map-horizons").children].map((s) => getComputedStyle(s.firstElementChild as Element).minHeight);
    expect(cells).toEqual(["42px", "42px", "42px"]);
  });

  it("the © inset reaches the map as given (MapProps.insetCredits), apart from the foot's", () => {
    render(at(412, <MapScreen {...BASE} insetBottom={54} insetCredits={0} />));
    expect([mapProps.last?.insetBottom, mapProps.last?.insetCredits]).toEqual([54, 0]);
  });
});

describe("T1212 the © corner (shell/mapCorner.ts)", () => {
  it("beside the Map tab's floating key the © takes the corner (0); over a bar, on Conditions or Runs, it stands above it", () => {
    const f = (tab: string, o: Partial<Parameters<typeof keyFloats>[0]> = {}) =>
      keyFloats({ tab, tabsRail: false, statusOk: true, waterDrawn: "regulations", ...o });
    expect([f("map"), f("conditions"), f("runs"), f("map", { tabsRail: true }), f("map", { statusOk: false }),
            f("map", { waterDrawn: "lakes" })]).toEqual([true, false, false, false, false, false]);
    expect([creditsInset(true, 54), creditsInset(false, 182)]).toEqual([0, 182]);
  });
});

describe("T1212 the © rule and the key agree; the key chip is on the 14 px edge", () => {
  const col = (over: Record<string, unknown>) => ({
    flowParam: "discharge", horizon: 0, mm: { onConditions: false }, cond: { marks: [] }, gearMarked: true, regsLayer: "standard",
    salmonOffered: true, statusOk: true, salmonFacts: null, setRegsLayer: () => {}, isolated: null, isolate: () => {},
    layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles: false } } }, ...over,
  }) as never;
  it("MapLegend draws the floating key exactly where keyFloats says the © may take the corner (phone width)", () => {
    for (const [statusOk, drawn] of [[true, "regulations"], [false, "regulations"], [true, "lakes"]] as const) {
      render(at(412, <MapLegend col={col({ statusOk, layers: { water: { drawn }, effective: { groups: { obstacles: false } } } })}
                                conditions={false} palette={LIGHT} theme="light" mapDay={null} />));
      const drawnChip = screen.queryByTestId("map-key-chip") !== null;
      expect([statusOk, drawn, drawnChip]).toEqual([statusOk, drawn, keyFloats({ tab: "map", tabsRail: false, statusOk, waterDrawn: drawn })]);
      if (drawnChip) expect((screen.getByTestId("map-key") as HTMLElement).style.paddingLeft).toBe("14px");
      cleanup();
    }
  });
});

describe("T1212 the walkthrough's day step points at the chip and says it is tapped", () => {
  it("the chip registers the 'days' target; the step's words name the tap, not the arrows", async () => {
    const { CoachContext } = await import("./CoachMarks");
    const { MapDayStrip } = await import("./DayCalendar");
    const { guideSteps } = await import("@app/core");
    const set = vi.fn();
    render(<CoachContext.Provider value={{ set } as never}>
      <MapDayStrip palette={LIGHT} day={null} onDay={() => {}} onOpen={() => {}} />
    </CoachContext.Provider>);
    const days = set.mock.calls.filter((c) => c[0] === "days" && c[1] !== null);
    expect([days.length > 0, (days[0]?.[1] as Element | undefined)?.getAttribute?.("data-testid")]).toEqual([true, "map-day"]);
    const step = guideSteps("app").find((s) => s.key === "days");
    expect(step?.body).toBe("Tap the day to choose another; the map's colours and every page follow it.");
  });
});

describe("T1212 the opening credit line never meets the key row or the scale (review, 320 wide)", () => {
  it("beside a floating key (plain, or with its isolate row) it stands over the foot block, its width stopping short of the scale", async () => {
    const { MapCredits, resetStartupCredit, startupMaxWidth } = await import("../../map/src/MapCredits");
    const { mapChrome } = await import("@app/map");
    const { GEOMETRY: G } = await import("../../map/src/chrome");
    const { Dimensions } = await import("react-native");
    // at 320, 360 and 412 the widest the line may be leaves it clear of the scale bar's widest end (it wraps, never reaches)
    for (const width of [320, 360, 412])
      expect([width, width - G.right - startupMaxWidth(width) >= G.scaleLeft + G.scaleMax + G.gap]).toEqual([width, true]);
    const look = { view: "plain", modes: {}, groups: {}, basemap: "drawn" } as never;
    for (const foot of [54, 106]) {   // the key chip's row; with the isolate row ("Showing: … / Show all") over it
      resetStartupCredit();
      render(<MapCredits look={look} chrome={mapChrome("light")} insetBottom={0} footBottom={foot} />);
      const s = getComputedStyle(screen.getByTestId("map-credits-startup"));
      expect([foot, parseFloat(s.bottom) >= foot + G.bottom, parseFloat(s.maxWidth)])
        .toEqual([foot, true, startupMaxWidth(Dimensions.get("window").width)]);
      cleanup();
    }
  });
});
