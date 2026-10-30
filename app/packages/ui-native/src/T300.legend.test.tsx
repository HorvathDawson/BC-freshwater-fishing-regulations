/**
 * T300 — the map key in few words, the fish obstacles' marks in it when that layer is on, the Runs map's vertical key,
 * and the More tab's copy of the map's sync button.
 */
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { MAP_KEY, OBSTACLE_ICON_KINDS, syncVM, type DataInput } from "@app/core";
import { LayoutWidthContext } from "@app/ui";
import { MapLegend, RunsKey } from "./shell/MapLegend";
import { MiniMap } from "./MiniMap";
import { MoreScreen } from "./menu/MoreScreen";
import { LIGHT } from "./theme";

afterEach(cleanup);

const col = (obstacles: boolean) => ({
  flowParam: "discharge", horizon: 0, mm: { onConditions: false }, cond: { marks: [] }, gearMarked: true, regsLayer: "standard",
  layers: { water: { drawn: "regulations" }, effective: { groups: { obstacles } } },
}) as never;
const legend = (obstacles: boolean) => render(
  <MapLegend col={col(obstacles)} conditions={false} palette={LIGHT} theme="light" mapDay={null} />);

describe("the map key (T300)", () => {
  it("says its rows in few words", () => {
    legend(false);
    const t = screen.getByTestId("legend-strip").textContent!;
    for (const w of ["Closed", MAP_KEY.status.own, MAP_KEY.status.base, MAP_KEY.status.no_access, MAP_KEY.gear.fly, MAP_KEY.gear.bait, MAP_KEY.notFishing])
      expect(t).toContain(w);
    expect(t).not.toContain("regulations");
    expect(t.length).toBeLessThan(90);
  });
  it("lists the fish obstacles' marks, one icon and a word each, only while that layer is on", () => {
    legend(false);
    expect(screen.queryByTestId("legend-obstacles")).toBeNull();
    cleanup();
    legend(true);
    const row = screen.getByTestId("legend-obstacles");
    expect(row.textContent).toBe(OBSTACLE_ICON_KINDS.map((k) => MAP_KEY.obstacle[k]).join(""));
    expect(row.querySelectorAll("svg").length).toBe(OBSTACLE_ICON_KINDS.length);
  });
});

describe("the Runs map's key (T300)", () => {
  it("is a vertical bar of the four states, each with its plain label, and takes no touch", () => {
    render(<RunsKey theme="light" palette={LIGHT} bottom={120} />);
    const key = screen.getByTestId("runs-key");
    expect(key.textContent).toBe(MAP_KEY.runs.rows.map((r) => r.label).join(""));
    expect(key.getAttribute("aria-label")).toBe(MAP_KEY.runs.describe);
    expect(getComputedStyle(key).pointerEvents).toBe("none");
  });
});

describe("the More tab's sync button (T300)", () => {
  const NOW = Date.now();
  const DATA: DataInput = {
    sources: { synopsis: { title: "Synopsis", date: null }, basemap: { date: "2026-03-05" } },
    validUntil: null, tiles: "d768d6e135e62b99.c9f9604f12bf", tilesPaired: true,
    feedFetchedAt: new Date(NOW - 3.6e6).toISOString(), now: NOW, newRelease: null, forecastIssuedAt: new Date(NOW - 3.6e6).toISOString(),
  };
  const sync = syncVM({ feed: true, feedFetchedAt: DATA.feedFetchedAt!, checkedAt: null, checking: false, newRelease: null, now: NOW });
  const more = (onSync: () => void, s = sync) => render(
    <LayoutWidthContext.Provider value={null}>
      <MoreScreen palette={LIGHT} data={DATA} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}}
                  onPick={() => {}} sync={s} onSync={onSync} />
    </LayoutWidthContext.Provider>);
  it("is the map's own button in the same state, and a press checks now", () => {
    const onSync = vi.fn();
    more(onSync);
    const b = screen.getByTestId("more-sync-button");
    expect(b.getAttribute("aria-label")).toBe(sync.label);
    fireEvent.click(b);
    expect(onSync).toHaveBeenCalledTimes(1);
  });
  it("shows the checking state as the map's does: disabled and busy", () => {
    const checking = syncVM({ feed: true, feedFetchedAt: DATA.feedFetchedAt!, checkedAt: null, checking: true, newRelease: null, now: NOW });
    more(() => {}, checking);
    expect(screen.getByTestId("more-sync-button").getAttribute("aria-busy")).toBe("true");
  });
  it("is not drawn with nothing to sync", () => {
    render(
      <LayoutWidthContext.Provider value={null}>
        <MoreScreen palette={LIGHT} data={DATA} back="Map" onBack={() => {}} contact={null} theme="light" onTheme={() => {}} onPick={() => {}} />
      </LayoutWidthContext.Provider>);
    expect(screen.queryByTestId("more-sync-button")).toBeNull();
  });
});

describe("a map inside a swiping page keeps its own touches on the web (T300)", () => {
  it("its box is touch-action none, so the page's pan-y does not take a vertical drag or a pinch from the map", async () => {
    render(<MiniMap at={{ atlas: "a", basemap: "b" } as never} palette={LIGHT} theme="light" hint="the map"
                    camera={{ lat: 49, lon: -122, zoom: 9 } as never} />);
    const box = screen.getByText("the map").parentElement!.parentElement!;
    expect(box.style.touchAction).toBe("none");
  });
});
