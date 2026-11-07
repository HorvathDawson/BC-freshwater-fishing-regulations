/**
 * (T476) THE DEVELOPER CARD behind the map credits' long press: a production build has no `onOpenLab`, no card; a
 * development or staging build opens it and reaches each of its four sections (Map lab, Data source / channel, Updates
 * log, Build info). The map is a stub (no WebGL here): it hands back the props the screen gave it, `onOpenLab` among them.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { setFailureSink } from "@app/core";
import { createUpdateLog } from "@app/data";
import { LIGHT } from "./theme";
import type { MenuInfo } from "./menu/MenuHost";
import type { UpdatesLogControls } from "./menu/UpdatesLogPage";

const gate = { on: true };
const mapProps: { last: Record<string, unknown> | null } = { last: null };
vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  get LAB_ON() { return gate.on; },
  Map: (p: Record<string, unknown>) => { mapProps.last = p; return null; },
}));

const { MapScreen } = await import("./MapScreen");

afterEach(() => { cleanup(); gate.on = true; mapProps.last = null; });

const BASE = {
  at: { atlas: "a", basemap: "b" } as never, palette: LIGHT, theme: "light", view: "plain",
  camera: { lon: -122, lat: 49, zoom: 8 } as never,
};
const snap = { checking: false, manual: null, feeds: [], release: null };
const picked: string[] = [];
const updatesLog: UpdatesLogControls = {
  log: createUpdateLog(), updates: { subscribe: () => () => {}, snapshot: () => snap } as never,
  header: () => ({ version: "0.6.2", platform: "android", env: "staging", origin: "https://data-staging.canifishthis.ca",
                   channel: "live", release: "r-1" }),
  channel: { current: "live", stored: null, pick: (k) => { picked.push(k); } },
  offline: { get: () => false, set() {} }, forceRedownload: async () => {}, clearCache: async () => {},
  apply() {}, copy: async () => true,
};
const info: MenuInfo = {
  appBuild: "0.6.2 (build 9)", contact: null, updatesLog,
  env: { about: { env: "staging", built: "staging", origin: "https://data-staging.canifishthis.ca", switchable: true },
         pending: null, pick: () => {} },
};
const screenWith = (developer = { info, release: "r-1" as string | null }) => render(<MapScreen {...BASE} developer={developer} />);
const text = (id: string) => screen.getByTestId(id).textContent;

describe("T476: the Developer card", () => {
  it("production: the map is handed no long-press handler and no card is drawn", () => {
    gate.on = false;
    screenWith();
    expect(mapProps.last!.onOpenLab).toBeUndefined();
    expect(screen.queryByTestId("developer-card")).toBeNull();
  });

  it("a build with no facts to show draws no card either", () => {
    render(<MapScreen {...BASE} />);
    expect(mapProps.last!.onOpenLab).toBeUndefined();
  });

  it("staging: the long press opens the card, each section is reachable, Close puts it away", () => {
    screenWith();
    expect(screen.queryByTestId("developer-card")).toBeNull();
    act(() => { (mapProps.last!.onOpenLab as () => void)(); });
    expect(screen.getByTestId("developer-card")).toBeTruthy();
    // Map lab (the first section)
    expect(text("map-lab-state")).toBe("Today's map.");
    // Data source / channel
    fireEvent.click(screen.getByTestId("developer-tab-source"));
    expect(text("dev-env-value")).toBe("Staging · data-staging.canifishthis.ca");
    fireEvent.click(screen.getByTestId("dev-env-local"));
    expect(text("dev-env-note")).toBe("Local is used from the next start. Close and reopen the app.");
    fireEvent.click(screen.getByTestId("updates-log-channel-test"));
    expect(picked).toEqual(["test"]);
    // Updates log
    fireEvent.click(screen.getByTestId("developer-tab-updates"));
    expect(screen.getByTestId("updates-log-rows")).toBeTruthy();
    expect(screen.queryByTestId("updates-log-channel-live")).toBeNull();
    // Build info
    fireEvent.click(screen.getByTestId("developer-tab-build"));
    expect(["app", "release", "env", "origin", "channel"].map((k) => text(`dev-build-${k}`)))
      .toEqual(["0.6.2 (build 9)", "r-1", "staging", "data-staging.canifishthis.ca", "live"]);
    fireEvent.click(screen.getByTestId("developer-close"));
    expect(screen.queryByTestId("developer-card")).toBeNull();
  });

  it("(T423) a data-source write that fails is shown and reported; a kept choice shows as pending on opening", () => {
    const seen: unknown[] = [];
    const off = setFailureSink((e) => seen.push(e));
    const boom = new Error("disk full");
    const about = info.env!.about;
    screenWith({ info: { ...info, env: { about, pending: null, pick: () => { throw boom; } } }, release: null });
    act(() => { (mapProps.last!.onOpenLab as () => void)(); });
    fireEvent.click(screen.getByTestId("developer-tab-source"));
    fireEvent.click(screen.getByTestId("dev-env-local"));
    expect([text("dev-env-failed"), seen]).toEqual(["Could not save Local. The data source is unchanged.", [boom]]);
    off();
    cleanup();
    screenWith({ info: { ...info, env: { about, pending: "local", pick() {} } }, release: null });
    act(() => { (mapProps.last!.onOpenLab as () => void)(); });
    fireEvent.click(screen.getByTestId("developer-tab-source"));
    expect(text("dev-env-note")).toBe("Local is used from the next start. Close and reopen the app.");
  });
});
