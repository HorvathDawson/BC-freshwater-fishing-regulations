/**
 * T49 THROUGH THE REAL SHELL: the map's calendar button and the Regulations page's date pill move ONE day (the Shell's
 * `mapDay`, D7). A day picked in the map's month grid colours the map for it (the status day the map is handed) and is
 * the day the Regulations page's pill shows; "Today" on the page's sheet puts the map back on today. Back closes the
 * date sheet before anything else.
 *
 * MUTATION PIN: the map and the page reading different days — a day state of the water screen's own, or a sheet that
 * moves a day the map does not read — fails here, because each step reads the OTHER surface.
 *
 * The map renderer is replaced by a line saying which day its status was handed, and a button that taps the
 * Chilliwack (no WebGL in jsdom). The status index is a stub on a proven pair; the regulations are the fixture release.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { Pressable, Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import type { StatusIndex } from "@app/core";
import { parseManifest, type ItemId } from "@app/data";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, type PairingInput, type StatusSource, type SystemBack } from "@app/ui";
import { LIGHT } from "./theme";

const TAP = { sid: 0 };
vi.mock("./MapScreen", () => ({
  MapScreen: (p: { status?: { day: Date } | null; onPressFeature?: (l: string, id: number) => void }) =>
    <View>
      <Text testID="map-status">{p.status ? p.status.day.toDateString() : "no status"}</Text>
      <Pressable accessibilityRole="button" accessibilityLabel="tap the map"
                 onPress={() => { void p.onPressFeature?.("streams", TAP.sid); }}><Text>map</Text></Pressable>
    </View>,
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
const { openRegsFixture } = await import("@app/data/release/node");

const H = "d768d6e135e62b99";
const INDEX = { handles: H, reachDigest: "", sections: 0, waters: 0, profiles: 0, codeOn: () => "base",
                profileOf: () => null, profileCodeOn: () => "base", waterCodeOn: () => "base" } as unknown as StatusIndex;
const STATUS: StatusSource = { key: "t49-status", load: async () => INDEX };
const PAIRING: PairingInput = {
  kind: "release",
  manifest: parseManifest({
    format: "app-manifest/2", data_release: "r-t49",
    tiles: { compatible: [`${H}.*`], assets: null, mask: null,
             current: { id: `${H}.aaa`, url: `tiles/${H}.aaa/atlas.pmtiles`, bytes: 1, sha256: "x" } },
    basemap: null, files: {}, regulations: { valid_until: null, versions: null },
  }),
  held: { tiles: null, basemap: null, release: "r-1", files: {} },
};

let androidBack: () => boolean = () => false;
const android: SystemBack = (back) => { androidBack = back; return { sync: () => {}, dispose: () => {} }; };

beforeAll(() => {
  // Wed Oct 21 2026: only the clock is faked, so the timers the render waits on still run
  vi.useFakeTimers({ toFake: ["Date"], now: new Date(2026, 9, 21, 9, 30) });
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});
afterAll(() => { vi.useRealTimers(); });
afterEach(cleanup);

/** A closed sheet slides out on react-native-web and is gone at the end of its animation, which jsdom never plays. */
const slidOut = () => act(() => {
  for (const el of document.querySelectorAll("div"))
    for (const type of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(type, { bubbles: true }));
});
/** What the reader sees of the day: the map's status day, its calendar button, the legend's closed line, the sheet. */
const look = () => {
  slidOut();
  const button = screen.queryAllByRole("button").find((b) => /^Map coloured for /.test(b.getAttribute("aria-label") ?? ""));
  const closed = screen.queryByText(/^Closed (today|on )/)?.textContent ?? "no legend";
  const sheet = screen.queryByText("Which day?") ? `sheet: ${screen.getByText(/^Showing /).textContent}` : "no sheet";
  return `map ${screen.queryByTestId("map-status")?.textContent ?? "gone"} | button ${button?.textContent ?? "none"}`
    + ` | ${closed} | ${sheet}`;
};
/** Android's back, as `BackHandler` reaches the one back: whether the app handled it. */
const back = () => { let handled = false; act(() => { handled = androidBack(); }); return handled; };
const press = (name: string) => act(() => { fireEvent.click(screen.getByRole("button", { name })); });

describe("the one day, from the map's calendar and the page's pill", () => {
  it("a day picked on the map colours the map and is the page's day; the page's Today is the map's today", async () => {
    const source = makeFixtureSource();
    TAP.sid = (await source.water("gnis:8634" as ItemId))!.sections[0]!;
    render(
      <GestureHandlerRootView>
        <Shell source={source} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} systemBack={android}
               status={STATUS} pairing={PAIRING} loader={openRegsFixture()} />
      </GestureHandlerRootView>);
    await waitFor(() => expect(screen.getByTestId("map-status").textContent).toBe("Wed Oct 21 2026"));
    const seen: string[] = [look()];

    press("Map coloured for today. Choose a day");
    seen.push(look());
    press("Saturday, October 10");
    seen.push(look());
    // back closes the sheet first, and only the sheet
    expect(back()).toBe(true);
    seen.push(look());

    // the water opens on the map's day: the page's pill says Oct 10
    await act(async () => { fireEvent.click(screen.getByLabelText("tap the map")); });
    await waitFor(() => expect(screen.getByTestId("regs-header")).toBeTruthy(), { timeout: 20_000 });
    const pill = () => within(screen.getByTestId("regs-header")).getByRole("button", { name: /^Change date, now / });
    seen.push(`pill ${pill().textContent}`);
    // the pill's sheet is the same calendar, on the same day; its Today puts THE day back on today
    fireEvent.click(pill());
    seen.push(look());
    press("Back to today");
    await waitFor(() => expect(pill().textContent).toBe("Today ▾Oct 21"));
    seen.push(`pill ${pill().textContent}`);
    // the page's reload for the new day did not close its sheet: back closes it, then leaves the water
    seen.push(`back ${back()}: ${look()} | ${screen.queryByTestId("regs-header") ? "on the water" : "left"}`);
    seen.push(`back ${back()}`);
    await waitFor(() => expect(screen.getByTestId("map-status")).toBeTruthy());
    seen.push(look());

    expect(seen).toEqual([
      "map Wed Oct 21 2026 | button Today | Closed today | no sheet",
      "map Wed Oct 21 2026 | button Today | Closed today | sheet: Showing today, Wednesday, October 21",
      "map Sat Oct 10 2026 | button Sat, Oct 10 | Closed on Oct 10 | sheet: Showing Saturday, October 10",
      "map Sat Oct 10 2026 | button Sat, Oct 10 | Closed on Oct 10 | no sheet",
      "pill Sat ▾Oct 10",
      "map gone | button none | no legend | sheet: Showing Saturday, October 10",
      "pill Today ▾Oct 21",
      "back true: map gone | button none | no legend | no sheet | on the water",
      "back true",
      "map Wed Oct 21 2026 | button Today | Closed today | no sheet",
    ]);
  }, 40_000);
});
