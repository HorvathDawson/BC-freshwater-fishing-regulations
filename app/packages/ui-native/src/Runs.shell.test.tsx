/**
 * (T261) THE RUNS TAB THROUGH THE REAL SHELL: its own bottom tab (Map · Conditions · Runs · Spots · More), the top bar
 * (search, sync, the bell) where it is on every map, the species chips (several at once, kept in the store), the date
 * slider (a key steps it, Today returns) painting the map by slot and pentad, and a tap raising the runs sheet with
 * the year chart and DFO's counts ("No counts" for a PSF run). The Map tab no longer offers Run timing.
 *
 * The map renderer is replaced by a line saying what it was handed (no WebGL in jsdom); the feed is a stub source
 * holding a two-run fixture bound to the tiles' atlas.
 */
import type { ReactNode } from "react";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { Pressable, Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { parseRunTimingIndex, RunTimingFacts, type PaintDoc, type RunsDoc, type StatusIndex } from "@app/core";
import { parseManifest, type RunTimingSource } from "@app/data";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, type PairingInput, type StatusSource } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("./MapScreen", () => ({
  MapScreen: (p: { runs?: { slots: readonly number[]; pentad: number } | null; modes?: { stream: string; basin: string };
                   onPressFeature?: (l: string, id: number) => void; corner?: ReactNode }) =>
    <View>
      {p.corner}
      <Text testID="map-runs">{p.runs ? `${p.runs.slots.join(",")}:${p.runs.pentad}` : "no runs"}</Text>
      <Text testID="map-modes">{`${p.modes?.stream} ${p.modes?.basin}`}</Text>
      <Pressable accessibilityRole="button" accessibilityLabel="tap the map"
                 onPress={() => { void p.onPressFeature?.("streams", 15); }}><Text>map</Text></Pressable>
    </View>,
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

const H = "d768d6e135e62b99";
const SLOTS = ["Chinook", "Chum", "Coho", "Pink (even)", "Pink (odd)", "Sockeye", "Steelhead"];
const hex = (ps: number[]) => ps.reduce((h, p) => h | (1n << BigInt(p)), 0n).toString(16).padStart(19, "0");
const RUNS: RunsDoc = {
  format: "runtiming-runs/1", slots: SLOTS, default: 6, relations: ["spawning", "passage", "observed"],
  relation_words: { spawning: "Spawns in this watershed", passage: "Passes through here",
                    observed: "Seen in this stream (DFO spawner surveys)" },
  sources: {
    psf: { credit: "Pacific Salmon Foundation (Salmon Watersheds Program)", title: "t", dataset: "d", updated: null,
           fetched: null, terms: "Used with the written permission of the Pacific Salmon Foundation.", url: "u" },
    dfo: { credit: "Fisheries and Oceans Canada", title: "t", dataset: "d", updated: null, fetched: null,
           terms: "Contains information licensed under the Open Government Licence – Canada.", url: "u" },
  },
  runs: [
    { id: "psf:1", src: "psf", sp: "Steelhead", slot: 6, name: "Chilliwack Winter", label: "winter", months: 0,
      pentads: hex([56, 57, 58, 59]) },
    { id: "dfo:9:Coho:-", src: "dfo", sp: "Coho", slot: 2, name: "Chilliwack River", months: 0, pentads: hex([58, 60]),
      years: [12, 2001, 2024], n: [[2020, 2600], [2021, 3100], [2022, 2900]] },
  ],
};
const PAINT: PaintDoc = {
  format: "runtiming-paint/1", section_handles: H, tile_version: `${H}.aaa`, runs_sha256: "s",
  profiles: [{ r: [1, 5], s: 0, m: [] }], sid: [15], p: [0], km: [0],
  basins: { ids: ["CHWK"], m: [[[2, hex([58]), hex([])], [6, hex([]), hex([56])]]] },
};
const INDEX = parseRunTimingIndex({ format: "runtiming-index/1", built_at: "2026-10-29T00:00:00Z", section_handles: H,
  tile_version: `${H}.aaa`, sources: {}, files: { runs: { url: "r", bytes: 1, sha256: "s" },
                                                  paint: { url: "p", bytes: 1, sha256: "q" } } })!;
const FEED: RunTimingSource = { latest: async () => ({ facts: new RunTimingFacts(INDEX, RUNS, PAINT), from: "live" }) };

const STATUS_INDEX = { handles: H, reachDigest: "", sections: 0, waters: 0, profiles: 0, codeOn: () => "base",
  profileOf: () => null, profileCodeOn: () => "base", waterCodeOn: () => "base" } as unknown as StatusIndex;
const STATUS: StatusSource = { key: "t255-status", load: async () => STATUS_INDEX };
const PAIRING: PairingInput = {
  kind: "release",
  manifest: parseManifest({
    format: "app-manifest/2", data_release: "r-t255",
    tiles: { compatible: [`${H}.*`], assets: null, mask: null,
             current: { id: `${H}.aaa`, url: `tiles/${H}.aaa/atlas.pmtiles`, bytes: 1, sha256: "x" } },
    basemap: null, files: {}, regulations: { valid_until: null, versions: null },
  }),
  held: { tiles: null, basemap: null, release: "r-1", files: {} },
};

beforeAll(() => {
  // Wed Oct 21 2026: pentad 58 (Oct 18–22); only the clock is faked, the render's timers still run
  vi.useFakeTimers({ toFake: ["Date"], now: new Date(2026, 9, 21, 9, 30) });
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});
afterAll(() => { vi.useRealTimers(); });
afterEach(cleanup);

const slidOut = () => act(() => {
  for (const el of document.querySelectorAll("div"))
    for (const type of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(type, { bubbles: true }));
});

describe("(T261) the Runs tab", () => {
  it("its own tab and top bar; species and the slider paint the map; a tap is the runs sheet with counts", async () => {
    const kept: { text: string | null } = { text: null };
    const store = { read: () => kept.text, write: (t: string) => { kept.text = t; } };
    render(
      <GestureHandlerRootView>
        <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME}
               status={STATUS} pairing={PAIRING} runTiming={FEED} runSpeciesStore={store} />
      </GestureHandlerRootView>);
    const bar = () => screen.getAllByRole("tab").map((t) => `${t.getAttribute("aria-label")}${
      t.getAttribute("aria-selected") === "true" ? "*" : ""}`).join(" ");
    // the Map tab: no Run timing among its switches any more
    await waitFor(() => expect(screen.getByTestId("map-modes").textContent).toBe("profile plain"));
    expect(screen.queryByRole("radio", { name: "Run timing" })).toBeNull();
    const topOnMap = [!!screen.queryByTestId("top-bar"), !!screen.queryByTestId("inseason-bell")];

    act(() => { fireEvent.click(screen.getByRole("tab", { name: "Runs" })); });
    await waitFor(() => expect(screen.getByTestId("map-runs").textContent).toBe("6:58"));
    const seen: string[] = [bar(), `top ${[!!screen.queryByTestId("top-bar"), !!screen.queryByTestId("inseason-bell")]}`,
                            screen.getByTestId("map-modes").textContent!, screen.getByTestId("run-day").textContent!];
    // a second species: both painted, and kept
    act(() => { fireEvent.click(screen.getByRole("checkbox", { name: "Coho" })); });
    await waitFor(() => expect(screen.getByTestId("map-runs").textContent).toBe("6,2:58"));
    seen.push(`kept ${kept.text}`, screen.getByTestId("runs-legend-title").textContent!);
    // the last species cannot be cleared: steelhead off, then coho stays
    act(() => { fireEvent.click(screen.getByRole("checkbox", { name: "Steelhead" })); });
    act(() => { fireEvent.click(screen.getByRole("checkbox", { name: "Coho" })); });
    await waitFor(() => expect(screen.getByTestId("map-runs").textContent).toBe("2:58"));
    // the slider: a key steps a pentad (painted after the throttle), Today returns
    act(() => { fireEvent.keyDown(screen.getByTestId("run-slider-track"), { key: "ArrowLeft" }); });
    seen.push(screen.getByTestId("run-day").textContent!);
    await waitFor(() => expect(screen.getByTestId("map-runs").textContent).toBe("2:57"));
    act(() => { fireEvent.click(screen.getByRole("button", { name: /^Today: / })); });
    await waitFor(() => expect(screen.getByTestId("map-runs").textContent).toBe("2:58"));
    seen.push(screen.getByTestId("run-day").textContent!);

    // a tap: the runs sheet, coho first (chosen), its counts; the PSF steelhead run says no counts
    await act(async () => { fireEvent.click(screen.getByLabelText("tap the map")); });
    slidOut();
    const sheet = await screen.findByTestId("runs-sheet");
    // the other species' runs fold behind one line
    seen.push(`${within(sheet).getAllByTestId("runs-row").length} | ${within(sheet).getByTestId("runs-more").textContent}`);
    act(() => { fireEvent.click(within(sheet).getByTestId("runs-more")); });
    const rows = within(sheet).getAllByTestId("runs-row").map((r) => r.textContent);
    seen.push(...rows.map((r) => r!.replace(/[JFMASOND]{12}/, "|months|")));

    expect(topOnMap).toEqual([true, topOnMap[1]]);
    expect(seen).toEqual([
      "Map Conditions Runs* Spots More",
      `top true,${topOnMap[1]}`,                                  // the bell is where it is on the Map tab
      "runs runs",
      "Oct 18–22 (today)",
      'kept ["Steelhead","Coho"]',
      "Steelhead and coho runs",
      "Oct 13–17",
      "Oct 18–22 (today)",
      "1 | 1 run of another species here",
      "CohoIn the water Oct 18–22Chilliwack River · Seen in this stream (DFO spawner surveys)|months|"
        + "DFO surveys: 12 years, 2001–20242020–2022Latest count 2,900 (2022)",
      "Steelhead — winterIn the water Oct 18–22Chilliwack Winter · Passes through here|months|No counts",
    ]);
  }, 40_000);
});
