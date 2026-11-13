/**
 * (T688) A WHOLLY TIDAL WATER'S MAP VIEW (Deas Slough on build 29). It lists no part, so the water map had no view model:
 * the camera stayed where the last water left it (the Okanagan) and the bar said "The parts of this water are not
 * available" — a failure that was not one. The map now frames the water itself and the bar says what is true. The fixture
 * release holds no wholly tidal water, so the Coquitlam (four parts) is made one: its store reads every piece as tidal
 * (the `xpart.province_except` "tidal" flag the release sets on a tidal cut's piece), everything else the real release's.
 * One load path: a search Open (no part) and a named piece (a tap, an address) frame and say the same.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ItemId } from "@app/data";
import type { MapProps } from "@app/map";
import { openRegsFixture, openRegsFixtureWithLines } from "@app/data/release/node";
import { openRegsStore, type RegsStore } from "@app/data/regs";
import { LIGHT } from "../theme";

const seen: { props: MapProps | null } = { props: null };
vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: (p: MapProps) => { seen.props = p; return null; },
}));

const COQ = "gnis:19986" as ItemId;
const real: RegsStore = await openRegsStore(openRegsFixtureWithLines());
/** the Coquitlam as a wholly tidal water */
const allTidal: RegsStore = { ...real, parts: async (ord: number) => {
  const ps = await real.parts(ord);
  return (await real.item(ord)).id === COQ ? ps.map((p) => ({ ...p, tidal: true })) : ps;
} } as RegsStore;
const held: { store: RegsStore } = { store: allTidal };
vi.mock("@app/ui", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  useRegsStore: () => ({ state: "ready", value: held.store, error: null }),
}));
const { WaterMapView } = await import("./WaterMapView");

afterEach(() => { cleanup(); seen.props = null; held.store = allTidal; });

const MAP = { at: { atlas: "a", basemap: "b" } as never, theme: "light", camera: { lon: -119.5, lat: 49.9, zoom: 9 }, view: "plain" };
function mount(part: number | null) {
  const calls: string[] = [];
  render(<WaterMapView loader={openRegsFixture()} item={COQ} part={part} tapSid={null} palette={LIGHT} map={MAP}
                       onOpenWater={() => {}} onShowRegs={(i, p) => calls.push(`regs ${i} part ${p}`)}
                       onOtherSection={() => {}} onCross={() => {}} />);
  return calls;
}
const shown = () => ({
  status: screen.queryByTestId("water-map-status")?.textContent ?? null,
  tidal: screen.queryByTestId("water-map-tidal")?.textContent ?? null,
  name: screen.queryByTestId("water-map-name")?.textContent ?? null,
  fit: seen.props?.fit ? { key: seen.props.fit.key, box: seen.props.fit.bbox } : null,
});

describe("(T688) a wholly tidal water's Map view", () => {
  it("frames the water itself and says it is tidal — no 'not available'; Show regulations opens its page", async () => {
    const calls = mount(null);
    await waitFor(() => expect(screen.queryByTestId("water-map-tidal")).not.toBeNull(), { timeout: 20_000 });
    const s = shown();
    expect([s.status, s.tidal, s.name]).toEqual([null, "Tidal water", "Coquitlam River"]);
    expect(s.fit?.key).toBe("gnis:19986:tidal");
    expect(s.fit?.box).toHaveLength(4);
    expect(screen.getByTestId("water-map-tidal-body").textContent).toMatch(/^Tidal water: a different regulation system\./);
    fireEvent.click(screen.getByRole("button", { name: "Show regulations for this water" }));
    expect(calls).toEqual(["regs gnis:19986 part 0"]);
  }, 30_000);

  it("one load path: opened on a named tidal piece (a tap, an address) it frames and says the same", async () => {
    mount(null);
    await waitFor(() => expect(screen.queryByTestId("water-map-tidal")).not.toBeNull(), { timeout: 20_000 });
    const whole = shown();
    cleanup(); seen.props = null;
    mount(2);
    await waitFor(() => expect(screen.queryByTestId("water-map-tidal")).not.toBeNull(), { timeout: 20_000 });
    expect(shown()).toEqual(whole);
  }, 30_000);

  it("a water that is not wholly tidal keeps its parts (the Coquitlam as released)", async () => {
    held.store = real;
    mount(1);
    await waitFor(() => expect(screen.queryByTestId("water-map-where")).not.toBeNull(), { timeout: 20_000 });
    expect([screen.queryByTestId("water-map-tidal"), screen.queryByTestId("water-map-status")]).toEqual([null, null]);
  }, 30_000);
});
