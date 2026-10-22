/**
 * P15: the water's Map view on THE COMMITTED REGS FIXTURE RELEASE. The map renderer is stubbed (no WebGL in jsdom) and
 * records what it is handed — the lit sections (`highlight` = `selected`, `water`), the camera's box, the marks — so
 * this pins what the map is TOLD; the controls are the real component.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import type { ItemId, SectionId } from "@app/data";
import type { MapProps } from "@app/map";
import { openRegsFixture } from "@app/data/release/node";
import { BackLayerContext, HOME, useBack, useRoute, type SystemBack } from "@app/ui";
import { LIGHT } from "../theme";

const seen: { props: MapProps | null } = { props: null };
vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: (p: MapProps) => { seen.props = p; return null; },
}));
const { WaterMapView } = await import("./WaterMapView");

afterEach(() => { cleanup(); seen.props = null; });

const loader = openRegsFixture();
const MAP = { at: { atlas: "a", basemap: "b" } as never, theme: "light", camera: { lon: -122, lat: 49, zoom: 8 }, view: "plain" };

function mount(item: string, part: number | null, tapSid: number | null = null, withLoader = true) {
  const calls: string[] = [];
  render(<WaterMapView loader={withLoader ? loader : null} item={item as ItemId} part={part}
                       tapSid={tapSid as SectionId | null} palette={LIGHT} map={MAP}
                       onOpenWater={(i, p) => calls.push(`open ${i} at ${p}`)}
                       onShowRegs={(i, p) => calls.push(`regs ${i} part ${p}`)}
                       onOtherSection={(s) => calls.push(`other ${s}`)} />);
  return calls;
}

const bar = () => {
  const steps = screen.getAllByRole("button").filter((b) => /Downstream|Upstream|Lowest|Highest/.test(b.textContent ?? ""))
    .map((b) => `${b.textContent}${b.getAttribute("aria-disabled") === "true" ? " (off)" : ""}`);
  return `${screen.getByTestId("water-map-where").textContent} | ${screen.getByTestId("water-map-name").textContent}`
    + ` | ${steps.join(" | ")}${screen.queryByTestId("water-map-exit") ? ` | ${screen.getByTestId("water-map-exit").textContent}` : ""}`;
};
const ready = () => waitFor(() => expect(screen.getByTestId("water-map-where")).toBeTruthy(), { timeout: 20_000 });
const lit = () => ({ highlight: [...(seen.props?.highlight ?? [])], water: [...(seen.props?.water ?? [])] });

describe("<WaterMapView>", () => {
  it("the Chilliwack from its mouth part: lit, framed, ▲ to the top; the ends disabled", async () => {
    mount("gnis:8634", 1);
    await ready();
    expect(bar()).toBe("Part 1 of 4 · Chilliwack River | Vedder Crossing Bridge → Tamihi Rapids Bridge"
      + " | Lowest part (off) | ▲ Upstream | Flows into Vedder River ›");
    expect(lit()).toEqual({
      highlight: [60210, 60211, 60212, 62453, 62571, 62823, 1235779, 1235780, 1235825],
      water: [1235781, 1235826, 1235827, 59894, 59900, 61178, 62859, 1235782, 1235783, 1235784, 1235785] });
    expect(seen.props?.fit).toEqual({ key: "gnis:8634:1", bbox: [-121.966394, 49.071523, -121.836034, 49.097385], refine: false });
    expect(seen.props?.marks?.map((m) => `${m.label ?? "(ring)"} ${m.a11y}`)).toEqual([
      "(ring) Boundary: Chilliwack River Ecological Reserve boundary",
      "Tamihi Rapids Bridge Boundary: Tamihi Rapids Bridge",
      "(ring) Boundary: fishing boundary signs 100 m below the Slesse Creek confluence"]);
    for (let k = 0; k < 3; k += 1) fireEvent.click(screen.getByRole("button", { name: "▲ Upstream" }));
    expect(bar()).toBe("Part 4 of 4 · Chilliwack River | Chilliwack River Ecological Reserve boundary → Source"
      + " | ▼ Downstream | Highest part (off)");
    expect([lit().highlight, seen.props?.fit?.key]).toEqual([[1235785], "gnis:8634:3"]);
    // the disabled end does nothing
    fireEvent.click(screen.getByRole("button", { name: "Highest part" }));
    expect(screen.getByTestId("water-map-where").textContent).toBe("Part 4 of 4 · Chilliwack River");
    fireEvent.click(screen.getByRole("button", { name: "▼ Downstream" }));
    expect(screen.getByTestId("water-map-where").textContent).toBe("Part 3 of 4 · Chilliwack River");
  }, 30_000);

  it("the mouth's exit opens the receiving water at the part it meets; Show regulations names this part", async () => {
    const calls = mount("gnis:8634", 1);
    await ready();
    fireEvent.click(screen.getByRole("link", { name: "Flows into Vedder River ›" }));
    fireEvent.click(screen.getByRole("button", { name: "▲ Upstream" }));
    expect(screen.queryByTestId("water-map-exit")).toBeNull();           // only at the lowest part
    fireEvent.click(screen.getByRole("button", { name: "Show regulations for this part" }));
    expect(calls).toEqual(["open gnis:3062 at 0", "regs gnis:8634 part 2"]);
  }, 30_000);

  it("⤢ frames the whole river and back to the part", async () => {
    mount("gnis:8634", 1);
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "Whole river" }));
    expect([screen.getByTestId("water-map-whole").textContent, seen.props?.fit]).toEqual(["This part",
      { key: "gnis:8634:whole", bbox: [-121.966394, 48.999855, -121.39612, 49.114995], refine: false }]);
    fireEvent.click(screen.getByRole("button", { name: "This part" }));
    expect([screen.getByTestId("water-map-whole").textContent, seen.props?.fit?.key]).toEqual(["⤢ Whole river", "gnis:8634:1"]);
  }, 30_000);

  it("a mark tapped shows its words and the parts above and below; a tap on a part selects it; another water's is handed on", async () => {
    const calls = mount("gnis:8634", 1);
    await ready();
    act(() => seen.props!.onPressMark!(2));
    const pop = within(screen.getByRole("dialog", { name: "fishing boundary signs 100 m below the Slesse Creek confluence" }));
    expect(screen.getByTestId("water-map-mark").textContent).toBe(
      "fishing boundary signs 100 m below the Slesse Creek confluence✕"
      + "Upstream100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundaryChilliwack River"
      + "DownstreamTamihi Rapids Bridge → 100 m below the Slesse Creek confluenceChilliwack River");
    expect(seen.props?.marks?.[2]?.on).toBe(true);
    fireEvent.click(pop.getByRole("button", { name: "Close" }));
    expect(screen.queryByTestId("water-map-mark")).toBeNull();
    act(() => seen.props!.onPressFeature!("stream", 1235785));
    expect(screen.getByTestId("water-map-where").textContent).toBe("Part 4 of 4 · Chilliwack River");
    act(() => seen.props!.onPressFeature!("stream", 62410));
    expect([screen.getByTestId("water-map-where").textContent, calls]).toEqual(["Part 4 of 4 · Chilliwack River", ["other 62410"]]);
  }, 30_000);

  it("a side channel: ▲ below the Zymoetz's canyon fork opens the chooser, the main stem first", async () => {
    mount("gnis:3137", 4);
    await ready();
    expect(screen.getByTestId("water-map-name").textContent)
      .toBe("Zymoetz Canyon lower entrance = transmission-line sign → Zymoetz Canyon upper entrance");
    fireEvent.click(screen.getByRole("button", { name: "▲ Upstream" }));
    const chooser = within(screen.getByRole("dialog", { name: "Which way upstream?" }));
    expect(chooser.getAllByRole("button").map((b) => b.getAttribute("aria-label"))).toEqual([
      "Close", "Zymoetz Canyon upper entrance → Limonite Creek confluence", "Side channel"]);
    fireEvent.click(chooser.getByRole("button", { name: "Side channel" }));
    expect([screen.queryByRole("dialog"), screen.getByTestId("water-map-name").textContent]).toEqual([null, "Side channel"]);
  }, 30_000);

  it("Android's back closes an open mark, then the chooser, before it leaves the water (back, everywhere)", async () => {
    let press: (() => boolean) | null = null;
    const system: SystemBack = (b) => { press = b; return { sync: () => {}, dispose: () => { press = null; } }; };
    const routes: string[] = [];
    function Harness({ item, part }: { item: string; part: number }) {
      const [route, go] = useRoute({ mode: "water", from: HOME,
                                     stack: [{ item: item as ItemId, part, tapSid: null, at: null, view: "map" }] });
      routes.push(route.mode);
      const { layers } = useBack(route, go, system);
      return (
        <BackLayerContext.Provider value={layers}>
          <WaterMapView loader={loader} item={item as ItemId} part={part} tapSid={null} palette={LIGHT} map={MAP}
                        onOpenWater={() => {}} onShowRegs={() => {}} onOtherSection={() => {}} />
        </BackLayerContext.Provider>);
    }
    const back = () => { let h = false; act(() => { h = press!(); }); return h; };
    const open = () => [screen.queryByRole("dialog")?.getAttribute("aria-label") ?? "nothing open", routes.at(-1)].join(" · ");

    render(<Harness item="gnis:8634" part={1} />);
    await ready();
    act(() => seen.props!.onPressMark!(2));
    const seenMark = [open(), `${back()} -> ${open()}`, `${back()} -> ${open()}`];
    cleanup(); routes.length = 0;
    render(<Harness item="gnis:3137" part={4} />);
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "▲ Upstream" }));
    const seenChooser = [open(), `${back()} -> ${open()}`];
    expect([...seenMark, ...seenChooser]).toEqual([
      "fishing boundary signs 100 m below the Slesse Creek confluence · water",
      "true -> nothing open · water",
      "true -> nothing open · browse",
      "Which way upstream? · water",
      "true -> nothing open · water",
    ]);
  }, 30_000);

  it("opened by a tap: starts on the tapped section's part (the Regulations view's part)", async () => {
    mount("gnis:12227", null, 59291);
    await ready();
    expect(bar()).toBe("Part 3 of 4 · Cowichan River | CNR Bridge (mile 70.2) → Greendale Trestle | ▼ Downstream | ▲ Upstream");
    expect(lit().highlight).toEqual([59291]);
  }, 30_000);

  it("KLWA from the Main Body: the three closure areas; Show regulations for another area names that water", async () => {
    const calls = mount("wbk:-20", 0);
    await ready();
    expect(bar()).toBe("Part 3 of 3 | Kootenay Lake — Main Body | ▼ Downstream | Highest part (off)");
    expect([screen.getByTestId("water-map-whole").textContent, seen.props?.marks]).toEqual(["⤢ Whole lake", []]);
    fireEvent.click(screen.getByRole("button", { name: "▼ Downstream" }));
    fireEvent.click(screen.getByRole("button", { name: "Show regulations for this part" }));
    expect([bar(), calls]).toEqual(["Part 2 of 3 | Kootenay Lake — Upper West Arm | ▼ Downstream | ▲ Upstream",
                                    ["regs wbk:-21 part 0"]]);
  }, 30_000);

  it("no release (the dev bundle): the map is drawn unlit and the bar says the parts are not available", async () => {
    mount("gnis:8634", null, null, false);
    await waitFor(() => expect(screen.getByTestId("water-map-status").textContent).toBe("The parts of this water are not available"));
    expect([lit(), seen.props?.fit, seen.props?.marks]).toEqual([{ highlight: [], water: [] }, null, []]);
  });
});
