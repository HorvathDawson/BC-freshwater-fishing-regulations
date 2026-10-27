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
                       onOtherSection={(s) => calls.push(`other ${s}`)}
                       onCross={(i, p) => calls.push(`cross ${i} at ${p}`)}
                       onPart={(i, p) => calls.push(`part ${i} ${p}`)} />);
  return calls;
}

const bar = () => {
  const steps = screen.getAllByRole("button").filter((b) => /Downstream|Upstream|Lowest|Highest/.test(b.textContent ?? ""))
    .map((b) => `${b.textContent}${b.getAttribute("aria-disabled") === "true" ? " (off)" : ""}`);
  return `${screen.getByTestId("water-map-where").textContent} | ${screen.getByTestId("water-map-name").textContent}`
    + ` | ${steps.join(" | ")}${screen.queryByTestId("water-map-exit") ? ` | ${screen.getByTestId("water-map-exit").textContent}` : ""}`;
};
/** ▼ / ▲'s words as drawn: the text, how many lines before it is cut (RN `numberOfLines`), its alignment. */
const stepText = (dir: "up" | "down") => {
  const e = screen.getByTestId(`water-map-step-${dir}`);
  return { text: e.textContent, lines: Number(e.style.webkitLineClamp), align: e.style.textAlign };
};
const ready = () => waitFor(() => expect(screen.getByTestId("water-map-where")).toBeTruthy(), { timeout: 20_000 });
const lit = () => ({ highlight: [...(seen.props?.highlight ?? [])], water: [...(seen.props?.water ?? [])] });

describe("<WaterMapView> (T150 / T151)", () => {
  it("T151: the map is told to mark the part by DIMMING the rest (cue \"dim\"), never the browse map's outline", async () => {
    mount("gnis:8634", 1);
    await ready();
    expect([seen.props?.selection, lit().highlight.length > 0, lit().water.length > 0]).toEqual(["dim", true, true]);
  });

  it("T150: the parts list — the toggle, the radio rows (normal weight), a pick handed to the frame, the list closed after it", async () => {
    const calls = mount("gnis:8634", 1);
    await ready();
    const toggle = screen.getByTestId("water-map-parts-toggle");
    expect([toggle.getAttribute("aria-label"), toggle.getAttribute("aria-expanded"), screen.queryByTestId("water-map-parts-list")])
      .toEqual(["Show all 4 parts", "false", null]);
    fireEvent.click(toggle);
    const radios = within(screen.getByTestId("water-map-parts-list")).getAllByRole("radio");
    expect(radios.map((r) => [r.getAttribute("aria-checked"), r.textContent])).toEqual([
      ["true", "✓Vedder Crossing Bridge → Tamihi Rapids Bridge"],
      ["false", "Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence"],
      ["false", "100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary"],
      ["false", "Chilliwack River Ecological Reserve boundary → Source"]]);
    // T151: no bold in the list — every text in it is the regular face
    const faces = [...screen.getByTestId("water-map-parts").querySelectorAll("div[dir], span")]
      .map((e) => getComputedStyle(e).fontFamily).filter(Boolean);
    expect([...new Set(faces)]).toEqual(["Archivo_400Regular"]);
    fireEvent.click(radios[2]!);
    await waitFor(() => expect(screen.getByTestId("water-map-where").textContent).toBe("Part 3 of 4 · Chilliwack River"));
    expect([calls, screen.queryByTestId("water-map-parts-list")]).toEqual([["part gnis:8634 0"], null]);
    // (T150 review) the picked row is gone with the list: focus is on the toggle, not dropped to the page
    expect(document.activeElement?.getAttribute("data-testid")).toBe("water-map-parts-toggle");
  });
});

describe("<WaterMapView>", () => {
  it("the Chilliwack from its mouth part: lit, framed, ▲ to the top; the ends disabled", async () => {
    mount("gnis:8634", 1);
    await ready();
    expect(bar()).toBe("Part 1 of 4 · Chilliwack River | Vedder Crossing Bridge → Tamihi Rapids Bridge"
      + " | Downstream into Vedder River | Upstream | Flows into Vedder River ›");
    expect(lit()).toEqual({
      highlight: [60214, 60215, 60216, 62459, 62577, 62829, 1235838, 1235839, 1235884],
      water: [1235840, 1235885, 1235886, 59898, 59904, 61183, 62865, 1235841, 1235842, 1235843, 1235844] });
    expect(seen.props?.fit).toEqual({ key: "gnis:8634:1", bbox: [-121.966394, 49.071523, -121.836034, 49.097385], refine: false });
    expect(seen.props?.marks?.map((m) => `${m.label ?? "(ring)"} ${m.a11y}`)).toEqual([
      "(ring) Boundary: Chilliwack River Ecological Reserve boundary",
      "Tamihi Rapids Bridge Boundary: Tamihi Rapids Bridge",
      "(ring) Boundary: fishing boundary signs 100 m below the Slesse Creek confluence"]);
    for (let k = 0; k < 3; k += 1) fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    expect(bar()).toBe("Part 4 of 4 · Chilliwack River | Chilliwack River Ecological Reserve boundary → Source"
      + " | Downstream | Highest part (off)");
    // the committed regs fixture's handle of the Chilliwack's part 4 (frozen until `pnpm regs-fixture` re-cuts it; T206,
    // r-f6918815ca78: 380887781:58646)
    expect([lit().highlight, seen.props?.fit?.key]).toEqual([[1235844], "gnis:8634:3"]);
    // the disabled end does nothing
    fireEvent.click(screen.getByRole("button", { name: "Highest part" }));
    expect(screen.getByTestId("water-map-where").textContent).toBe("Part 4 of 4 · Chilliwack River");
    fireEvent.click(screen.getByRole("button", { name: "Downstream" }));
    expect(screen.getByTestId("water-map-where").textContent).toBe("Part 3 of 4 · Chilliwack River");
  }, 30_000);

  it("the mouth's exit opens the receiving water at the part it meets; Show regulations names this part", async () => {
    const calls = mount("gnis:8634", 1);
    await ready();
    fireEvent.click(screen.getByRole("link", { name: "Flows into Vedder River ›" }));
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    expect(screen.queryByTestId("water-map-exit")).toBeNull();           // only at the lowest part
    fireEvent.click(screen.getByRole("button", { name: "Show regulations for this part" }));
    // the step is handed on as the frame's part (one water screen), before Show regulations names it
    expect(calls).toEqual(["open gnis:3062 at 0", "part gnis:8634 2", "regs gnis:8634 part 2"]);
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
      "fishing boundary signs 100 m below the Slesse Creek confluence"
      + "Upstream100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundaryChilliwack River"
      + "DownstreamTamihi Rapids Bridge → 100 m below the Slesse Creek confluenceChilliwack River");
    expect(seen.props?.marks?.[2]?.on).toBe(true);
    fireEvent.click(pop.getByRole("button", { name: "Close" }));
    expect(screen.queryByTestId("water-map-mark")).toBeNull();
    act(() => seen.props!.onPressFeature!("stream", 1235844));
    expect(screen.getByTestId("water-map-where").textContent).toBe("Part 4 of 4 · Chilliwack River");
    act(() => seen.props!.onPressFeature!("stream", 62416));   // the Vedder's 355999181:0 (T206)
    expect([screen.getByTestId("water-map-where").textContent, calls]).toEqual(["Part 4 of 4 · Chilliwack River",
      ["part gnis:8634 3", "other 62416"]]);
  }, 30_000);

  // (T67, r-0ed3c2b2a0a7) the Zymoetz's canyon side channel is no part of its own any more (E15's mark is gone); the
  // Skeena's Ecological Reserve part is a fork upstream. (T183, r-98c43091c440) Skeena section 2 starts at the Exchamsiks
  // cut: the Reserve is part 5 now (part 6 is the Exchamsiks host channel above the cut, still section 2), and its ▲ is
  // part 0 (which took in the mainstem up to the Exchamsiks) or that channel
  it("a fork upstream: ▲ on the Skeena's Ecological Reserve opens the chooser, the main stem first", async () => {
    mount("gnis:2936", 5);
    await ready();
    expect(screen.getByTestId("water-map-name").textContent).toBe("Within Skeena River Ecological Reserve");
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    const chooser = within(screen.getByRole("dialog", { name: "Which way upstream?" }));
    const other = "Side channel";
    // (T204) part 0's stretch above the Reserve is its own entry
    expect(chooser.getAllByRole("button").map((b) => b.getAttribute("aria-label"))).toEqual(
      ["Close", "3 stretches · stretch 2 of 3", other]);
    fireEvent.click(chooser.getByRole("button", { name: other }));
    expect([screen.queryByRole("dialog"), screen.getByTestId("water-map-name").textContent]).toEqual([null, other]);
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
                        onOpenWater={() => {}} onShowRegs={() => {}} onOtherSection={() => {}} onCross={() => {}} />
        </BackLayerContext.Provider>);
    }
    const back = () => { let h = false; act(() => { h = press!(); }); return h; };
    const open = () => [screen.queryByRole("dialog")?.getAttribute("aria-label") ?? "nothing open", routes.at(-1)].join(" · ");

    render(<Harness item="gnis:8634" part={1} />);
    await ready();
    act(() => seen.props!.onPressMark!(2));
    const seenMark = [open(), `${back()} -> ${open()}`, `${back()} -> ${open()}`];
    cleanup(); routes.length = 0;
    render(<Harness item="gnis:2936" part={5} />);   // (T183) the Skeena's Ecological Reserve is part 5
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    const seenChooser = [open(), `${back()} -> ${open()}`];
    expect([...seenMark, ...seenChooser]).toEqual([
      "fishing boundary signs 100 m below the Slesse Creek confluence · water",
      "true -> nothing open · water",
      "true -> nothing open · browse",
      "Which way upstream? · water",
      "true -> nothing open · water",
    ]);
  });

  it("(T150 review) Android's back closes the open parts list before it leaves the water", async () => {
    let press: (() => boolean) | null = null;
    const system: SystemBack = (b) => { press = b; return { sync: () => {}, dispose: () => { press = null; } }; };
    const routes: string[] = [];
    function Harness() {
      const [route, go] = useRoute({ mode: "water", from: HOME,
                                     stack: [{ item: "gnis:8634" as ItemId, part: 1, tapSid: null, at: null, view: "map" }] });
      routes.push(route.mode);
      const { layers } = useBack(route, go, system);
      return (
        <BackLayerContext.Provider value={layers}>
          <WaterMapView loader={loader} item={"gnis:8634" as ItemId} part={1} tapSid={null} palette={LIGHT} map={MAP}
                        onOpenWater={() => {}} onShowRegs={() => {}} onOtherSection={() => {}} onCross={() => {}} />
        </BackLayerContext.Provider>);
    }
    const back = () => { let h = false; act(() => { h = press!(); }); return h; };
    const open = () => [screen.queryByTestId("water-map-parts-list") ? "list open" : "list closed",
                        screen.getByTestId("water-map-parts-toggle").getAttribute("aria-expanded"), routes.at(-1)].join(" · ");
    render(<Harness />);
    await ready();
    fireEvent.click(screen.getByTestId("water-map-parts-toggle"));
    expect([open(), `${back()} -> ${open()}`, `${back()} -> ${routes.at(-1)}`]).toEqual([
      "list open · true · water",
      "true -> list closed · false · water",
      "true -> browse",
    ]);
  });

  it("across waters: ▲ on the Vedder opens the Chilliwack at its lowest part; ▼ on the Chilliwack's mouth part the Vedder", async () => {
    const calls = mount("gnis:3062", 0);
    await ready();
    // (the fixture's core does not hold the Sumas River, so the Vedder's mouth has no exit here)
    expect(bar()).toBe("Part 1 of 1 | Vedder River | Lowest part (off) | Upstream into Chilliwack River");
    // the words wrap (two lines at most, then cut with an ellipsis) inside the half-width button at 390 px
    expect(stepText("up")).toEqual({ text: "Upstream into Chilliwack River", lines: 2, align: "center" });
    fireEvent.click(screen.getByRole("button", { name: "Upstream into Chilliwack River" }));
    cleanup();
    const more = mount("gnis:8634", 1);
    await ready();
    expect(bar()).toBe("Part 1 of 4 · Chilliwack River | Vedder Crossing Bridge → Tamihi Rapids Bridge"
      + " | Downstream into Vedder River | Upstream | Flows into Vedder River ›");
    fireEvent.click(screen.getByRole("button", { name: "Downstream into Vedder River" }));
    // the map did not move inside the water: each is the caller's to open (through openWater, Map in front)
    expect([screen.getByTestId("water-map-where").textContent, [...calls, ...more]]).toEqual([
      "Part 1 of 4 · Chilliwack River", ["cross gnis:8634 at 1", "cross gnis:3062 at 0"]]);
  }, 30_000);

  it("(T204) ▲ from the Zymoetz's lake part steps onto the source part's upper stretch, named as one of two", async () => {
    const calls = mount("gnis:3137", 2);
    await ready();
    expect(screen.getByTestId("water-map-name").textContent).toBe("McDonell Lake (3000 m downstream) → McDonell Lake outlet");
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    expect([screen.queryByRole("dialog"), screen.getByTestId("water-map-name").textContent, calls]).toEqual([
      null, "Limonite Creek confluence → Source (2 stretches) · stretch 2 of 2", ["part gnis:3137 3"]]);
  }, 30_000);

  it("opened by a tap: starts on the tapped section's part (the Regulations view's part)", async () => {
    mount("gnis:12227", null, 59296);   // CNR Bridge -> Greendale (354155148:41391; T206, r-f6918815ca78; T185 r-ea426985a467: two cuts ahead, +2)
    await ready();
    expect(bar()).toBe("Part 3 of 4 · Cowichan River | CNR Bridge (mile 70.2) → Greendale Trestle | Downstream | Upstream");
    expect(lit().highlight).toEqual([59296]);   // the tapped handle given to mount()
  }, 30_000);

  it("KLWA from the Main Body: the three closure areas; Show regulations for another area names that water", async () => {
    const calls = mount("wbk:-20", 0);
    await ready();
    expect(bar()).toBe("Part 3 of 3 | Kootenay Lake — Main Body | Downstream | Highest part (off)");
    expect([screen.getByTestId("water-map-whole").textContent, seen.props?.marks]).toEqual(["⤢ Whole lake", []]);
    fireEvent.click(screen.getByRole("button", { name: "Downstream" }));
    fireEvent.click(screen.getByRole("button", { name: "Show regulations for this part" }));
    expect([bar(), calls]).toEqual(["Part 2 of 3 | Kootenay Lake — Upper West Arm | Downstream | Upstream",
                                    ["part wbk:-21 0", "regs wbk:-21 part 0"]]);
  }, 30_000);

  it("no release: the map is drawn unlit and the bar says the parts are not available", async () => {
    mount("gnis:8634", null, null, false);
    await waitFor(() => expect(screen.getByTestId("water-map-status").textContent).toBe("The parts of this water are not available"));
    expect([lit(), seen.props?.fit, seen.props?.marks]).toEqual([{ highlight: [], water: [] }, null, []]);
  });

  it("(T203) the exit chip is the LAST control: the steps and Show regulations never move under a finger", async () => {
    mount("gnis:8634", 1);
    await ready();
    const order = (ids: string[]) => ids.map((id) => screen.getByTestId(id))
      .sort((a, b) => (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1)).map((e) => e.dataset.testid);
    expect(order(["water-map-exit", "water-map-regs", "water-map-step-down", "water-map-step-up"]))
      .toEqual(["water-map-step-down", "water-map-step-up", "water-map-regs", "water-map-exit"]);
    // Show regulations sits straight under the steps row, with or without the chip
    const under = () => screen.getByTestId("water-map-regs").previousElementSibling?.contains(screen.getByTestId("water-map-step-down"));
    expect(under()).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    expect([screen.queryByTestId("water-map-exit"), under()]).toEqual([null, true]);
  }, 30_000);

  it("(T214 item 4) the 'Flows into' line has a place kept at every part: stepping on and off the mouth part moves nothing", async () => {
    mount("gnis:8634", 1);
    await ready();
    // what a finger can land on, and what sits above it: the bar's children before the steps row, the steps row, Show regulations
    const layout = () => {
      const slot = screen.getByTestId("water-map-exit-slot"), regs = screen.getByTestId("water-map-regs");
      const stepsRow = screen.getByTestId("water-map-step-down").closest("[role=button]")!.parentElement!;
      const bar = stepsRow.parentElement!;
      return { order: [...bar.children].map((c) => c === stepsRow ? "steps" : c === regs ? "regs" : c === slot ? "slot" : (c as HTMLElement).dataset.testid ?? "?"),
               slot: slot.style.minHeight, name: screen.getByTestId("water-map-name").style.minHeight,
               chip: !!screen.queryByTestId("water-map-exit") };
    };
    const atMouth = layout();
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    const up = layout();
    expect([atMouth.chip, up.chip]).toEqual([true, false]);
    // the same children in the same order, the slot the same height (the chip fills it, never enlarges it), the name's room the same
    expect([up.order, up.slot, up.name]).toEqual([atMouth.order, atMouth.slot, atMouth.name]);
    expect(atMouth.order.slice(-3)).toEqual(["steps", "regs", "slot"]);
    expect(atMouth.slot).toBe("44px");
    expect(atMouth.name).toBe("46px");
    fireEvent.click(screen.getByRole("button", { name: "Downstream" }));
    expect(layout()).toEqual(atMouth);
  }, 30_000);

  it("(T203) opened for a search result (`whole`): the camera frames the WHOLE water, offering 'This part'", async () => {
    render(<WaterMapView loader={loader} item={"gnis:8634" as ItemId} part={null} tapSid={null} palette={LIGHT} map={MAP} whole
                         onOpenWater={() => {}} onShowRegs={() => {}} onOtherSection={() => {}} onCross={() => {}} />);
    await ready();
    expect([screen.getByTestId("water-map-whole").textContent, seen.props?.fit?.key]).toEqual(["This part", "gnis:8634:whole"]);
  }, 30_000);
});
