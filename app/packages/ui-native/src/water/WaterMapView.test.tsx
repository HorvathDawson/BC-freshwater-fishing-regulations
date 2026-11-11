/**
 * P15: the water's Map view on THE COMMITTED REGS FIXTURE RELEASE. The map renderer is stubbed (no WebGL in jsdom) and
 * records what it is handed — the lit sections (`highlight` = `selected`, `water`), the camera's box, the marks — so
 * this pins what the map is TOLD; the controls are the real component.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { useState } from "react";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import type { ItemId, SectionId } from "@app/data";
import type { MapProps } from "@app/map";
import { openRegsFixture, openRegsFixtureWithLines, regsFixtureLines, sectionNames } from "@app/data/release/node";
import { openRegsStore } from "@app/data/regs";
import { setFailureSink } from "@app/core";
import { BackLayerContext, HOME, openWater, parseStartLink, partAction, partOfTap, reduce, useBack, useRoute,
         waterFrame, type Route, type StartLink, type SystemBack } from "@app/ui";
import { linkSearch, placeReads, resolveStartLink } from "@app/ui/place";
import { LIGHT } from "../theme";

const seen: { props: MapProps | null } = { props: null };
vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: (p: MapProps) => { seen.props = p; return null; },
}));
const { WaterMapView } = await import("./WaterMapView");

afterEach(() => { cleanup(); seen.props = null; });

const loader = openRegsFixture();
/** (T441) a section is named by what it IS (stream key + start metre), never by id: a re-cut renumbers them */
const sections = sectionNames(openRegsFixtureWithLines());
// (T517) the Chilliwack's mouth part (part_ix 0: the 200 m below the DFO salmon chain's cut above the Vedder Crossing
// bridge) lights its one section; the rest of the river is every other part's (the same part's `highlight` and `water`)
// (T667) the parts the tests mount, NAMED by their ends (`part_ix` is river order: one more cut renumbers every part above it)
const PARTS = {
  chwUpper: ["gnis:8634", "chilliwack_vedder_rivers__vedder_crossing_bridge_200_m_upstream", "chilliwack_vedder_rivers__tamihi_rapids_bridge"],
  coqMouth: ["gnis:19986", "coquitlam_river__mary_hill_bypass_bridge", "coquitlam_river__lougheed_highway_bridge"],
  skeenaReserve: ["gnis:2936", "area:SKEENA RIVER ECOLOGICAL RESERVE@km63.5",
    "area:SKEENA RIVER ECOLOGICAL RESERVE@near:skeena_river__exchamsiks_river_into_skeena_river"],
  vedderUpper: ["gnis:3062", "chilliwack_vedder_rivers__vedder_crossing_bridge_200_m_downstream", "chilliwack_vedder_rivers__vedder_crossing_bridge"],
  zymoetzLake: ["gnis:3137", "zymoetz__mcdonell_lake_3000_m_downstream", "lake_outlet:wbk:329682393"],
} as const;
const partOf = (k: keyof typeof PARTS) => { const [item, lo, hi] = PARTS[k]; return sections.part(item, lo, hi); };
const CHW0_LIT = [[380887781, 0]] as const;
const CHW0_WATER = [[355993454, 0], [355993455, 0], [355993456, 0], [355998527, 0], [355999283, 0], [355999620, 0],
                    [356000264, 0], [380887781, 200], [380887817, 0],
                    [380887781, 11988], [380887818, 0], [380887822, 0], [355992631, 0], [355992677, 0], [355995896, 0],
                    [356000359, 0], [380887781, 23325], [380887781, 28231], [380887781, 57967], [380887781, 58646]] as const;
/** the Chilliwack's part 4 (the source): 380887781 from 58646 m */
const CHW_SOURCE = [380887781, 58646] as const;
const MAP = { at: { atlas: "a", basemap: "b" } as never, theme: "light", camera: { lon: -122, lat: 49, zoom: 8 }, view: "plain" };

function mount(item: string, part: number | null, tapSid: number | null = null, withLoader = true) {
  const calls: string[] = [];
  // (T369 D7) ⤢ is the frame's: held here as the route holds it, and handed back down
  function Mounted() {
    const [whole, setWhole] = useState(false);
    return <WaterMapView loader={withLoader ? loader : null} item={item as ItemId} part={part}
                         tapSid={tapSid as SectionId | null} palette={LIGHT} map={MAP} whole={whole}
                         onWhole={() => { calls.push("whole"); setWhole((w) => !w); }}
                         onOpenWater={(i, p) => calls.push(`open ${i} at ${p}`)}
                         onShowRegs={(i, p) => calls.push(`regs ${i} part ${p}`)}
                         onOtherSection={(s) => calls.push(`other ${s}`)}
                         onCross={(i, p) => calls.push(`cross ${i} at ${p}`)}
                         onPart={(i, p) => calls.push(`part ${i} ${p}`)} />;
  }
  render(<Mounted />);
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
    mount("gnis:8634", await partOf("chwUpper"));
    await ready();
    expect([seen.props?.selection, lit().highlight.length > 0, lit().water.length > 0]).toEqual(["dim", true, true]);
  });

  it("T150: the parts list — the toggle, the radio rows (normal weight), a pick handed to the frame, the list closed after it", async () => {
    const calls = mount("gnis:8634", 0);
    await ready();
    const toggle = screen.getByTestId("water-map-parts-toggle");
    expect([toggle.getAttribute("aria-label"), toggle.getAttribute("aria-expanded"), screen.queryByTestId("water-map-parts-list")])
      .toEqual(["Show all 5 parts", "false", null]);
    fireEvent.click(toggle);
    const radios = within(screen.getByTestId("water-map-parts-list")).getAllByRole("radio");
    expect(radios.map((r) => [r.getAttribute("aria-checked"), r.textContent])).toEqual([
      ["true", "✓Vedder Crossing Bridge → Orange triangular boundary signs (200 m upstream of Vedder Crossing bridge)"],
      ["false", "Orange triangular boundary signs (200 m upstream of Vedder Crossing bridge) → Tamihi Rapids Bridge"],
      ["false", "Tamihi Rapids Bridge → 100 m below the Slesse Creek confluence"],
      ["false", "100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary"],
      ["false", "Chilliwack River Ecological Reserve boundary → Source"]]);
    // T151: no bold in the list — every text in it is the regular face
    const faces = [...screen.getByTestId("water-map-parts").querySelectorAll("div[dir], span")]
      .map((e) => getComputedStyle(e).fontFamily).filter(Boolean);
    expect([...new Set(faces)]).toEqual(["Archivo_400Regular"]);
    fireEvent.click(radios[2]!);
    await waitFor(() => expect(screen.getByTestId("water-map-where").textContent).toBe("Part 3 of 5 · Chilliwack River"));
    expect([calls, screen.queryByTestId("water-map-parts-list")]).toEqual([["part gnis:8634 2"], null]);
    // (T150 review) the picked row is gone with the list: focus is on the toggle, not dropped to the page
    expect(document.activeElement?.getAttribute("data-testid")).toBe("water-map-parts-toggle");
  });
});

describe("<WaterMapView>", () => {
  it("(T443, owner on 0.6.7) the bar's names never run into each other: each wraps to two lines at most, cut with an ellipsis, its full words on its label", async () => {
    mount("gnis:8634", 0);
    await ready();
    const name = screen.getByTestId("water-map-name");
    expect(name.getAttribute("aria-label")).toBe("Vedder Crossing Bridge → Orange triangular boundary signs (200 m upstream of Vedder Crossing bridge)");
    const down = screen.getByTestId("water-map-step-down"), up = screen.getByTestId("water-map-step-up");
    for (const el of [name, down, up]) expect(el.style.getPropertyValue("-webkit-line-clamp"), el.textContent ?? "").toBe("2");
    // each step's button holds its own words (a shrinkable column of half the bar), and names the whole way for a reader
    expect(down.closest("[role=button]")?.getAttribute("aria-label")).toBe("Downstream into Vedder River");
    // (T459) and the map is told to grey every other water, keeping the one it flows into (the Vedder's sections)
    await waitFor(() => expect((seen.props?.near ?? []).length).toBeGreaterThan(0));
    expect(seen.props?.focus).toBe(true);
  });

  it("the Chilliwack from its mouth part: lit, framed, ▲ to the top; the ends disabled", async () => {
    mount("gnis:8634", 0);
    await ready();
    expect(bar()).toBe("Part 1 of 5 · Chilliwack River | Vedder Crossing Bridge → Orange triangular boundary signs (200 m upstream of Vedder Crossing bridge)"
      + " | Downstream into Vedder River | Upstream | Flows into Vedder River ›");
    expect(lit()).toEqual({
      highlight: await sections.streams(CHW0_LIT), water: await sections.streams(CHW0_WATER) });
    expect(seen.props?.fit).toEqual({ key: "gnis:8634:0", bbox: [-121.96469, 49.096026, -121.962945, 49.097385], refine: false,
      maxZoom: 12 });
    expect(seen.props?.marks?.map((m) => `${m.label ?? "(ring)"} ${m.a11y}`)).toEqual([
      "Orange triangular boundary signs (200 m upstream of Vedder Crossing bridge) Boundary: Orange triangular boundary signs (200 m upstream of Vedder Crossing bridge)",
      "(ring) Boundary: Tamihi Rapids Bridge",
      "(ring) Boundary: 100 m below the Slesse Creek confluence",
      "(ring) Boundary: Chilliwack River Ecological Reserve boundary"]);
    for (let k = 0; k < 4; k += 1) fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    expect(bar()).toBe("Part 5 of 5 · Chilliwack River | Chilliwack River Ecological Reserve boundary → Source"
      + " | Downstream | Highest part (off)");
    // the committed regs fixture's handle of the Chilliwack's part 4 (frozen until `pnpm regs-fixture` re-cuts it; T206,
    // r-f6918815ca78: 380887781:58646; T220 re-cut it to 1236842; T222 r-445850b69ae1 to 1236847, handles +5; T289 r-ce76b4ed3624: handles -2, the Thomas / Angelo lake stubs re-joined)
    expect([lit().highlight, seen.props?.fit?.key]).toEqual([[await sections.stream(...CHW_SOURCE)], "gnis:8634:4"]);
    // the disabled end does nothing
    fireEvent.click(screen.getByRole("button", { name: "Highest part" }));
    expect(screen.getByTestId("water-map-where").textContent).toBe("Part 5 of 5 · Chilliwack River");
    fireEvent.click(screen.getByRole("button", { name: "Downstream" }));
    expect(screen.getByTestId("water-map-where").textContent).toBe("Part 4 of 5 · Chilliwack River");
  }, 30_000);

  it("(T345) a mouth on TIDAL water: 'Flows into the tidal Fraser River' is plain words, no link, nothing opened", async () => {
    const calls = mount("gnis:19986", await partOf("coqMouth"));   // (T517) its mouth part is part_ix 1 (river order; no part 0 on the map)
    await ready();
    const exit = screen.getByTestId("water-map-exit");
    expect([exit.textContent, screen.queryByRole("link", { name: /Flows into/ }), exit.closest("[role=link], [role=button]")])
      .toEqual(["Flows into the tidal Fraser River", null, null]);
    fireEvent.click(exit);
    expect(calls).toEqual([]);
  }, 30_000);

  it("(T444) the frame moved to the water it flows into: the map never pairs the Vedder's part with the Chilliwack's group", async () => {
    const failures: unknown[] = [];
    const off = setFailureSink((e) => failures.push(e));
    try {
      let go: (() => void) | null = null;
      function Moved() {
        const [f, setF] = useState<{ item: string; part: number }>({ item: "gnis:8634", part: 1 });
        go = () => setF({ item: "gnis:3062", part: 0 });
        return <WaterMapView loader={loader} item={f.item as ItemId} part={f.part} tapSid={null} palette={LIGHT}
                             map={MAP} whole={false} onWhole={() => {}} onOpenWater={() => {}} onShowRegs={() => {}}
                             onOtherSection={() => {}} onCross={() => {}} onPart={() => {}} />;
      }
      render(<Moved />);
      await ready();
      act(() => go!());
      await waitFor(() => expect(screen.getByTestId("water-map-name").textContent).toMatch(/Vedder/), { timeout: 20_000 });
      expect(failures.map(String)).toEqual([]);
    } finally { off(); }
  }, 30_000);

  it("the mouth's exit opens the receiving water at the part it meets; Show regulations names this part", async () => {
    const calls = mount("gnis:8634", 0);
    await ready();
    fireEvent.click(screen.getByRole("link", { name: "Flows into Vedder River ›" }));
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    expect(screen.queryByTestId("water-map-exit")).toBeNull();           // only at the lowest part
    fireEvent.click(screen.getByRole("button", { name: "Show regulations for this part" }));
    // the step is handed on as the frame's part (one water screen), before Show regulations names it
    // (T517) the mouth meets the Vedder's top part (1)
    expect(calls).toEqual(["open gnis:3062 at 1", "part gnis:8634 1", "regs gnis:8634 part 1"]);
  }, 30_000);

  it("⤢ frames the whole river and back to the part", async () => {
    mount("gnis:8634", 0);
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "Whole river" }));
    expect([screen.getByTestId("water-map-whole").textContent, seen.props?.fit]).toEqual(["This part",
      { key: "gnis:8634:whole", bbox: [-121.964762, 48.999855, -121.39612, 49.114995], refine: false,
        maxZoom: 12 }]);
    fireEvent.click(screen.getByRole("button", { name: "This part" }));
    expect([screen.getByTestId("water-map-whole").textContent, seen.props?.fit?.key]).toEqual(["⤢ Whole river", "gnis:8634:0"]);
  }, 30_000);

  it("a mark tapped shows its words and the parts above and below; a tap on a part selects it; another water's is handed on", async () => {
    const calls = mount("gnis:8634", 0);
    await ready();
    act(() => seen.props!.onPressMark!(2));
    // (T517) a mark says the same words as the ends of the parts it joins
    const pop = within(screen.getByRole("dialog", { name: "100 m below the Slesse Creek confluence" }));
    expect(screen.getByTestId("water-map-mark").textContent).toBe(
      "100 m below the Slesse Creek confluence"
      + "Upstream100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundaryChilliwack River"
      + "DownstreamTamihi Rapids Bridge → 100 m below the Slesse Creek confluenceChilliwack River");
    expect(seen.props?.marks?.[2]?.on).toBe(true);
    fireEvent.click(pop.getByRole("button", { name: "Close" }));
    expect(screen.queryByTestId("water-map-mark")).toBeNull();
    const SOURCE = await sections.stream(...CHW_SOURCE), VEDDER = await sections.stream(355999181, 0);   // the Vedder's stretch
    act(() => seen.props!.onPressFeature!("stream", SOURCE));
    expect(screen.getByTestId("water-map-where").textContent).toBe("Part 5 of 5 · Chilliwack River");
    act(() => seen.props!.onPressFeature!("stream", VEDDER));
    expect([screen.getByTestId("water-map-where").textContent, calls]).toEqual(["Part 5 of 5 · Chilliwack River",
      ["part gnis:8634 4", `other ${VEDDER}`]]);
  }, 30_000);

  // (T67, r-0ed3c2b2a0a7) the Zymoetz's canyon side channel is no part of its own any more (E15's mark is gone); the
  // Skeena's Ecological Reserve part is a fork upstream. (T183, r-98c43091c440) Skeena section 2 starts at the Exchamsiks
  // cut: the Reserve is part 5 now (part 6 is the Exchamsiks host channel above the cut, still section 2), and its ▲ is
  // part 0 (which took in the mainstem up to the Exchamsiks) or that channel. (T517) part_ix is the river order: the
  // Reserve is part 1 (named by its two crossings of the Reserve's boundary), the mainstem round it part 2 (from the tidal
  // boundary: it joins the stem below and above the Reserve), the channel (no ends: said as its water) part 17
  it("a fork upstream: ▲ on the Skeena's Ecological Reserve opens the chooser, the main stem first", async () => {
    mount("gnis:2936", await partOf("skeenaReserve"));
    await ready();
    expect(screen.getByTestId("water-map-name").textContent).toBe(
      "Skeena River Ecological Reserve boundary (km 63.5) → Skeena River Ecological Reserve boundary near the Exchamsiks River confluence");
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    const chooser = within(screen.getByRole("dialog", { name: "Which way upstream?" }));
    const other = "Skeena River";
    expect(chooser.getAllByRole("button").map((b) => b.getAttribute("aria-label"))).toEqual(
      ["Close", "Tidal boundary → Exchamsiks River confluence", other]);
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
    render(<Harness item="gnis:2936" part={1} />);   // (T517) the Skeena's Ecological Reserve is part 1 (river order)
    await ready();
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    const seenChooser = [open(), `${back()} -> ${open()}`];
    expect([...seenMark, ...seenChooser]).toEqual([
      "100 m below the Slesse Creek confluence · water",
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

  it("across waters: ▲ on the Vedder's top part opens the Chilliwack at its lowest part; ▼ on the Chilliwack's mouth part the Vedder", async () => {
    // (T517) the Vedder is two parts of one run (the DFO salmon chain cuts it at the cable car crossing): its mouth part
    // flows into the Sumas River (the fixture's core names every water a kept water flows into), ▲ a step
    mount("gnis:3062", 0);
    await ready();
    expect(bar()).toBe("Part 1 of 2 · Vedder River | Mouth → Cable car crossing (200 m downstream of Vedder Crossing bridge) | Lowest part (off) | Upstream | Flows into Sumas River ›");
    cleanup();
    const calls = mount("gnis:3062", await partOf("vedderUpper"));
    await ready();
    expect(bar()).toBe("Part 2 of 2 · Vedder River | Cable car crossing (200 m downstream of Vedder Crossing bridge) → Vedder Crossing Bridge | Downstream | Upstream into Chilliwack River");
    // the words wrap (two lines at most, then cut with an ellipsis) inside the half-width button at 390 px
    expect(stepText("up")).toEqual({ text: "Upstream into Chilliwack River", lines: 2, align: "center" });
    fireEvent.click(screen.getByRole("button", { name: "Upstream into Chilliwack River" }));
    cleanup();
    const more = mount("gnis:8634", 0);
    await ready();
    expect(bar()).toBe("Part 1 of 5 · Chilliwack River | Vedder Crossing Bridge → Orange triangular boundary signs (200 m upstream of Vedder Crossing bridge)"
      + " | Downstream into Vedder River | Upstream | Flows into Vedder River ›");
    fireEvent.click(screen.getByRole("button", { name: "Downstream into Vedder River" }));
    // the map did not move inside the water: each is the caller's to open (through openWater, Map in front)
    expect([screen.getByTestId("water-map-where").textContent, [...calls, ...more]]).toEqual([
      "Part 1 of 5 · Chilliwack River", ["cross gnis:8634 at 0", "cross gnis:3062 at 1"]]);
  }, 30_000);

  it("(T517) ▲ from the Zymoetz's lake part steps onto the source part, a part of its own named by its own ends", async () => {
    const calls = mount("gnis:3137", await partOf("zymoetzLake"));
    await ready();
    expect(screen.getByTestId("water-map-name").textContent).toBe("McDonell Lake (3000 m downstream) → McDonell Lake outlet");
    fireEvent.click(screen.getByRole("button", { name: "Upstream" }));
    expect([screen.queryByRole("dialog"), screen.getByTestId("water-map-name").textContent, calls]).toEqual([
      null, "McDonell Lake → Source", ["part gnis:3137 5"]]);
  }, 30_000);

  it("opened by a tap: starts on the tapped section's part (the Regulations view's part)", async () => {
    const CNR_TO_GREENDALE = await sections.stream(354155148, 41391);
    mount("gnis:12227", null, CNR_TO_GREENDALE);   // (T289 r-ce76b4ed3624: -> 59247; T278 r-62e2f3a8c81e: same section, -> 59249) CNR Bridge -> Greendale (354155148:41391; T206, r-f6918815ca78; T185 r-ea426985a467: two cuts ahead, +2; T220 r-46be3c135e68: 59296 -> 59304; T222 r-445850b69ae1: -> 59305)
    await ready();
    expect(bar()).toBe("Part 4 of 5 · Cowichan River | CNR Bridge (mile 70.2) → Greendale Trestle | Downstream | Upstream");
    expect(lit().highlight).toEqual([CNR_TO_GREENDALE]);   // the tapped section given to mount()
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
    mount("gnis:8634", 0);
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
    mount("gnis:8634", 0);
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

  /*
   * (T345, T369, owner 2026-11-03) ONE LOAD PATH: the water map draws the same whatever way the frame was reached — a
   * reload of its URL, a search Open then a pick, a map tap (the frame holds `tapSid`), a ▼ step, a parts-list pick, and
   * the URL each of those leaves behind, reloaded. What is compared is everything the map is TOLD: the lit sections
   * (`selected`, `water`), the camera's key and box, the marks — and the bar. The frame is the one input (`useRoute`
   * here, as the Shell: every step is a route action), so a held ref, a stepped index or a tapped section cannot make one
   * way in draw what another does not. (What a phone and the web then drew from it: `@app/map`
   * native-feature-state.test.ts and adapter-contract's mid-burst highlight.)
   */
  describe("(T345, T369) one load path: the same part by every way in draws the same", () => {
    // (T369) the fixture with its stream lines (`section_line`, which the committed fixture was cut before): a link's
    // PLACE is made and resolved in it, through the same functions the web's address bar and start use
    const lined = openRegsFixtureWithLines();
    const store = openRegsStore(lined);
    /** The tap's own resolver (`WaterSource.itemForSection`: the source's ITEM_FOR_SECTION, over this release's core). */
    const itemFor = { itemForSection: async (sid: SectionId) => {
      const r = await (await (await store).core()).get(
        "SELECT i.item_id FROM item_section s JOIN item i ON i.ord = s.ord WHERE s.sid = ? LIMIT 1", sid);
      return r ? (String(r.item_id) as ItemId) : null;
    } };
    /** A URL as the web opens it: parsed, then resolved in the release (`resolveStartLink`). */
    const places = store.then((st) => placeReads(st, lined));
    const opened = async (url: string): Promise<StartLink> => resolveStartLink(parseStartLink(url)!, await places, itemFor);
    const addressOf = async (r: Route) => linkSearch(await places, r);
    const routes: Route[] = [];
    function Harness({ init }: { init: Route }) {
      const [route, go] = useRoute(init);
      routes.push(route);
      const f = waterFrame(route);
      if (!f || f.item === null) return null;
      const item = f.item;
      return <WaterMapView loader={lined} item={item} part={f.part} tapSid={f.tapSid} whole={f.whole}
                           palette={LIGHT} map={MAP} onOpenWater={() => {}} onShowRegs={() => {}} onOtherSection={() => {}}
                           onCross={() => {}} onPart={(i, p) => go(partAction(item, i, p))}
                           onWhole={() => go({ type: "whole" })} />;
    }
    const drawn = () => JSON.stringify({
      where: screen.getByTestId("water-map-where").textContent, name: screen.getByTestId("water-map-name").textContent,
      highlight: lit().highlight, water: [...lit().water].sort((a, b) => Number(a) - Number(b)),
      fit: seen.props?.fit, marks: seen.props?.marks });
    const settle = () => act(() => new Promise((r) => setTimeout(r, 30)));
    const open = (o: Parameters<typeof openWater>[0]) => reduce(HOME, openWater(o));
    /** (T369 D3) a TAP opens as the app's opener opens it: at the part the tapped section is in */
    const tap = async (item: ItemId, sid: number, view: "map" | "regs" | "conditions" = "map") =>
      open({ item, tapSid: sid as SectionId, view, ...(await partOfTap(await store, item, sid as SectionId)) });
    /**
     * What a frame's screens are drawn from: its water, part, view and ⤢, and its tap (T369 D5: the address
     * carries a tap as its place, `tap=1`, never a section id).
     */
    const drawnFrom = (r: Route) => {
      const f = waterFrame(r);
      return f && { item: f.item, part: f.part, view: f.view, whole: f.whole || undefined,
                    tapSid: f.tapSid };
    };
    /** Every URL a way in's routes made (the frame at every render): none may carry a section id (AGENTS rule 5). */
    const urls: string[] = [];
    /** Mount the frame, do `steps`: what is drawn — and the URL the route leaves, reloaded: what that draws. */
    async function wayIn(init: Route, steps: (() => void)[] = []): Promise<[string, string]> {
      routes.length = 0;
      render(<Harness init={init} />);
      await ready();
      for (const s of steps) { s(); await settle(); }
      await settle();
      const out = drawn(), live = routes.at(-1)!, url = await addressOf(live);
      urls.push(...await Promise.all(routes.map(addressOf)));
      cleanup(); seen.props = null;
      routes.length = 0;
      const link = await opened(url);
      // the reload's frame IS the live route's (one load path), and it says nothing (nothing was lost)
      expect({ url, frame: drawnFrom(link.route), notice: link.notice }).toEqual({ url, frame: drawnFrom(live), notice: null });
      render(<Harness init={link.route} />);
      await ready(); await settle();
      const again = drawn();
      urls.push(...await Promise.all(routes.map(addressOf)));
      cleanup(); seen.props = null;
      return [out, again];
    }
    const up = () => fireEvent.click(screen.getByTestId("water-map-step-up"));
    const whole = () => fireEvent.click(screen.getByTestId("water-map-whole"));
    const down = () => fireEvent.click(screen.getByTestId("water-map-step-down"));
    const pick = (row: number) => () => {
      fireEvent.click(screen.getByTestId("water-map-parts-toggle"));
      fireEvent.click(within(screen.getByTestId("water-map-parts-list")).getAllByRole("radio")[row]!);
    };
    const C = "gnis:19986" as ItemId;

    it("the Coquitlam's Mary Hill → Lougheed part (part_ix 1, T517 river order): reload, search Open + pick, tap, ▼, list pick", async () => {
      const MARY_HILL = await sections.streams([[356364584, 312], [356364584, 6590]]);   // the part's two sections
      const ways: Record<string, [string, string]> = {
        // (T369) the part's own address — a place (its midpoint section's middle), never `part=1`
        reload: await wayIn((await opened(await addressOf(open({ item: C, part: 1, view: "map" })))).route),
        "search Open, then the list": await wayIn(open({ item: C, view: "map", whole: true, fromSearch: true }), [pick(0)]),
        "tap (356364584:312)": await wayIn(await tap(C, MARY_HILL[0]!)),
        "▼ from Lougheed → boundary": await wayIn(open({ item: C, part: 2, view: "map" }), [down]),
        "the list, from the top part": await wayIn(open({ item: C, part: 3, view: "map" }), [pick(0)]),
        "tap, ▲, the list's lowest (the owner's)": await wayIn(await tap(C, MARY_HILL[0]!), [up, pick(0)]),
        // (T369 D7) ⤢ on and off again is the part's frame; ⤢ on is the route's, reloaded the same (below)
        "⤢ twice": await wayIn(open({ item: C, part: 1, view: "map" }), [whole, whole]),
      };
      const first = ways.reload![0];
            expect(JSON.parse(first)).toMatchObject({ where: "Part 1 of 3 · Coquitlam River", highlight: MARY_HILL,
                                                fit: { key: "gnis:19986:1", bbox: [-122.808104, 49.226226, -122.78, 49.269798] } });
      for (const [way, [drew, reloaded]] of Object.entries(ways)) expect({ way, drew, reloaded }).toEqual({ way, drew: first, reloaded: first });
      // the tapped frame OPENED at its part (T369 D3: no adopting); no URL ever held a section id
      const sids = new Set([...JSON.parse(first).highlight, ...JSON.parse(first).water].map(String));
      expect(urls.length).toBeGreaterThan(0);
      expect(urls.filter((u) => /(^|[?&])(sid|part)=/.test(u) || u.split(/[^0-9]+/).some((n) => sids.has(n)))).toEqual([]);
      expect([...new Set(urls)].filter((u) => u.includes("view=map") && !u.includes("blk="))).toEqual(
        // only a search Open's whole-water frame has no place before its pick
        [`?regs=${C}&view=map&whole=1`]);
      // the part's place: the Coquitlam's stream key, the middle of the part's run (T388 `part_line`: 312–7028 m; the
      // fixture re-cut on r-4230bc0c1dd5 carries it, the one before took the midpoint section's middle, 3451); the
      // other places are the frames the ways in started on (parts 2 and 3). A tap's is its part's too, said as a tap
      expect([...new Set(urls)].filter((u) => u.includes("blk=")).sort()).toEqual([
        `?regs=${C}&blk=356364584&m=11668&view=map`, `?regs=${C}&blk=356364584&m=3670&tap=1&view=map`,
        `?regs=${C}&blk=356364584&m=3670&view=map`, `?regs=${C}&blk=356364584&m=3670&view=map&whole=1`,
        `?regs=${C}&blk=356364584&m=39450&view=map`]);
      // (T369 D7) ⤢ pressed is the route's: the frame says it, its address says it, and its reload draws it
      const [wholeDrew, wholeReloaded] = await wayIn(open({ item: C, part: 1, view: "map" }), [whole]);
      expect([JSON.parse(wholeDrew).fit.key, wholeReloaded]).toEqual([`${C}:whole`, wholeDrew]);
    }, 60_000);

    it("(T369) an UNNAMED reach: its address is its place alone (no item, no section id); reloaded, the same reach", async () => {
      const { unnamed, lines } = regsFixtureLines();
      const [, blk, from, to] = lines.find(([h]) => h === unnamed)!;
      const m = Math.round((from + to) / 2);
      const out: unknown[] = [];
      for (const view of ["regs", "conditions"] as const) {
        const live = open({ item: null, tapSid: unnamed as SectionId, view });
        const url = await addressOf(live), link = await opened(url);
        out.push([url, drawnFrom(link.route), link.notice]);
        expect(drawnFrom(link.route)).toEqual(drawnFrom(live));
      }
      // the reach's stream key and its section's middle
      expect(out).toEqual([[`?blk=${blk}&m=${m}`, drawnFrom(open({ item: null, tapSid: unnamed as SectionId })), null],
                           [`?blk=${blk}&m=${m}&view=conditions`,
                            drawnFrom(open({ item: null, tapSid: unnamed as SectionId, view: "conditions" })), null]]);
      expect(String(unnamed).length > 0 && out.some((o) => String((o as string[])[0]).includes(String(unnamed)))).toBe(false);
    }, 30_000);

    it("(T369) a LAKE and a SPLIT LAKE's closure area: the item is the address (no place); its part and ⤢ reload the same", async () => {
      const TESLIN = "wbk:328961703" as ItemId, UPPER_WEST_ARM = "wbk:-21" as ItemId;
      const ways: Record<string, [string, string]> = {
        "Teslin Lake": await wayIn(open({ item: TESLIN, part: 0, view: "map" })),
        "Teslin Lake, ⤢": await wayIn(open({ item: TESLIN, view: "map", whole: true })),
        "Kootenay Lake — Upper West Arm": await wayIn(open({ item: UPPER_WEST_ARM, part: 0, view: "map" })),
        "the Main Body, ▼ to the Upper West Arm": await wayIn(open({ item: "wbk:-20" as ItemId, part: 0, view: "map" }), [down]),
      };
      for (const [way, [drew, reloaded]] of Object.entries(ways)) expect({ way, reloaded }).toEqual({ way, reloaded: drew });
      expect(JSON.parse(ways["the Main Body, ▼ to the Upper West Arm"]![0]).name).toBe(JSON.parse(ways["Kootenay Lake — Upper West Arm"]![0]).name);
      expect(await Promise.all([open({ item: TESLIN, part: 0, view: "map" }), open({ item: UPPER_WEST_ARM, part: 0 }),
                                open({ item: TESLIN, view: "map", whole: true })].map(addressOf)))
        .toEqual([`?regs=${TESLIN}&view=map`, `?regs=${UPPER_WEST_ARM}`, `?regs=${TESLIN}&view=map&whole=1`]);
      // the Regulations view of an area: the same frame back
      const regs = open({ item: UPPER_WEST_ARM, part: 0 });
      expect(drawnFrom((await opened(await addressOf(regs))).route)).toEqual(drawnFrom(regs));
    }, 60_000);

    it("(T517) the Zymoetz's source part above its lake part: ▲ into it and its URL reloaded draw the same", async () => {
      const Z = "gnis:3137" as ItemId;
      // part_ix 4 is "McDonell Lake (3000 m downstream) → McDonell Lake outlet"; ▲ is the source part (part_ix 5)
      const [stepped, reloaded] = await wayIn(open({ item: Z, part: 4, view: "map" }), [up]);
      expect(JSON.parse(stepped)).toMatchObject({ where: "Part 6 of 6 · Zymoetz River", name: "McDonell Lake → Source" });
      expect(reloaded).toEqual(stepped);
    }, 60_000);
  });
});

describe("(T508) a water's Map view held over the browse map is shown only once its map has drawn its part", () => {
  it("held while the part loads and while its map draws it; shown on the map's word, which it is never handed before " +
     "the part (a first draw of nothing does not count)", async () => {
    const { PaneReadyContext, usePaneHold } = await import("../shell/paneReveal");
    const handed: (MapProps["onDrawn"])[] = [];
    function Held({ open }: { open: boolean }) {
      // as the Shell: the hold derived from a browse → water change made over the map
      const pane = usePaneHold(open, true);
      return (
        <>
          <span data-testid="state">{!open ? "browse" : pane.holding ? "held" : "shown"}</span>
          {open && <PaneReadyContext.Provider value={pane.hold}>
            <WaterMapView loader={loader} item={"gnis:8634" as ItemId} part={1} tapSid={null} palette={LIGHT} map={MAP}
                          onOpenWater={() => {}} onShowRegs={() => {}} onOtherSection={() => {}} onCross={() => {}} />
          </PaneReadyContext.Provider>}
        </>
      );
    }
    const r = render(<Held open={false} />);
    r.rerender(<Held open />);
    const state = () => screen.getByTestId("state").textContent;
    await waitFor(() => expect(screen.getByTestId("water-map-where")).toBeTruthy(), { timeout: 20_000 });
    handed.push(seen.props?.onDrawn);
    expect(lit().highlight.length).toBeGreaterThan(0);
    expect(state()).toBe("held");                      // the bar is in; the map has not drawn the part
    expect(typeof handed[0]).toBe("function");
    await act(async () => { seen.props!.onDrawn!(); });
    expect(state()).toBe("shown");
  }, 30_000);

  it("the map is handed no `onDrawn` while the part is still loading", async () => {
    const { PaneReadyContext } = await import("../shell/paneReveal");
    const hold = { wait: () => () => {} };
    render(
      <PaneReadyContext.Provider value={hold}>
        <WaterMapView loader={loader} item={"gnis:8634" as ItemId} part={1} tapSid={null} palette={LIGHT} map={MAP}
                      onOpenWater={() => {}} onShowRegs={() => {}} onOtherSection={() => {}} onCross={() => {}} />
      </PaneReadyContext.Provider>);
    // the first render of the view: no view model yet, nothing lit, no word to give
    expect(seen.props?.highlight ?? []).toEqual([]);
    expect(seen.props?.onDrawn).toBeUndefined();
    await waitFor(() => expect(seen.props?.onDrawn).toBeTypeOf("function"), { timeout: 20_000 });
    expect(lit().highlight.length).toBeGreaterThan(0);
  }, 30_000);
});
