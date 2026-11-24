/**
 * T23 — the Regulations screen (mounted in the water frame, T44, which draws the water's header row): the regs header FIXED above ONE ScrollView of the four tab
 * sections (header-one-shape, one scroll, licence-last, no part strip), the date pill (D7 a; T49: it asks the water
 * screen for "Which day?", the map's own calendar sheet — WaterScreen.regs-props.test), and (T29) a tab press scrolling to its section. Mounted over the frozen golden page
 * (fixtures/vedder_1010.json, buildRegsPage over the parity store).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { Text } from "react-native";
import { inSeasonBanner, type RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { WaterFrame } from "../WaterFrame";
import { forgetRegsScroll, recallRegsScroll, regsScrollKey, rememberRegsScroll } from "./scrollMemory";
import { RegsScreen, type RegsScreenProps } from "./RegsScreen";
import { createTabStore } from "./tabStore";
import vedder1010 from "./fixtures/vedder_1010.json";
import { TEST_SOURCES, TEST_TROUT_SOURCES } from "./testing/sources";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const page = vedder1010 as unknown as RegsPage;
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };
/** The screen as the app draws it: inside the water frame (T44), which draws the back and the view strip above it. */
function mount(p: Partial<RegsScreenProps> = {}, onBack?: () => void) {
  return render(<WaterFrame palette={LIGHT} face="regulations" onBack={onBack}>
                  <RegsScreen troutSources={TEST_TROUT_SOURCES} sources={TEST_SOURCES} page={page} palette={LIGHT} actions={NOOP} onDate={() => {}}
                              gear={<Text>gear section</Text>} methods={<Text>methods section</Text>}
                              licence={<Text>licence section</Text>} {...p} />
                </WaterFrame>);
}
const scrollEl = () => screen.getByTestId("regs-scroll");
describe("<RegsScreen>", () => {
  it("header-one-shape: the header is not in the scroll, and a scroll leaves its words and test ids as they were", () => {
    mount();
    const header = screen.getByTestId("regs-header");
    const before = { text: header.textContent, ids: [...header.querySelectorAll("[data-testid]")].map((e) => e.getAttribute("data-testid")) };
    expect(scrollEl().contains(header)).toBe(false);
    fireEvent.scroll(scrollEl(), { target: { scrollTop: 900 } });
    expect({ text: header.textContent, ids: [...header.querySelectorAll("[data-testid]")].map((e) => e.getAttribute("data-testid")) })
      .toEqual(before);
    // (the place label twice: the shown label, then its invisible measuring twin)
    expect(before.text).toBe("Vedder RiverTodayOct 10Mouth → Cable car crossing (200 m downstream of Vedder Crossing bridge)Mouth → Cable car crossing (200 m downstream of Vedder Crossing bridge)What you can keepGearMethodsLicence");
  });

  it("one scroll: exactly one vertical scroll container on the screen", () => {
    const { container } = mount();
    const scrolls = [...container.querySelectorAll<HTMLElement>("*")].filter((e) => /auto|scroll/.test(getComputedStyle(e).overflowY));
    expect(scrolls).toEqual([scrollEl()]);
  });

  it("(T565) an in-season notice is the first thing on the page: above the salmon card and every calendar and species block, below only a lapsed edition's line", () => {
    const change = { id: "is-a", region: "8", water: "Fraser River", from: "2026-10-15", to: "2026-11-30",
                     text: "No fishing for sturgeon.", source: "https://www2.gov.bc.ca/x", waters: null, words: {} };
    const order = () => [...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid"));
    mount({ inSeason: [inSeasonBanner(change, { where: null })], salmon: <Text testID="salmon-card">salmon card</Text>, lapsed: "Out of date." });
    expect(order().slice(0, 4)).toEqual(["regs-lapsed", "inseason-banners", "salmon-card", "keep-tab"]);
    cleanup();
    mount({ inSeason: [inSeasonBanner(change, { where: null })] });
    expect(order().slice(0, 2)).toEqual(["inseason-banners", "keep-tab"]);
    // inside the Keep tab there is no notice any more: it is not drawn twice, and not under the year bar
    expect(screen.getByTestId("keep-tab").querySelector("[data-testid=inseason-banners]")).toBeNull();
    cleanup();
    mount({ inSeason: [] });
    expect(screen.queryByTestId("inseason-banners")).toBeNull();
  });

  it("a lapsed edition: core's sentence leads the page, above Keep; in force (or no stamp) it is not drawn", () => {
    const said = "Out of date: these regulations were valid to Mar 31, 2027. Check the current Synopsis and the " +
      "Province's notices before you fish.";
    mount({ lapsed: said });
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")).slice(0, 2))
      .toEqual(["regs-lapsed", "keep-tab"]);
    expect(screen.getByTestId("regs-lapsed").textContent).toBe(said);
    cleanup();
    mount({ lapsed: null });
    expect(screen.queryByTestId("regs-lapsed")).toBeNull();
  });

  it("(T1042) the words every water repeats are never Google's snippet: the licence block, the reference and the legal footer carry data-nosnippet; the rest does not", () => {
    mount();
    const marked = [...screen.getByTestId("regs-content").children].filter((e) => e.hasAttribute("data-nosnippet")).map((e) => e.getAttribute("data-testid"));
    expect(marked).toEqual(["regs-licence", "regs-reference", "regs-legal"]);
  });

  it("licence-last: Keep, Gear, Methods, Licence, then the footer, in the one scroll; absent sections are not drawn", () => {
    mount({ footer: <Text testID="foot">foot</Text> });
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")))
      .toEqual(["keep-tab", "regs-gear", "regs-methods", "regs-licence", "regs-reference", "regs-legal", "regs-all-sources", "foot"]);
    cleanup();
    mount({ gear: undefined, methods: undefined, licence: undefined });
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid"))).toEqual(["keep-tab", "regs-legal", "regs-all-sources"]);
    // (T227, owner 2026-10-28) a Gear / Methods section not drawn (nothing may be fished that day) is not offered as a tab
    expect(within(screen.getByRole("tablist", { name: "Section" })).getAllByRole("tab").map((t) => t.textContent))
      .toEqual(["What you can keep", "Licence"]);
  });

  it("(T516, owner 2026-11-08) the Handling fish reference is the last thing on the page: after Licence, before the legal foot", () => {
    mount({ footer: <Text testID="foot">foot</Text> });
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")))
      .toEqual(["keep-tab", "regs-gear", "regs-methods", "regs-licence", "regs-reference", "regs-legal", "regs-all-sources", "foot"]);
    expect(within(screen.getByTestId("regs-methods")).queryByTestId("methods-reference")).toBeNull();
    expect(within(screen.getByTestId("regs-reference")).getByTestId("methods-reference-summary").textContent)
      .toContain("Handling fish");
    cleanup();
    mount({ methods: undefined });
    expect(screen.queryByTestId("regs-reference")).toBeNull();
  });

  it("(owner 2026-10-29) no fish-picture credits at the page's foot: they live on More → Sources and licences", () => {
    mount();
    expect(screen.queryByTestId("regs-fish-credits")).toBeNull();
  });

  it("no part strip: nothing between the header and the Keep tab's year strip", () => {
    mount();
    const keep = screen.getByTestId("keep-tab");
    expect(keep.children[0]!.getAttribute("data-testid")).toBe("keep-strip");
    expect(screen.queryAllByRole("radiogroup").filter((r) => !/Regulations|Conditions/.test(r.textContent ?? ""))).toEqual([]);
  });

  it("the water's header row is the frame's (T44): back and the view strip (FaceBar), Conditions offered; the screen draws neither", () => {
    const back = vi.fn();
    mount({}, back);
    const frameHeader = screen.getByTestId("water-frame-header");
    expect([frameHeader.contains(screen.getByRole("button", { name: "Back" })),
            screen.getByTestId("regs-view").contains(screen.getAllByRole("tablist")[0]!)]).toEqual([true, false]);
    const strip = screen.getAllByRole("tablist")[0]!;
    expect(within(strip).getAllByRole("tab").map((t) => [t.getAttribute("aria-label"), t.getAttribute("aria-selected")]))
      .toEqual([["Regulations", "true"], ["Conditions", "false"]]);
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(back).toHaveBeenCalledTimes(1);
  });

  it("(owner call 3: T71 Map, T150 Parts) no Map or Parts button in the regs header, and no slot for one", () => {
    mount();
    expect([screen.queryByTestId("regs-action"), screen.queryByTestId("regs-action-slot"),
            within(screen.getByTestId("regs-header")).queryAllByRole("button", { name: /map|parts/i }).length])
      .toEqual([null, null, 0]);
  });

  it("the pill asks for 'Which day?' (T49: the water screen's sheet, so a reload does not close it); no sheet of its own", () => {
    const onDate = vi.fn();
    mount({ onDate });
    fireEvent.click(screen.getByRole("button", { name: "Today Oct 10, change date" }));
    expect([onDate.mock.calls.length, screen.queryByText("Which day?")]).toEqual([1, null]);
  });

  it("(T29) a tab press selects it at once and jumps to its section top + 2 (prototype 2000, T1025: no animated pass); Keep jumps to 0", () => {
    const store = createTabStore();
    mount({ store });
    // react-native-web's ScrollView.scrollTo is the DOM node's scroll(): smooth when animated, auto (a jump) when not
    const to = vi.fn();
    (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
    act(() => store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 }));
    fireEvent.click(screen.getByRole("tab", { name: "Gear" }));
    expect(screen.getByRole("tab", { name: "Gear" }).getAttribute("aria-selected")).toBe("true");
    fireEvent.click(screen.getByRole("tab", { name: "What you can keep" }));
    expect(to.mock.calls).toEqual([[{ top: 1502, left: 0, behavior: "auto" }], [{ top: 0, left: 0, behavior: "auto" }]]);
  });

  it("(T29) the tab follows the scroll: past the Gear section's top the Gear tab is selected", () => {
    vi.useFakeTimers();
    try {
      const store = createTabStore();
      mount({ store });
      act(() => {
        store.setViewport(584); store.setContentHeight(3300);
        store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 });
      });
      const sel = () => screen.getAllByRole("tab").filter((t) => t.getAttribute("aria-selected") === "true").map((t) => t.textContent);
      act(() => { fireEvent.scroll(scrollEl(), { target: { scrollTop: 1488 } }); vi.advanceTimersByTime(200); });
      expect(sel()).toEqual(["Regulations", "Gear"]);
      act(() => { fireEvent.scroll(scrollEl(), { target: { scrollTop: 0 } }); vi.advanceTimersByTime(200); });
      expect(sel()).toEqual(["Regulations", "What you can keep"]);
    } finally { vi.useRealTimers(); }
  });

  it("(T41) a start link: its sheet is open at once; once the tab's section is measured the page jumps there (+ scroll), selects it, says started", () => {
    vi.useFakeTimers();
    try {
      const store = createTabStore(), onStarted = vi.fn();
      const withSheets = { ...page, sheet: (k: string) => (page as unknown as { sheets: Record<string, unknown> }).sheets[k] ?? null } as RegsPage;
      mount({ page: withSheets, store, start: { tab: "gear", sheet: "pos:pos", scroll: 40 }, onStarted });
      const to = vi.fn();
      (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
      const heading = screen.getAllByRole("heading").map((h) => h.textContent).filter((t) => t === "Possession");
      act(() => { vi.advanceTimersByTime(500); });
      const before = [to.mock.calls.length, onStarted.mock.calls.length];   // no section measured yet: waits
      act(() => { store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 }); vi.advanceTimersByTime(200); });
      act(() => { vi.advanceTimersByTime(2000); });
      expect({ heading, before, calls: to.mock.calls, started: onStarted.mock.calls.length,
               tab: screen.getByRole("tab", { name: "Gear" }).getAttribute("aria-selected") })
        .toEqual({ heading: ["Possession"], before: [0, 0], calls: [[{ top: 1542, left: 0, behavior: "auto" }]], started: 1, tab: "true" });
    } finally { vi.useRealTimers(); }
  });

  it("(T1049) the start's jump follows its section while the page above it is still growing, and says started once it holds", () => {
    vi.useFakeTimers();
    try {
      const store = createTabStore(), onStarted = vi.fn();
      mount({ store, start: { tab: "licence", sheet: null, scroll: 0 }, onStarted });
      const to = vi.fn();
      (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
      act(() => { store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 }); vi.advanceTimersByTime(100); });
      act(() => { vi.advanceTimersByTime(100); });                          // held one tick: not settled yet
      const early = onStarted.mock.calls.length;
      // the Keep tab grows (an image, a font) just after the first layout: every section below moves down 201 px
      act(() => { store.setSections({ gear: 1701, methods: 2301, methodsHeight: 400, licence: 2901 }); vi.advanceTimersByTime(100); });
      act(() => { vi.advanceTimersByTime(100); });                          // the new top held one tick: still following
      const following = onStarted.mock.calls.length;
      act(() => { vi.advanceTimersByTime(1000); });
      expect({ early, following, tops: to.mock.calls.map((c) => (c[0] as { top: number }).top), started: onStarted.mock.calls.length,
               tab: screen.getByRole("tab", { name: "Licence" }).getAttribute("aria-selected") })
        .toEqual({ early: 0, following: 0, tops: [2702, 2903], started: 1, tab: "true" });
    } finally { vi.useRealTimers(); }
  });

  describe("(T1049) the reader takes the page while a start still follows its section: it stops there", () => {
    const S = { gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 }, GROWN = { gear: 1701, methods: 2301, methodsHeight: 400, licence: 2901 };
    const setup = () => {
      const store = createTabStore(), onStarted = vi.fn();
      mount({ store, start: { tab: "licence", sheet: null, scroll: 0 }, onStarted });
      const to = vi.fn();
      (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
      const tops = () => to.mock.calls.map((c) => (c[0] as { top: number }).top);
      const fire = (y: number) => act(() => { fireEvent.scroll(scrollEl(), { target: { scrollTop: y } }); });
      /** the page's size as the scroll event reports it (react-native-web reads scrollHeight / offsetHeight off the node) */
      const size = (content: number, view: number) => {
        Object.defineProperty(scrollEl(), "scrollHeight", { configurable: true, value: content });
        Object.defineProperty(scrollEl(), "offsetHeight", { configurable: true, value: view });
      };
      /** the native drag: react-native-web never fires onScrollBeginDrag itself, so the prop is called off its fiber */
      const drag = () => {
        const key = Object.keys(scrollEl()).find((k) => k.startsWith("__reactFiber"))!;
        let f = (scrollEl() as unknown as Record<string, { return: unknown; memoizedProps?: Record<string, unknown> }>)[key] as
          { return: unknown; memoizedProps?: Record<string, unknown> } | null;
        while (f && typeof f.memoizedProps?.onScrollBeginDrag !== "function") f = f.return as typeof f;
        if (!f) throw new Error("no onScrollBeginDrag on the page's scroll view");
        act(() => { (f!.memoizedProps!.onScrollBeginDrag as () => void)(); });
      };
      return { store, onStarted, tops, fire, size, drag };
    };
    it("a tab press: no jump back to the start's tab", () => {
      vi.useFakeTimers();
      try {
        const { store, onStarted, tops } = setup();
        act(() => { store.setSections(S); vi.advanceTimersByTime(100); });   // the start's jump
        act(() => { fireEvent.click(screen.getByRole("tab", { name: "Methods" })); });
        act(() => { store.setSections(GROWN); vi.advanceTimersByTime(1000); });
        expect({ tops: tops(), started: onStarted.mock.calls.length }).toEqual({ tops: [2702, 2102], started: 1 });
      } finally { vi.useRealTimers(); }
    });
    it("a scroll of the reader's (wheel, keys, a web drag): no jump back; the start's own jump's event is not one", () => {
      vi.useFakeTimers();
      try {
        const own = setup();
        act(() => { own.store.setSections(S); vi.advanceTimersByTime(100); });
        act(() => { vi.advanceTimersByTime(50); });
        own.size(2000 + 584, 584);                                            // the page ends at y 2000: the jump to 2702 lands there
        own.fire(2000);                                                       // the jump's own event (clamped): still following
        act(() => { own.store.setSections(GROWN); vi.advanceTimersByTime(1000); });
        const ownTops = own.tops();
        cleanup();
        const r = setup();
        act(() => { r.fire(0); });                                            // the page settling at the top: not the reader
        act(() => { r.store.setSections(S); vi.advanceTimersByTime(200); });  // jumped, held one tick
        act(() => { vi.advanceTimersByTime(60); });
        r.fire(500);                                                          // the reader
        act(() => { r.store.setSections(GROWN); vi.advanceTimersByTime(1000); });
        expect({ ownTops, tops: r.tops(), started: r.onStarted.mock.calls.length })
          .toEqual({ ownTops: [2702, 2903], tops: [2702], started: 1 });
      } finally { vi.useRealTimers(); }
    });
    it("a drag of the reader's (native onScrollBeginDrag): no jump back, the start is done", () => {
      vi.useFakeTimers();
      try {
        const { store, onStarted, tops, drag } = setup();
        act(() => { store.setSections(S); vi.advanceTimersByTime(100); });   // the start's jump
        drag();
        act(() => { store.setSections(GROWN); vi.advanceTimersByTime(1000); });
        expect({ tops: tops(), started: onStarted.mock.calls.length }).toEqual({ tops: [2702], started: 1 });
      } finally { vi.useRealTimers(); }
    });
    it("the start's own jump's event arriving LATE (a slow phone, the target moved since) is still its own: the follow goes on", () => {
      vi.useFakeTimers();
      try {
        const { store, onStarted, tops, fire } = setup();
        act(() => { store.setSections(S); vi.advanceTimersByTime(100); });                       // jump to 2702
        act(() => { store.setSections(GROWN); vi.advanceTimersByTime(100); });                   // moved: jump to 2903
        act(() => { vi.advanceTimersByTime(160); });                                             // 160 ms on: past the old 150 ms window, before the follow is done
        fire(2702);                                                                              // the FIRST jump's event, only now
        act(() => { store.setSections({ ...GROWN, licence: 3001 }); vi.advanceTimersByTime(1000); });
        expect({ tops: tops(), started: onStarted.mock.calls.length }).toEqual({ tops: [2702, 2903, 3003], started: 1 });
      } finally { vi.useRealTimers(); }
    });
    it("a reader's scroll right after the jump (inside the old 150 ms window) is the reader's, whatever the time", () => {
      vi.useFakeTimers();
      try {
        const { store, onStarted, tops, fire } = setup();
        act(() => { store.setSections(S); vi.advanceTimersByTime(100); });   // jump to 2702
        fire(2650);                                                          // not the y asked for, 0 ms after
        act(() => { store.setSections(GROWN); vi.advanceTimersByTime(1000); });
        expect({ tops: tops(), started: onStarted.mock.calls.length }).toEqual({ tops: [2702], started: 1 });
      } finally { vi.useRealTimers(); }
    });
    it("a clamped own jump (asked 2702, the page ends at 2000) is not the reader; a scroll short of the end is", () => {
      vi.useFakeTimers();
      try {
        const a = setup();
        a.size(2584, 584);
        act(() => { a.store.setSections(S); vi.advanceTimersByTime(100); });
        act(() => { a.store.setSections(GROWN); vi.advanceTimersByTime(100); });   // moved on: jump to 2903, also beyond the end
        act(() => { vi.advanceTimersByTime(300); });
        a.fire(2000);                                                        // late AND clamped
        act(() => { vi.advanceTimersByTime(1000); });
        const clamped = a.tops();
        cleanup();
        const b = setup();
        b.size(2584, 584);
        act(() => { b.store.setSections(S); vi.advanceTimersByTime(100); });
        b.fire(1900);
        act(() => { b.store.setSections(GROWN); vi.advanceTimersByTime(1000); });
        expect({ clamped, short: b.tops() }).toEqual({ clamped: [2702, 2903], short: [2702] });
      } finally { vi.useRealTimers(); }
    });
  });

  it("(T1049) a start whose tab this page never draws: said after ~5 s and done, never polled for ever", async () => {
    vi.useFakeTimers();
    const { setFailureSink } = await import("@app/core");
    const seen: string[] = [];
    setFailureSink((e) => seen.push((e as Error).message));
    try {
      const store = createTabStore(), onStarted = vi.fn();
      mount({ store, start: { tab: "gear", sheet: null, scroll: 0 }, onStarted });
      act(() => { vi.advanceTimersByTime(4800); });
      const before = onStarted.mock.calls.length;
      act(() => { vi.advanceTimersByTime(400); });
      expect({ before, started: onStarted.mock.calls.length, seen })
        .toEqual({ before: 0, started: 1, seen: ["start link: tab=gear has no section on this page: the page opens at its top"] });
    } finally { setFailureSink(null); vi.useRealTimers(); }
  });

  it("(T1049 cold start) a page that never holds still: followed ~30 s from its section being drawn, then stopped and SAID", async () => {
    vi.useFakeTimers();
    const { setFailureSink } = await import("@app/core");
    const seen: string[] = [];
    setFailureSink((e) => seen.push((e as Error).message));
    try {
      const store = createTabStore(), onStarted = vi.fn();
      mount({ store, start: { tab: "gear", sheet: null, scroll: 0 }, onStarted });
      (scrollEl() as unknown as { scroll: () => void }).scroll = () => {};
      for (let i = 0; i < 295; i++) act(() => { store.setSections({ gear: 1500 + i, methods: 2100, methodsHeight: 400, licence: 2700 }); vi.advanceTimersByTime(100); });
      const before = onStarted.mock.calls.length;
      for (let i = 295; i < 320; i++) act(() => { store.setSections({ gear: 1500 + i, methods: 2100, methodsHeight: 400, licence: 2700 }); vi.advanceTimersByTime(100); });
      expect({ before, started: onStarted.mock.calls.length, seen })
        .toEqual({ before: 0, started: 1, seen: ["start link: tab=gear: the page was still moving 30 s after its section was drawn, with nothing left to come: the follow stopped where the page was"] });
    } finally { setFailureSink(null); vi.useRealTimers(); }
  });

  describe("(T1049 cold start) a slow phone: a part above the sections lands seconds after the page is drawn", () => {
    const S = { gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 };
    const grown = (d: number) => ({ gear: S.gear + d, methods: S.methods + d, methodsHeight: 400, licence: S.licence + d });
    const setup = (coming: boolean) => {
      const store = createTabStore(), onStarted = vi.fn();
      const start = { tab: "licence" as const, sheet: null, scroll: 0 };   // the host's one start (a new one would be a new link)
      const tree = (c: boolean) => (
        <WaterFrame palette={LIGHT} face="regulations">
          <RegsScreen troutSources={TEST_TROUT_SOURCES} sources={TEST_SOURCES} page={page} palette={LIGHT} actions={NOOP} onDate={() => {}}
                      gear={<Text>gear section</Text>} methods={<Text>methods section</Text>} licence={<Text>licence section</Text>}
                      store={store} start={start} onStarted={onStarted} coming={c} />
        </WaterFrame>);
      const { rerender } = render(tree(coming));
      const to = vi.fn();
      (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
      return { store, onStarted, tops: () => to.mock.calls.map((c) => (c[0] as { top: number }).top), arrive: () => rerender(tree(false)) };
    };
    it("the salmon card, still coming, lands 201 px above the sections 6 s on (the 0.9.0 device pass): the start waits for it and lands", () => {
      vi.useFakeTimers();
      try {
        const p = setup(true);
        act(() => { p.store.setSections(S); vi.advanceTimersByTime(6000); });   // drawn, still for 6 s: the card still coming
        const waiting = p.onStarted.mock.calls.length;
        p.arrive();                                                          // the card is in: a new render
        act(() => { p.store.setSections(grown(201)); vi.advanceTimersByTime(1000); });
        expect({ waiting, tops: p.tops(), started: p.onStarted.mock.calls.length,
                 tab: screen.getByRole("tab", { name: "Licence" }).getAttribute("aria-selected") })
          .toEqual({ waiting: 0, tops: [2702, 2903], started: 1, tab: "true" });
      } finally { vi.useRealTimers(); }
    });
    it("a page growing in steps over 22 s, a 3 s pause among them, while a part is still coming: lands on the last top", () => {
      vi.useFakeTimers();
      try {
        const p = setup(true);
        act(() => { p.store.setSections(S); vi.advanceTimersByTime(2000); });
        act(() => { p.store.setSections(grown(40)); vi.advanceTimersByTime(2000); });
        act(() => { p.store.setSections(grown(90)); vi.advanceTimersByTime(3000); });     // the pause: 30 ticks of nothing
        act(() => { p.store.setSections(grown(150)); vi.advanceTimersByTime(15_000); });
        const waiting = p.onStarted.mock.calls.length;
        p.arrive();                                                          // the card is in: a new render
        act(() => { p.store.setSections(grown(351)); vi.advanceTimersByTime(1000); });
        expect({ waiting, tops: p.tops(), started: p.onStarted.mock.calls.length })
          .toEqual({ waiting: 0, tops: [2702, 2742, 2792, 2852, 3053], started: 1 });
      } finally { vi.useRealTimers(); }
    });
    it("the reader's drag while the card is still coming: the follow stops there, no jump when it lands", () => {
      vi.useFakeTimers();
      try {
        const p = setup(true);
        act(() => { p.store.setSections(S); vi.advanceTimersByTime(3000); });
        const key = Object.keys(scrollEl()).find((k) => k.startsWith("__reactFiber"))!;
        let f = (scrollEl() as unknown as Record<string, { return: unknown; memoizedProps?: Record<string, unknown> }>)[key] as
          { return: unknown; memoizedProps?: Record<string, unknown> } | null;
        while (f && typeof f.memoizedProps?.onScrollBeginDrag !== "function") f = f.return as typeof f;
        act(() => { (f!.memoizedProps!.onScrollBeginDrag as () => void)(); vi.advanceTimersByTime(100); });
        const after = p.onStarted.mock.calls.length;
        p.arrive();                                                          // the card is in: a new render
        act(() => { p.store.setSections(grown(201)); vi.advanceTimersByTime(1000); });
        expect({ after, tops: p.tops(), started: p.onStarted.mock.calls.length }).toEqual({ after: 1, tops: [2702], started: 1 });
      } finally { vi.useRealTimers(); }
    });
  });

  describe("(T1049 review) a start is spent once, for its page; the reader's sheet ends it; a slow line is not a defect", () => {
    const S = { gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 };
    const grown = (d: number) => ({ gear: S.gear + d, methods: S.methods + d, methodsHeight: 400, licence: S.licence + d });
    /** a prop of the page's scroll view, called as the platform would (react-native-web never fires some of them in jsdom) */
    const scrollProp = (name: string) => {
      const key = Object.keys(scrollEl()).find((k) => k.startsWith("__reactFiber"))!;
      let f = (scrollEl() as unknown as Record<string, { return: unknown; memoizedProps?: Record<string, unknown> }>)[key] as
        { return: unknown; memoizedProps?: Record<string, unknown> } | null;
      while (f && typeof f.memoizedProps?.[name] !== "function") f = f.return as typeof f;
      if (!f) throw new Error(`no ${name} on the page's scroll view`);
      return f.memoizedProps![name] as (...a: unknown[]) => void;
    };
    const withSheets = { ...page, sheet: (k: string) => (page as unknown as { sheets: Record<string, unknown> }).sheets[k] ?? null } as RegsPage;
    const setup = (o: { coming?: boolean; scrollKey?: string } = {}) => {
      const store = createTabStore(), onStarted = vi.fn();
      const start = { tab: "licence" as const, sheet: null, scroll: 0 };
      const tree = (p: { coming?: boolean; scrollKey?: string }) => (
        <WaterFrame palette={LIGHT} face="regulations">
          <RegsScreen troutSources={TEST_TROUT_SOURCES} sources={TEST_SOURCES} page={withSheets} palette={LIGHT} actions={NOOP} onDate={() => {}}
                      gear={<Text>gear section</Text>} methods={<Text>methods section</Text>} licence={<Text>licence section</Text>}
                      store={store} start={start} onStarted={onStarted} coming={p.coming ?? false} scrollKey={p.scrollKey ?? "gnis:3062||"} />
        </WaterFrame>);
      const r = render(tree(o));
      const to = vi.fn();
      (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
      return { store, onStarted, start, r, tree, tops: () => to.mock.calls.map((c) => (c[0] as { top: number }).top) };
    };
    it("torn down unfinished (the page unmounted while the card is still coming): the start is spent, once", () => {
      vi.useFakeTimers();
      try {
        const p = setup({ coming: true });
        act(() => { p.store.setSections(S); vi.advanceTimersByTime(3000); });
        const following = p.onStarted.mock.calls.length;
        p.r.unmount();
        expect({ following, spent: p.onStarted.mock.calls.map((c) => c[0] === p.start) }).toEqual({ following: 0, spent: [true] });
      } finally { vi.useRealTimers(); }
    });
    it("the page shows another water while the start still follows: spent, and never landed there", () => {
      vi.useFakeTimers();
      try {
        const p = setup({ coming: true });
        act(() => { p.store.setSections(S); vi.advanceTimersByTime(1000); });
        p.r.rerender(p.tree({ coming: true, scrollKey: "gnis:9999||" }));
        act(() => { p.store.setSections(grown(201)); vi.advanceTimersByTime(2000); });
        expect({ tops: p.tops(), spent: p.onStarted.mock.calls.map((c) => c[0] === p.start) }).toEqual({ tops: [2702], spent: [true] });
      } finally { vi.useRealTimers(); }
    });
    it("a sheet the reader opens while the start follows: it stops there, no jump when the card lands", () => {
      vi.useFakeTimers();
      try {
        const p = setup({ coming: true });
        act(() => { p.store.setSections(S); vi.advanceTimersByTime(1000); });
        const opener = screen.getAllByRole("button").find((b) => /possession/i.test(b.textContent ?? "") || /possession/i.test(b.getAttribute("aria-label") ?? ""));
        if (!opener) throw new Error("no Possession button on the page");
        act(() => { fireEvent.click(opener); vi.advanceTimersByTime(200); });
        const sheetUp = screen.getAllByRole("heading").map((h) => h.textContent).includes("Possession");
        p.r.rerender(p.tree({ coming: false }));
        act(() => { p.store.setSections(grown(201)); vi.advanceTimersByTime(1000); });
        expect({ sheetUp, tops: p.tops(), started: p.onStarted.mock.calls.length }).toEqual({ sheetUp: true, tops: [2702], started: 1 });
      } finally { vi.useRealTimers(); }
    });
    it("the card still coming at 30 s is not a defect: no report, the follow goes on; at the line's own bound it is dropped, not reported", async () => {
      vi.useFakeTimers();
      const { setFailureSink, START_COMING_MAX_MS } = await import("@app/core");
      const seen: string[] = [];
      setFailureSink((e) => seen.push((e as Error).message));
      try {
        const p = setup({ coming: true });
        act(() => { p.store.setSections(S); vi.advanceTimersByTime(35_000); });
        const at35 = { seen: [...seen], started: p.onStarted.mock.calls.length };
        act(() => { vi.advanceTimersByTime(START_COMING_MAX_MS); });
        expect({ at35, seen, started: p.onStarted.mock.calls.length }).toEqual({ at35: { seen: [], started: 0 }, seen: [], started: 1 });
      } finally { setFailureSink(null); vi.useRealTimers(); }
    });
    it("time in the background is not the follow's: away 60 s with the page still moving, no cap on the way back", async () => {
      vi.useFakeTimers();
      const { AppState } = await import("react-native");
      let tell: (s: string) => void = () => {};
      const spy = vi.spyOn(AppState, "addEventListener").mockImplementation(((_: string, h: (s: string) => void) => { tell = h; return { remove() {} }; }) as never);
      try {
        const p = setup();
        for (let i = 0; i < 100; i++) act(() => { p.store.setSections(grown(i)); vi.advanceTimersByTime(100); });   // 10 s moving
        act(() => { tell("background"); });
        for (let i = 100; i < 700; i++) act(() => { p.store.setSections(grown(i)); vi.advanceTimersByTime(100); });   // 60 s away
        act(() => { tell("active"); });
        for (let i = 700; i < 800; i++) act(() => { p.store.setSections(grown(i)); vi.advanceTimersByTime(100); });   // 10 s back
        const back = p.onStarted.mock.calls.length;
        for (let i = 800; i < 1000; i++) act(() => { p.store.setSections(grown(i)); vi.advanceTimersByTime(100); });   // 20 s more
        expect({ back, after: p.onStarted.mock.calls.length }).toEqual({ back: 0, after: 1 });
      } finally { spy.mockRestore(); vi.useRealTimers(); }
    });
    it("a jump clamped at a page end the page has since grown past is the start's own; an early layout's end (0) never is", () => {
      vi.useFakeTimers();
      try {
        const size = (content: number, view: number) => {
          Object.defineProperty(scrollEl(), "scrollHeight", { configurable: true, value: content });
          Object.defineProperty(scrollEl(), "offsetHeight", { configurable: true, value: view });
        };
        const fire = (y: number) => act(() => { fireEvent.scroll(scrollEl(), { target: { scrollTop: y } }); });
        // own: the page ends at 2000 (2584 - 584) when the jump to 2702 is asked; it grows to end at 2300 before the event
        const a = setup({ coming: true });
        act(() => { scrollProp("onLayout")({ nativeEvent: { layout: { x: 0, y: 0, width: 390, height: 584 } } }); });
        act(() => { a.store.setSections(S); scrollProp("onContentSizeChange")(390, 2584); vi.advanceTimersByTime(100); });
        act(() => { scrollProp("onContentSizeChange")(390, 2884); });
        size(2884, 584);
        fire(2000);                                                           // the clamped jump's event, late
        act(() => { a.store.setSections(grown(100)); vi.advanceTimersByTime(500); });
        const own = [...new Set(a.tops())], ownSpent = a.onStarted.mock.calls.length;   // still following: it jumps to the grown top
        cleanup();
        // not own: an early layout (content = the view, under flexGrow) gave an end of 0 before the section was drawn
        const b = setup({ coming: true });
        act(() => { scrollProp("onLayout")({ nativeEvent: { layout: { x: 0, y: 0, width: 390, height: 584 } } }); scrollProp("onContentSizeChange")(390, 584); });
        act(() => { b.store.setSections(S); scrollProp("onContentSizeChange")(390, 3200); vi.advanceTimersByTime(100); });
        size(3200, 584);
        fire(0);                                                              // the reader: Home
        act(() => { b.store.setSections(grown(100)); vi.advanceTimersByTime(500); });
        expect({ own, ownSpent, home: b.tops(), homeSpent: b.onStarted.mock.calls.length })
          .toEqual({ own: [2702, 2802], ownSpent: 0, home: [2702], homeSpent: 1 });
      } finally { vi.useRealTimers(); }
    });
  });

  it("(T1038) a NEW start on the page already open (a link to the water on screen) is applied again: tab, scroll and sheet", () => {
    vi.useFakeTimers();
    try {
      const store = createTabStore(), onStarted = vi.fn();
      const withSheets = { ...page, sheet: (k: string) => (page as unknown as { sheets: Record<string, unknown> }).sheets[k] ?? null } as RegsPage;
      const tree = (start: RegsScreenProps["start"]) => (
        <WaterFrame palette={LIGHT} face="regulations">
          <RegsScreen troutSources={TEST_TROUT_SOURCES} sources={TEST_SOURCES} page={withSheets} palette={LIGHT} actions={NOOP} onDate={() => {}}
                      gear={<Text>gear section</Text>} methods={<Text>methods section</Text>} licence={<Text>licence section</Text>}
                      store={store} start={start} onStarted={onStarted} />
        </WaterFrame>);
      const { rerender } = render(tree({ tab: "gear", sheet: null, scroll: 0 }));
      const to = vi.fn();
      (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
      act(() => { store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 }); vi.advanceTimersByTime(600); });   // (T1049) a start is done once its top has held (300 ms of wall time)
      rerender(tree(null));                                                   // the host took it (onStarted)
      rerender(tree({ tab: "licence", sheet: "pos:pos", scroll: 30 }));      // the next link's start, same page
      act(() => { vi.advanceTimersByTime(600); });
      const sheetUp = screen.getAllByRole("heading").map((h) => h.textContent).includes("Possession");
      rerender(tree(null));
      rerender(tree({ tab: "keep", sheet: null, scroll: 0 }));               // and one that lands on the top, sheet shut
      act(() => { vi.advanceTimersByTime(600); });
      expect({ calls: to.mock.calls.map((c) => (c[0] as { top: number }).top), started: onStarted.mock.calls.length, sheetUp,
               sheetAfter: screen.getAllByRole("heading").map((h) => h.textContent).includes("Possession"),
               tab: screen.getByRole("tab", { name: "What you can keep" }).getAttribute("aria-selected") })
        .toEqual({ calls: [1502, 2732, 0], started: 3, sheetUp: true, sheetAfter: false, tab: "true" });
    } finally { vi.useRealTimers(); }
  });

  it("(T1038) a new start on the open page wins over a scroll memory still being restored", () => {
    vi.useFakeTimers();
    forgetRegsScroll();
    try {
      const store = createTabStore(), key = regsScrollKey("gnis:1", null, 7)!;
      rememberRegsScroll(key, 1234);
      const tree = (start: RegsScreenProps["start"]) => (
        <WaterFrame palette={LIGHT} face="regulations">
          <RegsScreen troutSources={TEST_TROUT_SOURCES} sources={TEST_SOURCES} page={page} palette={LIGHT} actions={NOOP} onDate={() => {}}
                      gear={<Text>gear section</Text>} methods={<Text>methods section</Text>} licence={<Text>licence section</Text>}
                      store={store} scrollKey={key} start={start} onStarted={() => {}} />
        </WaterFrame>);
      const { rerender } = render(tree(null));                               // restoring 1234: content not tall enough yet
      const to = vi.fn();
      (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
      act(() => { store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 }); vi.advanceTimersByTime(300); });
      rerender(tree({ tab: "licence", sheet: null, scroll: 30 }));
      act(() => { vi.advanceTimersByTime(4000); });                          // the memory's ~3 s give-up has passed
      expect(to.mock.calls.map((c) => (c[0] as { top: number }).top)).toEqual([2732]);
    } finally { vi.useRealTimers(); forgetRegsScroll(); }
  });

  it("(T41) the dev-only walk hook: opens a sheet by key (a bad key: false); gone on unmount and outside dev (T810 cutover: no pin bar lines)", () => {
    type Hook = { openSheet(k: string): boolean } | undefined;
    const g = globalThis as { __DEV__?: boolean; __regsDebug?: Hook };
    mount();
    const outside = g.__regsDebug;
    cleanup();
    g.__DEV__ = true;
    try {
      const withSheets = { ...page, sheet: (k: string) => (page as unknown as { sheets: Record<string, unknown> }).sheets[k] ?? null } as RegsPage;
      mount({ page: withSheets });
      const opened = [g.__regsDebug!.openSheet("nope")];
      act(() => { opened.push(g.__regsDebug!.openSheet("pos:pos")); });
      const heading = screen.getAllByRole("heading").map((h) => h.textContent).filter((t) => t === "Possession");
      cleanup();
      expect({ outside, opened, heading, after: g.__regsDebug }).toEqual({ outside: undefined, opened: [false, true], heading: ["Possession"], after: undefined });
    } finally { delete g.__DEV__; }
  });

  it("(T41) a start link on Keep with no offset and no sheet does nothing to the scroll, and says started at once", () => {
    const onStarted = vi.fn();
    mount({ start: { tab: "keep", sheet: null, scroll: 0 }, onStarted });
    const to = vi.fn();
    (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
    expect([to.mock.calls.length, onStarted.mock.calls.length]).toEqual([0, 1]);
  });

  it("(T50) the scroll is kept across a remount (view / tab switch, back, a recreation): back there once the page is tall enough; a start link wins", () => {
    vi.useFakeTimers();
    forgetRegsScroll();
    try {
      const key = regsScrollKey("gnis:1", null, 7)!;
      const fire = (y: number) => act(() => { fireEvent.scroll(scrollEl(), { target: { scrollTop: y } }); vi.advanceTimersByTime(50); });
      mount({ scrollKey: key });
      fire(1234);
      const kept = recallRegsScroll(key);
      cleanup();
      mount({ scrollKey: key });
      const to = vi.fn();
      (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
      fire(0);                                                   // the page settling at the top does not overwrite it
      act(() => { vi.advanceTimersByTime(500); });              // jsdom has no layout: content 0 tall — it waits…
      const waited = to.mock.calls.length;
      act(() => { vi.advanceTimersByTime(3000); });             // …then goes as far as it can
      const restored = to.mock.calls, after = recallRegsScroll(key);
      cleanup();
      mount({ scrollKey: key, start: { tab: "keep", sheet: null, scroll: 0 } });
      const to2 = vi.fn();
      (scrollEl() as unknown as { scroll: typeof to2 }).scroll = to2;
      act(() => { vi.advanceTimersByTime(4000); });
      expect({ kept, after, waited, restored, startWins: to2.mock.calls.length, other: regsScrollKey("gnis:1", 2, 7) === key,
               none: regsScrollKey(null, null, null) })
        .toEqual({ kept: { y: 1234 }, after: { y: 1234 }, waited: 0, restored: [[{ top: 1234, left: 0, behavior: "auto" }]], startWins: 0,
                   other: false, none: null });
    } finally { vi.useRealTimers(); }
  });
});
