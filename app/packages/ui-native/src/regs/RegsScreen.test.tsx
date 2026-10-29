/**
 * T23 — the Regulations screen (mounted in the water frame, T44, which draws the water's header row): the regs header FIXED above ONE ScrollView of the four tab
 * sections (header-one-shape, one scroll, licence-last, no part strip), the date pill (D7 a; T49: it asks the water
 * screen for "Which day?", the map's own calendar sheet — WaterScreen.regs-props.test), and (T29) a tab press scrolling to its section. Mounted over the frozen golden page
 * (fixtures/vedder_1010.json, buildRegsPage over the parity store).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { Text } from "react-native";
import type { RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { WaterFrame } from "../WaterFrame";
import { RegsScreen, type RegsScreenProps } from "./RegsScreen";
import { createPinStore } from "./pinStore";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const page = vedder1010 as unknown as RegsPage;
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };
/** The screen as the app draws it: inside the water frame (T44), which draws the back and the view strip above it. */
function mount(p: Partial<RegsScreenProps> = {}, onBack?: () => void) {
  return render(<WaterFrame palette={LIGHT} face="regulations" onBack={onBack}>
                  <RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}}
                              gear={<Text>gear section</Text>} methods={<Text>methods section</Text>}
                              licence={<Text>licence section</Text>} {...p} />
                </WaterFrame>);
}
const scrollEl = () => screen.getByTestId("regs-scroll");
/** (T43) the frozen page's own pictures (Keep groups + Other fish thumbs; a fixture has no sheets to open), creditsFor's lines */
const VEDDER_KEEP_CREDITS: readonly string[] = [
  "Black crappie — Duane Raver / USFWS (fws.gov)",
  "Brook trout — Freshwater Fisheries Society of BC (GoFishBC)",
  "Brown trout — Duane Raver / USFWS (fws.gov)",
  "Bull trout — Joel Sartore / National Geographic (Commons)",
  "Coastal cutthroat trout — Freshwater Fisheries Society of BC (GoFishBC)",
  "Dolly Varden — WDFW (eRegulations WA)",
  "Kokanee · spawning — WDFW (sockeye male) (eRegulations WA)",
  "Kokanee — Freshwater Fisheries Society of BC (GoFishBC)",
  "Lake trout — Sherman F. Denton (Commons)",
  "Lake whitefish — Sherman F. Denton (Commons)",
  "Largemouth bass — Duane Raver / USFWS (Commons)",
  "Rainbow trout — Freshwater Fisheries Society of BC (GoFishBC)",
  "Steelhead · ocean — Freshwater Fisheries Society of BC (GoFishBC)",
  "Steelhead · spawning — WDFW (eRegulations WA)",
  "Westslope cutthroat trout — Freshwater Fisheries Society of BC (GoFishBC)",
  "White sturgeon — Joseph Huët (Paris Museum) (Commons)",
];

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
    expect(before.text).toBe("Vedder RiverTodayOct 10Mouth → Vedder Crossing BridgeMouth → Vedder Crossing BridgeWhat you can keepGearMethodsLicence");
  });

  it("one scroll: exactly one vertical scroll container on the screen", () => {
    const { container } = mount();
    const scrolls = [...container.querySelectorAll<HTMLElement>("*")].filter((e) => /auto|scroll/.test(getComputedStyle(e).overflowY));
    expect(scrolls).toEqual([scrollEl()]);
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

  it("licence-last: Keep, Gear, Methods, Licence, then the footer, in the one scroll; absent sections are not drawn", () => {
    mount({ footer: <Text testID="foot">foot</Text> });
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")))
      .toEqual(["keep-tab", "regs-gear", "regs-methods", "regs-licence", "regs-legal", "regs-fish-credits", "foot"]);
    cleanup();
    mount({ gear: undefined, methods: undefined, licence: undefined });
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid"))).toEqual(["keep-tab", "regs-legal", "regs-fish-credits"]);
    // (T227, owner 2026-10-28) a Gear / Methods section not drawn (nothing may be fished that day) is not offered as a tab
    expect(within(screen.getByRole("tablist", { name: "Section" })).getAllByRole("tab").map((t) => t.textContent))
      .toEqual(["What you can keep", "Licence"]);
  });

  it("(T43) the foot credits the pictures THIS page shows, after Licence: \"FISH PICTURES\", then each line whole, not the app's 39", () => {
    mount();
    const foot = screen.getByTestId("regs-fish-credits");
    expect([...foot.querySelectorAll("div[dir=auto]")].map((e) => e.textContent)).toEqual(["FISH PICTURES", ...VEDDER_KEEP_CREDITS]);
    // a page that pictures nothing has no credits block at all (no empty heading)
    cleanup();
    mount({ page: { ...page, keep: { ...page.keep, order: [], groups: [], plainRows: [], otherFish: null, caveats: [] } } as RegsPage });
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
    fireEvent.click(screen.getByRole("button", { name: "Change date, now Oct 10" }));
    expect([onDate.mock.calls.length, screen.queryByText("Which day?")]).toEqual([1, null]);
  });

  it("(T29) a tab press selects it at once and scrolls to its section top + 2 (prototype 2000); Keep scrolls to 0", () => {
    const store = createPinStore();
    mount({ store });
    // react-native-web's ScrollView.scrollTo is the DOM node's scroll(): smooth when animated
    const to = vi.fn();
    (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
    act(() => store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 }));
    fireEvent.click(screen.getByRole("tab", { name: "Gear" }));
    expect(screen.getByRole("tab", { name: "Gear" }).getAttribute("aria-selected")).toBe("true");
    fireEvent.click(screen.getByRole("tab", { name: "What you can keep" }));
    expect(to.mock.calls).toEqual([[{ top: 1502, left: 0, behavior: "smooth" }], [{ top: 0, left: 0, behavior: "smooth" }]]);
  });

  it("(T29) the tab follows the scroll: past the Gear section's top the Gear tab is selected", () => {
    vi.useFakeTimers();
    try {
      const store = createPinStore();
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
      const store = createPinStore(), onStarted = vi.fn();
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

  it("(T41) the dev-only walk hook: opens a sheet by key (a bad key: false) and reads the pin bar's lines; gone on unmount and outside dev", () => {
    type Hook = { openSheet(k: string): boolean; pinLines(): string[] } | undefined;
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
      const lines = g.__regsDebug!.pinLines();
      cleanup();
      expect({ outside, opened, heading, lines, after: g.__regsDebug }).toEqual({ outside: undefined, opened: [false, true], heading: ["Possession"], lines: [], after: undefined });
    } finally { delete g.__DEV__; }
  });

  it("(T41) a start link on Keep with no offset and no sheet does nothing to the scroll, and says started at once", () => {
    const onStarted = vi.fn();
    mount({ start: { tab: "keep", sheet: null, scroll: 0 }, onStarted });
    const to = vi.fn();
    (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
    expect([to.mock.calls.length, onStarted.mock.calls.length]).toEqual([0, 1]);
  });
});
