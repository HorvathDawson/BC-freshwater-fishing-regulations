/**
 * T45 — the Regulations view on a wide screen, through the component (the frozen golden pages, mounted in the water
 * frame as the app draws them). The arrangement differs; the words do not:
 *
 *   phone    nothing wraps the page, nothing is beside it (the design: golden parity, the existing render tests)
 *   tablet   the page and its pin bar in the reading column; the header as on a phone
 *   desktop  the reading column beside the side column (the date pill, the sections, the pinned lines); the header
 *            keeps the name row and the place label
 *
 * The page's words, every class, every state: the same, in the same order. The view's words on a desktop: the phone's,
 * with the date pill read after the place label (it moved to the side column) — asserted as the whole string.
 * What jsdom cannot lay out (the line length, the left edge, no sideways scroll) is measured in Chrome by
 * tools/regs-wide-layout.test.ts.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { Text } from "react-native";
import type { LayoutClass } from "@app/ui";
import { type PinAnchor, type PinLine, type RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { WaterFrame } from "../WaterFrame";
import { RegsScreen, type RegsScreenProps } from "./RegsScreen";
import { createPinStore } from "./pinStore";
import slice from "./fixtures/golden-pins.json";
import vedder1010 from "./fixtures/vedder_1010.json";
import klwa1010 from "./fixtures/klwa_1010.json";
import bc1501 from "./fixtures/bc1_501.json";

afterEach(() => { cleanup(); vi.useRealTimers(); });

const PAGES = { vedder_1010: vedder1010, klwa_1010: klwa1010, bc1_501: bc1501 } as unknown as Record<string, RegsPage>;
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };
const CLASSES: LayoutClass[] = ["phone", "tablet", "desktop"];

function mount(id: string, layout: LayoutClass, p: Partial<RegsScreenProps> = {}) {
  return render(<WaterFrame palette={LIGHT} face="regulations">
                  <RegsScreen page={PAGES[id]!} palette={LIGHT} actions={NOOP} onDate={() => {}} layout={layout}
                              gear={<Text>gear section</Text>} methods={<Text>methods section</Text>}
                              licence={<Text>licence section</Text>} {...p} />
                </WaterFrame>);
}
const text = (id: string) => screen.getByTestId(id).textContent ?? "";
const ids = (el: Element | null): (string | null)[] => {
  const out: (string | null)[] = [];
  for (let e = el?.parentElement ?? null; e && e.getAttribute("data-testid") !== "regs-view"; e = e.parentElement) {
    const t = e.getAttribute("data-testid");
    if (t) out.push(t);
  }
  return out;
};

describe("T45 the Regulations view on a wide screen: same words, another arrangement", () => {
  it.each(Object.keys(PAGES))("%s: the page draws the same words in the same order on a phone, a tablet and a desktop", (id) => {
    const seen: Record<string, string> = {};
    for (const c of CLASSES) { mount(id, c); seen[c] = text("regs-content"); cleanup(); }
    expect(seen.phone!.length).toBeGreaterThan(500);
    expect(seen).toEqual({ phone: seen.phone, tablet: seen.phone, desktop: seen.phone });
  });

  it.each(Object.keys(PAGES))("%s: the view's words — a tablet's are a phone's; a desktop's read the date pill after the place label", (id) => {
    const seen: Record<string, string> = {};
    for (const c of CLASSES) { mount(id, c); seen[c] = text("regs-view"); cleanup(); }
    const h = PAGES[id]!.header;
    const date = `${h.dayWord}${h.date}`, place = h.place ? `${h.place.full}${h.place.full}` : "";
    const tabs = "What you can keepGearMethodsLicence";
    // (owner call 3: T71 Map, T150 Parts) the header draws no action: the name, the pill, the place, the tabs
    const phoneHead = `${h.name}${date}${place}${tabs}`;
    expect(seen.phone!.startsWith(phoneHead)).toBe(true);
    expect(seen).toEqual({
      phone: seen.phone,
      tablet: seen.phone,
      desktop: `${h.name}${place}${date}${tabs}${seen.phone!.slice(phoneHead.length)}`,
    });
  });

  it("vedder_1010 in words: the desktop header and side column, whole", () => {
    mount("vedder_1010", "desktop");
    expect([text("regs-header"), text("regs-side")]).toEqual([
      "Vedder RiverMouth → Vedder Crossing BridgeMouth → Vedder Crossing Bridge",
      "TodayOct 10What you can keepGearMethodsLicence",
    ]);
  });

  it("where things are: a phone wraps nothing; a tablet holds the page in the reading column; a desktop puts it beside the side column", () => {
    const where: Record<string, unknown> = {};
    for (const c of CLASSES) {
      mount("vedder_1010", c);
      where[c] = { content: ids(screen.getByTestId("regs-content")),
                   pill: ids(screen.getByTestId("regs-date")).includes("regs-side") ? "side" : "header",
                   sectionTabs: screen.getAllByRole("tablist", { name: "Section" }).map((t) => t.getAttribute("data-testid") ?? "header row") };
      cleanup();
    }
    expect(where).toEqual({
      phone: { content: ["regs-scroll"], pill: "header", sectionTabs: ["header row"] },
      tablet: { content: ["regs-reading", "regs-scroll"], pill: "header", sectionTabs: ["header row"] },
      desktop: { content: ["regs-reading", "regs-scroll", "regs-columns"], pill: "side", sectionTabs: ["regs-section-list"] },
    });
  });

  it("the reading column is the token's measure, centred; the side column the token's width (computed styles)", () => {
    mount("vedder_1010", "desktop");
    const col = getComputedStyle(screen.getByTestId("regs-reading"));
    const side = getComputedStyle(screen.getByTestId("regs-side"));
    expect([col.maxWidth, col.alignSelf, side.width]).toEqual(["560px", "center", "264px"]);
  });

  it("one scroll on every class (the side column does not scroll; the page does)", () => {
    for (const c of CLASSES) {
      const { container } = mount("vedder_1010", c);
      const scrolls = [...container.querySelectorAll<HTMLElement>("*")].filter((e) => /auto|scroll/.test(getComputedStyle(e).overflowY));
      expect([c, scrolls.map((e) => e.getAttribute("data-testid"))]).toEqual([c, ["regs-scroll"]]);
      cleanup();
    }
  });

  it("desktop: a section press in the side column selects it at once and scrolls the page's column to it (T29 tabScrollY)", () => {
    const store = createPinStore();
    mount("vedder_1010", "desktop", { store });
    const to = vi.fn();
    (screen.getByTestId("regs-scroll") as unknown as { scroll: typeof to }).scroll = to;
    act(() => store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 }));
    const list = screen.getByTestId("regs-section-list");
    fireEvent.click(within(list).getByRole("tab", { name: "Licence" }));
    const sel = within(list).getAllByRole("tab").map((t) => `${t.textContent}:${t.getAttribute("aria-selected")}`);
    fireEvent.click(within(list).getByRole("tab", { name: "What you can keep" }));
    expect([sel, to.mock.calls]).toEqual([
      ["What you can keep:false", "Gear:false", "Methods:false", "Licence:true"],
      [[{ top: 2702, left: 0, behavior: "smooth" }], [{ top: 0, left: 0, behavior: "smooth" }]],
    ]);
  });

  it("desktop: the date pill in the side column asks 'Which day?' as the header's does", () => {
    const onDate = vi.fn();
    mount("vedder_1010", "desktop", { onDate });
    fireEvent.click(within(screen.getByTestId("regs-side")).getByRole("button", { name: "Change date, now Oct 10" }));
    expect(onDate).toHaveBeenCalledTimes(1);
  });
});

/* THE PIN BAR STILL WORKS: the golden bars replayed through the page's scroll, drawn in the side column (desktop) and over
   the reading column (tablet) — the same lines at the same offsets as on a phone (PinBar.test.tsx). */
type Slice = { heights: Record<string, number>;
  states: Record<string, { viewport: { height: number }; anchors: PinAnchor[]; samples: { y: number; lines: PinLine[] }[] }> };
const P = slice as unknown as Slice;

describe("T45 the pin bar on a wide screen replays the golden bars", () => {
  beforeEach(() => { vi.useFakeTimers(); });
  const bar = () => {
    const b = screen.queryByTestId("pin-bar");
    if (!b) return [];
    return [...b.children].map((line) => [...line.children].map((t) => t.textContent));
  };
  const want = (lines: readonly PinLine[]) => lines.map((l) => (l.sub === null ? [l.text] : [l.text, l.sub]));

  it.each([["vedder_1010", "desktop"], ["klwa_1010", "desktop"], ["vedder_1010", "tablet"]] as const)("%s %s", (id, layout) => {
    const s = P.states[id]!, store = createPinStore({ heights: new Map(Object.entries(P.heights)) });
    render(<RegsScreen page={PAGES[id]!} palette={LIGHT} actions={NOOP} onDate={() => {}} store={store} layout={layout} />);
    act(() => { store.setViewport(s.viewport.height); store.setAnchors(s.anchors); });
    let drawn = 0;
    for (const x of s.samples) {
      act(() => { fireEvent.scroll(screen.getByTestId("regs-scroll"), { target: { scrollTop: x.y } }); vi.advanceTimersByTime(200); });
      expect([x.y, bar()]).toEqual([x.y, want(x.lines)]);
      const b = screen.queryByTestId("pin-bar");
      if (b) {
        drawn++;
        expect([x.y, layout === "desktop" ? ids(b).includes("regs-side") : getComputedStyle(b).position])
          .toEqual([x.y, layout === "desktop" ? true : "absolute"]);
      }
    }
    expect(drawn).toBeGreaterThan(0);
  });
});
