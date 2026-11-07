/**
 * T29 — the pin bar through the component: RegsScreen mounted over a frozen page with a pin store holding that state's
 * anchors and measured line heights (fixtures/pin-geometry.json, app coordinates: jsdom has no layout), the ScrollView
 * scrolled by DOM events to chosen offsets, the bar's drawn words compared with what is written here. (T439: the replay of
 * every golden bar is gone, with golden-pins.json; the geometry fixture holds three states' inputs, no expected bars.)
 * Then the drawing: the bold first numbers, the surface, aria-hidden, wrapping lines, and that a scroll re-renders the
 * bar and the tabs but never the Keep tab.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { PinAnchor, RegsPage } from "@app/core";
import { FACE } from "../type";
import { LIGHT } from "../theme";
import { createPinStore } from "./pinStore";
import slice from "./fixtures/pin-geometry.json";
import vedder1010 from "./fixtures/vedder_1010.json";
import glenn501 from "./fixtures/glenn_501.json";
import klwa1010 from "./fixtures/klwa_1010.json";

const keepRenders = vi.hoisted(() => ({ n: 0 }));
vi.mock("./KeepTab", async (importOriginal) => {
  const real = await importOriginal<typeof import("./KeepTab")>();
  return { KeepTab: (p: Parameters<typeof real.KeepTab>[0]) => { keepRenders.n++; return real.KeepTab(p); } };
});
const { RegsScreen } = await import("./RegsScreen");

type Slice = { heights: Record<string, number>;
  states: Record<string, { viewport: { height: number }; anchors: PinAnchor[] }> };
const P = slice as unknown as Slice;
const PAGES: Record<string, unknown> = { vedder_1010: vedder1010, glenn_501: glenn501, klwa_1010: klwa1010 };

beforeEach(() => { vi.useFakeTimers(); keepRenders.n = 0; });
afterEach(() => { cleanup(); vi.useRealTimers(); });

function mount(id: string) {
  const s = P.states[id]!, store = createPinStore({ heights: new Map(Object.entries(P.heights)) });
  const r = render(<RegsScreen page={PAGES[id] as RegsPage} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onBook() {} }}
                               onDate={() => {}} store={store} />);
  act(() => { store.setViewport(s.viewport.height); store.setAnchors(s.anchors); });
  return { r, store };
}
/** scroll the ScrollView's DOM node (react-native-web reads scrollTop), past the 16 ms event throttle */
const scrollTo = (y: number) => act(() => {
  fireEvent.scroll(screen.getByTestId("regs-scroll"), { target: { scrollTop: y } });
  vi.advanceTimersByTime(200);
});
/** the bar's lines as [text, sub] */
const bar = () => {
  const b = screen.queryByTestId("pin-bar");
  if (!b) return [];
  return [...b.querySelectorAll('[data-testid="pin-group"], [data-testid="pin-tier"]')].map((line) => [...line.children].map((t) => t.textContent));
};
describe("<PinBar> through the ScrollView", () => {
  it("vedder_1010 in words (T481): nothing at the top; the water's limit; + the size breadcrumb; the exception tier never pins", () => {
    mount("vedder_1010");
    const seen: unknown[] = [];
    for (const y of [0, 128, 300, 912, 968, 1128, 1624]) { scrollTo(y); seen.push(bar()); }
    const G = ["Trout and char · 4 a day — up to 2 of any kind, the rest hatchery rainbow"];
    expect(seen).toEqual([
      [],
      [G],
      [G, ["↳ 1 of them may be over 50 cm (20 in) — 2 if one is a hatchery steelhead"]],
      [G, ["↳ 1 of them may be over 50 cm (20 in) — 2 if one is a hatchery steelhead"]],
      [G],
      [G],
      [],
    ]);
  });

  it("glenn_501 (T481): the Duncan River's own tier beside the share is said in the first line and never pins", () => {
    mount("glenn_501");
    scrollTo(720);
    expect(bar()).toEqual([["Trout and char · 5 a day — up to 2 of any kind, the rest rainbow"]]);
  });

  it("klwa_1010 (no form): one line, no sub", () => {
    mount("klwa_1010");
    scrollTo(128);
    expect(bar()).toEqual([["Trout and char · 2 a day"]]);
    scrollTo(744);
    expect(bar()).toEqual([]);
  });
});

describe("<PinBar> drawing", () => {
  it("each line's first whole number (and its sub's) is a nested Text in the heavy face", () => {
    mount("vedder_1010");
    scrollTo(300);
    const heavy = [...screen.getByTestId("pin-bar").querySelectorAll<HTMLElement>("div[dir] > span, div[dir] > div")]
      .filter((e) => e.style.fontFamily === FACE.textHeavy).map((e) => e.textContent);
    expect(heavy).toEqual(["4", "1"]);
  });

  it("the surface (T481): absolute at the scroll's top edge, ONE solid card block with 8 px under its lines, a 24 px fade, hidden from screen readers", () => {
    mount("vedder_1010");
    scrollTo(128);
    const b = screen.getByTestId("pin-bar");
    expect([b.style.position, b.style.top, b.getAttribute("aria-hidden")]).toEqual(["absolute", "0px", "true"]);
    const solid = b.firstElementChild as HTMLElement, fade = screen.getByTestId("pin-fade");
    expect([solid.style.backgroundColor, solid.style.paddingBottom, fade.style.height, b.lastElementChild === fade])
      .toEqual([toRgb(LIGHT.card), "8px", "24px", true]);
    // it overlays the scroll; it is not in it
    expect(screen.getByTestId("regs-scroll").contains(b)).toBe(false);
  });

  it("the size line wears the size section's rule colour and bar", () => {
    mount("vedder_1010");
    scrollTo(300);
    const size = screen.getAllByTestId("pin-tier")[0]!;
    expect([size.style.borderLeftWidth, size.style.borderLeftColor, size.style.paddingLeft, (size.firstElementChild as HTMLElement).style.color])
      .toEqual(["3px", toRgb(LIGHT.rule), "13px", toRgb(LIGHT.rule)]);
  });

  it("r20d-pinned-stack (render form): no line is clamped (v33: lines wrap)", () => {
    mount("vedder_1010");
    scrollTo(300);
    const clamped = [...screen.getByTestId("pin-bar").querySelectorAll<HTMLElement>("*")].filter((e) => e.style.webkitLineClamp || e.style.textOverflow);
    expect(clamped).toEqual([]);
  });

  it("re-render scope: 20 scrolls change the bar and never re-render the Keep tab", () => {
    mount("vedder_1010");
    const before = keepRenders.n;
    expect(before).toBeGreaterThan(0);   // the counter does see the Keep tab render
    for (let i = 0; i < 20; i++) scrollTo([128, 300, 928, 0][i % 4]!);
    expect([keepRenders.n - before, bar().length]).toEqual([0, 0]);
  });
});

function toRgb(hex: string) {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
}
