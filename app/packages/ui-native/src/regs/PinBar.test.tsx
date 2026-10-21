/**
 * T29 — the pin bar through the component: RegsScreen mounted over a frozen golden page with a pin store holding that
 * state's golden anchors and measured line heights (fixtures/golden-pins.json, app coordinates: jsdom has no layout), the
 * ScrollView scrolled by DOM events to each golden sample offset, the bar's drawn words compared with the golden bar.
 * Then the drawing: the bold first numbers, the surface, aria-hidden, wrapping lines, and that a scroll re-renders the
 * bar and the tabs but never the Keep tab.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { PinAnchor, PinLine, RegsPage } from "@app/core";
import { FACE } from "../type";
import { LIGHT } from "../theme";
import { createPinStore } from "./pinStore";
import slice from "./fixtures/golden-pins.json";
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
  states: Record<string, { viewport: { height: number }; anchors: PinAnchor[]; samples: { y: number; lines: PinLine[] }[] }> };
const P = slice as unknown as Slice;
const PAGES: Record<string, unknown> = { vedder_1010: vedder1010, glenn_501: glenn501, klwa_1010: klwa1010 };

beforeEach(() => { vi.useFakeTimers(); keepRenders.n = 0; });
afterEach(() => { cleanup(); vi.useRealTimers(); });

function mount(id: string) {
  const s = P.states[id]!, store = createPinStore({ heights: new Map(Object.entries(P.heights)) });
  const r = render(<RegsScreen page={PAGES[id] as RegsPage} palette={LIGHT} actions={{ onSheet() {}, onDay() {}, onBook() {} }}
                               day={null} onDay={() => {}} store={store} />);
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
  return [...b.children].map((line) => [...line.children].map((t) => t.textContent));
};
const want = (lines: readonly PinLine[]) => lines.map((l) => (l.sub === null ? [l.text] : [l.text, l.sub]));

describe("<PinBar> replays the golden bars through the ScrollView", () => {
  it.each(Object.keys(PAGES))("%s: every golden sample offset draws the golden bar", (id) => {
    mount(id);
    for (const x of P.states[id]!.samples) {
      scrollTo(x.y);
      expect([x.y, bar()]).toEqual([x.y, want(x.lines)]);
    }
  });

  it("vedder_1010 in words: nothing at the top; the group line and its total; + the size line; + the rainbow line; nothing past the group", () => {
    mount("vedder_1010");
    const seen: unknown[] = [];
    for (const y of [0, 128, 240, 872, 928, 1088, 1584]) { scrollTo(y); seen.push(bar()); }
    const G = ["Trout and char · 2 a day from streams", "(of 4 across Region 2)"];
    expect(seen).toEqual([
      [],
      [G],
      [G, ["↳ of those, 1 over 50 cm (2 if one is a hatchery steelhead)"]],
      [G],
      [G, ["↳ 4 hatchery rainbow trout a day under this river’s rule, not part of the 2 from streams"]],
      [G],
      [],
    ]);
  });

  it("glenn_501 (form a): the share's own '↳ … from streams' never shows; the Duncan River's line does", () => {
    mount("glenn_501");
    scrollTo(720);
    expect(bar()).toEqual([["Trout and char · 2 a day from streams", "(of 5 across Region 4)"],
      ["↳ 5 rainbow trout a day under the Duncan River’s rule, not part of the 2 from streams"]]);
  });

  it("klwa_1010 (no form): one line, no sub", () => {
    mount("klwa_1010");
    scrollTo(128);
    expect(bar()).toEqual([["Trout and char · This lake’s rule: 2 a day"]]);
    scrollTo(744);
    expect(bar()).toEqual([]);
  });
});

describe("<PinBar> drawing", () => {
  it("each line's first whole number (and its sub's) is a nested Text in the heavy face", () => {
    mount("vedder_1010");
    scrollTo(240);
    const heavy = [...screen.getByTestId("pin-bar").querySelectorAll<HTMLElement>("div[dir] > span, div[dir] > div")]
      .filter((e) => e.style.fontFamily === FACE.textHeavy).map((e) => e.textContent);
    expect(heavy).toEqual(["2", "4", "1"]);
  });

  it("the surface: absolute at the scroll's top edge, the card, a hairline under it, hidden from screen readers", () => {
    mount("vedder_1010");
    scrollTo(128);
    const b = screen.getByTestId("pin-bar");
    expect([b.style.position, b.style.top, b.style.backgroundColor, b.style.borderBottomWidth, b.style.borderBottomColor,
            b.getAttribute("aria-hidden")])
      .toEqual(["absolute", "0px", toRgb(LIGHT.card), "1px", toRgb(LIGHT.line), "true"]);
    // it overlays the scroll; it is not in it
    expect(screen.getByTestId("regs-scroll").contains(b)).toBe(false);
  });

  it("the size line wears the size section's rule colour and bar", () => {
    mount("vedder_1010");
    scrollTo(240);
    const size = screen.getAllByTestId("pin-tier")[0]!;
    expect([size.style.borderLeftWidth, size.style.borderLeftColor, size.style.paddingLeft, (size.firstElementChild as HTMLElement).style.color])
      .toEqual(["3px", toRgb(LIGHT.rule), "13px", toRgb(LIGHT.rule)]);
  });

  it("r20d-pinned-stack (render form): at most 3 lines on any golden bar, and no line is clamped (v33: lines wrap)", () => {
    const most = Math.max(...Object.values(P.states).flatMap((s) => s.samples.map((x) => x.lines.length)));
    expect(most).toBeLessThanOrEqual(3);
    mount("vedder_1010");
    scrollTo(240);
    const clamped = [...screen.getByTestId("pin-bar").querySelectorAll<HTMLElement>("*")].filter((e) => e.style.webkitLineClamp || e.style.textOverflow);
    expect(clamped).toEqual([]);
  });

  it("re-render scope: 20 scrolls change the bar and never re-render the Keep tab", () => {
    mount("vedder_1010");
    const before = keepRenders.n;
    expect(before).toBeGreaterThan(0);   // the counter does see the Keep tab render
    for (let i = 0; i < 20; i++) scrollTo([128, 240, 928, 0][i % 4]!);
    expect([keepRenders.n - before, bar().length]).toEqual([0, 0]);
  });
});

function toRgb(hex: string) {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
}
