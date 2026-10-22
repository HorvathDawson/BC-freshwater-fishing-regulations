// @vitest-environment jsdom
/**
 * T44 — A SWIPE MOVES NOTHING THE VIEWS SHARE. A sideways drag between the water's views (Regulations · Conditions ·
 * Map) is heard in three places — the frame's header (back and strip), the regs header (the name row, place and
 * section tabs), and the view's body — and wherever it starts, only the view's own content follows the finger. The
 * back, the strip and the name row hold still; so does the regs header (it is one page with them: a drag on it must
 * not split the page into parts that slide separately — review of T44, 2026-10-22).
 *
 * The real Shell on a water (the fixture bundle and the committed regs release), each detector driven through RNGH's
 * own test utility. The spring home is held so the drag's last offset stays to be read: what follows the finger is
 * every element whose ancestors carry a translateX.
 */
import "./render-setup.ts";
import { createElement as h, type ReactNode } from "react";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";

/** The mutant: every detector follows the finger itself (`still` ignored), as the headers did before this fix. */
const MUTANT = { on: false };
vi.mock("../packages/ui-native/src/ViewSwipe", async (importOriginal) => {
  const real = await importOriginal<typeof import("../packages/ui-native/src/ViewSwipe")>();
  return { ...real, ViewSwipe: (p: Parameters<typeof real.ViewSwipe>[0]) =>
    h(real.ViewSwipe, MUTANT.on ? { ...p, still: false } : p) };
});
vi.mock("../packages/ui-native/src/MapScreen", async () => {
  const { View } = await import("react-native");
  return { MapScreen: () => h(View, { testID: "map-canvas", style: { flex: 1 } }) };
});
vi.mock("@app/map", async (importOriginal) => {
  const real = await importOriginal<typeof import("@app/map")>();
  const { View } = await import("react-native");
  return { ...real, Map: () => h(View, { testID: "map-canvas", style: { flex: 1 } }) };
});

const { Animated } = await import("react-native");
const { GestureHandlerRootView } = await import("react-native-gesture-handler");
const { fireGestureHandler, getByGestureTestId } = await import("react-native-gesture-handler/jest-utils");
const { makeFixtureSource } = await import("@app/data/fixture");
const { openRegsFixture } = await import("@app/data/release/node");
const { HOME } = await import("@app/ui");
const { Shell } = await import("../packages/ui-native/src/Shell");
const { LIGHT } = await import("../packages/ui-native/src/theme");

const NAME = "Chilliwack River";
type View3 = "Regulations" | "Conditions" | "Map";

// a phone-wide window (jsdom's is 0 x 0, which no swipe can start in); the spring home held (see above)
const spring = vi.spyOn(Animated, "spring").mockImplementation(() => ({ start: () => {}, stop: () => {}, reset: () => {} }) as never);
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
  window.dispatchEvent(new Event("resize"));
});
afterAll(() => spring.mockRestore());

/** A drag of 30 px left from mid-screen: short of a move, so the view stays and the drag's offset is what is left. */
const drag = [
  { absoluteX: 200, x: 200, translationX: 0, translationY: 0 },
  { absoluteX: 185, x: 185, translationX: -15, translationY: 0 },
  { absoluteX: 170, x: 170, translationX: -30, translationY: 0, velocityX: 0, velocityY: 0 },
];

/** How far an element has been carried sideways: the sum of every translateX on it and above it. */
function carried(el: Element | null): number | string {
  if (!el) return "missing";
  let x = 0;
  for (let e: Element | null = el; e; e = e.parentElement) {
    const t = (e as HTMLElement).style?.transform ?? "";
    for (const m of t.matchAll(/translateX\((-?[\d.]+)px\)/g)) x += Number(m[1]);
  }
  return Math.round(x);
}

/** What a reader would see move: the shared rows, the regs header's own rows, and the view's content. */
function where(view: View3): Record<string, number | string> {
  const out: Record<string, number | string> = {
    back: carried(screen.getAllByLabelText("Back")[0] ?? null),
    strip: carried(screen.queryByRole("tablist", { name: "Views of this water" })),
    name: carried(screen.queryByTestId("water-title")),
  };
  if (view === "Regulations") {
    out.place = carried(screen.queryByTestId("regs-place"));
    out.content = carried(screen.queryByTestId("keep-tab"));
  } else if (view === "Conditions") out.content = carried(screen.queryAllByRole("radiogroup")[0] ?? null);
  else out.content = carried(screen.queryByTestId("map-canvas"));
  return out;
}

const READY: Record<View3, () => unknown> = {
  Regulations: () => screen.queryByTestId("keep-tab"), Conditions: () => screen.queryAllByRole("radiogroup")[0],
  Map: () => screen.queryByTestId("water-map-where"),
};
const VIEW_KEY: Record<View3, string> = { Regulations: "regs", Conditions: "conditions", Map: "map" };

/** Open the water on `view`, drag on the detector `id`, and say where everything was carried. */
async function dragOn(view: View3, id: string): Promise<Record<string, number | string>> {
  const source = makeFixtureSource();
  const water = await source.water("gnis:8634" as never);
  render(h(GestureHandlerRootView, { style: { flex: 1 } } as never, h(Shell, {
    source, palette: LIGHT, theme: "light", themeName: "light", onTheme: () => {},
    tiles: { atlas: "a", basemap: "b" } as never, loader: openRegsFixture(),
    initialRoute: { mode: "water", from: { ...HOME, tab: "map" },
                    stack: [{ item: "gnis:8634" as never, part: 0, tapSid: water!.sections[0]!, at: null,
                              view: VIEW_KEY[view] }] },
  } as never) as ReactNode));
  await waitFor(() => expect([!!READY[view](), screen.queryAllByText(NAME).length > 0]).toEqual([true, true]),
                { timeout: 20_000 });
  act(() => { fireGestureHandler(getByGestureTestId(id), drag); });
  const selected = screen.getByRole("tab", { name: view }).getAttribute("aria-selected");
  const out = { ...where(view), still: selected === "true" ? view : "moved" };
  cleanup();
  return out;
}

/** Every drag, and where it carried things (content: "followed" when it went left with the finger). */
async function drags() {
  const seen = {
      "Regulations, frame header": await dragOn("Regulations", "view-swipe-header"),
      "Regulations, regs header": await dragOn("Regulations", "view-swipe-regs-header"),
      "Regulations, body": await dragOn("Regulations", "view-swipe-body"),
      "Conditions, frame header": await dragOn("Conditions", "view-swipe-header"),
      "Conditions, body": await dragOn("Conditions", "view-swipe-conditions"),
      "Map, frame header": await dragOn("Map", "view-swipe-header"),
  };
  const followed = (c: number | string) => typeof c === "number" && c < 0 ? "followed" : c;
  return Object.fromEntries(Object.entries(seen).map(([k, s]) => [k, { ...s, content: followed(s.content!) }]));
}

describe("T44 a swipe moves nothing the views share", () => {
  it("wherever a drag starts, only the view's own content follows the finger", async () => {
    const fixed = { back: 0, strip: 0, name: 0 };
    expect(await drags()).toEqual({
      "Regulations, frame header": { ...fixed, place: 0, content: "followed", still: "Regulations" },
      "Regulations, regs header": { ...fixed, place: 0, content: "followed", still: "Regulations" },
      "Regulations, body": { ...fixed, place: 0, content: "followed", still: "Regulations" },
      "Conditions, frame header": { ...fixed, content: "followed", still: "Conditions" },
      "Conditions, body": { ...fixed, content: "followed", still: "Conditions" },
      "Map, frame header": { ...fixed, content: "followed", still: "Map" },
    });
  }, 120_000);

  it("MUTATION: headers that follow the finger themselves (the T44 review's defect) fail it", async () => {
    MUTANT.on = true;
    try {
      const seen = await drags();
      const moved = Object.entries(seen).flatMap(([k, s]) =>
        Object.entries(s).filter(([part, v]) => part !== "still" && v !== 0 && v !== "followed").map(([part]) => `${k}: ${part}`));
      // with the frame's one offset, a header that follows it is carried by every drag, wherever it began
      const all = (k: string, parts: string[]) => parts.map((p) => `${k}: ${p}`);
      const regs = ["back", "strip", "name", "place"], shared = ["back", "strip", "name"];
      expect(moved).toEqual([
        ...all("Regulations, frame header", regs), ...all("Regulations, regs header", regs), ...all("Regulations, body", regs),
        ...all("Conditions, frame header", shared), ...all("Conditions, body", shared), ...all("Map, frame header", shared),
      ]);
    } finally { MUTANT.on = false; }
  }, 120_000);
});
