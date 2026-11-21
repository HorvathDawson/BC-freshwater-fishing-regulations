/**
 * (T984, owner 2026-11-21: "the tutorial should be only a walk through not modify the state … right now it changes data
 * and is very slow in highlighting parts. which the user could zoom past and break it") THE TOUR CHANGES NOTHING.
 *  - Through the real Shell: a full tour (every Next, then Skip on another run) leaves the app exactly as the app drawn
 *    with no tour at all; the only store written is the tour's own seen line.
 *  - A step's highlight is on screen within a budget, never waiting on the map or the network.
 *  - The dim takes every touch (the hole too), wherever its scroll carries it.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, renderHook, screen } from "@testing-library/react";
import { Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { WALKTHROUGH_VERSION, WALKTHROUGH_WORDS } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, LayoutWidthContext } from "@app/ui";
import { LIGHT } from "./theme";
import { GUIDE_STEPS, GuideCard, panes, type Guide } from "./Walkthrough";
import { useCoachRegistry, type Rect } from "./CoachMarks";

vi.mock("./MapScreen", () => ({ MapScreen: () => <View testID="map-stub"><Text>map</Text></View> }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
const { openRegsFixtureWithLines } = await import("@app/data/release/node");
afterEach(cleanup);

const W = WALKTHROUGH_WORDS;
const settle = (ms = 600) => act(async () => { await new Promise((r) => setTimeout(r, ms)); });

function mount(store: { read(): string | null; write(t: string): void }) {
  render(
    <LayoutWidthContext.Provider value={390}><GestureHandlerRootView>
      <Shell source={makeFixtureSource()} loader={openRegsFixtureWithLines()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} walkthrough={store}
             systemBack={() => ({ sync: () => {}, dispose: () => {} })}
             feed={{ index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never}
             spots={[]} onSaveSpot={async () => {}} />
    </GestureHandlerRootView></LayoutWidthContext.Provider>);
}
/** the app as drawn, the tour's own layer left out */
const app = () => {
  const c = document.body.cloneNode(true) as HTMLElement;
  c.querySelectorAll('[data-testid="guide"]').forEach((e) => e.remove());
  // the search field's hint is picked at random on each mount (`placeholderFor`): the one thing that differs by chance
  c.querySelectorAll("[placeholder]").forEach((e) => e.removeAttribute("placeholder"));
  return c.innerHTML;
};

describe("(T984) the tour changes nothing", () => {
  it("every step leaves the app as the app with no tour, a full tour and a skip alike; only its own line is written", async () => {
    // the app with no tour: the walkthrough already seen
    mount({ read: () => WALKTHROUGH_VERSION, write: () => { throw new Error("no write expected"); } });
    await settle();
    expect(screen.queryByTestId("guide")).toBeNull();
    const before = app();
    cleanup();

    const writes: string[] = [];
    const store = { v: null as string | null, read: () => store.v, write: (t: string) => { writes.push(t); store.v = t; } };
    mount(store);
    await screen.findByTestId("guide", {}, { timeout: 4000 });
    await settle();
    for (let i = 0; i < GUIDE_STEPS.length; i++) {
      expect(screen.getByTestId(`guide-step-${GUIDE_STEPS[i]!.key}`)).toBeTruthy();
      expect(app(), `step ${GUIDE_STEPS[i]!.key} changed the app`).toBe(before);
      act(() => { fireEvent.click(screen.getByText(i === GUIDE_STEPS.length - 1 ? W.done : W.next)); });
      await settle(80);
    }
    expect(screen.queryByTestId("guide")).toBeNull();
    expect(app(), "after the tour").toBe(before);
    expect(store.v).toBe(WALKTHROUGH_VERSION);
    // every write is the tour's own line: its place (guide-at) and its seen version
    for (const t of writes) expect(t.split("\n").every((l) => l.startsWith("guide-at:") || l === WALKTHROUGH_VERSION), t).toBe(true);
  });

  it("a skip part-way leaves the app as it was", async () => {
    mount({ read: () => WALKTHROUGH_VERSION, write: () => {} });
    await settle();
    const before = app();
    cleanup();
    const store = { v: null as string | null, read: () => store.v, write: (t: string) => { store.v = t; } };
    mount(store);
    await screen.findByTestId("guide", {}, { timeout: 4000 });
    act(() => { fireEvent.click(screen.getByText(W.next)); });
    act(() => { fireEvent.click(screen.getByText(W.next)); });
    act(() => { fireEvent.click(screen.getByTestId("guide-skip")); });
    await settle();
    expect(screen.queryByTestId("guide")).toBeNull();
    expect(app()).toBe(before);
  });
});

describe("(T984) the highlight is at once", () => {
  /** budget, ms from a step's card up to its ring on screen: the check runs every 32 ms and wants two agreeing looks */
  const BUDGET_MS = 250;
  it(`each step's ring is drawn within ${BUDGET_MS} ms, with nothing to wait for`, async () => {
    for (const s of GUIDE_STEPS.filter((x) => x.targets.length > 0)) {
      const reg = renderHook(() => useCoachRegistry()).result.current;
      for (const k of s.targets) reg.set(k, { measureInWindow: (cb) => cb(16, 120, 200, 40) });
      const g: Guide = { open: true, pending: true, at: GUIDE_STEPS.indexOf(s), reopen() {}, finish() {}, next() {}, back() {} };
      const t0 = performance.now();
      render(<GuideCard guide={g} palette={LIGHT} registry={reg} top={60} bottom={64} />);
      await screen.findByTestId("guide-ring", {}, { timeout: 2000 });
      const ms = performance.now() - t0;
      expect(ms, `${s.key} took ${Math.round(ms)} ms`).toBeLessThan(BUDGET_MS);
      cleanup();
    }
  });
});

const PHONE = { w: 390, h: 844 };
const meets = (a: Rect, b: Rect) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
describe("(T842, T984) the dim round the highlight", () => {
  it("covers the screen but the hole, wherever its scroll carries it (the hole has its own touch-taking layer)", () => {
    const win = PHONE, hole = { x: 20, y: 300, w: 200, h: 60 };
    const ps = panes(hole, win).map((p) => ({ x: p.left, y: p.top, w: p.width, h: p.height }));
    for (const p of ps) expect(meets(p, hole), JSON.stringify(p)).toBe(false);
    for (const dy of [-win.h, -200, 0, 200, win.h]) for (let x = 0; x < win.w; x += 13) for (let y = 0; y < win.h; y += 13) {
      const inHole = x >= hole.x && x < hole.x + hole.w && y >= hole.y + dy && y < hole.y + hole.h + dy;
      const covered = ps.some((p) => x >= p.x && x < p.x + p.w && y >= p.y + dy && y < p.y + p.h + dy);
      expect(covered, `${x},${y} carried ${dy}`).toBe(!inHole);
    }
  });
});
