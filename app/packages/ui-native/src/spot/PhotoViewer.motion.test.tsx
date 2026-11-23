/**
 * (T836) THE PHOTO GROWS FROM ITS THUMBNAIL AND SHRINKS BACK INTO IT; WITH REDUCE MOTION IT FADES.
 *
 * Sampled through the opening and the closing on the real `Animated` (as SheetMotion.test does: react-native-web's test
 * double ends every animation at once, which would pass any order of events), on the JS driver react-native-web runs;
 * the native driver drives the same values on a phone. A 800 x 600 photo fitted to a 400 x 600 stage is drawn 400 x 300;
 * its 100 px thumbnail at (20, 400) covers it at 1/3 scale, centred 130 px left of and 150 px below the stage's centre.
 * Mutation-checked 2026-11-17: the Reduce Motion check in `fromThumb`, the thumbnail-on-screen test, and the close's
 * shrink back (a fade instead) each fail a test here.
 */
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Image } from "react-native";
import { PHOTO_VIEWER } from "@app/core";
import { PHOTO_WORDS } from "@app/data/spots";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { LIGHT } from "../theme";

declare const setReduceMotion: (on: boolean) => void;

// the real Animated: NODE_ENV reads "development" while the imports below load (see SheetMotion.test.tsx)
const NODE_ENV = vi.hoisted(() => { const was = process.env.NODE_ENV; process.env.NODE_ENV = "development"; return was; });
beforeAll(() => { process.env.NODE_ENV = NODE_ENV; });
const { PhotoViewer } = await import("./PhotoViewer");

vi.stubGlobal("ResizeObserver", class {
  constructor(private cb: (e: unknown[]) => void) {}
  observe(target: Element) { this.cb([{ target }]); }
  unobserve() {} disconnect() {}
});
for (const [k, v] of [["offsetWidth", 400], ["offsetHeight", 600]] as const)
  Object.defineProperty(HTMLElement.prototype, k, { configurable: true, get: () => v });
// the stage measures as the window (react-native-web's measureInWindow reads the bounding box)
HTMLElement.prototype.getBoundingClientRect = () =>
  ({ left: 0, top: 0, x: 0, y: 0, width: 400, height: 600, right: 400, bottom: 600, toJSON: () => ({}) }) as DOMRect;

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date", "performance", "requestAnimationFrame", "cancelAnimationFrame", "setTimeout", "clearTimeout"] });
  vi.spyOn(Image, "getSize").mockImplementation((_uri, ok) => { ok(800, 600); });
});
afterEach(() => { cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); setReduceMotion(false); });

const THUMB = { x: 20, y: 400, w: 100, h: 100 };
const AT_THUMB = "translateX(-130px) translateY(150px) scale(0.333";
const FULL = "translateX(0px) translateY(0px) scale(1)";

async function open(thumb: typeof THUMB | null = THUMB) {
  const onClose = vi.fn();
  render(<PhotoViewer uris={["a.jpg", "b.jpg"]} index={0} onIndex={() => {}} onClose={onClose} palette={LIGHT}
                      thumb={async () => thumb} />);
  // the measurements answer (timers of 0 ms), and the opening's first frame is drawn
  await act(async () => { await vi.advanceTimersByTimeAsync(1); });
  return { onClose };
}
const look = () => {
  const el = screen.getByTestId("photo-open") as HTMLElement;
  return { t: el.style.transform, o: el.style.opacity };
};
const ground = () => Number((screen.getByTestId("photo-ground") as HTMLElement).style.opacity || 1);
const advance = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms); });

describe("the photo grows from its thumbnail (T836)", () => {
  it("opens on the thumbnail and grows to full screen as the ground fades in", async () => {
    await open();
    expect(look().t).toContain(AT_THUMB);
    expect(ground()).toBeLessThan(0.2);
    await advance(PHOTO_VIEWER.openMs + 50);
    expect(look().t).toBe(FULL);
    expect(ground()).toBe(1);
  });
  it("the ✕ shrinks it back into the thumbnail, then closes", async () => {
    const { onClose } = await open();
    await advance(PHOTO_VIEWER.openMs + 50);
    act(() => { fireEvent.click(screen.getByLabelText(PHOTO_WORDS.close)); });
    await advance(1);
    expect(onClose).not.toHaveBeenCalled();
    await advance(PHOTO_VIEWER.closeMs - 20);
    expect(look().t).toMatch(/scale\(0\.[3-4]/);
    await advance(100);
    expect(onClose).toHaveBeenCalledTimes(1);
  });
  it("a thumbnail scrolled off screen: it fades in instead", async () => {
    await open({ ...THUMB, y: 2000 });
    expect(look().t).toBe("");
    expect(Number(look().o)).toBeLessThan(0.2);
    await advance(PHOTO_VIEWER.fadeMs + 50);
    expect(Number(look().o || 1)).toBe(1);
  });
  it("Reduce Motion: it fades in and out, never grows or shrinks", async () => {
    setReduceMotion(true);
    const { onClose } = await open();
    expect(look().t).toBe("");
    expect(Number(look().o)).toBeLessThan(0.2);
    await advance(PHOTO_VIEWER.fadeMs + 50);
    act(() => { fireEvent.click(screen.getByLabelText(PHOTO_WORDS.close)); });
    await advance(1); // the close asks Reduce Motion, then starts
    await advance(PHOTO_VIEWER.fadeMs / 2);
    expect(look().t).toBe("");
    expect(Number(look().o)).toBeLessThan(1);
    await advance(PHOTO_VIEWER.fadeMs);
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});

describe("PhotoViewer chrome while dragging down (T970) and the counter's backing (T976)", () => {
  const opacityOf = (id: string) => Number((screen.getByTestId(id) as HTMLElement).style.opacity || 1);
  it("the buttons and the counter are gone before the ground is half gone, and come back on a spring-back", async () => {
    await open();
    await advance(PHOTO_VIEWER.openMs + 50);
    expect(opacityOf("photo-chrome")).toBe(1);
    // a quarter of the dismiss fade distance (H * 0.5 * 0.25): the ground is at 3/4, the chrome at 1/2
    const h = 600, quarter = h * PHOTO_VIEWER.dismissFadeShare * 0.25;
    const pan = (dy: number) => act(() => { fireGestureHandler(getByGestureTestId("photo-pan"),
      [{ translationX: 0, translationY: 0 }, { translationX: 0, translationY: dy }, { translationX: 0, translationY: dy }]); });
    pan(quarter);
    expect(opacityOf("photo-ground")).toBeCloseTo(0.75, 2);
    expect(opacityOf("photo-chrome")).toBeCloseTo(0.5, 2);
    const half = h * PHOTO_VIEWER.dismissFadeShare * 0.5;
    // the finger lifted short of closing: the spring-back brings the chrome back
    await advance(PHOTO_VIEWER.settleMs + 100);
    expect(opacityOf("photo-chrome")).toBe(1);
    pan(half);
    expect(opacityOf("photo-ground")).toBeCloseTo(0.5, 2);
    expect(opacityOf("photo-chrome")).toBe(0);
  });
  it("the counter has a dim backing, as the buttons have a disc: the ground colour at 0.45", async () => {
    await open();
    await advance(PHOTO_VIEWER.openMs + 50);
    const pill = screen.getByTestId("photo-counter-pill");
    expect(pill.contains(screen.getByTestId("photo-counter"))).toBe(true);
    const backing = screen.getByTestId("photo-counter-backing") as HTMLElement;
    expect(pill.contains(backing)).toBe(true);
    // the colour as the browser normalises it (a hex is read back as rgb), and the opacity the style must state
    const want = document.createElement("div"); want.style.backgroundColor = LIGHT.photo.ground;
    expect(want.style.backgroundColor).not.toBe("");
    expect(backing.style.backgroundColor).toBe(want.style.backgroundColor);
    expect(backing.style.opacity).toBe("0.45");
  });
});
