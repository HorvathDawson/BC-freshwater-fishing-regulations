/**
 * (T454) The photo viewer, mounted: a pinch / double-tap zooms the picture, a drag pans it while zoomed, and a swipe
 * changes photo at the fit but not while zoomed. (T836) One pan recogniser now carries the pan, the strip and the drag
 * down (`photoDragKind`); the motions run on react-native-web's test Animated, which ends each animation at once. Driven through RNGH's `fireGestureHandler` (which skips the recognisers'
 * activation thresholds); the maths themselves are pinned in core's photoZoom.test.ts. The feel is the owner's.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { PhotoViewer } from "./PhotoViewer";
import { PHOTO_VIEWER } from "@app/core";
import { PHOTO_WORDS, photoCounter } from "@app/data/spots";
import { LIGHT, THEMES } from "../theme";

afterEach(cleanup);
// jsdom has no layout: react-native-web measures a view from its offset size when a ResizeObserver reports it, so give
// every element 400 x 600 and an observer that reports on observe
vi.stubGlobal("ResizeObserver", class {
  constructor(private cb: (e: unknown[]) => void) {}
  observe(target: Element) { this.cb([{ target }]); }
  unobserve() {} disconnect() {}
});
for (const [k, v] of [["offsetWidth", 400], ["offsetHeight", 600]] as const)
  Object.defineProperty(HTMLElement.prototype, k, { configurable: true, get: () => v });
const fire = (id: string, events: object[]) => act(() => { fireGestureHandler(getByGestureTestId(id), events); });

async function mount(index = 1) {
  const onIndex = vi.fn(), onClose = vi.fn();
  const r = (i: number) => (
    <PhotoViewer uris={["a.jpg", "b.jpg", "c.jpg"]} index={i} onIndex={onIndex} onClose={onClose} palette={LIGHT} />
  );
  const view = render(r(index));
  await act(async () => { await new Promise((res) => setTimeout(res, 20)); }); // rn-web measures on a timer
  return { onIndex, onClose, rerender: (i: number) => view.rerender(r(i)) };
}
const scaleOf = () => {
  const t = (screen.getByTestId("photo-image") as HTMLElement).style.transform;
  return { t, scale: Number(/scale\(([\d.]+)\)/.exec(t)?.[1] ?? 1) };
};
const doubleTap = (x: number, y: number) =>
  fire("photo-double-tap", [{ x, y }, { x, y }]);
const styleOf = (id: string) => (screen.getByTestId(id) as HTMLElement).style.transform;
/** one finger: a sideways drag of dx (the strip at the fit) */
const swipe = (dx: number) => drag(dx, 0);
// an update with no movement first, as a browser's recogniser sends one (it must not decide the drag's kind)
const drag = (dx: number, dy: number) =>
  fire("photo-pan", [{ translationX: 0, translationY: 0 }, { translationX: 0, translationY: 0 },
                     { translationX: dx, translationY: dy }, { translationX: dx, translationY: dy }]);
/** the close measures (nothing here) and runs its animation (react-native-web's test Animated ends at once) */
const settle = () => act(async () => { await new Promise((res) => setTimeout(res, 200)); });

describe("PhotoViewer zoom", () => {
  it("a double-tap zooms in and another returns to the fit", async () => {
    await mount();
    expect(scaleOf().scale).toBe(1);
    doubleTap(300, 450);
    expect(scaleOf().scale).toBe(2.5);
    doubleTap(10, 10);
    expect(scaleOf().scale).toBe(1);
  });
  it("a pinch zooms about the fingers, and settles back to 5x past it when let go", async () => {
    await mount();
    fire("photo-pinch", [{ scale: 1, focalX: 200, focalY: 300 }, { scale: 2, focalX: 200, focalY: 300 }, { scale: 20, focalX: 200, focalY: 300 }]);
    expect(scaleOf().scale).toBe(5);
  });
  it("a drag pans while zoomed and settles at the photo's edge", async () => {
    await mount();
    doubleTap(200, 300);
    fire("photo-pan", [{ translationX: 0, translationY: 0 }, { translationX: 100, translationY: 0 }, { translationX: 5000, translationY: 0 }, { translationX: 5000, translationY: 0 }]);
    expect(scaleOf().t).toContain("translateX(300px)");
  });
  it("a swipe changes photo at the fit but not while zoomed", async () => {
    const { onIndex } = await mount(1);
    swipe(-200);
    expect(onIndex).toHaveBeenCalledWith(2);
    onIndex.mockClear();
    doubleTap(200, 300);
    swipe(-200);
    expect(onIndex).not.toHaveBeenCalled();
  });
  it("the zoom resets on a photo change", async () => {
    const { rerender } = await mount(0);
    doubleTap(200, 300);
    expect(scaleOf().scale).toBe(2.5);
    rerender(1);
    expect(scaleOf().scale).toBe(1);
  });
});

/*
 * (T836) THE STANDARD PHONE PHOTO VIEWER, mounted. The thresholds are pinned in core's photoViewer.test.ts; these pin the
 * wiring: each fails when its gesture's line in PhotoViewer.tsx is removed (mutation-checked 2026-11-17).
 */
describe("PhotoViewer as the phone photo viewers behave (T836)", () => {
  const chromeShown = () => screen.getByTestId("photo-chrome").getAttribute("aria-hidden") !== "true";
  it("a drag down at the fit closes it", async () => {
    const { onClose } = await mount(1);
    drag(0, 300);
    await settle();
    expect(onClose).toHaveBeenCalledTimes(1);
  });
  it("a short drag down springs back and stays open", async () => {
    const { onClose } = await mount(1);
    drag(0, 30);
    await settle();
    expect(onClose).not.toHaveBeenCalled();
    expect(styleOf("photo-open-drag")).toContain("translateY(0px)");
  });
  it("a drag down while zoomed pans the photo, never closes", async () => {
    const { onClose } = await mount(1);
    doubleTap(200, 300);
    drag(0, 300);
    await settle();
    expect(onClose).not.toHaveBeenCalled();
    expect(scaleOf().scale).toBe(2.5);
  });
  it("the strip follows a sideways drag and settles back when short", async () => {
    const { onIndex } = await mount(1);
    swipe(-40);
    expect(onIndex).not.toHaveBeenCalled();
    // at rest on photo 2 of 3: the strip is one photo and one gap to the left
    expect(styleOf("photo-strip")).toContain("translateX(-416px)");
  });
  it("one tap hides the buttons and the counter, another brings them back; a double tap leaves them", async () => {
    await mount(1);
    expect(screen.getByTestId("photo-counter").textContent).toBe(photoCounter(2, 3));
    expect(chromeShown()).toBe(true);
    fire("photo-tap", [{ x: 10, y: 10 }]);
    expect(chromeShown()).toBe(false);
    fire("photo-tap", [{ x: 10, y: 10 }]);
    expect(chromeShown()).toBe(true);
    doubleTap(200, 300);
    expect(chromeShown()).toBe(true);
  });
  it("(T836) a finger that moves as far as a drag is decided is no tap: a quick swipe moves the strip, the buttons stay", async () => {
    await mount(1);
    // fireGestureHandler skips the recognisers' thresholds, so the limit is read off what reaches the native handler
    // (`maxDistance` is sent as `maxDist`): with none
    // (the handler's default) a swipe faster than the tap's 500 ms also hid the buttons on the iPhone simulator
    for (const id of ["photo-tap", "photo-double-tap"])
      expect((getByGestureTestId(id) as unknown as { config: { maxDist?: number } }).config.maxDist, id)
        .toBe(PHOTO_VIEWER.decideAfter);
  });
  it("the buttons' full-window layers pass a touch through to the photo (only the buttons take one)", async () => {
    await mount(1);
    // every layer between a button and the chrome's root spans the window: each must let a tap or a drag fall through
    // (on the web an inline `pointerEvents` style is dropped, and the insets' layer took every touch)
    const root = screen.getByTestId("photo-chrome");
    let el: HTMLElement | null = screen.getByLabelText(PHOTO_WORDS.next).parentElement;
    const layers: string[] = [];
    while (el && el !== root.parentElement) { layers.push(getComputedStyle(el).pointerEvents); el = el.parentElement; }
    expect(layers.length).toBeGreaterThan(2);
    expect(layers.filter((p) => p !== "none")).toEqual([]);
  });
  it("the counter reads \"2 of 3\"; one photo has none", async () => {
    expect(photoCounter(2, 3)).toBe("2 of 3");
    render(<PhotoViewer uris={["a.jpg"]} index={0} onIndex={() => {}} onClose={() => {}} palette={LIGHT} />);
    expect(screen.queryByTestId("photo-counter")).toBeNull();
  });
  it("the ✕ closes it after its fade (no thumbnail handed)", async () => {
    const { onClose } = await mount(1);
    act(() => { fireEvent.click(screen.getByLabelText(PHOTO_WORDS.close)); });
    await settle();
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});

/*
 * (T819 item 4) THE VIEWER IS BLACK IN EVERY THEME: it drew on `palette.ink`, which in the dark theme is light grey, so
 * a photograph opened in dark mode sat on a pale ground. Mutation: back to `palette.ink` fails the dark theme.
 */
describe("PhotoViewer ground (T819)", () => {
  const lum = (hex: string) => {
    const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16) / 255)
      .map((c) => (c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4)) as [number, number, number];
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  };
  const rgb = (hex: string) => `rgb(${[1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16)).join(", ")})`;
  it.each(Object.entries(THEMES))("%s: a near-black ground, light words on it", async (_name, palette) => {
    expect(lum(palette.photo.ground)).toBeLessThan(0.02);
    expect(lum(palette.photo.ink)).toBeGreaterThan(0.8);
    render(<PhotoViewer uris={["a.jpg"]} index={0} onIndex={() => {}} onClose={() => {}} palette={palette} />);
    // (T836) the ground is its own layer, so it can fade with the opening and the drag down
    expect((screen.getByTestId("photo-ground") as HTMLElement).style.backgroundColor).toBe(rgb(palette.photo.ground));
  });
});
