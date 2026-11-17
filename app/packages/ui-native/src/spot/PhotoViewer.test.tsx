/**
 * (T454) The photo viewer, mounted: a pinch / double-tap zooms the picture, a drag pans it while zoomed, and a swipe
 * changes photo at the fit but not while zoomed. Driven through RNGH's `fireGestureHandler` (which skips the recognisers'
 * activation thresholds); the maths themselves are pinned in core's photoZoom.test.ts. The feel is the owner's.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { PhotoViewer } from "./PhotoViewer";
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

describe("PhotoViewer zoom", () => {
  it("a double-tap zooms in and another returns to the fit", async () => {
    await mount();
    expect(scaleOf().scale).toBe(1);
    doubleTap(300, 450);
    expect(scaleOf().scale).toBe(2.5);
    doubleTap(10, 10);
    expect(scaleOf().scale).toBe(1);
  });
  it("a pinch zooms about the fingers, capped at 5x", async () => {
    await mount();
    fire("photo-pinch", [{ scale: 1, focalX: 200, focalY: 300 }, { scale: 2, focalX: 200, focalY: 300 }, { scale: 20, focalX: 200, focalY: 300 }]);
    expect(scaleOf().scale).toBe(5);
  });
  it("a drag pans while zoomed and stops at the photo's edge", async () => {
    await mount();
    doubleTap(200, 300);
    fire("photo-pan", [{ translationX: 0, translationY: 0 }, { translationX: 100, translationY: 0 }, { translationX: 5000, translationY: 0 }, { translationX: 5000, translationY: 0 }]);
    expect(scaleOf().t).toContain("translateX(300px)");
  });
  it("a swipe changes photo at the fit but not while zoomed", async () => {
    const { onIndex } = await mount(1);
    fire("photo-swipe", [{ translationX: 0 }, { translationX: -90 }]);
    expect(onIndex).toHaveBeenCalledWith(2);
    onIndex.mockClear();
    doubleTap(200, 300);
    expect(() => getByGestureTestId("photo-swipe")).not.toThrow();
    fire("photo-swipe", [{ translationX: 0 }, { translationX: -90 }]);
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
    expect((screen.getByTestId("photo-viewer") as HTMLElement).style.backgroundColor).toBe(rgb(palette.photo.ground));
  });
});
