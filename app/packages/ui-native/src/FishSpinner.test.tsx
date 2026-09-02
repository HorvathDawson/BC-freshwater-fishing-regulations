/**
 * The loading fish, as mobile web renders it.
 *
 * vitest resolves `.web.tsx` first, so this exercises `FishSpinner.web.tsx` — which is
 * correct: that is the file a phone browser runs. The native variant's one real property
 * (that it hands both loops to the platform driver) is a source-level assertion in
 * `tools/no-per-frame-animation.test.ts`, because a jsdom test cannot observe a native
 * animation driver at all.
 *
 * What matters about this component is not what it looks like — the artwork is baked and
 * reviewed as an image — but that it never asks the JS thread for a frame, and that the
 * geometry it pages with matches the strip it was given.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { FishSpinner } from "./FishSpinner";
import { FISH_SPRITE } from "./fish-sprite.generated";
import { LIGHT, DARK } from "./theme";

declare const setReduceMotion: (on: boolean) => void;

afterEach(() => { cleanup(); setReduceMotion(false); });

const strip = (c: HTMLElement) => c.querySelector<HTMLElement>("[data-testid='fish-sprite']")!;
const ring = (c: HTMLElement) => c.querySelector<HTMLElement>("[data-testid='fish-ring']")!;

/** `#E5E6E1` -> `rgb(229, 230, 225)`, which is what the DOM reports back. */
const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

describe("<FishSpinner>", () => {
  it("announces itself as a progress indicator, not as decoration", () => {
    const { getByLabelText } = render(<FishSpinner palette={LIGHT} label="Loading rivers" />);
    expect(getByLabelText("Loading rivers").getAttribute("role")).toBe("progressbar");
  });

  it("sizes the strip to exactly the frames the baked file holds", () => {
    // The guard against the generated sprite and the component drifting apart: change the
    // frame count in the builder without rebuilding and this is what catches it.
    const { container } = render(<FishSpinner palette={LIGHT} size={120} />);
    expect(strip(container).style.width).toBe(`${120 * FISH_SPRITE.frames}px`);
    expect(strip(container).style.height).toBe("120px");
  });

  it("shows one frame at a time, by clipping", () => {
    const { container } = render(<FishSpinner palette={LIGHT} size={96} />);
    const window = strip(container).parentElement!;
    expect(window.style.overflow).toBe("hidden");
    expect(parseFloat(strip(container).style.width))
      .toBeGreaterThan(parseFloat(getComputedStyle(window).width) || 96);
  });

  it("steps the strip in PIXELS, not a percentage", () => {
    // A percentage translate on a strip N times wider than its box does not move by one
    // frame — it resolves against (box - content) and lands nowhere near a frame boundary.
    const { container } = render(<FishSpinner palette={LIGHT} size={96} />);
    expect(strip(container).style.getPropertyValue("--fish-strip"))
      .toBe(`-${96 * FISH_SPRITE.frames}px`);
  });

  it("runs on CSS, so the JS thread is never asked for a frame", () => {
    const { container } = render(<FishSpinner palette={LIGHT} />);
    expect(strip(container).style.animation).toContain("steps");
    expect(strip(container).parentElement!.style.animation).toContain("fish-orbit");
  });

  it("tints the one grayscale strip per palette, rather than baking a strip per theme", () => {
    const light = render(<FishSpinner palette={LIGHT} />);
    expect(strip(light.container).style.backgroundColor).toBe(rgb(LIGHT.live));
    cleanup();
    const dark = render(<FishSpinner palette={DARK} />);
    expect(strip(dark.container).style.backgroundColor).toBe(rgb(DARK.live));
    expect(LIGHT.live).not.toBe(DARK.live);
  });

  it("paints the fish in the water colour, not the selection colour", () => {
    // `accent` means "you chose this" and belongs on controls. A loading fish is about
    // water — the archived canvas loader was steel blue for the same reason.
    const { container } = render(<FishSpinner palette={LIGHT} />);
    expect(strip(container).style.backgroundColor).toBe(rgb(LIGHT.live));
    expect(strip(container).style.backgroundColor).not.toBe(rgb(LIGHT.accent));
  });

  it("draws the orbit ring in its own colour rather than baking it into the strip", () => {
    // One tint covers everything in the strip, so a baked ring is the fish's colour.
    const { container } = render(<FishSpinner palette={LIGHT} size={100} />);
    expect(ring(container).style.borderColor).toBe(rgb(LIGHT.line));
    expect(parseFloat(ring(container).style.width)).toBeLessThan(100);
  });

  it("holds still when the viewer has asked for reduced motion", () => {
    setReduceMotion(true);
    const { container } = render(<FishSpinner palette={LIGHT} />);
    expect(strip(container).style.animationPlayState).toBe("paused");
    expect(strip(container).parentElement!.style.animationPlayState).toBe("paused");
  });

  it("animates when it has not", () => {
    const { container } = render(<FishSpinner palette={LIGHT} />);
    expect(strip(container).style.animationPlayState).toBe("running");
  });

  it("unmounts cleanly even where the browser has no matchMedia", () => {
    const saved = globalThis.matchMedia;
    // @ts-expect-error deliberately removing it, which is the situation being tested
    delete globalThis.matchMedia;
    try {
      const { unmount } = render(<FishSpinner palette={LIGHT} />);
      expect(() => unmount()).not.toThrow();
    } finally {
      globalThis.matchMedia = saved;
    }
  });
});
