import { afterEach, describe, expect, it } from "vitest";
import { cameraMs, setReducedMotion } from "./motion";

afterEach(() => setReducedMotion(false));

describe("(T243) camera moves and reduce motion", () => {
  it("glide as asked, and jump while reduce motion is on", () => {
    const before = cameraMs(450);
    setReducedMotion(true);
    const on = [cameraMs(450), cameraMs(300)];
    setReducedMotion(false);
    expect([before, on, cameraMs(300)]).toEqual([450, [0, 0], 300]);
  });
});
