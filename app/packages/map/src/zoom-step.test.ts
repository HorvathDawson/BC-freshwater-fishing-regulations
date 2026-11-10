import { describe, expect, it } from "vitest";
import { CAMERA_MAX_ZOOM, MIN_ZOOM } from "@app/core";
import { zoomStep } from "./zoom-step";

describe("(T621, G15) the phone's zoom buttons", () => {
  it("step one level and stop at the camera's limits", () => {
    expect([zoomStep(8, 1), zoomStep(8, -1), zoomStep(CAMERA_MAX_ZOOM, 1), zoomStep(MIN_ZOOM, -1)])
      .toEqual([9, 7, CAMERA_MAX_ZOOM, MIN_ZOOM]);
  });
});
