/**
 * (T812, #50 review) The web circles go under the streams; a style without the stream layer is reported, never drawn on top.
 */
import { describe, expect, it } from "vitest";
import { setFailureSink } from "@app/core";
import { SALMON_CIRCLE_BEFORE } from "../salmon-circles";
import { circlesBefore } from "./useSalmonCircles";

describe("(T812) where the web map puts the closed-area circles", () => {
  it("under the streams when the style has them; none, and reported, when it does not", () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      expect(circlesBefore({ getLayer: (id: string) => (id === SALMON_CIRCLE_BEFORE ? ({ id } as never) : undefined) }))
        .toBe(SALMON_CIRCLE_BEFORE);
      expect(seen).toEqual([]);
      expect(circlesBefore({ getLayer: () => undefined })).toBeNull();
      expect(seen.map((e) => (e as Error).message)).toEqual(
        [`salmon circles: the map style has no "${SALMON_CIRCLE_BEFORE}" layer to draw them under`]);
    } finally { undo(); }
  });
});
