import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";

/**
 * G1: the web map is CONSTRUCTED with the one gesture policy (`@app/map` maplibreGestures, from `@app/core`
 * MAP_GESTURES). The options themselves are tested beside them; this holds that they are applied, which no unit test
 * of the options can see. In tools/ because it reads source off disk (see no-per-frame-animation.test.ts).
 */
describe("the web map applies the gesture policy", () => {
  it("spreads the options into the constructor and turns pinch-rotation off after it", () => {
    const src = readFileSync(new URL("../packages/map/src/Map.web.tsx", import.meta.url), "utf8");
    const ctor = src.slice(src.indexOf("new maplibregl.Map({"), src.indexOf("});", src.indexOf("new maplibregl.Map({")));
    expect([ctor.includes("...maplibreGestures(),"),
            src.includes("if (!touchRotation()) m.touchZoomRotate.disableRotation();")]).toEqual([true, true]);
  });
});
