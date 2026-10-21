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
  it("turns keyboard rotation off and shows the compass only where the map may turn", () => {
    const src = readFileSync(new URL("../packages/map/src/Map.web.tsx", import.meta.url), "utf8");
    expect([src.includes("if (!keyboardRotation()) m.keyboard.disableRotation();"),
            /showCompass: compassShown\(\)/.test(src), /showCompass: true/.test(src)]).toEqual([true, true, false]);
  });
});

/**
 * N2: the NATIVE map is mounted with the same policy (`nativeMapGestures()`, spread onto MapLibre Native's `Map`) and
 * colours sections by feature-state on the tiles' MVT id, through the handle both maps share. The behaviour is proved
 * against a recording binding in `packages/map/src/adapter-contract.render.test.tsx`; this holds the wiring in the file
 * that ships, so a later edit cannot quietly route around it.
 */
describe("the native map applies the gesture policy and feature-state by id", () => {
  const src = readFileSync(new URL("../packages/map/src/Map.native.tsx", import.meta.url), "utf8");
  const mapEl = src.slice(src.indexOf("<MLMap"), src.indexOf("</MLMap>"));
  it("spreads the policy onto the map element (no switch set beside it), and says the tap slop is MapLibre's own", () => {
    expect([mapEl.includes("{...nativeMapGestures()}"), /touchRotate|touchPitch|compass=|dragPan|touchZoom/.test(mapEl),
            src.includes("The 8 px TAP SLOP is MapLibre Native's own here")]).toEqual([true, false, true]);
  });
  it("sets feature-state on the atlas source by { id, sourceLayer } through the shared state handle", () => {
    expect([src.includes("return stateHandle({"),
            src.includes("src.setFeatureState({ id, sourceLayer: t.sourceLayer }, s as never)"),
            src.includes("src.removeFeatureState({ id, sourceLayer: t.sourceLayer }, key)"),
            src.includes('<VectorSource id="atlas" ref={atlasAttached} />'),
            src.includes("nativeStyle({")]).toEqual([true, true, true, true, true]);
  });
});
