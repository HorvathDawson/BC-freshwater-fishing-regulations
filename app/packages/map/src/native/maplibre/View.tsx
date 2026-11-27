/**
 * (T1154) MAPLIBRE NATIVE'S MAP, the engine's component tree: `../../Map.native.tsx`'s map element and its children,
 * moved here unchanged in order and nesting. The shell decides every value and every handler (`EngineViewProps`,
 * `../engine.ts`); this file names the binding's elements and props, nothing else. Its traps, each a native bug once fixed:
 *   - NO WRAPPER VIEW around the map: the shell renders this element as its first child and its furniture as siblings (a
 *     wrapped map left a black rectangle over the next screen on Android when it unmounted; N2 review);
 *   - the binding's own scale bar stays ON, parked off screen (`scaleBar`, below);
 *   - the atlas source's ref is the shell's callback (`atlasAttached`): attach-once is the shell's;
 *   - a source the style declares is adopted by id and never handed the data it was mounted with (T831).
 */
import type { ComponentProps } from "react";
import { CAMERA_BOUNDS, CAMERA_MAX_ZOOM, MIN_ZOOM } from "@app/core";
import { Camera, GeoJSONSource, Images, Layer, Map as MLMap, VectorSource } from "@maplibre/maplibre-react-native";
import { nativeMapGestures } from "../../gestures";
import { SALMON_CIRCLE_BEFORE, SALMON_CIRCLE_SOURCE } from "../../salmon-circles";
import { SPOT_SOURCE } from "../../spot-marks";
import { MARK_SOURCE, PIN_SOURCE } from "../../point-marks";
import { COPY_SOURCE } from "../../water-copy";
import { ME_SOURCE } from "../../me";
import { TRACE_PINS_SOURCE, TRACE_SOURCE } from "../../trace-overlay";
import type { EngineViewProps } from "../engine";
import { bindingRefs } from "./handle";
import { HeldLabels } from "./HeldLabels";
import { REGION_LABEL_DATA, REGION_LABEL_SOURCE, TOWN_DATA, TOWN_SOURCE } from "../../look-a";

/** (T317) a style-spec layer as the binding's `Layer` takes it (its source is the enclosing `GeoJSONSource`) */
type LayerSpec = ComponentProps<typeof Layer>;

/** `CAMERA_BOUNDS` (`[[w, s], [e, n]]`) in the binding's flat `[w, s, e, n]`. */
const MAX_BOUNDS: [number, number, number, number] =
  [CAMERA_BOUNDS[0][0], CAMERA_BOUNDS[0][1], CAMERA_BOUNDS[1][0], CAMERA_BOUNDS[1][1]];
/** Where the binding's own scale bar is parked: present (see the `scaleBar` prop), never seen. */
const OFF_SCREEN = { top: -10000, left: -10000 } as const;

export function EngineView({ refs, atlasAttached, style, onLayout, mapStyle, pixelRatio, prefetchDelta, initial,
                             onPress, onTapUp, onLongPress, onRegionWillChange, onRegionIsChanging, onRegionDidChange,
                             onFrame, onFrameFull, onStyleLoaded, onRenderedFully, onFail,
                             held: { store: hold, ids: adopted, now: heldNow }, images, gauges: gaugeData, trace, tracePins,
                             copy: copyData, circles: { data: circleJson, layers: circleLook },
                             me: { data: meJson, look: meLook }, pins: { data: pinJson, layer: pinLook },
                             spots: { data: spotJson, layers: spotLook }, marks: { data: markJson, layers: markLook } }: EngineViewProps) {
  const binding = bindingRefs(refs);
  return (
    <MLMap
      ref={binding.map}
      style={[{ flex: 1 }, style]}
      onLayout={onLayout}
      mapStyle={mapStyle as never}
      // G1: one gesture policy for both maps — pan, pinch, double-tap zoom; no rotate, no tilt, no compass
      {...nativeMapGestures()}
      // (T424) the binding's own attribution "i" is OFF: the credits are the shell's `MapCredits`, the web's own component. A
      // bare map is an illustration: no furniture, no credits (the page that carries it credits the data, MapProps.bare)
      attribution={false}
      // (T448) at most 2 device pixels per dp, like the web (`pixel-ratio.ts`): the patched binding draws into a smaller
      // surface the compositor scales up (Android) — fewer pixels filled, the same scale, gestures and taps
      maxPixelRatio={pixelRatio}
      // (T401) parent tiles loaded as placeholders while a tile loads (patched binding, Android; iOS ignores it)
      prefetchZoomDelta={prefetchDelta}
      logo={false}
      // OURS is drawn below (`metricScale`): the binding's is top-left only on Android, in the device's units. But
      // the binding's must EXIST on Android: switched off, the map's surface left a black hole over the next screen
      // when the map unmounted (emulator, N2 review; bisected to this one prop). So it is on, and parked off screen
      scaleBar
      scaleBarPosition={OFF_SCREEN}
      onPress={onPress as never}
      onTapUp={onTapUp as never}
      onLongPress={onLongPress as never}
      onRegionDidChange={onRegionDidChange as never}
      onRegionWillChange={onRegionWillChange as never}
      onRegionIsChanging={onRegionIsChanging as never}
      // a frame drawn, and a frame drawn with every tile in
      onDidFinishRenderingFrame={onFrame as never}
      onDidFinishRenderingFrameFully={onFrameFull as never}
      onDidFinishLoadingStyle={onStyleLoaded}
      onDidFinishRenderingMapFully={onRenderedFully}
      onDidFailLoadingMap={onFail}
    >
      <Camera ref={binding.camera} initialViewState={initial} minZoom={MIN_ZOOM} maxZoom={CAMERA_MAX_ZOOM}
              maxBounds={MAX_BOUNDS} />
      <VectorSource id="atlas" ref={atlasAttached}>
        <HeldLabels store={hold} ids={adopted.atlas} now={heldNow.atlas} />
      </VectorSource>
      {/* (T876) the basemap's and the units' names, adopted by id for the hold (the style's own sources) */}
      <VectorSource id="basemap"><HeldLabels store={hold} ids={adopted.basemap} now={heldNow.basemap} /></VectorSource>
      <VectorSource id="mus"><HeldLabels store={hold} ids={adopted.mus} now={heldNow.mus} /></VectorSource>
      {/* (T74) the hatches, the gauge pill and the route's pin, in this theme */}
      <Images images={images as never} />
      <GeoJSONSource id="gauges" data={gaugeData}>
        <HeldLabels store={hold} ids={adopted.gauges} now={heldNow.gauges} />
      </GeoJSONSource>
      {/* (T1208, T1209) the towns' and the region names' sources, the style's own by id: their names wait out a zoom too */}
      <GeoJSONSource id={TOWN_SOURCE} data={TOWN_DATA as never}>
        <HeldLabels store={hold} ids={adopted[TOWN_SOURCE]} now={heldNow[TOWN_SOURCE]} />
      </GeoJSONSource>
      <GeoJSONSource id={REGION_LABEL_SOURCE} data={REGION_LABEL_DATA as never}>
        <HeldLabels store={hold} ids={adopted[REGION_LABEL_SOURCE]} now={heldNow[REGION_LABEL_SOURCE]} />
      </GeoJSONSource>
      {/* (T831) the route map's lines, gauge dots and "you are here" pin: the style's own sources by id, as `gauges`
          (what the map hands them is pushed to the source the binding adopted: it never pushes what it was mounted with) */}
      <GeoJSONSource id={TRACE_SOURCE} data={trace as never} />
      <GeoJSONSource id={TRACE_PINS_SOURCE} data={tracePins as never} />
      {/* (T175) the water copy: the style's own source by id, its lines from the tiles (`keepCopy`) */}
      <GeoJSONSource id={COPY_SOURCE} ref={binding.copy} data={copyData as never} />
      {/* (T812) THE CLOSED-AREA CIRCLES, under the streams: never asked for a feature, so a tap opens the lake under them */}
      <GeoJSONSource id={SALMON_CIRCLE_SOURCE} data={circleJson as never}>
        {circleLook.map((l) => <Layer key={l.id} {...(l as unknown as LayerSpec)} beforeId={SALMON_CIRCLE_BEFORE} />)}
      </GeoJSONSource>
      {/* (T303) your location: the accuracy circle, then the dot over it, above every map layer */}
      <GeoJSONSource id={ME_SOURCE} data={meJson}>
        <Layer id="me-accuracy" type="circle" paint={meLook.accuracy as never} />
        <Layer id="me-dot" type="circle" paint={meLook.dot as never} />
      </GeoJSONSource>
      {/* (T317) the "where I tapped" marker and a panel's named points: dots drawn by the map, not view annotations */}
      <GeoJSONSource id={PIN_SOURCE} data={pinJson as never}>
        <Layer {...(pinLook as unknown as LayerSpec)} />
      </GeoJSONSource>
      {/* (T131) YOUR SPOTS: `spotMarkers` at the zoom the camera settled at — (T316) map layers, not view annotations,
          so they move with the map; a tap is the map's (`onPress`: `spotUnder` → `pressSpot`) */}
      <GeoJSONSource id={SPOT_SOURCE} data={spotJson as never}>
        {spotLook.map((l) => <Layer key={l.id} {...(l as unknown as LayerSpec)} />)}
      </GeoJSONSource>
      {/* THE BOUNDARY MARKS (P15): the ring (filled when open) and its label above it in a box — map layers (T317), so
          they move with the map; a tap is the layers' (`onPress`, `markHit`) */}
      <GeoJSONSource id={MARK_SOURCE} data={markJson as never}>
        {markLook.map((l) => <Layer key={l.id} {...(l as unknown as LayerSpec)} />)}
      </GeoJSONSource>
    </MLMap>
  );
}
