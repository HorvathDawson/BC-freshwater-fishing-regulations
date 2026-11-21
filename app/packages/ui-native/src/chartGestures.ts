/**
 * (T985 #4, owner pick B + hold) THE CHART'S GESTURES, as one hook. Pure measuring: each recogniser hands a pixel to the
 * chart's own actions (`probeAt`, `panBy`, `zoomAt`, `fit`), which are @app/ui's; nothing is decided here.
 *
 *   - BODY: a pinch zooms; a tap reads a value (the bar jumps there); a double-tap shows the whole chart; touch and HOLD,
 *     then drag, scrubs the bar under the finger; a quick one-finger drag is NOT claimed, so the page scrolls.
 *   - HANDLE (on the bar): a drag scrubs the date. Its keys / screen-reader actions step it (`step`).
 *   - AXIS STRIP (under the chart, shown while zoomed): a drag pans along the window.
 *
 * The recognisers are react-native-gesture-handler's on both targets; the browser's `touch-action` rides in each config
 * (`withWebProps`): the body leaves vertical scrolling to the page, the handle and the strip take every touch.
 */
import { useMemo, useRef } from "react";
import { usePanGesture, usePinchGesture, useExclusiveGestures, useSimultaneousGestures, useTapGesture,
         type PanGestureConfig, type PinchGestureConfig, type TapGestureConfig } from "react-native-gesture-handler";
import { withWebProps } from "./gestureWeb";
import type { ChartActions } from "./Hydrograph";

/** the browser keeps vertical scrolling over the chart body */
const BODY_WEB = { touchAction: "pan-y" } as const;
/** the handle and the strip own every touch that starts on them */
const OWN_WEB = { touchAction: "none" } as const;

/** how long a finger rests before a drag on the body scrubs instead of scrolling (ms) */
const HOLD_MS = 350;
/** the plot's width divided by this is one keyboard / screen-reader step of the bar */
const STEPS_ACROSS = 40;

export function useChartGestures(view: ChartActions, plotX: number, plotWidth: number) {
  // The latest actions, read at event time: the gestures are rebuilt per state, not per render.
  const live = useRef(view);
  live.current = view;
  const readX = () => live.current.readout?.x ?? plotX + plotWidth / 2;
  const clamp = (x: number) => Math.min(plotX + plotWidth, Math.max(plotX, x));

  const pinchConfig = useMemo((): PinchGestureConfig => {
    let lastScale = 1;
    return withWebProps({
      runOnJS: true, disableReanimated: true, testID: "chart-pinch",
      onActivate: () => { lastScale = 1; },
      onUpdate: (e) => { const f = e.scale / (lastScale || 1); lastScale = e.scale; live.current.zoomAt(e.focalX, f); },
    } satisfies PinchGestureConfig, BODY_WEB);
  }, []);
  const holdConfig = useMemo((): PanGestureConfig => withWebProps({
    runOnJS: true, disableReanimated: true, testID: "chart-hold", maxPointers: 1, activateAfterLongPress: HOLD_MS,
    onActivate: (e) => { live.current.probeAt(clamp(e.x)); },
    onUpdate: (e) => { live.current.probeAt(clamp(e.x)); },
  } satisfies PanGestureConfig, BODY_WEB), [plotX, plotWidth]);
  const doubleConfig = useMemo((): TapGestureConfig => withWebProps({
    runOnJS: true, disableReanimated: true, testID: "chart-double-tap", numberOfTaps: 2,
    onDeactivate: (e) => { if (!e.canceled) live.current.fit(); },
  } satisfies TapGestureConfig, BODY_WEB), []);
  const tapConfig = useMemo((): TapGestureConfig => withWebProps({
    runOnJS: true, disableReanimated: true, testID: "chart-tap",
    onDeactivate: (e) => { if (!e.canceled && e.x >= plotX) live.current.probeAt(e.x); },
  } satisfies TapGestureConfig, BODY_WEB), [plotX]);
  const body = useSimultaneousGestures(usePinchGesture(pinchConfig), usePanGesture(holdConfig),
                                       useExclusiveGestures(useTapGesture(doubleConfig), useTapGesture(tapConfig)));

  const handleConfig = useMemo((): PanGestureConfig => {
    let from = 0;
    return withWebProps({
      runOnJS: true, disableReanimated: true, testID: "chart-handle", maxPointers: 1,
      onActivate: () => { from = readX(); },
      onUpdate: (e) => { live.current.probeAt(clamp(from + e.translationX)); },
    } satisfies PanGestureConfig, OWN_WEB);
  }, [plotX, plotWidth]);
  const handle = usePanGesture(handleConfig);

  const axisConfig = useMemo((): PanGestureConfig => {
    let lastX = 0;
    return withWebProps({
      runOnJS: true, disableReanimated: true, testID: "chart-axis", maxPointers: 1,
      onActivate: () => { lastX = 0; },
      onUpdate: (e) => { live.current.panBy(e.translationX - lastX); lastX = e.translationX; },
    } satisfies PanGestureConfig, OWN_WEB);
  }, []);
  const axis = usePanGesture(axisConfig);

  /** one step of the bar, left (-1) or right (1): the keyboard's arrows and a screen reader's increment / decrement */
  const step = useMemo(() => (dir: -1 | 1) => live.current.probeAt(clamp(readX() + dir * (plotWidth / STEPS_ACROSS))),
                       [plotX, plotWidth]);
  return { body, handle, axis, step };
}
