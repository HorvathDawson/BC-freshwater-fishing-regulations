import { GestureStateManager, type PanGestureConfig } from "react-native-gesture-handler";

/**
 * (T300) ON A PHONE THE SHIELD MUST KEEP THE SWIPE FROM ACTIVATING, not only from moving. A recogniser that goes active
 * takes the touch: the native root view then cancels the map under the finger, so a sideways drag on a gauge pane's map
 * stopped dead 15 px in and a pinch lost its second finger — the "glitchy" pan. The shield only set a flag (`held`), too
 * late: the pan was already active. So on a device the pan is activated by hand, and a press that began on a shielded
 * thing fails it instead (the web twin leaves the pan as it is: the browser's map keeps the pointer; `touch-action` is its
 * half).
 */
export function activateByHand(held: () => boolean): Partial<PanGestureConfig> {
  let sx = 0, sy = 0;
  // (T250) RNGH 3: the touch callbacks get no state manager; the global one acts on the gesture by its handler tag
  return {
    manualActivation: true,
    onTouchesDown: (e) => { const t = e.allTouches[0]; sx = t?.absoluteX ?? 0; sy = t?.absoluteY ?? 0; },
    onTouchesMove: (e) => {
      const t = e.allTouches[0];
      if (!t) return;
      if (held() || e.numberOfTouches > 1 || Math.abs(t.absoluteY - sy) > 12) { GestureStateManager.fail(e.handlerTag); return; }
      if (Math.abs(t.absoluteX - sx) > 15) GestureStateManager.activate(e.handlerTag);
    },
  };
}
