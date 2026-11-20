/**
 * THE LEFT-EDGE BACK SWIPE (G1) — the prototype's, and only where the platform has no system back of its own: iOS.
 * On Android the system's edge gesture owns that strip (and reaches the same back through `BackHandler`); on the web
 * the browser owns it. `drag.available` says which (`@app/ui` `useEdgeBack`, from `@app/core` `edgeBackEnabled`).
 *
 * Measures and draws only: the view follows the finger, and on release `drag.release` decides (past 80 px or a
 * flick) and calls the one back. Everything else springs home.
 */
import { useLayoutEffect, useMemo, useRef } from "react";
import { Animated } from "react-native";
import { GestureDetector, usePanGesture, type PanGestureConfig } from "react-native-gesture-handler";
import { now } from "@app/core";
import type { EdgeBackDrag } from "@app/ui";
import { NATIVE_DRIVER } from "./motion";

export function EdgeBack({ drag, children }: { drag: EdgeBackDrag; children: React.ReactNode }) {
  // `available` is fixed per platform, so this never remounts the screen underneath. (T250) The recogniser lives in its
  // own component: RNGH 3's hook gestures exist from the hook call, so where there is no edge swipe there is no handler.
  if (!drag.available) return <>{children}</>;
  return <EdgePan drag={drag}>{children}</EdgePan>;
}

/**
 * (T928) How long a drag may stand off home with nothing heard from it: a swipe that went back, held where it was let
 * go for the back's render (T821), and a finger that sent no move. Past it, the screen is put home.
 */
export const EDGE_BACK_HOLD_MS = 1500;

function EdgePan({ drag, children }: { drag: EdgeBackDrag; children: React.ReactNode }) {
  const x = useRef(new Animated.Value(0)).current;
  /*
   * (T928, a tester on iPhone 0.8.3: "my map is only 2/3 my screen ... even when I change tabs", put right only by a
   * restart) THE SCREEN NEVER STAYS PART-WAY. The browse layer stood ~135 pt to the right, the offset a drag gives this
   * view, through every later render: only a swipe that went back was ever put home by a render (T821); every other end
   * relied on its one spring home being run and landing. Now:
   *   - whether a finger is on the edge (`live`) is ONE flag for the view: each handler config had its own, so a config
   *     made mid-drag (`drag` changes with the back depth) saw no finger down when that drag ended;
   *   - the view may stand off home (`off`) from the drag's first move until a render puts it home with a hard
   *     `setValue(0)` (the spring's own end does not clear it: a native-driven value whose view was not updated is the
   *     case this is for), so every render with no finger down — a tab change, a route, the app back in front, which
   *     all render `Shell` — puts it home at once; a finger silent for `EDGE_BACK_HOLD_MS` counts as no finger;
   *   - a swipe that went back is held for the back's render (T821) at most `EDGE_BACK_HOLD_MS`, then put home.
   */
  const live = useRef(false);
  const off = useRef(false);
  const heard = useRef(0);
  const backed = useRef<ReturnType<typeof setTimeout> | null>(null);
  const homeNow = () => {
    if (backed.current) { clearTimeout(backed.current); backed.current = null; }
    x.stopAnimation();
    x.setValue(0);
    off.current = false;
  };
  // (T821) a swipe that went back leaves the screen where the finger let go until the screen it went back to is
  // committed, and only then comes home (this layout effect: after that render, before it is painted). Putting it home
  // when the back was decided stood the screen just swiped away whole again for as long as the back's render took, the
  // owner's "it flashes the menu back for a second before it goes back" (iPhone build 36).
  useLayoutEffect(() => {
    if (backed.current || (off.current && (!live.current || now() - heard.current > EDGE_BACK_HOLD_MS))) homeNow();
  });
  useLayoutEffect(() => () => { if (backed.current) clearTimeout(backed.current); }, []);
  // (T250) RNGH 3's hook API: one config object (memoised: a new object reconfigures the native handler), callbacks on JS
  const config = useMemo((): PanGestureConfig => {
    const home = () => Animated.spring(x, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 }).start();
    return {
      runOnJS: true, disableReanimated: true, enabled: drag.enabled, testID: "edge-back",
      // only a press that starts in the edge zone is ours: everything else (the map's pan) never sees this handler
      hitSlop: { left: 0, width: drag.zone },
      activeOffsetX: 10, failOffsetY: [-20, 20],
      onBegin: (e) => { live.current = drag.starts(e.absoluteX); heard.current = now(); },
      onUpdate: (e) => {
        if (!live.current) return;
        off.current = true;
        heard.current = now();
        x.setValue(drag.offset(e.translationX));
      },
      // `canceled` when the system cancelled the swipe after it went active: that is not a release
      onDeactivate: (e) => {
        if (live.current && !e.canceled && drag.release(e.translationX, e.velocityX)) {
          if (backed.current) clearTimeout(backed.current);
          backed.current = setTimeout(() => { backed.current = null; if (!live.current) homeNow(); }, EDGE_BACK_HOLD_MS);
        } else home();
        live.current = false;
      },
      // an end with no deactivation (it never went active), whichever config saw it begin: home
      onFinalize: () => { if (live.current) home(); live.current = false; },
    };
  }, [drag, x]);  // eslint-disable-line react-hooks/exhaustive-deps -- homeNow reads only refs and `x`
  const gesture = usePanGesture(config);
  return (
    <GestureDetector gesture={gesture}>
      <Animated.View style={{ flex: 1, transform: [{ translateX: x }] }}>{children}</Animated.View>
    </GestureDetector>
  );
}
