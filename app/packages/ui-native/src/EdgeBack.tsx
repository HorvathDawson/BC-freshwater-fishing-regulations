/**
 * THE LEFT-EDGE BACK SWIPE (G1) — the prototype's, and only where the platform has no system back of its own: iOS.
 * On Android the system's edge gesture owns that strip (and reaches the same back through `BackHandler`); on the web
 * the browser owns it. `drag.available` says which (`@app/ui` `useEdgeBack`, from `@app/core` `edgeBackEnabled`).
 *
 * Measures and draws only: the view follows the finger, and on release `drag.release` decides (past 80 px or a
 * flick) and calls the one back. Everything else springs home.
 */
import { useEffect, useLayoutEffect, useMemo, useReducer, useRef } from "react";
import { Animated, AppState } from "react-native";
import { GestureDetector, usePanGesture, type PanGestureConfig } from "react-native-gesture-handler";
import type { EdgeBackDrag } from "@app/ui";
import { NATIVE_DRIVER } from "./motion";

export function EdgeBack({ drag, children }: { drag: EdgeBackDrag; children: React.ReactNode }) {
  // `available` is fixed per platform, so this never remounts the screen underneath. (T250) The recogniser lives in its
  // own component: RNGH 3's hook gestures exist from the hook call, so where there is no edge swipe there is no handler.
  if (!drag.available) return <>{children}</>;
  return <EdgePan drag={drag}>{children}</EdgePan>;
}

/**
 * (T928) How long a swipe that went back holds the screen where it was let go, waiting for the back's render (T821),
 * before it is put home anyway: a back whose render never reaches this view would otherwise leave it there.
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
   *     case this is for), so a render with no finger down and no spring running — a tab change, a route — puts it
   *     home. A finger held still is still down: it is never sent home under it; and a spring home that is running is
   *     let finish;
   *   - the app going to the background or coming back puts it home whatever was in flight (the system ends every touch
   *     then, so an end the view never heard cannot keep it off home);
   *   - a swipe that went back is held for the back's render (T821) at most `EDGE_BACK_HOLD_MS`, then put home.
   */
  const live = useRef(false);
  const off = useRef(false);
  const springing = useRef(false);
  const backed = useRef<ReturnType<typeof setTimeout> | null>(null);
  /*
   * (T938) HOME IS COMMITTED BY REACT TOO. Once a spring home has run on the native driver, `x` is native for good, and
   * a `setValue` on a native value goes to the native side only: React Native does not flush it back to React (it
   * re-renders the view only when a native ANIMATION ends). React's copy of the transform kept the drag's offset, and a
   * later commit of this view put it back on screen. iPhone simulator, 2026-11-20: two edge drags that landed on a water
   * page's map were cancelled (each spring home made `x` native), a third went back, and the browse map and its day bar
   * stood ~75 % to the right, through pans (`docs/ios/shots/t938/`), as the T928 tester's phone did. So every
   * hard home also re-renders this view (`sync`): React reads `x` again (0) and commits it.
   */
  const [, sync] = useReducer((n: number) => n + 1, 0);
  const homeNow = () => {
    if (backed.current) { clearTimeout(backed.current); backed.current = null; }
    x.stopAnimation();
    x.setValue(0);
    off.current = false;
    springing.current = false;
    sync();
  };
  // (T821) a swipe that went back leaves the screen where the finger let go until the screen it went back to is
  // committed, and only then comes home (this layout effect: after that render, before it is painted). Putting it home
  // when the back was decided stood the screen just swiped away whole again for as long as the back's render took, the
  // owner's "it flashes the menu back for a second before it goes back" (iPhone build 36).
  useLayoutEffect(() => {
    if (backed.current || (off.current && !live.current && !springing.current)) homeNow();
  });
  useEffect(() => {
    const sub = AppState.addEventListener("change", (st) => {
      if (st !== "background" && st !== "active") return;
      live.current = false;
      if (off.current || backed.current) homeNow();
    });
    return () => { sub?.remove(); if (backed.current) clearTimeout(backed.current); };
  }, []);  // eslint-disable-line react-hooks/exhaustive-deps -- homeNow reads only refs and `x`
  // (T250) RNGH 3's hook API: one config object (memoised: a new object reconfigures the native handler), callbacks on JS
  const config = useMemo((): PanGestureConfig => {
    const home = () => {
      springing.current = true;
      Animated.spring(x, { toValue: 0, useNativeDriver: NATIVE_DRIVER, bounciness: 0 })
        .start(() => { springing.current = false; });
    };
    return {
      runOnJS: true, disableReanimated: true, enabled: drag.enabled, testID: "edge-back",
      // only a press that starts in the edge zone is ours: everything else (the map's pan) never sees this handler
      hitSlop: { left: 0, width: drag.zone },
      activeOffsetX: 10, failOffsetY: [-20, 20],
      onBegin: (e) => { live.current = drag.starts(e.absoluteX); },
      onUpdate: (e) => {
        if (!live.current) return;
        off.current = true;
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
