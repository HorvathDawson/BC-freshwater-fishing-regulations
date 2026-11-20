/**
 * (T938) A hard home re-renders the edge view, so React commits the offset it now reads (0). On a phone the offset turns
 * native once a spring home has run on the native driver, and from then on `setValue` reaches the native side only:
 * React Native does not flush it to React, whose copy keeps the drag's offset (iPhone simulator 2026-11-20: the browse
 * map left ~75 % to the right after a back that followed two cancelled edge drags). Here that is modelled on the web's
 * Animated by marking the value native after the drag: `setValue` then updates the value without telling the view, as
 * on a phone. Kept apart from gestures.test.tsx, which runs every value JS-driven.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { Animated, Text } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { createGestureController } from "react-native-gesture-handler/jest-utils";
import { useEdgeBack } from "@app/ui";
import { EDGE_BACK_HOLD_MS, EdgeBack } from "./EdgeBack";

afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.useRealTimers(); });

function Edge({ back }: { back: () => boolean }) {
  const drag = useEdgeBack(back, "ios", 1);
  return <GestureHandlerRootView><EdgeBack drag={drag}><Text>the water</Text></EdgeBack></GestureHandlerRootView>;
}
const shift = () => (screen.getByText("the water").parentElement as HTMLElement).style.transform;

describe("(T938) a native offset is put home in React too", () => {
  it("a swipe that went back, its offset then native: after the hold the view is home, not left at the drag's offset", () => {
    vi.useFakeTimers();
    render(<Edge back={() => true} />);              // nothing else re-renders the view
    const c = createGestureController("edge-back");
    act(() => { c.begin({ absoluteX: 5, x: 5 }); c.activate({ absoluteX: 5, x: 5 }); });
    act(() => { c.update({ absoluteX: 125, x: 125, translationX: 120 }); });
    act(() => { c.end({ absoluteX: 125, x: 125, translationX: 120, velocityX: 0 }); });   // past 80 px: back
    const held = shift();
    // from here the value behaves as a native one does on a phone: `setValue` stores it and flushes nothing to the view
    vi.spyOn(Animated.Value.prototype, "setValue").mockImplementation(function (this: Animated.Value, v: number) {
      (this as unknown as { _updateValue: (v: number, flush: boolean) => void })._updateValue(v, false);
    });
    act(() => { vi.advanceTimersByTime(EDGE_BACK_HOLD_MS); });
    expect([held, shift()]).toEqual(["translateX(120px)", "translateX(0px)"]);
  });
});
