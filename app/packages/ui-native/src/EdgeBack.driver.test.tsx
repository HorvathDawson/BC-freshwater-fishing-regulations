/**
 * (T938) The edge swipe's offset never goes to the native driver, even where the app's motion uses it (a phone):
 * `./motion` is faked to say the native driver is on, as it is on iOS. The drag moves the offset from JS every frame;
 * a native-driven spring home on the same value made it native for good, after which a `setValue` reached the native
 * side only and React kept the drag's offset (iPhone simulator 2026-11-20: the browse map left ~75 % to the right after
 * a back that followed two cancelled edge drags). Kept apart from gestures.test.tsx, which runs with the web's driver.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { Animated, Text } from "react-native";
import { GestureHandlerRootView, State } from "react-native-gesture-handler";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { useEdgeBack } from "@app/ui";

vi.mock("./motion", async (orig) => ({ ...(await orig<typeof import("./motion")>()), NATIVE_DRIVER: true }));
const { EdgeBack } = await import("./EdgeBack");
const { NATIVE_DRIVER } = await import("./motion");

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

function Edge({ back }: { back: () => boolean }) {
  const drag = useEdgeBack(back, "ios", 1);
  return <GestureHandlerRootView><EdgeBack drag={drag}><Text>the water</Text></EdgeBack></GestureHandlerRootView>;
}
const pan = (dx: number) => [
  { absoluteX: 5, x: 5, translationX: 0 },
  { absoluteX: 5 + dx / 2, x: 5 + dx / 2, translationX: dx / 2 },
  { absoluteX: 5 + dx, x: 5 + dx, translationX: dx, velocityX: 0 },
];

describe("(T938) the edge offset stays JS-driven", () => {
  it("a short drag's spring home and a cancelled drag's run on the JS driver though the app's motion is native", () => {
    expect(NATIVE_DRIVER).toBe(true);                 // the fake took: a phone's motion
    const spring = vi.spyOn(Animated, "spring");
    render(<Edge back={() => true} />);
    act(() => { fireGestureHandler(getByGestureTestId("edge-back"), pan(40)); });       // short: springs home
    const events = pan(135);
    act(() => { fireGestureHandler(getByGestureTestId("edge-back"),
      [...events.slice(0, -1), { ...events.at(-1)!, state: State.CANCELLED }]); });   // cancelled: springs home
    const drivers = spring.mock.calls.map(([, c]) => (c as { useNativeDriver?: boolean }).useNativeDriver);
    expect(drivers).toEqual([false, false]);
  });
});
