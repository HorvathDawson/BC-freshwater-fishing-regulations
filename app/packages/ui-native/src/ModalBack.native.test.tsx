/**
 * (T723 F7) The modal's left-edge back, mounted: on iOS a swipe from the edge calls the modal's own close, once; on
 * Android (its system back reaches the Modal through `onRequestClose`) and the web there is no recogniser at all, so the
 * modal's content is drawn untouched. Driven through RNGH's `fireGestureHandler`, as `gestures.test.tsx` drives EdgeBack:
 * the recogniser's own activation and how it feels under a thumb are the device's (handoff/PLAN/ios/shots/t723/).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Keyboard, Platform, Text } from "react-native";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { ModalBack } from "./ModalBack.native";
import { DoneBar as DoneBarNative } from "./DoneBar.native";
import { DoneBar as DoneBarWeb } from "./DoneBar.web";
import { ModalBack as ModalBackWeb } from "./ModalBack.web";
import { LIGHT } from "./theme";

const OS = Platform.OS;
const as = (os: string) => { (Platform as { OS: string }).OS = os; };
afterEach(() => { cleanup(); as(OS); });

const pan = (x0: number, dx: number, vx = 0) => [
  { absoluteX: x0, x: x0, translationX: 0, translationY: 0 },
  { absoluteX: x0 + dx / 2, x: x0 + dx / 2, translationX: dx / 2, translationY: 0 },
  { absoluteX: x0 + dx, x: x0 + dx, translationX: dx, translationY: 0, velocityX: vx },
];

describe("<ModalBack> (T723 F7)", () => {
  it("iOS: an edge swipe past 80 px closes the modal once; a short slow one and one from mid-screen do not", () => {
    as("ios");
    const out = [["edge 100 px", pan(5, 100)], ["edge 40 px slow", pan(5, 40)], ["from 60 px in", pan(60, 200)]].map(([k, ev]) => {
      const back = vi.fn();
      render(<ModalBack onBack={back}><Text>About</Text></ModalBack>);
      act(() => { fireGestureHandler(getByGestureTestId("edge-back"), ev as object[]); });
      cleanup();
      return [k, back.mock.calls.length];
    });
    expect(out).toEqual([["edge 100 px", 1], ["edge 40 px slow", 0], ["from 60 px in", 0]]);
  });

  it("Android and the web: no recogniser, the content as it is", () => {
    const seen = ["android", "web"].map((os) => {
      as(os);
      render(<ModalBack onBack={() => {}}><Text>About</Text></ModalBack>);
      let handler = true;
      try { getByGestureTestId("edge-back"); } catch { handler = false; }
      const shown = screen.getByText("About") !== null;
      cleanup();
      return [os, handler, shown];
    });
    render(<ModalBackWeb onBack={() => {}}><Text>Help</Text></ModalBackWeb>);
    expect([seen, screen.getByText("Help") !== null]).toEqual([[["android", false, true], ["web", false, true]], true]);
  });
});

describe("<DoneBar> (T723 G10)", () => {
  it("draws nothing off iOS (React Native's InputAccessoryView is iOS-only) and nothing on the web", () => {
    as("android");
    const a = render(<DoneBarNative id="t-done" palette={LIGHT} />);
    const android = a.container.innerHTML;
    cleanup();
    const w = render(<DoneBarWeb id="t-done" palette={LIGHT} />);
    expect([android, w.container.innerHTML]).toEqual(["", ""]);
  });
  it("iOS: its Done drops the keyboard", () => {
    as("ios");
    const dismiss = vi.spyOn(Keyboard, "dismiss").mockImplementation(() => {});
    render(<DoneBarNative id="t-done" palette={LIGHT} />);
    fireEvent.click(screen.getByTestId("t-done-done"));
    expect(dismiss).toHaveBeenCalledTimes(1);
    dismiss.mockRestore();
  });
});
