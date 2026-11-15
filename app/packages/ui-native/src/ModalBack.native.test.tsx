/**
 * (T723 F7) The modal's left-edge back: on iOS a drag from the edge calls the modal's own back, once; on Android (its
 * system back reaches the Modal through `onRequestClose`) and the web the drag is never taken and the content is drawn
 * as it is. The responder's callbacks are driven by hand (the browser has no touch responder to drive): how it feels
 * under a thumb is the device's (handoff/PLAN/ios/shots/t723/).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Animated, Keyboard, Platform, Text } from "react-native";
import { useEdgeBack } from "@app/ui";
import { ModalBack } from "./ModalBack.native";
import { modalEdgeResponder } from "./modalEdge";
import { DoneBar as DoneBarNative } from "./DoneBar.native";
import { DoneBar as DoneBarWeb } from "./DoneBar.web";
import { ModalBack as ModalBackWeb } from "./ModalBack.web";
import { LIGHT } from "./theme";

const OS = Platform.OS;
const as = (os: string) => { (Platform as { OS: string }).OS = os; };
afterEach(() => { cleanup(); as(OS); });


/** A finished drag as the responder reports it: pressed at `x0`, `dx` / `dy` away, leaving at `vx` px per ms. */
const g = (x0: number, dx: number, dy = 0, vx = 0) => ({ x0, dx, dy, vx }) as never;
const ev = {} as never;

/** One drag through the responder's callbacks: does it take the drag, and does the modal close? */
function drag(os: string, x0: number, dx: number, dy = 0, vx = 0) {
  const back = vi.fn(() => true);
  let decided: ReturnType<typeof useEdgeBack> | null = null;
  function Probe() { decided = useEdgeBack(back, os, 1); return null; }
  render(<Probe />);
  const r = modalEdgeResponder(decided!, new Animated.Value(0));
  const took = r.onMoveShouldSetPanResponderCapture!(ev, g(x0, Math.min(dx, 12), dy, vx));
  if (took) r.onPanResponderRelease!(ev, g(x0, dx, dy, vx));
  cleanup();
  return [took, back.mock.calls.length];
}

describe("<ModalBack> (T723 F7)", () => {
  it("iOS: a drag from the edge past 80 px, or flicked, closes the modal once; short and slow, from mid-screen, or mostly vertical, it is not taken", () => {
    expect([drag("ios", 5, 100), drag("ios", 5, 40, 0, 0.6), drag("ios", 5, 40), drag("ios", 60, 200), drag("ios", 5, 100, 30)])
      .toEqual([[true, 1], [true, 1], [true, 0], [false, 0], [false, 0]]);
  });

  it("Android and the web: never taken (Android's system back closes the Modal through onRequestClose)", () => {
    expect([drag("android", 5, 100), drag("web", 5, 100)]).toEqual([[false, 0], [false, 0]]);
  });

  it("drawn: iOS wraps the content in the responder's view; elsewhere the content is drawn as it is", () => {
    const seen = ["ios", "android"].map((os) => {
      as(os);
      render(<ModalBack onBack={() => {}}><Text>About</Text></ModalBack>);
      const out = [os, screen.queryByTestId("modal-edge-back") !== null, screen.getByText("About") !== null];
      cleanup();
      return out;
    });
    render(<ModalBackWeb onBack={() => {}}><Text>Help</Text></ModalBackWeb>);
    expect([seen, screen.getByText("Help") !== null]).toEqual([[["ios", true, true], ["android", false, true]], true]);
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
