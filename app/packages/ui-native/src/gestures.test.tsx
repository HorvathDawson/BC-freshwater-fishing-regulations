/**
 * The gesture components, mounted (G1): `<EdgeBack>`, `<ViewSwipe>` and the `Sheet` drag, driven through
 * react-native-gesture-handler's own test utility (`fireGestureHandler`), which delivers a handler's event sequence
 * (began → active → end) to the callbacks the component registered.
 *
 * What this proves: the components hand what they measure to the hooks, and the hooks' decisions reach the route or
 * the close. What it does NOT: the recognisers' own activation (`activeOffsetX`, `failOffsetY`, `hitSlop`) —
 * `fireGestureHandler` skips them — nor how any of it feels under a finger. That is the owner's, on a device.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { Text } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import type { ItemId } from "@app/data";
import { HOME, useEdgeBack, useRoute, useViewSwipe, waterFrame, type Route } from "@app/ui";
import { EdgeBack } from "./EdgeBack";
import { ViewSwipe } from "./ViewSwipe";
import { Sheet } from "./Sheet";
import { LIGHT } from "./theme";

afterEach(cleanup);

/** A pan's events: pressed at `x0`, ending `dx`/`dy` away and leaving at `vx`/`vy`. */
const pan = (x0: number, dx: number, dy = 0, vx = 0, vy = 0) => [
  { absoluteX: x0, x: x0, translationX: 0, translationY: 0 },
  { absoluteX: x0 + dx / 2, x: x0 + dx / 2, translationX: dx / 2, translationY: dy / 2 },
  { absoluteX: x0 + dx, x: x0 + dx, translationX: dx, translationY: dy, velocityX: vx, velocityY: vy },
];
const fire = (id: string, events: object[]) => act(() => { fireGestureHandler(getByGestureTestId(id), events); });

function Edge({ back, os = "ios", depth = 1 }: { back: () => boolean; os?: string; depth?: number }) {
  const drag = useEdgeBack(back, os, depth);
  return <GestureHandlerRootView><EdgeBack drag={drag}><Text>the water</Text></EdgeBack></GestureHandlerRootView>;
}

describe("<EdgeBack> (iOS only)", () => {
  it("a swipe from the edge past 80 px, or flicked, goes back; short, slow or from mid-screen does not", () => {
    const rows: [string, object[]][] = [
      ["edge, 100 px", pan(5, 100)], ["edge, 79 px slow", pan(5, 79)], ["edge, 30 px flicked", pan(5, 30, 0, 600)],
      ["edge, 30 px at 499 px/s", pan(5, 30, 0, 499)], ["from 30 px in, 150 px", pan(30, 150)],
      ["edge, dragged left", pan(20, -100, 0, -900)],
    ];
    const out = rows.map(([k, events]) => {
      const back = vi.fn(() => true);
      render(<Edge back={back} />);
      fire("edge-back", events);
      cleanup();
      return [k, back.mock.calls.length];
    });
    expect(out).toEqual([["edge, 100 px", 1], ["edge, 79 px slow", 0], ["edge, 30 px flicked", 1],
                         ["edge, 30 px at 499 px/s", 0], ["from 30 px in, 150 px", 0], ["edge, dragged left", 0]]);
  });

  it("is off with nothing to go back to", () => {
    const back = vi.fn(() => true);
    render(<Edge back={back} depth={0} />);
    fire("edge-back", pan(5, 200));
    expect(back.mock.calls.length).toBe(0);
  });

  it("is not there at all on Android or the web (their system back owns the edge)", () => {
    const found = ["android", "web"].map((os) => {
      render(<Edge back={() => true} os={os} />);
      let has = true;
      try { getByGestureTestId("edge-back"); } catch { has = false; }
      const text = screen.getByText("the water").textContent;
      cleanup();
      return [os, has, text];
    });
    expect(found).toEqual([["android", false, "the water"], ["web", false, "the water"]]);
  });
});

const VEDDER = "gnis:3062" as ItemId;
const on = (view: "regs" | "conditions" | "map"): Route =>
  ({ mode: "water", from: HOME, stack: [{ item: VEDDER, part: 2, tapSid: null, at: null, view }] });

function Views({ start }: { start: Route }) {
  const [route, go] = useRoute(start);
  const swipe = useViewSwipe(route, go, () => {});
  const f = waterFrame(route);
  return (
    <GestureHandlerRootView>
      <ViewSwipe swipe={swipe}><Text>{`${f?.view} · part ${f?.part}`}</Text></ViewSwipe>
    </GestureHandlerRootView>
  );
}

describe("<ViewSwipe> (owner 2026-10-21: the swipe changes the VIEW, not the section)", () => {
  // a phone-wide window (jsdom's is 0 x 0, which no swipe can start in): the edge zones are [0, 24) and (366, 390]
  beforeAll(() => {
    Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
    Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
    window.dispatchEvent(new Event("resize"));
  });
  const swipe = (start: Route, events: object[]) => {
    render(<Views start={start} />);
    fire("view-swipe", events);
    const text = screen.getByText(/ · part /).textContent;
    cleanup();
    return text;
  };

  it("finger left brings in the view to the right (Regulations · Conditions · Map), right the one to the left; no wrap", () => {
    expect([
      swipe(on("regs"), pan(200, -100)), swipe(on("conditions"), pan(200, -100)), swipe(on("map"), pan(200, -100)),
      swipe(on("map"), pan(200, 100)), swipe(on("conditions"), pan(200, 100)), swipe(on("regs"), pan(200, 100)),
    ]).toEqual(["conditions · part 2", "map · part 2", "map · part 2",
                "conditions · part 2", "regs · part 2", "regs · part 2"]);
  });

  it("a scroll, a short drag and a start in either edge zone leave the view alone", () => {
    expect([
      swipe(on("regs"), pan(200, -100, 80)), swipe(on("regs"), pan(200, -40)), swipe(on("regs"), pan(10, -100)),
      swipe(on("regs"), pan(380, -100)),
    ]).toEqual(["regs · part 2", "regs · part 2", "regs · part 2", "regs · part 2"]);
  });

  it("a flick moves after 20 px", () => {
    expect([swipe(on("regs"), pan(200, -25, 0, -700)), swipe(on("regs"), pan(200, -25, 0, -300))])
      .toEqual(["conditions · part 2", "regs · part 2"]);
  });

  it("is off while browsing the map", () => {
    render(<GestureHandlerRootView><BrowseProbe /></GestureHandlerRootView>);
    fire("view-swipe", pan(200, 100));
    expect(screen.getByText(/^mode /).textContent).toBe("mode browse");
  });
});

function BrowseProbe() {
  const [route, go] = useRoute(HOME);
  const swipe = useViewSwipe(route, go, () => {});
  return <ViewSwipe swipe={swipe}><Text>{`mode ${route.mode}`}</Text></ViewSwipe>;
}

describe("the Sheet drag (Layers, and the sheets to come)", () => {
  const drag = (events: object[]) => {
    const onClose = vi.fn();
    render(<Sheet open onClose={onClose} title="Layers" palette={LIGHT}><Text>choices</Text></Sheet>);
    fire("sheet-drag", events);
    cleanup();
    return onClose.mock.calls.length;
  };
  // jsdom lays nothing out, so the sheet's height is 0 here and only the flick rule can close it: the third-of-height
  // rule is proved in @app/core (sheetDismiss), not here.
  it("closes on a downward flick; a slow drag or an upward one springs back", () => {
    const vpan = (dy: number, vy: number) => [
      { translationY: 0 }, { translationY: dy / 2 }, { translationY: dy, velocityY: vy }];
    expect([drag(vpan(40, 900)), drag(vpan(40, 300)), drag(vpan(-60, -900))]).toEqual([1, 0, 0]);
  });
  it("keeps Done and the scrim: the gesture is an addition", () => {
    const onClose = vi.fn();
    render(<Sheet open onClose={onClose} title="Layers" palette={LIGHT}><Text>choices</Text></Sheet>);
    act(() => { screen.getByLabelText("Done").click(); screen.getByLabelText("Close").click(); });
    expect(onClose.mock.calls.length).toBe(2);
  });
});
