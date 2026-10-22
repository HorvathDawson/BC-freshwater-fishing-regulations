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
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text, View } from "react-native";
import { GestureHandlerRootView, State } from "react-native-gesture-handler";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import type { ItemId } from "@app/data";
import { BackLayerContext, HOME, useEdgeBack, useRoute, useViewSwipe, waterFrame, type BackLayers,
         type Route } from "@app/ui";
import { EdgeBack } from "./EdgeBack";
import { ViewSwipe, WEB_PAGE_PROPS, useSwipeShield } from "./ViewSwipe";
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
/** The same events, the last one CANCELLED: the system took the gesture after it went active (RNGH `success` false). */
const cancelled = (events: object[]) => [...events.slice(0, -1), { ...events.at(-1)!, state: State.CANCELLED }];

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

  it("a swipe the system cancels after it went active is not a release: no back", () => {
    const out = [pan(5, 100), pan(5, 30, 0, 600)].map((events) => {
      const back = vi.fn(() => true);
      render(<Edge back={back} />);
      fire("edge-back", cancelled(events));
      cleanup();
      return back.mock.calls.length;
    });
    expect(out).toEqual([0, 0]);
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
  const swipe = useViewSwipe(route, go);
  const f = waterFrame(route);
  return (
    <GestureHandlerRootView>
      <ViewSwipe swipe={swipe}>
        <Text>{`${f?.view} · part ${f?.part}`}</Text>
        <Canvas />
      </ViewSwipe>
    </GestureHandlerRootView>
  );
}

/** Something a finger works on (a map, a chart), shielded as the Conditions view's are. */
function Canvas() {
  return <View testID="canvas" {...useSwipeShield()} />;
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

  it("a swipe the system cancels after it went active is not a release: the view stays", () => {
    expect([swipe(on("regs"), cancelled(pan(200, -100))), swipe(on("conditions"), cancelled(pan(200, 100, 0, 900)))])
      .toEqual(["regs · part 2", "conditions · part 2"]);
  });

  /*
   * On the web RNGH writes its own touch-action, user-select and context-menu handling onto the wrapped view while the
   * gesture is enabled — and the wrapped view is a whole Regulations or Conditions body. Its defaults ("none") would
   * take the page's pinch-zoom (WCAG 1.4.4, resize text) and the reader's selecting and copying of regulation text.
   */
  it("on the web the body keeps pinch-zoom, vertical scroll, text selection and the context menu", () => {
    render(<Views start={on("regs")} />);
    let el: HTMLElement | null = screen.getByText(/ · part /);
    while (el && !el.style.touchAction) el = el.parentElement;
    const menu = new MouseEvent("contextmenu", { bubbles: true, cancelable: true });
    el?.dispatchEvent(menu);
    expect({ props: WEB_PAGE_PROPS, touchAction: el?.style.touchAction, userSelect: el?.style.userSelect,
             menuBlocked: menu.defaultPrevented })
      .toEqual({ props: { touchAction: "pan-y pinch-zoom", userSelect: "auto", enableContextMenu: true },
                 touchAction: "pan-y pinch-zoom", userSelect: "auto", menuBlocked: false });
  });

  beforeAll(() => {
    Element.prototype.setPointerCapture ??= () => {};
    Element.prototype.releasePointerCapture ??= () => {};
    Element.prototype.hasPointerCapture ??= () => false;
  });
  it("a drag that starts on a shielded canvas (a map, the chart) is the canvas's; around it the body still swipes", () => {
    const onCanvas = (start: Route, events: object[], press: "touchStart" | "pointerDown") => {
      render(<Views start={start} />);
      // a whole touch (react-native-web's responder reads its force) and a pointer the recogniser can capture (jsdom has
      // no pointer capture): the press as a browser delivers it
      const t = { identifier: 1, force: 1, clientX: 200, clientY: 100, pageX: 200, pageY: 100, target: null };
      act(() => {
        if (press === "touchStart")
          fireEvent.touchStart(screen.getByTestId("canvas"), { touches: [t], changedTouches: [t] });
        else fireEvent.pointerDown(screen.getByTestId("canvas"), { pointerId: 1, pointerType: "mouse" });
      });
      fire("view-swipe", events);
      const text = screen.getByText(/ · part /).textContent;
      cleanup();
      return text;
    };
    expect([onCanvas(on("conditions"), pan(200, -100), "touchStart"),
            onCanvas(on("conditions"), pan(200, 100, 0, 900), "pointerDown"),
            swipe(on("conditions"), pan(200, 100))])
      .toEqual(["conditions · part 2", "conditions · part 2", "regs · part 2"]);
  });

  it("is off while browsing the map", () => {
    render(<GestureHandlerRootView><BrowseProbe /></GestureHandlerRootView>);
    fire("view-swipe", pan(200, 100));
    expect(screen.getByText(/^mode /).textContent).toBe("mode browse");
  });
});

function BrowseProbe() {
  const [route, go] = useRoute(HOME);
  const swipe = useViewSwipe(route, go);
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
  it("a drag the system cancels after it went active is not a release: the sheet stays open", () => {
    expect(drag([{ translationY: 0 }, { translationY: 20 },
                 { translationY: 40, velocityY: 900, state: State.CANCELLED }])).toBe(0);
  });
  it("is a back layer while open: the platform's own back for it (the Modal's onRequestClose) is the one back", () => {
    const log: string[] = [];
    const layers: BackLayers = {
      register: (close) => { log.push("register"); void close; return () => { log.push("unregister"); }; },
      back: () => { log.push("back"); return true; },
    };
    const onClose = () => { log.push("onClose"); };
    const sheet = (open: boolean) => (
      <BackLayerContext.Provider value={layers}>
        <Sheet open={open} onClose={onClose} title="Layers" palette={LIGHT}><Text>choices</Text></Sheet>
      </BackLayerContext.Provider>);
    const { rerender } = render(sheet(false));
    log.push("opened"); rerender(sheet(true));
    // react-native-web's Modal calls onRequestClose on Escape (Android: the system back while it is up) once its
    // slide-in has ended — which jsdom never plays, so the animation's end is fired on the portal's animated view
    let shown: HTMLElement = screen.getByText("choices");
    while (shown.parentElement && shown.parentElement.parentElement !== document.body) shown = shown.parentElement;
    // (jsdom has no AnimationEvent, so React listens under the prefixed name; both are sent)
    for (const t of ["animationend", "webkitAnimationEnd"])
      act(() => { shown.dispatchEvent(new Event(t, { bubbles: true })); });
    act(() => { document.dispatchEvent(new KeyboardEvent("keyup", { key: "Escape", bubbles: true })); });
    log.push("closed"); rerender(sheet(false));
    expect(log).toEqual(["opened", "register", "back", "closed", "unregister"]);
  });

  it("a scrolling body: the drag also starts from the content, but only while it is scrolled to its top (G1)", async () => {
    const flick = [{ translationY: 0 }, { translationY: 20 }, { translationY: 40, velocityY: 900 }];
    // react-native-web throttles scroll ticks and reports the last position when the scroll settles (100 ms later)
    const settle = () => act(() => new Promise((r) => setTimeout(r, 150)));
    const run = async (scrolls: number[]) => {
      const onClose = vi.fn();
      render(<Sheet open scroll onClose={onClose} title="Possession" palette={LIGHT}><Text>rows</Text></Sheet>);
      const body = screen.getByTestId("sheet-scroll");
      for (const top of scrolls) { body.scrollTop = top; fireEvent.scroll(body); await settle(); }
      fire("sheet-body-drag", flick);
      cleanup();
      return onClose.mock.calls.length;
    };
    // never scrolled · scrolled down (the content scrolls, the sheet stays) · scrolled down then back to the top
    expect([await run([]), await run([120]), await run([120, 0])]).toEqual([1, 0, 1]);
  });

  it("a sheet without a scrolling body has no body drag (its content is short; the handle is the drag)", () => {
    render(<Sheet open onClose={() => {}} title="Layers" palette={LIGHT}><Text>choices</Text></Sheet>);
    expect(() => getByGestureTestId("sheet-body-drag")).toThrow();
  });

  it("keeps Done and the scrim: the gesture is an addition", () => {
    const onClose = vi.fn();
    render(<Sheet open onClose={onClose} title="Layers" palette={LIGHT}><Text>choices</Text></Sheet>);
    act(() => { screen.getByLabelText("Done").click(); screen.getByLabelText("Close").click(); });
    expect(onClose.mock.calls.length).toBe(2);
  });
});
