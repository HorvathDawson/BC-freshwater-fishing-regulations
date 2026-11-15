/**
 * (T746) THE APPLY CURTAIN: takes taps from the first moment, is drawn (fish + the whole line) only after 300 ms and then
 * held at least 400 ms, never flashes for a quick apply, and says a failed apply in plain words until closed.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";

import { BackLayerContext, useBack, useRoute, type Back, type Route, type RouteAction } from "@app/ui";
import { ApplyCurtain } from "./ApplyCurtain";
import { LIGHT } from "./theme";

const curtain = (applying: boolean, failed: Error | null = null, onClose = () => {}) =>
  <ApplyCurtain palette={LIGHT} applying={applying} failed={failed} onClose={onClose} line="Landing the new regulations…" />;
const tick = (ms: number) => act(() => { vi.advanceTimersByTime(ms); });

describe("ApplyCurtain (T746)", () => {
  beforeEach(() => { vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] }); });
  afterEach(() => { cleanup(); vi.useRealTimers(); });

  it("nothing when no apply is in flight", () => {
    render(curtain(false));
    expect(screen.queryByTestId("apply-curtain")).toBeNull();
  });

  it("takes taps at once, draws nothing for 300 ms, then the fish and the whole line, drawn at least 400 ms", () => {
    const { rerender } = render(curtain(true));
    expect(screen.getByTestId("apply-curtain")).toBeTruthy();       // the tap stopper is up from the first frame
    expect(screen.queryByTestId("apply-line")).toBeNull();           // not drawn yet: a quick apply never flashes
    tick(299);
    expect(screen.queryByTestId("apply-line")).toBeNull();
    tick(1);
    expect(screen.getByTestId("apply-line").textContent).toBe("Landing the new regulations…");
    // the screen reader hears the line that is drawn
    expect(screen.getByLabelText("Landing the new regulations…")).toBeTruthy();
    tick(100);
    rerender(curtain(false));                                         // the new set committed
    expect(screen.getByTestId("apply-line")).toBeTruthy();            // held: drawn 400 ms in all, no flicker
    tick(299);
    expect(screen.getByTestId("apply-line")).toBeTruthy();
    tick(1);
    expect(screen.queryByTestId("apply-curtain")).toBeNull();
  });

  it("an apply quicker than 300 ms: the curtain goes without ever being drawn", () => {
    const { rerender } = render(curtain(true));
    tick(200);
    rerender(curtain(false));
    expect(screen.queryByTestId("apply-curtain")).toBeNull();
    tick(1000);
    expect(screen.queryByTestId("apply-line")).toBeNull();
  });

  it("the system back is held while the set is swapped (the route stays), and closes a failed apply's message", () => {
    const out: { b?: Back; r?: Route; go?: (a: RouteAction) => void } = {};
    function Host({ applying, failed, onClose }: { applying: boolean; failed: Error | null; onClose: () => void }) {
      const [route, go] = useRoute();
      out.b = useBack(route, go); out.r = route; out.go = go;
      return <BackLayerContext.Provider value={out.b.layers}>
        <ApplyCurtain palette={LIGHT} applying={applying} failed={failed} onClose={onClose} line="Landing the new regulations…" />
      </BackLayerContext.Provider>;
    }
    const onClose = vi.fn();
    const { rerender } = render(<Host applying={false} failed={null} onClose={onClose} />);
    act(() => { out.go!({ type: "openWater", item: "gnis:1" as never }); });
    const before = JSON.stringify(out.r);
    rerender(<Host applying failed={null} onClose={onClose} />);
    act(() => { out.b!.back(); });
    expect(JSON.stringify(out.r)).toBe(before);                      // held: nothing underneath moved
    rerender(<Host applying={false} failed={new Error("x")} onClose={onClose} />);
    act(() => { out.b!.back(); });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("a failed apply is said at once, in the whole sentence, until the reader closes it", () => {
    const onClose = vi.fn();
    render(curtain(false, new Error("core: open failed"), onClose));
    expect(screen.getByTestId("apply-failed").textContent)
      .toBe("The new regulations could not be put in place. The app is still using the data it had.");
    expect(screen.queryByTestId("apply-line")).toBeNull();
    fireEvent.click(screen.getByTestId("apply-failed-ok"));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
