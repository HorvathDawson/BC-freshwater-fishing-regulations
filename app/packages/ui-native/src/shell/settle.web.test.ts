/**
 * (T910) The web's unasked downloads wait for the page's load and then QUIET_MS with no long task (settle.web.ts); a long
 * task starts the spell again; a page never quiet still gets them by the cap; cancel stops it; the phone goes at once.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { afterLoadQuiet, QUIET_MS } from "./settle.web";
import { afterLoadQuiet as phone } from "./settle.native";

let observers: ((list: unknown) => void)[] = [];
class FakeObserver {
  static supportedEntryTypes = ["longtask"];
  constructor(private f: (list: unknown) => void) {}
  observe() { observers.push(this.f); }
  disconnect() { observers = observers.filter((o) => o !== this.f); }
}
const longTask = () => observers.forEach((o) => o({}));
let ready: DocumentReadyState = "complete";

describe("(T910) the web's unasked downloads wait for a quiet page", () => {
  beforeEach(() => {
    vi.useFakeTimers(); observers = []; ready = "complete";
    vi.stubGlobal("PerformanceObserver", FakeObserver);
    vi.spyOn(document, "readyState", "get").mockImplementation(() => ready);
  });
  afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

  it("is the owner's 5 s", () => expect(QUIET_MS).toBe(5000));

  it("waits for the load event, then the quiet spell", () => {
    ready = "loading";
    const go = vi.fn();
    afterLoadQuiet(go);
    vi.advanceTimersByTime(20_000);
    expect(go).not.toHaveBeenCalled();                 // not loaded: nothing starts
    window.dispatchEvent(new Event("load"));
    vi.advanceTimersByTime(QUIET_MS - 1);
    expect(go).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(go).toHaveBeenCalledTimes(1);
    expect(observers).toEqual([]);                      // the watching stops
  });

  it("each long task starts the spell again", () => {
    const go = vi.fn();
    afterLoadQuiet(go);
    vi.advanceTimersByTime(4000); longTask();
    vi.advanceTimersByTime(4000); longTask();
    vi.advanceTimersByTime(4999);
    expect(go).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(go).toHaveBeenCalledTimes(1);
  });

  it("a page never quiet still gets its files 30 s after load, once", () => {
    const go = vi.fn();
    afterLoadQuiet(go);
    for (let t = 0; t < 40_000; t += 1000) { vi.advanceTimersByTime(1000); longTask(); }
    expect(go).toHaveBeenCalledTimes(1);
  });

  it("no long-task entries in this browser: QUIET_MS after load", () => {
    FakeObserver.supportedEntryTypes = [];
    const go = vi.fn();
    afterLoadQuiet(go);
    vi.advanceTimersByTime(QUIET_MS);
    expect([go.mock.calls.length, observers.length]).toEqual([1, 0]);
    FakeObserver.supportedEntryTypes = ["longtask"];
  });

  it("cancelled: never", () => {
    const go = vi.fn();
    afterLoadQuiet(go)();
    vi.advanceTimersByTime(60_000);
    expect(go).not.toHaveBeenCalled();
  });

  it("the phone's twin goes at once (nothing to wait for)", () => {
    const go = vi.fn();
    phone(go);
    expect(go).toHaveBeenCalledTimes(1);
  });
});
