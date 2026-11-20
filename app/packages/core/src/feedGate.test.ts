/** (T676 item 2) The feed gate: an index stamped for a newer app is refused, the need held until a readable one passes. */
import { describe, expect, it } from "vitest";
import { appTooOld, createFeedGate, FeedNeedsNewerApp, isFeedTooNew } from "./feedGate";
import { FEED_MIN_APP } from "./feedFormats.generated";
import { resolveAppVersion } from "./appVersion";

const APP = resolveAppVersion({ version: "0.8.4" });

describe("createFeedGate", () => {
  it("passes an index for this app or older, and one that names no version", () => {
    const g = createFeedGate(APP);
    for (const min of [undefined, null, "", "0.0.0", "0.8.4", "0.8.4-rc.1"]) expect(() => g.check({ minAppVersion: min }, "dfo")).not.toThrow();
    expect(() => g.check(null, "dfo")).not.toThrow();
    expect(g.needs()).toBeNull();
  });

  it("today's stamps pass every app that carries the gate", () => {
    const g = createFeedGate(resolveAppVersion({}));          // an unstamped build: 0.0.0
    for (const v of Object.values(FEED_MIN_APP)) expect(() => g.check({ minAppVersion: v }, "gauges")).not.toThrow();
  });

  it("refuses an index for a newer app, holds the need per feed, and clears it when a readable one passes", () => {
    const g = createFeedGate(APP);
    let told = 0;
    g.subscribe(() => { told += 1; });
    let e: unknown;
    try { g.check({ format: "dfo-feed/4", minAppVersion: "0.9.0" }, "dfo"); } catch (x) { e = x; }
    expect(e).toBeInstanceOf(FeedNeedsNewerApp);
    expect(isFeedTooNew(e) && [e.feed, e.need]).toEqual(["dfo", "0.9.0"]);
    expect(() => g.check({ minAppVersion: "0.10.0" }, "runtiming")).toThrow(FeedNeedsNewerApp);
    expect([g.needs(), g.refused()]).toEqual(["0.10.0", { dfo: "0.9.0", runtiming: "0.10.0" }]);
    g.check({ minAppVersion: "0.0.0" }, "runtiming");
    expect([g.needs(), told]).toEqual(["0.9.0", 3]);
    g.check({ minAppVersion: "0.8.0" }, "dfo");
    expect([g.needs(), told]).toEqual([null, 4]);
    // the same answer again tells nobody
    g.check({ minAppVersion: "0.8.0" }, "dfo");
    expect(told).toBe(4);
  });

  it("a value that is not a version is refused (the app cannot check it), as the release gate does", () => {
    expect(() => createFeedGate(APP).check({ minAppVersion: "soon" }, "inseason")).toThrow(FeedNeedsNewerApp);
  });

  it("no app version (the web): every index passes", () => {
    const g = createFeedGate(null);
    expect(() => g.check({ minAppVersion: "99.0.0" }, "dfo")).not.toThrow();
    expect(g.needs()).toBeNull();
  });

  it("isFeedTooNew: only the gate's refusal", () => {
    expect(isFeedTooNew(new FeedNeedsNewerApp("gauges", "1.0.0"))).toBe(true);
    expect(isFeedTooNew(new Error("dfo index.json: HTTP 500"))).toBe(false);
    expect(isFeedTooNew(null)).toBe(false);
  });
});

describe("appTooOld: the strong prompt's one test", () => {
  it("a release refused for this app, or a live feed asking for a newer one", () => {
    expect(appTooOld(null)).toBe(false);
    expect(appTooOld({ release: { phase: "current" }, needsApp: null })).toBe(false);
    expect(appTooOld({ release: { phase: "app-too-old" } })).toBe(true);
    expect(appTooOld({ release: null, needsApp: "0.9.0" })).toBe(true);
  });
});
