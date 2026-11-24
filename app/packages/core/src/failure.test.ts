import { afterEach, describe, expect, it } from "vitest";
import { environmentError, failuresSent, isEnvironment, offlineAs, reported, reportFailure, reportOnce, setFailureSink, setFailureTriage } from "./failure";

let got: unknown[] = [];
let undo = setFailureSink((e) => got.push(e));
afterEach(() => { got = []; });

describe("the environment is shown, never reported (T368, owner Q1)", () => {
  it("knows the line being down from a defect", () => {
    expect(isEnvironment(Object.assign(new Error("x"), { environment: true }))).toBe(true);
    expect(isEnvironment(environmentError(new Error("permission refused")))).toBe(true);
    const abort = new Error("aborted"); abort.name = "AbortError";
    expect(isEnvironment(abort)).toBe(true);
    for (const m of ["Network request failed", "Failed to fetch", "Load failed"]) expect(isEnvironment(new TypeError(m))).toBe(true);
    expect(isEnvironment(new TypeError("x is not a function"))).toBe(false);
    expect(isEnvironment(new Error("Failed to fetch"))).toBe(false);          // only the platform's TypeError
    expect(isEnvironment(Object.assign(new Error("HTTP 500"), { environment: false }))).toBe(false);
    expect(isEnvironment(null)).toBe(false);
    expect(isEnvironment("offline")).toBe(false);
  });

  it("reportFailure sends a defect and drops the environment's", () => {
    reportFailure(new Error("bad"));
    reportFailure(new TypeError("Failed to fetch"));
    expect(got.map((e) => (e as Error).message)).toEqual(["bad"]);
  });

  it("reportOnce sends a condition once a run", () => {
    reportOnce("k", new Error("one"));
    reportOnce("k", new Error("two"));
    expect(got.map((e) => (e as Error).message)).toEqual(["one"]);
  });

  it("reported(v) answers v and reports; offlineAs(v) answers v offline only, else throws on", async () => {
    expect(await Promise.reject(new Error("r")).catch(reported(null))).toBeNull();
    expect(got.map((e) => (e as Error).message)).toEqual(["r"]);
    expect(await Promise.reject(new TypeError("Load failed")).catch(offlineAs(7))).toBe(7);
    await expect(Promise.reject(new Error("broken")).catch(offlineAs(7))).rejects.toThrow("broken");
  });

  it("nothing is sent without a sink, and the undo restores the one before", () => {
    undo();
    reportFailure(new Error("lost"));
    expect(got).toEqual([]);
    undo = setFailureSink((e) => got.push(e));
  });
});

describe("(T1016) the second look before a report", () => {
  it("failuresSent(): waits for every look in flight to reach the sink, then for the sink's own sent()", async () => {
    const order: string[] = [];
    let answer = (_x: unknown) => {};
    let handOff = () => {};
    const offSink = setFailureSink((e) => { order.push(`sink:${(e as Error).message}`); },
                                   () => new Promise<void>((ok) => { handOff = () => { order.push("on the line"); ok(); }; }));
    const off = setFailureTriage(() => new Promise((ok) => { answer = ok; }));
    reportFailure(new Error("a"));
    const waited = failuresSent().then(() => order.push("sent"));
    await new Promise((r) => setTimeout(r, 0));
    expect(order).toEqual([]);                        // the look has not answered: nothing in the sink, not sent
    answer(new Error("renamed"));
    await new Promise((r) => setTimeout(r, 0));
    expect(order).toEqual(["sink:renamed"]);          // in the sink, not yet on the line
    handOff();
    await waited;
    expect(order).toEqual(["sink:renamed", "on the line", "sent"]);
    off(); offSink();
    // no look, no sink of its own: at once
    await failuresSent();
  });
  const settle = () => new Promise((r) => setTimeout(r, 0));
  it("null: reported as it is, at once; a promise: what it resolves to, once it does; the environment's: nothing", async () => {
    const renamed = new Error("renamed");
    const off = setFailureTriage((e) => {
      const m = (e as Error).message;
      return m === "plain" ? null : m === "look" ? Promise.resolve(renamed) : Promise.resolve(environmentError(new Error("line")));
    });
    reportFailure(new Error("plain"));
    expect(got.map((e) => (e as Error).message)).toEqual(["plain"]);
    reportFailure(new Error("look"));
    reportFailure(new Error("down"));
    expect(got).toHaveLength(1);
    await settle();
    expect(got.map((e) => (e as Error).message)).toEqual(["plain", "renamed"]);
    off();
    reportFailure(new Error("look"));
    expect(got.map((e) => (e as Error).message)).toEqual(["plain", "renamed", "look"]);   // undone: no look
  });

  it("a look that throws or rejects: the failure is reported as it is, and the look's own failure beside it", async () => {
    const off = setFailureTriage((e) => { if ((e as Error).message === "a") throw new Error("look threw"); return Promise.reject(new Error("look rejected")); });
    reportFailure(new Error("a"));
    reportFailure(new Error("b"));
    await settle();
    expect(got.map((e) => (e as Error).message)).toEqual(["a", "look threw", "b", "look rejected"]);
    off();
  });

  it("the environment's failure is never looked at", () => {
    let looked = 0;
    const off = setFailureTriage(() => { looked++; return null; });
    reportFailure(new TypeError("Failed to fetch"));
    expect([looked, got.length]).toEqual([0, 0]);
    off();
  });
});
