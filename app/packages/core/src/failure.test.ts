import { afterEach, describe, expect, it } from "vitest";
import { environmentError, isEnvironment, offlineAs, reported, reportFailure, reportOnce, setFailureSink } from "./failure";

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
