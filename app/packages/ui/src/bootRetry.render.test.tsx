/**
 * (T760, owner 2026-11-15 "keep trying by itself") A FAILED FIRST START THAT IS THE CONNECTION'S IS TRIED AGAIN ONCE A
 * MINUTE BY THE APP, behind its words: the failure stays on screen until the try gets past the manifest; one try at a
 * time (a tap while the app's own try runs shows that try, it never starts a second install); the clock stops while no
 * failure is shown or the app is in the background; every failed try is logged and reported as a tapped one is.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, renderHook } from "@testing-library/react";
import { useEffect } from "react";
import { INSTALL_WORDS, setFailureSink } from "@app/core";
import type { BootOptions, Booted } from "@app/data/release";

interface Run { opts: BootOptions; ok: (b: Booted) => void; fail: (e: unknown) => void }
const runs: Run[] = [];
vi.mock("@app/data/release", async (orig) => ({
  ...(await orig<typeof import("@app/data/release")>()),
  bootRelease: (opts: BootOptions) => new Promise<Booted>((ok, fail) => { runs.push({ opts, ok, fail }); }),
  warmLike: async () => {},
}));

import { BootError } from "@app/data/release";
import { RETRY_BY_ITSELF_MS, retriesByItselfNow, useBootTries, useReleaseBoot, useRetryByItself, type BootState } from "./boot";

const OPTS = {} as BootOptions;
const offline = () => new BootError("first run: the manifest could not be read (manifest: no answer in 5000 ms)", true);

/** The app's wiring (App.tsx), with the failure shown at once and the foreground a prop. */
function useStart({ active }: { active: boolean }) {
  const tries = useBootTries();
  const boot = useReleaseBoot(OPTS, 0, tries.attempt, tries.quiet);
  const settle = tries.settle;
  useEffect(() => settle(boot), [boot, settle]);
  useRetryByItself(retriesByItselfNow(boot, boot.state === "failed", active), () => tries.again(true));
  return { boot, tap: tries.tap, again: tries.again };
}

describe("a connection's failure keeps trying by itself (T760)", () => {
  let reported: unknown[] = [];
  let unsink = () => {};
  beforeEach(() => {
    vi.useFakeTimers({ now: 0 });
    runs.length = 0; reported = [];
    unsink = setFailureSink((e) => reported.push(e));
    vi.spyOn(console, "error").mockImplementation(() => {});
  });
  afterEach(() => { unsink(); cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); });
  const at = (ms: number) => act(() => { vi.advanceTimersByTime(ms); });
  const settle = async (f: () => void) => { await act(async () => { f(); await Promise.resolve(); }); };

  it("one minute after the failure is shown a quiet try starts; the words stay until the line answers, then the loading screen", async () => {
    expect(RETRY_BY_ITSELF_MS).toBe(60_000);
    const { result } = renderHook(useStart, { initialProps: { active: true } });
    await settle(() => runs[0]!.fail(offline()));
    expect(result.current.boot.state).toBe("failed");
    at(59_999);
    expect(runs).toHaveLength(1);
    at(1);
    expect(runs).toHaveLength(2);
    // the try runs behind the failure: still its words, marked as trying
    expect(result.current.boot).toMatchObject({ state: "failed", firstRunOffline: true, trying: true });
    act(() => runs[1]!.opts.onStage?.("manifest"));
    expect(result.current.boot.state).toBe("failed");
    // past the manifest: the line answered, the install is shown as any try's
    act(() => runs[1]!.opts.onStage?.("install"));
    expect(result.current.boot).toMatchObject({ state: "loading", stage: "install" });
  });

  it("a quiet try that fails is logged and reported as a tapped one, and the clock starts again; never two tries at once", async () => {
    const { result } = renderHook(useStart, { initialProps: { active: true } });
    const err = (n: number) => Object.assign(offline(), { message: `first run: the manifest could not be read (${n})` });
    await settle(() => runs[0]!.fail(err(0)));
    at(RETRY_BY_ITSELF_MS);
    expect(runs).toHaveLength(2);
    at(5 * RETRY_BY_ITSELF_MS);                                  // a try still running: the clock does not start another
    expect(runs).toHaveLength(2);
    await settle(() => runs[1]!.fail(err(1)));
    expect(result.current.boot).toMatchObject({ state: "failed", trying: false });
    expect((console.error as unknown as { mock: { calls: unknown[][] } }).mock.calls.map((c) => c[0])).toEqual([
      "[boot] start failed: first run: the manifest could not be read (0)",
      "[boot] start failed: first run: the manifest could not be read (1)"]);
    expect(reported.map((e) => (e as Error).message)).toEqual([
      "first run: the manifest could not be read (0)", "first run: the manifest could not be read (1)"]);
    at(RETRY_BY_ITSELF_MS);
    expect(runs).toHaveLength(3);
  });

  it("Try again while the app's own try runs shows that try at once and starts no second one; with none running it starts one", async () => {
    const { result } = renderHook(useStart, { initialProps: { active: true } });
    await settle(() => runs[0]!.fail(offline()));
    at(RETRY_BY_ITSELF_MS);
    expect(result.current.boot).toMatchObject({ state: "failed", trying: true });
    act(() => result.current.tap());
    expect(runs).toHaveLength(2);
    expect(result.current.boot.state).toBe("loading");
    act(() => result.current.tap());                             // (no button on the loading screen; a stray call too)
    expect(runs).toHaveLength(2);
    await settle(() => runs[1]!.fail(offline()));
    act(() => result.current.tap());
    expect(runs).toHaveLength(3);
    expect(result.current.boot.state).toBe("loading");
  });

  it("a tap and the clock in the same tick start one try, whichever comes first", async () => {
    const { result } = renderHook(useStart, { initialProps: { active: true } });
    await settle(() => runs[0]!.fail(offline()));
    act(() => { vi.advanceTimersByTime(RETRY_BY_ITSELF_MS); result.current.tap(); });
    expect(runs).toHaveLength(2);
    await settle(() => runs[1]!.fail(offline()));
    at(RETRY_BY_ITSELF_MS - 1);
    act(() => { result.current.tap(); vi.advanceTimersByTime(1); });
    expect(runs).toHaveLength(3);
  });

  it("the app's own try is never started while another runs (a silent restart or the clock, a render apart)", async () => {
    const { result } = renderHook(useStart, { initialProps: { active: true } });
    await settle(() => runs[0]!.fail(offline()));
    act(() => result.current.again(true));
    expect(runs).toHaveLength(2);
    act(() => result.current.again(false));
    act(() => result.current.again(true));
    expect(runs).toHaveLength(2);
    expect(result.current.boot).toMatchObject({ state: "failed", trying: true });
  });

  it("the clock stops in the background and starts its minute over on return", async () => {
    const { rerender } = renderHook(useStart, { initialProps: { active: true } });
    await settle(() => runs[0]!.fail(offline()));
    at(50_000);
    rerender({ active: false });
    at(10 * RETRY_BY_ITSELF_MS);
    expect(runs).toHaveLength(1);
    rerender({ active: true });
    at(RETRY_BY_ITSELF_MS - 1);
    expect(runs).toHaveLength(1);
    at(1);
    expect(runs).toHaveLength(2);
  });

  it("the stuck download and a failure that is not the connection's wait for the tap", async () => {
    for (const e of [new Error(INSTALL_WORDS.stuck), new Error("release: x does not match the manifest (the same size, other bytes)")]) {
      runs.length = 0;
      const { result, unmount } = renderHook(useStart, { initialProps: { active: true } });
      await settle(() => runs[0]!.fail(e));
      expect(result.current.boot.state).toBe("failed");
      at(10 * RETRY_BY_ITSELF_MS);
      expect(runs).toHaveLength(1);
      unmount();
    }
  });
});

describe("retriesByItselfNow (T760)", () => {
  const failed = (error: Error, firstRunOffline: boolean, trying = false): BootState =>
    ({ state: "failed", error, firstRunOffline, said: null, trying });
  const line = Object.assign(new Error("x: no answer in 20000 ms"), { environment: true });
  it("only a connection's failure, on screen, in the foreground, with no try of the app's own running", () => {
    expect(retriesByItselfNow(failed(line, false), true, true)).toBe(true);
    expect(retriesByItselfNow(failed(new Error("first run: x"), true), true, true)).toBe(true);
    expect(retriesByItselfNow(failed(line, false), false, true)).toBe(false);          // not shown yet (the silent restarts)
    expect(retriesByItselfNow(failed(line, false), true, false)).toBe(false);          // in the background
    expect(retriesByItselfNow(failed(line, false, true), true, true)).toBe(false);     // a try already runs
    expect(retriesByItselfNow(failed(new Error(INSTALL_WORDS.stuck), true), true, true)).toBe(false);
    expect(retriesByItselfNow({ state: "loading", stage: "install", progress: null }, true, true)).toBe(false);
  });
});
