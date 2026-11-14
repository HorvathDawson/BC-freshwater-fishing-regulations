/**
 * (T360) AN APPLIED UPDATE RETIRES THE SET IT REPLACED: once the new set is committed (ready on screen), the old loader is
 * retired — its database connections close, a late read is still answered — and a failed close is reported. A first start retires
 * nothing; a failed apply keeps the old set (nothing retired).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, renderHook, waitFor } from "@testing-library/react";
import { setFailureSink } from "@app/core";
import { SpaceError, type BootOptions, type Booted } from "@app/data/release";

const boots: (() => Promise<Booted>)[] = [];
vi.mock("@app/data/release", async (orig) => ({
  ...(await orig<typeof import("@app/data/release")>()),
  bootRelease: () => boots.shift()!(),
  warmLike: async () => {},
}));

import { useReleaseBoot } from "./boot";

const set = (name: string, retire = vi.fn(async () => 3)) =>
  ({ booted: { loader: { release: name, retire } } as unknown as Booted, retire });
const OPTS = {} as BootOptions;

describe("useReleaseBoot: the replaced set is retired (T360)", () => {
  let reported: unknown[] = [];
  let unsink = () => {};
  beforeEach(() => { reported = []; unsink = setFailureSink((e) => reported.push(e)); boots.length = 0; });
  afterEach(() => { unsink(); cleanup(); });

  it("first start retires nothing; an applied update retires the old set once the new one is on screen", async () => {
    const a = set("r-a"), b = set("r-b");
    boots.push(async () => a.booted, async () => b.booted);
    const { result, rerender } = renderHook(({ g }) => useReleaseBoot(OPTS, g), { initialProps: { g: 0 } });
    await waitFor(() => expect(result.current.state).toBe("ready"));
    expect(a.retire).not.toHaveBeenCalled();
    rerender({ g: 1 });
    await waitFor(() => expect(result.current.state === "ready" && result.current.booted).toBe(b.booted));
    await waitFor(() => expect(a.retire).toHaveBeenCalledTimes(1));
    (a.retire.mock.calls[0] as unknown as [(e: unknown) => void])[0](new Error("late close"));   // its reporter: the app's
    expect(reported.map(String)).toEqual(["Error: late close"]);
    reported.length = 0;
    expect([b.retire.mock.calls.length, reported]).toEqual([0, []]);
  });

  it("a failed close of the old set is reported; a failed apply retires nothing", async () => {
    const a = set("r-a", vi.fn(async () => { throw new Error("core: close failed"); })), b = set("r-b");
    boots.push(async () => a.booted, async () => b.booted, async () => { throw new Error("offline"); });
    const { result, rerender } = renderHook(({ g }) => useReleaseBoot(OPTS, g), { initialProps: { g: 0 } });
    await waitFor(() => expect(result.current.state).toBe("ready"));
    rerender({ g: 1 });
    await waitFor(() => expect(reported.map(String)).toEqual(["Error: core: close failed"]));
    vi.spyOn(console, "error").mockImplementation(() => {});
    rerender({ g: 2 });
    await waitFor(() => expect(result.current.state === "ready" && result.current.applyError?.message).toBe("offline"));
    expect(b.retire).not.toHaveBeenCalled();
  });

  it("(T722 F9) a download that will not fit fails in its own words (said, reported); any other failure says none", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    const words = "Not enough free space: the download needs about 482.0 MB and this phone has 120.0 MB free. "
      + "Free up some space and try again.";
    boots.push(async () => { throw new SpaceError(words); }, async () => { throw new Error("offline"); });
    const { result, rerender } = renderHook(({ a }) => useReleaseBoot(OPTS, 0, a), { initialProps: { a: 0 } });
    await waitFor(() => expect(result.current.state).toBe("failed"));
    expect(result.current.state === "failed" && result.current.said).toBe(words);
    expect(reported.map(String)).toEqual([`Error: ${words}`]);
    rerender({ a: 1 });
    await waitFor(() => expect(result.current.state === "failed" && result.current.error.message).toBe("offline"));
    expect(result.current.state === "failed" && result.current.said).toBeNull();
  });
});
