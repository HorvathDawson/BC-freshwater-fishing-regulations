/** (T277) The satellite probe asks a failed tile once more before it says the imagery is down. */
import { afterEach, describe, expect, it, vi } from "vitest";
import { imageryProbe } from "./imageryProbe";

const T = "https://example.test/tile/{z}/{y}/{x}";
afterEach(() => { vi.unstubAllGlobals(); });

describe("imageryProbe (T277)", () => {
  it("one failed request is asked again: a second answer is a yes", async () => {
    const f = vi.fn().mockRejectedValueOnce(new Error("dropped")).mockResolvedValueOnce({ ok: true, status: 200 });
    vi.stubGlobal("fetch", f);
    expect(await imageryProbe(T, 1000, 1)()).toBe(true);
    expect(f).toHaveBeenCalledTimes(2);
  });
  it("two failures are a no, and the log says why", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 503 }));
    const warn = vi.spyOn(console, "error").mockImplementation(() => {});
    expect(await imageryProbe(T, 1000, 1)()).toBe(false);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("HTTP 503"));
    warn.mockRestore();
  });
  it("a first yes asks nothing more", async () => {
    const f = vi.fn().mockResolvedValue({ ok: true, status: 200 });
    vi.stubGlobal("fetch", f);
    expect(await imageryProbe(T)()).toBe(true);
    expect(f).toHaveBeenCalledTimes(1);
  });
});
