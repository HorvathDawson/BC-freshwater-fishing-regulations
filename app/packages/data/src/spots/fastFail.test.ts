import { describe, expect, it, vi } from "vitest";
import { fastFail, fastPorts } from "./fastFail";

describe("(T307) fast-fail capture ports", () => {
  it("offline: no ask is made, the answer is the failed-fetch answer, at once", async () => {
    const gaugeNow = vi.fn(async () => ({ value: 1 }));
    const at = vi.fn(async () => ({ t: 1 }));
    const index = vi.fn(async () => ({}));
    const p = fastPorts({ source: { gaugeNow, panelsFor: async () => 7 } as any, weather: { at } as any, feed: { index } },
                        () => false);
    const t0 = Date.now();
    expect(await p.source.gaugeNow("x" as any)).toBeNull();
    expect(await p.weather!.at(1, 2, new Date())).toBeNull();
    await expect(p.feed!.index()).rejects.toThrow();
    expect(await (p.source as any).panelsFor()).toBe(7);                 // on-device lookups pass through
    expect(gaugeNow).not.toHaveBeenCalled(); expect(at).not.toHaveBeenCalled(); expect(index).not.toHaveBeenCalled();
    expect(Date.now() - t0).toBeLessThan(100);
  });
  it("online but hanging: cut at the deadline; a fast answer passes", async () => {
    vi.useFakeTimers();
    const hang = fastFail(() => new Promise<number>(() => {}), () => true, () => -1, 2500);
    await vi.advanceTimersByTimeAsync(2500);
    expect(await hang).toBe(-1);
    vi.useRealTimers();
    expect(await fastFail(async () => 5, () => true, () => -1)).toBe(5);
  });
});
