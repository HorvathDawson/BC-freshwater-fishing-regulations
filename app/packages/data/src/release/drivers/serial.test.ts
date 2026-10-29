/** (T264) the native SQLite calls run one at a time, in order, and a failure does not stop the queue. */
import { describe, expect, it } from "vitest";
import { serialQueue } from "./serial";

describe("serialQueue (T264)", () => {
  it("never runs two calls at once, and runs them in the order asked", async () => {
    const q = serialQueue();
    let inFlight = 0, most = 0;
    const order: number[] = [];
    const call = (i: number) => q(async () => {
      inFlight++; most = Math.max(most, inFlight);
      await new Promise((r) => setTimeout(r, i % 3));
      order.push(i);
      inFlight--;
      return i;
    });
    const out = await Promise.all(Array.from({ length: 50 }, (_, i) => call(i)));
    expect(most).toBe(1);
    expect(order).toEqual(out);
    expect(out).toEqual(Array.from({ length: 50 }, (_, i) => i));
  });

  it("rejects only the call that failed; the next one still runs", async () => {
    const q = serialQueue();
    const bad = q(async () => { throw new Error("no"); });
    const good = q(async () => 7);
    await expect(bad).rejects.toThrow("no");
    await expect(good).resolves.toBe(7);
  });
});
