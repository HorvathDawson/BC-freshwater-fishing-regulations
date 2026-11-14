/** (T716) a click reads its own tap's lift, and nothing else's. */
import { describe, expect, it } from "vitest";
import { LIFT_MAX_MS, LIFT_SAME_DP, liftFor, type Lift } from "./tapLift";

const lift = (answer: { hit: number | null } | null): Lift<number> => ({ x: 100, y: 200, at: 1000, answer: Promise.resolve(answer) });

describe("liftFor", () => {
  it("the same tap: the lift's answer — a water, no water, or undecided", async () => {
    expect(await liftFor(lift({ hit: 7 }), 101, 199, 1300)).toEqual({ hit: 7 });
    expect(await liftFor(lift({ hit: null }), 100, 200, 1300)).toEqual({ hit: null });
    expect(await liftFor(lift(null), 100, 200, 1300)).toBeNull();
  });

  it("another point, too late, before the lift, or no lift: the click asks for itself", () => {
    expect([liftFor(lift({ hit: 7 }), 100 + LIFT_SAME_DP + 1, 200, 1300), liftFor(lift({ hit: 7 }), 100, 200, 1000 + LIFT_MAX_MS + 1),
            liftFor(lift({ hit: 7 }), 100, 200, 999), liftFor(null, 100, 200, 1300)]).toEqual([null, null, null, null]);
  });
});
