/**
 * (T53) `downstreamWater`: the named water an unnamed reach flows into, down its flow pointers — the Regulations header's
 * "Tributary of …" line. Over a hand-built chain of pointers; the live release's three ticket examples are
 * app/conformance/src/regs/unnamed.release.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "./index";
import { DOWNSTREAM_WALK, downstreamWater } from "./downstream";

describe("(T53) downstreamWater: the named water an unnamed reach flows into", () => {
  // flow pointers: 1 → 2 → 3 (unnamed) → 4 → 5 (Eve River); 9 → nothing; 20 → 21 → … → 100 (all unnamed, past the bound)
  const down: Record<number, number> = { 1: 2, 2: 3, 3: 4, 4: 5 };
  for (let s = 20; s < 100; s++) down[s] = s + 1;
  const asked: number[] = [];
  const source = {
    traceToGauge: async (from: SectionId) => {
      const path = [from];
      for (let at = down[from]; at !== undefined; at = down[at]) path.push(at as SectionId);
      return path;
    },
    waterFor: async (s: SectionId) => {
      asked.push(s);
      return s === 4 || s === 5 ? { item: "gnis:31112" as ItemId, name: "Eve River", kind: "stream" } : null;
    },
  };

  it("walks down past unnamed sections to the first named one; none when the pointers end or the walk is bounded", async () => {
    const rows = [await downstreamWater(source, 1 as SectionId), await downstreamWater(source, 3 as SectionId),
                  await downstreamWater(source, 9 as SectionId)];
    expect(rows).toEqual([{ item: "gnis:31112", name: "Eve River", kind: "stream" },
                          { item: "gnis:31112", name: "Eve River", kind: "stream" }, null]);
    // never the tapped section itself, and it stops at the first named one (5 is never asked)
    expect(asked).toEqual([2, 3, 4, 4]);
    asked.length = 0;
    expect(await downstreamWater(source, 20 as SectionId)).toBeNull();
    expect([asked.length, asked[0], asked.at(-1)]).toEqual([DOWNSTREAM_WALK, 21, 20 + DOWNSTREAM_WALK]);
  });
});
