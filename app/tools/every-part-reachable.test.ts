/**
 * (T464) EVERY PART OF EVERY WATER IS STEPPED TO, on the live release: from a water's lowest part, ▲ and ▼ reach every
 * other part (each stretch its own). The Bulkley had 6 of its 12 parts no step reached, and nothing said so: the parts
 * list still opened them, so only a reader stepping up the river would find the gap. Every water with two or more parts
 * (stretches counted) is walked through the app's own `loadWaterGroup`.
 */
import { describe, expect, it } from "vitest";
import { stepsDown, stepsUp, type WaterGroup } from "@app/core";
import { loadWaterGroup, openRegsStore } from "@app/data/regs";
import { liveReleaseAt, openReleaseDir } from "../conformance/src/fixtureRelease";

const at = liveReleaseAt();

it(`T464 every part reachable on the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T464 skipped: ${at.reason}`);
});

/** The parts of `g` that no chain of ▲ / ▼ steps reaches from its lowest part. */
function unreached(g: WaterGroup): number[] {
  const seen = new Set([0]), queue = [0];
  while (queue.length) {
    const i = queue.shift()!;
    for (const j of [...stepsUp(g, i), ...stepsDown(g, i)]) if (!seen.has(j)) { seen.add(j); queue.push(j); }
  }
  return g.parts.map((_, i) => i).filter((i) => !seen.has(i));
}

describe.skipIf(!at.dir)("(T464) every part of every water is reached by ▲ / ▼ from its mouth", () => {
  it("no water has a part no step reaches", async () => {
    const loader = openReleaseDir(at.dir!);
    const store = await openRegsStore(loader);
    const items = new Map((await (await loader.db("core")).all("SELECT ord, item_id FROM item"))
      .map((r) => [Number(r.ord), String(r.item_id)]));
    const ords = (await (await loader.db("water")).all("SELECT item_ord AS ord FROM part GROUP BY item_ord HAVING count(*) > 1"))
      .map((r) => Number(r.ord));
    expect(ords.length).toBeGreaterThan(500);
    const bad: string[] = [];
    for (const ord of ords) {
      const g = await loadWaterGroup(store, items.get(ord)!);
      const miss = unreached(g);
      if (miss.length) bad.push(`${g.parts[0]!.itemName}: ${miss.map((i) => g.parts[i]!.short).join("; ")}`);
    }
    expect(bad).toEqual([]);
  }, 300_000);
});
