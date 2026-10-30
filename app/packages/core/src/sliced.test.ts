/** (T287) The sliced decoders give the one-go answer, and a slice gives the event loop its turn. */
import { describe, expect, it } from "vitest";
import { decodeMethod, decodeMethodAsync, methodSteps } from "./method";
import { decodePaint, decodePaintAsync } from "./paint";
import { PAINT_FIXTURE as F } from "./paint.fixture";
import { runNow, runSliced, YIELD_EVERY, type Steps } from "./sliced";

const b64 = (s: string) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));
const HANDLES = "b6b5000e171023c9";
const varint = (n: number): number[] => { const o: number[] = []; do { let b = n % 128; n = Math.floor(n / 128); if (n) b |= 0x80; o.push(b); } while (n); return o; };

/** A method/1 file of `runs` one-section runs on one profile (bait ban all year), every other section. */
function bigMethod(runs: number): Uint8Array {
  const out = [0x42, 0x43, 0x4d, 0x54, 1, ...HANDLES.match(/../g)!.map((h) => parseInt(h, 16))];
  out.push(...varint(1), ...varint(1), ...varint(366 * 4 + 1));
  out.push(...varint(runs));
  for (let k = 0; k < runs; k++) out.push(...varint(1));     // gaps
  for (let k = 0; k < runs; k++) out.push(...varint(1));     // lengths
  for (let k = 0; k < runs; k++) out.push(...varint(0));     // profile
  return new Uint8Array(out);
}

describe("sliced decodes (T287)", () => {
  it("runSliced turns the event loop between slices and gives runNow's answer", async () => {
    function* steps(): Steps<number> { let s = 0; for (let i = 0; i < 10; i++) { s += i; yield; } return s; }
    let turns = 0;
    const v = await runSliced(steps(), 0, async () => { turns += 1; });
    expect(v).toBe(runNow(steps()));
    expect(turns).toBe(10);
  });

  it("a big method file decodes the same in slices, with turns between", async () => {
    const b = bigMethod(YIELD_EVERY * 3);
    const one = decodeMethod(b, HANDLES);
    let turns = 0;
    const sl = await runSliced(methodSteps(b, HANDLES), 0, async () => { turns += 1; });
    expect(turns).toBeGreaterThanOrEqual(6);
    for (const sid of [0, 1, 2, 3, 2 * YIELD_EVERY + 1, 6 * YIELD_EVERY - 1, 6 * YIELD_EVERY + 5])
      expect(sl.profileOf(sid)).toBe(one.profileOf(sid));
    expect((await decodeMethodAsync(b, HANDLES)).profileOf(1)).toBe(0);
  });

  it("paint decodes the same in slices", async () => {
    const a = decodePaint(b64(F.paint), F.handles, F.reach), s = await decodePaintAsync(b64(F.paint), F.handles, F.reach);
    expect(s.members).toEqual(a.members);
    expect(s.basins).toEqual(a.basins);
    expect(s.stations).toEqual(a.stations);
    expect(s.status.sections).toBe(a.status.sections);
    const sids = [...new Set(F.access_rows.map(([x]) => x as number))];
    expect(sids.map((x) => s.access.of(x))).toEqual(sids.map((x) => a.access.of(x)));
  });

  it("a refused file is refused in slices too", async () => {
    await expect(decodePaintAsync(b64(F.paint), "0000000000000000")).rejects.toThrow();
  });
});
