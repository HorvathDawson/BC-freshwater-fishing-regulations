/**
 * (T741) A LAUNCH decodes the paint in long slices (`LAUNCH_SLICE_MS`: the boot screen is all there is); an update applied
 * while the map is on screen (`keepReplaced`) and every other caller keep `SLICE_MS`. Pinned at the three joints: the boot's
 * choice (`paintSliceFor`) and the loader handing the choice to the decoder; the boot's open path asking with it is
 * `tools/launch-slice.test.ts` (a source read: packages/data may not import node:fs).
 */
import { describe, expect, it, vi } from "vitest";

const asked: (number | undefined)[] = [];
vi.mock("@app/core", async (orig) => {
  const core = await orig<typeof import("@app/core")>();
  return { ...core, decodePaintAsync: vi.fn(async (_b: Uint8Array, _h: string | null, _r: string | null, sliceMs?: number) => {
    asked.push(sliceMs);
    return { sectionCount: 0 } as unknown as import("@app/core").Paint;
  }) };
});

const { LAUNCH_SLICE_MS, SLICE_MS } = await import("@app/core");
const { paintSliceFor } = await import("./boot");
const { releaseLoader } = await import("./loader");
const { parseManifest } = await import("../manifest");

const m = parseManifest({
  format: "app-manifest/2", data_release: "r-1",
  tiles: { compatible: ["d768d6e135e62b99.*"], current: null, assets: null, mask: null }, basemap: null,
  files: { paint: { url: "r-1/paint-x.bin.gz", bytes: 1, gz: 0, sha256: "0".repeat(64), format: "paint/5", section_handles: "d768d6e135e62b99" } },
  regulations: { valid_until: null, versions: null },
});
const open = async () => { throw new Error("no database in this test"); };

describe("(T741) the launch's paint slice", () => {
  it("a launch asks for the long slice; an update applied with the map on screen keeps the short one", () => {
    expect(paintSliceFor({})).toEqual({ sliceMs: LAUNCH_SLICE_MS });
    expect(paintSliceFor({ keepReplaced: false })).toEqual({ sliceMs: LAUNCH_SLICE_MS });
    expect(paintSliceFor({ keepReplaced: true })).toEqual({ sliceMs: SLICE_MS });
    expect(SLICE_MS).toBe(8);
    expect(LAUNCH_SLICE_MS).toBeLessThan(50);                       // under a long task; far under the 300 ms stall limit
  });

  it("the loader hands the first caller's slice to the decoder, and the default to every other path", async () => {
    asked.length = 0;
    const src = { bytes: async () => new Uint8Array([1]) };
    await releaseLoader(m, src, open).paint({ sliceMs: LAUNCH_SLICE_MS });
    await releaseLoader(m, src, open).paint();
    const L = releaseLoader(m, src, open);
    await L.paint(); await L.paint({ sliceMs: LAUNCH_SLICE_MS });  // decoded once: the first caller decided
    expect(asked).toEqual([LAUNCH_SLICE_MS, undefined, undefined]);
  });

});
