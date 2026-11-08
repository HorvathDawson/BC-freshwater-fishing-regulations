/**
 * (T509) A STRETCH IS NAMED BY ITS OWN ENDS, on the live release. The Fraser's lowest listed part ("Upstream of the CPR
 * Bridge at Mission", cut in two by the 138 m Landstrom Bar sign zone) read "2 stretches · stretch 1 of 2" — the
 * part's short name says nothing where a stretch ends on an area boundary. Through the app's own `loadWaterGroup` (the
 * one load path: the header, the parts list and the screen reader all say `partName`).
 */
import { describe, expect, it } from "vitest";
import { partName } from "@app/core";
import { loadWaterGroup, openRegsStore } from "@app/data/regs";
import { liveReleaseAt, openReleaseDir } from "../conformance/src/fixtureRelease";

const at = liveReleaseAt();

it(`T509 stretch names on the live release${at.dir ? "" : ` — SKIPPED: ${at.reason}`}`, () => {
  if (!at.dir) console.warn(`T509 skipped: ${at.reason}`);
});

describe.skipIf(!at.dir)("(T509) a part's stretches are named by their own ends", () => {
  it("the Fraser's lowest part: Mission → Landstrom Bar, then Landstrom Bar → Spuzzum Creek", async () => {
    const store = await openRegsStore(openReleaseDir(at.dir!));
    const g = await loadWaterGroup(store, "gnis:39325");
    const names = g.parts.map((_, i) => partName(g, i));
    expect(names[0]).toBe("The CPR Bridge at Mission → Landstrom Bar sign zone boundary");
    expect(names).toContain("Landstrom Bar sign zone boundary → Spuzzum Creek confluence");
    // (owner 2026-11-08 "Ends only") a stretch with both ends named carries no "stretch k of n"; no bare count left
    expect(names.filter((n) => / → .* · stretch \d+ of \d+$/.test(n) && !/^\d+ stretches/.test(n))).toEqual([]);
    expect(names.filter((n) => /^\d+ stretches/.test(n))).toEqual([]);
  }, 60_000);
});
