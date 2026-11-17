/**
 * (trout table plan, ticket I0) The regs files' `cell` columns are GENERATED into core (`../regsCell.generated.ts`, by
 * `python -m pipeline.tools.emit.pack` from the release contract). Until the app chat's ticket M imports the generated
 * `CellIds` in `./inputs`, the hand-typed one must name the same sections: tsc fails below if either side has a field
 * the other lacks, so a column the pipeline adds cannot be missed by the reader.
 */
import { describe, expect, it } from "vitest";
import { CELL_COLUMNS, CELL_SECTIONS, type CellIds as GeneratedCellIds, type CellSection } from "../regsCell.generated";
import type { CellIds } from "./inputs";

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
const sameKeys: Same<keyof CellIds, CellSection> = true;
const sameShape: Same<CellIds, GeneratedCellIds> = true;

describe("generated cell columns", () => {
  it("are the key and segment, then one frame id per section, and core's CellIds names those sections", () => {
    expect(CELL_COLUMNS.slice(0, 2)).toEqual(["akey", "s"]);
    expect(CELL_COLUMNS.slice(2)).toEqual([...CELL_SECTIONS]);
    expect([sameKeys, sameShape]).toEqual([true, true]);
  });
});
