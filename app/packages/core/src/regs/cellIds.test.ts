/**
 * (trout table plan, tickets I0 and M) The regs files' `cell` columns are GENERATED into core (`../regsCell.generated.ts`,
 * by `python -m pipeline.tools.emit.pack` from the release contract), and core's `CellIds` IS the generated one
 * (`./inputs` re-exports it; no hand-typed copy is left). Its keys are the generated column list less the key and the
 * segment: tsc fails below if they part, so a column the pipeline adds cannot be missed by the reader. An optional
 * section's column (`trout`) is not among them: `@app/data` names it only when the store holds the section.
 */
import { describe, expect, it } from "vitest";
import { CELL_COLUMNS, CELL_SECTIONS, type CellIds as GeneratedCellIds, type CellSection } from "../regsCell.generated";
import type { CellIds } from "./inputs";

type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
type Columns = Exclude<(typeof CELL_COLUMNS)[number], "akey" | "s">;
const sameKeys: Same<keyof CellIds, Columns> = true;
const sameSections: Same<CellSection, Columns> = true;
const sameShape: Same<CellIds, GeneratedCellIds> = true;

describe("generated cell columns", () => {
  it("are the key and segment, then one frame id per section, and core's CellIds' keys are exactly those sections", () => {
    expect(CELL_COLUMNS.slice(0, 2)).toEqual(["akey", "s"]);
    expect(CELL_COLUMNS.slice(2)).toEqual([...CELL_SECTIONS]);
    expect([sameKeys, sameSections, sameShape]).toEqual([true, true, true]);
  });
});
