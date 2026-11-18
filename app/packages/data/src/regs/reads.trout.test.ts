/**
 * (T810 M) THE TROUT SECTION, READ, on the trout fixture release (`app/fixtures/trout/regs-release/`, a regs fixture cut
 * from a side release built with `--trout-table`): the store holds the section, `cell`'s SELECT is the generated column
 * list plus `trout`, every cell's trout frame decodes (each row's fish sheet back on it), and a NULL cell stays null.
 * On the normal regs fixture (no section) the reads are as before: no `trout` in the SELECT, none on a cell or the
 * inputs. That every vector's decoded value equals Python's (`trout-vectors.generated.json`) is the conformance
 * `inputs.release.test.ts`'s.
 */
import { describe, expect, it } from "vitest";
import { CELL_COLUMNS, OPTIONAL_VERSIONS } from "@app/core";
import type { Db } from "../sql/db";
import { openRegsFixture, openTroutFixture } from "../sql/drivers/releaseDir";
import type { ReleaseLoader } from "../release/loader";
import { loadRegsInputs, targetFor } from "./inputs";
import { openRegsStore } from "./store";

/** The loader, every SQL a region file is asked recorded in `asked`. */
function spied(open: (t: { db(name: string, d: Db): Db }) => ReleaseLoader) {
  const asked: string[] = [];
  const loader = open({
    db: (name, d) => (/^regs-\d/.test(name)
      ? { ...d, all: (q, ...p) => { asked.push(q); return d.all(q, ...p); }, get: (q, ...p) => { asked.push(q); return d.get(q, ...p); } }
      : d),
  });
  return { loader, cellSelects: () => asked.filter((q) => / FROM cell /.test(q)) };
}

/** Every key a region file of the release holds cells for. */
async function keysOf(loader: ReleaseLoader): Promise<number[]> {
  const out = new Set<number>();
  for (const name of Object.keys(loader.manifest.files).filter((n) => /^regs-\d/.test(n))) {
    for (const r of await (await loader.db(name as never)).all("SELECT DISTINCT akey FROM cell")) out.add(Number(r.akey));
  }
  return [...out].sort((a, b) => a - b);
}

describe("the trout section on the trout fixture release", () => {
  it("the store holds it; every cell's trout frame decodes, a NULL cell is null; the SELECT names `trout` after the generated columns", async () => {
    const { loader, cellSelects } = spied(openTroutFixture);
    const st = await openRegsStore(loader);
    expect(st.meta.optional).toEqual({ trout: OPTIONAL_VERSIONS.trout });
    let tables = 0, nulls = 0;
    for (const akey of await keysOf(loader)) {
      for (const c of await st.cells(akey)) {
        expect(Object.keys(c)).toEqual([...CELL_COLUMNS.slice(2), "trout"]);
        if (c.trout === null) { nulls++; continue; }
        const v = await st.trout(akey, c.trout!);
        expect(Object.keys(v).sort()).toEqual(["covers", "dates", "table"]);
        expect(v.covers.length).toBeGreaterThan(0);
        for (const b of v.table.boxes) {
          for (const r of b.rows) expect(typeof r.sheet.head, `akey ${akey} trout ${c.trout} row ${r.key}`).toBe("string");
        }
        tables++;
      }
    }
    // the fixture's cases hold both: tables, and cells the page draws no table for (Q-D3)
    expect(tables).toBeGreaterThan(20);
    expect(nulls).toBeGreaterThan(0);
    const selects = cellSelects();
    expect(selects.length).toBeGreaterThan(0);
    for (const q of selects) expect(q).toMatch(new RegExp(`^SELECT ${[...CELL_COLUMNS, "trout"].join(", ")} FROM cell `));
  });
});

describe("a release without the section reads as before", () => {
  it("no `trout` in the SELECT, on a cell, or on the inputs (absent, not null)", async () => {
    const { loader, cellSelects } = spied(openRegsFixture);
    const st = await openRegsStore(loader);
    expect(st.meta.optional).toEqual({});
    const inp = await loadRegsInputs(st, await targetFor(st, "gnis:3062", null), { month: 7, day: 15 }, "Wednesday", 2026);
    expect(inp.cell).not.toBeNull();
    expect("trout" in inp.cell!).toBe(false);
    expect("trout" in inp).toBe(false);
    const selects = cellSelects();
    expect(selects.length).toBeGreaterThan(0);
    for (const q of selects) expect(q).toMatch(new RegExp(`^SELECT ${CELL_COLUMNS.join(", ")} FROM cell `));
  });
});
