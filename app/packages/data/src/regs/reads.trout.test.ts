/**
 * (T810 M, S) THE TROUT SECTION, READ. From the cutover (rows 34) the section is required: every regs release holds it,
 * `cell`'s SELECT is the generated column list (`trout` among it), every cell's trout frame decodes (each row's fish sheet
 * back on it), and a NULL cell stays null (contract 2.1, Q-D3), never frame 0. Both fixtures: the regs fixture release
 * and the trout fixture release (`app/fixtures/trout/regs-release/`, the vectors' side release). That every vector's
 * decoded value equals Python's (`trout-vectors.generated.json`) is the conformance `inputs.release.test.ts`'s.
 */
import { describe, expect, it } from "vitest";
import { CELL_COLUMNS, KNOWN_VERSIONS } from "@app/core";
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

describe.each([
  ["the regs fixture release", openRegsFixture, 1],
  ["the trout fixture release", openTroutFixture, 20],
] as const)("the trout section on %s (required from rows 34)", (_, open, least) => {
  it("the store holds it; every cell's trout frame decodes, a NULL cell is null; the SELECT is the generated columns", async () => {
    const { loader, cellSelects } = spied(open);
    const st = await openRegsStore(loader);
    expect([st.meta.versions.trout, st.meta.optional]).toEqual([KNOWN_VERSIONS.trout, {}]);
    expect(CELL_COLUMNS).toContain("trout");
    let tables = 0, nulls = 0;
    for (const akey of await keysOf(loader)) {
      for (const c of await st.cells(akey)) {
        expect(Object.keys(c)).toEqual([...CELL_COLUMNS.slice(2)]);
        if (c.trout === null) { nulls++; continue; }
        expect(Number.isInteger(c.trout) && c.trout >= 0, `akey ${akey} trout ${c.trout}`).toBe(true);   // frame 0 is a frame
        const v = await st.trout(akey, c.trout);
        // (T768 / T993 item 10) the fields the reader needs, present: a frame may gain one within its version
        expect(Object.keys(v)).toEqual(expect.arrayContaining(["covers", "dates", "table"]));
        expect(v.covers.length).toBeGreaterThan(0);
        for (const b of v.table.boxes) {
          for (const r of b.rows) expect(typeof r.sheet.head, `akey ${akey} trout ${c.trout} row ${r.key}`).toBe("string");
        }
        tables++;
      }
    }
    // the cases hold both: tables, and cells the page draws no table for (Q-D3)
    expect(tables).toBeGreaterThanOrEqual(least);
    expect(nulls).toBeGreaterThan(0);
    const selects = cellSelects();
    expect(selects.length).toBeGreaterThan(0);
    for (const q of selects) expect(q).toMatch(new RegExp(`^SELECT ${CELL_COLUMNS.join(", ")} FROM cell `));
  });
});

describe("the inputs of a keyed tap always carry the trout cell (null or the frame), never leave it out", () => {
  it("gnis:3062 on Jul 15", async () => {
    const st = await openRegsStore(spied(openRegsFixture).loader);
    const inp = await loadRegsInputs(st, await targetFor(st, "gnis:3062", null), { month: 7, day: 15 }, "Wednesday", 2026);
    expect(inp.cell).not.toBeNull();
    expect("trout" in inp.cell!).toBe(true);
    expect(inp.trout === null || Array.isArray(inp.trout?.covers)).toBe(true);
  });
});
