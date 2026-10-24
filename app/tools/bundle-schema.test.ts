/**
 * The pipeline bundle's format is defined in ONE place, `pipeline/deliver/bundle/schema.sql`, which the production
 * bundler executes and the release copies its tables from. Two DDLs would diverge the first time one was edited, and
 * the failure would appear as a client query returning nothing.
 *
 * (Its second writer, the dev fixture's generator `build-fixture.mjs`, went with the dev bundle in T61; the release
 * contract's columns are held by `pack.generated.ts` and the pipeline's own tests.)
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
const schema = readFileSync(here("../../pipeline/deliver/bundle/schema.sql"), "utf8");
/** The DDL alone. The comments EXPLAIN what is excluded, so they name it. */
const ddl = schema.replace(/^\s*--.*$/gm, "").replace(/--.*$/gm, "");

/** Every table the contract defines. */
const TABLES = [...ddl.matchAll(/CREATE TABLE (\w+)/g)].map((m) => m[1]!);

describe("the bundle format", () => {
  it("declares the tables the app reads", () => {
    // Named individually: if one is dropped, the failure should say which. No regulation
    // table is listed — the app reads none (see `regulations.ts` in @app/core).
    for (const t of ["item", "alias", "item_section",
                     "gauge", "section_gauge", "section_down", "gauge_clim", "chart", "release",
                     "place", "place_water"])
      expect(TABLES, `${t} is missing from schema.sql`).toContain(t);
  });

  it("keeps geometry and identity OUT of the bundle", () => {
    // The tiles carry these. A second copy is a second source of truth, and the one that
    // goes stale is always the copy nobody remembered was a copy.
    for (const col of ["geometry", "magnitude", "strahler", "coordinates", "lines"])
      expect(ddl.toLowerCase(), `a column is named ${col}`).not.toContain(col);
  });
});
