/**
 * The development fixture and the production bundler must be the same format.
 *
 * They are different programs in different languages — `tools/build-fixture.mjs` in Node
 * and `python -m pipeline.deliver.bundle` — and the whole point of the SQLite decision is that a
 * client reads one shape whichever produced it. Two DDLs would diverge the first time one
 * was edited, and the failure would appear as a client query returning nothing.
 *
 * So there is one file, and this checks both that it is the one being used and that the
 * fixture it produced actually has the contract's tables in it.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
const schema = readFileSync(here("../../pipeline/deliver/bundle/schema.sql"), "utf8");
/** The DDL alone. The comments EXPLAIN what is excluded, so they name it. */
const ddl = schema.replace(/^\s*--.*$/gm, "").replace(/--.*$/gm, "");
const fixture = here("../packages/data/dev/bundle.sqlite");

/** Every table the contract defines. */
const TABLES = [...ddl.matchAll(/CREATE TABLE (\w+)/g)].map((m) => m[1]!);

describe("the bundle format", () => {
  it("is defined in exactly one place", () => {
    const builder = readFileSync(here("build-fixture.mjs"), "utf8");
    expect(builder).toContain("pipeline/deliver/bundle/schema.sql");
    // no second DDL hiding in the packager
    expect(builder, "the fixture builder declares its own tables").not.toMatch(/CREATE TABLE/);
  });

  it("declares the tables the data contract names", () => {
    // Named individually: if one is dropped, the failure should say which.
    for (const t of ["item", "alias", "item_section", "entry", "rule", "section_ruleset", "ruleset",
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

  it("produced a fixture with every table present", () => {
    expect(existsSync(fixture), "run `pnpm fixture`").toBe(true);
    const db = new DatabaseSync(fixture, { readOnly: true });
    const got = new Set(db.prepare(
      "SELECT name FROM sqlite_master WHERE type='table'").all().map((r) => r.name as string));
    db.close();
    for (const t of TABLES) expect(got, `${t} missing from the built fixture`).toContain(t);
  });

  it("keys rules on (entry_id, rule_id), never on rule_id alone", () => {
    // 49 rule_ids collide corpus-wide (AGENTS rule 8). A table keyed on rule_id alone
    // silently merges two different waters' rules.
    expect(ddl).toMatch(/PRIMARY KEY \(entry_id, rule_id\)/);
  });
});
