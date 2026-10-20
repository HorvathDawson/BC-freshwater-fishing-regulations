/**
 * The dev fixture's DDL IS the bundle's DDL, table by table and index by index (P03, audit #5).
 *
 * `tools/bundle-schema.test.ts` proves every contract table is PRESENT. It cannot see a table whose columns changed
 * after the fixture was built: the committed fixture ran six tables behind `schema.sql`, with an old `section_ruleset`,
 * and a client query against a stale column fails only at run time. Here every `CREATE TABLE` / `CREATE INDEX` in
 * `schema.sql` + `indexes.sql` must match the statement SQLite stored in the fixture's `sqlite_master`, and the
 * fixture may hold nothing those files do not declare. A mismatch means: run `pnpm fixture` and commit the result.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
const FIXTURE = process.env.FIXTURE_SCHEMA_DB ?? here("./bundle.sqlite");
const SQL = ["schema.sql", "indexes.sql"].map((f) =>
  readFileSync(here(`../../../../pipeline/deliver/bundle/${f}`), "utf8")).join("\n");

/** One spelling per statement: comments out, whitespace collapsed, none beside punctuation, lower case. */
export const norm = (s: string) => s.replace(/--.*$/gm, "").replace(/\s+/g, " ")
  .replace(/ ?([(),;]) ?/g, "$1").replace(/;$/, "").trim().toLowerCase();

/** name → normalised statement, for every CREATE TABLE / CREATE INDEX in the contract files. */
function declared(): Map<string, string> {
  const out = new Map<string, string>();
  const stmts = SQL.replace(/--.*$/gm, "").split(";").map((s) => s.trim()).filter(Boolean);
  for (const s of stmts) {
    const m = /^CREATE\s+(?:UNIQUE\s+)?(TABLE|INDEX)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/i.exec(s);
    if (m) out.set(m[2]!, norm(s));
  }
  return out;
}

function stored(): Map<string, string> {
  const db = new DatabaseSync(FIXTURE, { readOnly: true });
  const rows = db.prepare("SELECT name, sql FROM sqlite_master WHERE type IN ('table','index') AND sql IS NOT NULL")
    .all() as { name: string; sql: string }[];
  db.close();
  return new Map(rows.map((r) => [r.name, norm(r.sql)]));
}

describe("the dev fixture's schema", () => {
  const want = declared(), got = stored();

  it("reads a non-trivial contract", () => {
    expect(want.size).toBeGreaterThan(40);
  });

  it("has every declared table and index, with the declared definition", () => {
    const missing = [...want.keys()].filter((n) => !got.has(n));
    const changed = [...want].filter(([n, s]) => got.has(n) && got.get(n) !== s).map(([n]) => n);
    expect({ missing, changed }, "stale fixture: run `pnpm fixture` and commit it").toEqual({ missing: [], changed: [] });
  });

  it("holds nothing the contract does not declare", () => {
    expect([...got.keys()].filter((n) => !want.has(n))).toEqual([]);
  });
});
