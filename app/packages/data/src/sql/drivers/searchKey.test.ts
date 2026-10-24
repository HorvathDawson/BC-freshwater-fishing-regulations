/**
 * (T114) ONE SEARCH KEY: "does this query match this water's name or alias?" is answered by `searchKey` alone — the SQL
 * filter compares the STORED keys (`item.name_key`, `alias.alias_key`) with the query's key (`nameNeedle`), and the
 * ranking keys with the same function. Before, the SQL filtered the raw name (LIKE folds ASCII case only): "barriere"
 * returned none of the six Barrière waters and "childrens" never found Children's Fishing Pond.
 *
 * The store is built in memory from the release CONTRACT's core and near columns (`PACK_FILES`, generated from
 * `pipeline/deliver/release/contract.py`), behind the release reader. The keys are
 * written by `searchKey`; the pipeline writes them with `search_key`, held to the same case table. They are seeded THIN, as
 * the pipeline stores them (`stored_key`): NULL for the ASCII names whose key is their lower case (Barrie Creek, Dore
 * River, Mabel Lake and its alias), so the reader's `COALESCE(key, lower(text))` is exercised on both branches.
 *
 * Mutations: put `i.name LIKE` / `a.alias LIKE` back in either SEARCH → "barriere" finds 0 and "childrens" 0 (the
 * acceptance cases) and the grep pin fails; `nameNeedle` = `likeEscape` without `searchKey` → "BARRIÈRE" finds 0;
 * drop the key-length guard → "é'" (key "e") answers every water holding an e. ("''" alone does not pin the guard:
 * its key is "", and `rankWaters` drops every row for an empty key, so it answers nothing either way.)
 */
import { readFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import { searchKey } from "@app/core";
import { PACK_FILES } from "../../pack.generated";
import { releaseLoader } from "../../release/loader";
import { makeReleaseSource } from "../../release/source";
import { parseManifest } from "../../manifest";
import { sha256Hex } from "../../sha256";
import type { Db, Row } from "../db";
import type { WaterSource } from "../../index";
import * as Q from "../queries";
import * as R from "../../release/queries";

/** [item_id, name, kind, aliases]: the six Barrière waters, the pond, and waters that must NOT match. */
const WATERS: [string, string, string, string[]][] = [
  ["wbk:1", "Barrière Lake", "lake", []],
  ["wbk:2", "North Barrière Lake", "lake", []],
  ["wbk:3", "East Barrière Lake", "lake", []],
  ["gnis:4", "Barrière River", "stream", []],
  ["gnis:5", "North Barrière River", "stream", []],
  ["gnis:6", "East Barrière River", "stream", []],
  ["wbk:7", "Children's Fishing Pond", "lake", []],
  ["gnis:8", "Barrie Creek", "stream", []],
  ["gnis:9", "Dore River", "stream", ["Doré Creek"]],
  ["wbk:10", "Mabel Lake", "lake", ["Mable Lake"]],
];
const BARRIERE = ["gnis:4", "gnis:5", "gnis:6", "wbk:1", "wbk:2", "wbk:3"];
/** What the pipeline leaves NULL (`stored_key`): spelled out, not computed, so the test holds no copy of the rule. */
const THIN = new Set(["Barrie Creek", "Dore River", "Mabel Lake", "Mable Lake"]);
const stored = (t: string): string | null => (THIN.has(t) ? null : searchKey(t));

/** The release's core + near, columns as the contract states them, behind a loader that verifies nothing. */
function releaseStore(): WaterSource {
  const make = (file: "core" | "near") => {
    const d = new DatabaseSync(":memory:");
    for (const [t, cols] of Object.entries(PACK_FILES[file]!.tables)) d.exec(`CREATE TABLE ${t} (${cols.join(", ")})`);
    return d;
  };
  const core = make("core"), near = make("near");
  WATERS.forEach(([id, name, kind, aliases], ord) => {
    core.prepare("INSERT INTO item (ord, item_id, name, kind, name_key, pieces) VALUES (?, ?, ?, ?, ?, 1)")
      .run(ord, id, name, kind, stored(name));
    for (const a of aliases)
      core.prepare("INSERT INTO alias (item_id, alias, alias_key) VALUES (?, ?, ?)").run(id, a, stored(a));
  });
  const H = "d768d6e135e62b99";
  const manifest = parseManifest({
    format: "app-manifest/2", data_release: "r-1",
    tiles: { compatible: [`${H}.*`], current: null, assets: null, mask: null }, basemap: null,
    files: Object.fromEntries(["core", "near"].map((k) => [k, {
      url: `r-1/${k}.sqlite.gz`, bytes: 1, gz: 0, sha256: sha256Hex(new TextEncoder().encode(k)), format: `${k}/1`,
      section_handles: H }])),
    regulations: { valid_until: null, versions: null },
  });
  const db = (d: DatabaseSync, file: string): Db => ({
    all: async (sql, ...p) => (sql.startsWith("SELECT k, v FROM meta")
      ? [{ k: "format", v: `${file}/1` }, { k: "section_handles", v: H }]
      : d.prepare(sql).all(...(p as never[])) as unknown as Row[]),
    get: async (sql, ...p) => d.prepare(sql).get(...(p as never[])) as unknown as Row | undefined,
  });
  const loader = releaseLoader(manifest, { bytes: async () => new Uint8Array() },
                               async (name) => (name === "core" ? db(core, "core") : db(near, "near")));
  return makeReleaseSource(loader);
}

const READERS: [string, () => WaterSource][] = [["the release reader", releaseStore]];

for (const [reader, store] of READERS) {
  describe(`one search key — ${reader}`, () => {
    it("\"barriere\" finds the six Barrière waters, and not Barrie Creek", async () => {
      const hits = await store().searchNames("barriere", 20);
      expect(hits.map((h) => h.item).sort()).toEqual(BARRIERE);
    });

    it("\"BARRIÈRE\" (accent, capitals) finds the same six", async () => {
      expect((await store().searchNames("BARRIÈRE", 20)).map((h) => h.item).sort()).toEqual(BARRIERE);
    });

    it("\"childrens\" finds Children's Fishing Pond, and so does \"children’s\"", async () => {
      const s = store();
      for (const q of ["childrens", "children’s", "Children's Fishing Pond"]) {
        expect((await s.searchNames(q, 10)).map((h) => [h.item, h.name, h.matchedAs]))
          .toEqual([["wbk:7", "Children's Fishing Pond", null]]);
      }
    });

    it("an alias matches by its key too, and says which alias it was", async () => {
      expect((await store().searchNames("dore creek", 10)).map((h) => [h.item, h.matchedAs]))
        .toEqual([["gnis:9", "Doré Creek"]]);
    });

    it("a water stored with no key (NULL: its key is its lower case) is found through lower(name), alias too", async () => {
      const s = store();
      for (const q of ["mabel", "MABEL LAKE"])
        expect((await s.searchNames(q, 10)).map((h) => [h.item, h.matchedAs])).toEqual([["wbk:10", null]]);
      expect((await s.searchNames("mable", 10)).map((h) => [h.item, h.matchedAs])).toEqual([["wbk:10", "Mable Lake"]]);
      expect((await s.searchNames("barrie creek", 10)).map((h) => h.item)).toEqual(["gnis:8"]);
    });

    it("a query whose key is under two characters answers nothing (\"''\" would otherwise be LIKE '%%')", async () => {
      expect(await store().searchNames("''", 10)).toEqual([]);
      expect(await store().searchNames("é'", 10)).toEqual([]);
    });
  });
}

describe("one search key — the pin (no second copy)", () => {
  const read = (p: string) => readFileSync(new URL(p, import.meta.url), "utf8");
  const MODULES = { "sql/queries.ts": read("../queries.ts"), "release/queries.ts": read("../../release/queries.ts") };

  it("no LIKE compares a raw name or alias in either query module", () => {
    for (const [file, text] of Object.entries(MODULES))
      expect([file, text.match(/\b(?:i\.name|a\.alias|item\.name|alias\.alias)\s+LIKE\b/g)]).toEqual([file, null]);
  });

  it("both SEARCH forms filter on the stored keys, in both halves of the union", () => {
    for (const sql of [Q.SEARCH, R.SEARCH]) {
      expect(sql).not.toMatch(/\b(?:name|alias)\s+LIKE\b/);
      expect(sql).toContain("COALESCE(i.name_key, lower(i.name)) LIKE '%' || ?1 || '%'");
      expect(sql).toContain("COALESCE(a.alias_key, lower(a.alias)) LIKE '%' || ?1 || '%'");
    }
  });

  it("the reader binds SEARCH's ?1 with nameNeedle — the query's searchKey — and nothing else", () => {
    const release = read("../../release/source.ts");
    expect(release).toMatch(/BQ\.nameNeedle\(query\)/);
    expect(Q.nameNeedle(" Barrière_Lake ")).toBe("barriere\\_lake");
  });
});
