/**
 * A WATER SPLIT INTO CLOSURE AREAS IS PLACED BY THEM (owner, 2026-10-20). Kootenay, Williston and Shannon lakes are
 * curated lake splits: each closure area is its own item `part_of` the lake, the areas hold every section, and the lake
 * itself owns no section, no part and so no `part_geo` row. The bundle reader's `waterBox` reads the lake's own parts
 * and its areas' — here over a copy of the dev bundle with Kootenay Lake's shape (release r-6960fe8201e3) written in.
 */
import { copyFileSync, mkdtempSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { parseManifest, type ItemId } from "@app/data";
import { makeBundleSource } from "@app/data/bundle";
import { DEV_BUNDLE, openBundle } from "@app/data/bundle/node";
import { makeReleaseSource, releaseLoader } from "@app/data/release";

const AREAS: [number, string, string, [number, number, number, number]][] = [
  [9002, "wbk:-20", "Kootenay Lake — Main Body", [-117.130272, 49.246325, -116.479453, 50.180355]],
  [9003, "wbk:-21", "Kootenay Lake — Upper West Arm", [-117.219097, 49.551213, -116.967467, 49.639755]],
  [9004, "wbk:-22", "Kootenay Lake — Lower West Arm", [-117.468881, 49.447024, -117.213168, 49.592476]],
];

function kootenay(): string {
  const path = join(mkdtempSync(join(tmpdir(), "bundle-geo-")), "bundle.sqlite");
  copyFileSync(DEV_BUNDLE, path);
  const w = new DatabaseSync(path);
  w.exec("INSERT INTO item (ord, item_id, name, kind) VALUES (9001, 'wbk:328974235', 'Kootenay Lake', 'lake')");
  for (const [ord, id, name, [we, s, e, n]] of AREAS) {
    w.prepare("INSERT INTO item (ord, item_id, name, kind, part_of) VALUES (?, ?, ?, 'lake', 'wbk:328974235')")
      .run(ord, id, name);
    w.prepare("INSERT INTO part (ord, part_ix, province_except, steelhead_water, steelhead_rules, home_regions, " +
              "tidal, sections, rep_sid) VALUES (?, 0, '', 0, 0, '', 0, 1, 1)").run(ord);
    w.prepare("INSERT INTO part_geo VALUES (?, 0, 0, 1, ?, ?, ?, ?, ?, ?)").run(ord, (we + e) / 2, (s + n) / 2, we, s, e, n);
  }
  w.close();
  return path;
}

/**
 * THE RELEASE READER'S SQL, over real SQLite: a core and a water file shaped as the release writes them (core's `item`
 * with `part_of`, water's `part_geo` in integer microdegrees), so the two keyed reads are the ones that run.
 */
function releaseFiles(): { core: string; water: string } {
  const dir = mkdtempSync(join(tmpdir(), "release-geo-"));
  const meta = (d: DatabaseSync, format: string) => {
    d.exec("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL) WITHOUT ROWID");
    d.prepare("INSERT INTO meta VALUES ('format', ?), ('section_handles', ?)").run(format, H);
  };
  const core = new DatabaseSync(join(dir, "core.sqlite"));
  meta(core, "core/1");
  core.exec("CREATE TABLE item (ord INTEGER PRIMARY KEY, item_id TEXT NOT NULL UNIQUE, name TEXT NOT NULL, kind TEXT, " +
            "part_of TEXT); CREATE INDEX item_by_parent ON item(part_of) WHERE part_of IS NOT NULL");
  core.exec("INSERT INTO item VALUES (11837, 'wbk:328972518', 'Kooryet Lake', 'lake', NULL), " +
            "(11862, 'wbk:328974235', 'Kootenay Lake', 'lake', NULL)");
  for (const [ord, id, name] of AREAS)
    core.prepare("INSERT INTO item VALUES (?, ?, ?, 'lake', 'wbk:328974235')").run(ord, id, name);
  core.close();
  const water = new DatabaseSync(join(dir, "water.sqlite"));
  meta(water, "water/1");
  water.exec("CREATE TABLE part_geo (ord INTEGER NOT NULL, part_ix INTEGER NOT NULL, rank INTEGER NOT NULL, " +
             "mid_sid INTEGER NOT NULL, mid_lon INTEGER NOT NULL, mid_lat INTEGER NOT NULL, w INTEGER, s INTEGER, " +
             "e INTEGER, n INTEGER, PRIMARY KEY (ord, part_ix)) WITHOUT ROWID");
  const e6 = (v: number) => Math.round(v * 1e6);
  const rows: [number, number[]][] = [[11837, [-130.043061, 53.333981, -129.902673, 53.361987]],
                                      ...AREAS.map(([ord, , , b]) => [ord, b] as [number, number[]])];
  for (const [ord, [w, s, e, n]] of rows)
    water.prepare("INSERT INTO part_geo VALUES (?, 0, 0, 1, ?, ?, ?, ?, ?, ?)")
      .run(ord, e6((w! + e!) / 2), e6((s! + n!) / 2), e6(w!), e6(s!), e6(e!), e6(n!));
  water.close();
  return { core: join(dir, "core.sqlite"), water: join(dir, "water.sqlite") };
}
const H = "d768d6e135e62b99";

describe("the release reader places every water by its parts — real SQL", () => {
  it("Kooryet Lake is its part_geo; Kootenay Lake (no part of its own) is its three areas", async () => {
    const f = releaseFiles();
    const m = parseManifest({
      format: "app-manifest/2", data_release: "r-1",
      tiles: { compatible: [`${H}.*`], current: null, assets: null, mask: null }, basemap: null,
      files: Object.fromEntries((["core", "water"] as const).map((k) => [k, {
        url: `r-1/${k}.sqlite.gz`, bytes: 1, gz: 0, sha256: "0".repeat(64), format: `${k}/1`, section_handles: H }])),
      regulations: { valid_until: null, versions: null },
    });
    const loader = releaseLoader(m, { bytes: async () => new Uint8Array() },
                                 async (name) => openBundle(name === "core" ? f.core : f.water));
    const src = makeReleaseSource(loader);
    expect(await src.waterBox("wbk:328972518" as ItemId)).toEqual([-130.043061, 53.333981, -129.902673, 53.361987]);
    expect(await src.waterBox("wbk:328974235" as ItemId)).toEqual([-117.468881, 49.246325, -116.479453, 50.180355]);
    expect(await src.waterBox("wbk:-22" as ItemId)).toEqual([-117.468881, 49.447024, -117.213168, 49.592476]);
  });
});

describe("a lake split into closure areas is placed by its areas", () => {
  it("the lake, owning no part, is the union of its three areas; each area is its own box", async () => {
    const db = openBundle(kootenay());
    const src = makeBundleSource(db);
    expect(await src.waterBox("wbk:328974235" as ItemId)).toEqual([-117.468881, 49.246325, -116.479453, 50.180355]);
    expect(await src.waterBox("wbk:-21" as ItemId)).toEqual([-117.219097, 49.551213, -116.967467, 49.639755]);
    db.close?.();
  });
});
