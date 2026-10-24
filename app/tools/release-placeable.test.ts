/**
 * EVERY NAMED WATER IS PLACEABLE (owner, 2026-10-20: "Kooryet Lake — the map cannot place it yet … all named waterbodies
 * should be placeable"). Over a LIVE release, through the reader the app ships (`makeReleaseSource`, the loader, every
 * file verified against its manifest): `waterBox` gives every item in core a box, and the extent the search map opens on
 * (`extentOf`) is its outline — never the gauge-and-town evidence, never nothing.
 *
 * DATA, so Tier 2 (`scripts/test-full.sh` runs it with TIER2=1):
 *   RELEASE_DIR=data/release/r-… TIER2=1 pnpm -C app exec vitest run release-placeable
 * Under TIER2 a missing release FAILS — a gate that passes with nothing to check is the false pass AGENTS warns of. In
 * `pnpm check` (no data, no TIER2) it is skipped, by name.
 */
import { describe, expect, it } from "vitest";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { extentOf } from "@app/core";
import { parseManifest, verify, type ItemId } from "@app/data";
import { openSqlite } from "@app/data/sql/node";
import { makeReleaseSource, releaseLoader } from "@app/data/release";

const ROOT = resolve(__dirname, "../..");
const REL = process.env.RELEASE_DIR ? resolve(ROOT, process.env.RELEASE_DIR) : null;
const TIER2 = process.env.TIER2 === "1";

describe.skipIf(!REL && !TIER2)("Tier 2: every water in the live release is placeable", () => {
  it("has a release to check (RELEASE_DIR)", () => {
    expect(REL, "Tier 2 needs RELEASE_DIR=data/release/r-…").not.toBeNull();
  });

  it("every item in core has a box from its own geometry, and opens on its outline", async () => {
    const m = parseManifest(JSON.parse(readFileSync(join(REL!, "manifest.json"), "utf8")));
    const tmp = mkdtempSync(join(tmpdir(), "placeable-"));
    const loader = releaseLoader(m, {
      bytes: async (name) => {
        const spec = m.files[name];
        if (!spec) throw new Error(`no ${name}`);
        const gz = new Uint8Array(readFileSync(join(REL!, "..", spec.url)));
        return verify(spec.url, gz, spec);
      },
    }, async (name, bytes) => {
      const p = join(tmp, `${name}.sqlite`);
      writeFileSync(p, await bytes());
      return openSqlite(p);
    });
    const source = makeReleaseSource(loader);
    await loader.db("core");
    const core = new DatabaseSync(join(tmp, "core.sqlite"), { readOnly: true });
    const items = core.prepare("SELECT item_id, name FROM item WHERE name <> '' ORDER BY ord").all()
      .map((r) => ({ item: String(r.item_id) as ItemId, name: String(r.name) }));
    core.close();
    expect(items.length).toBeGreaterThan(19_000);

    const unplaced: string[] = [];
    const notOutline: string[] = [];
    for (const { item, name } of items) {
      const box = await source.waterBox(item);
      if (!box) { unplaced.push(`${item} ${name}`); continue; }
      if (extentOf(box, null)?.basis !== "outline") notOutline.push(`${item} ${name}`);
    }
    expect({ unplaced, notOutline }).toEqual({ unplaced: [], notOutline: [] });

    // the ticket's own waters, exactly: Kooryet Lake is its part_geo; the three lakes split into closure areas (which
    // own no part) are their areas' union
    expect(await source.waterBox("wbk:328972518" as ItemId))
      .toEqual([-130.043061, 53.333981, -129.902673, 53.361987]);
    expect(await source.waterBox("wbk:328974235" as ItemId))
      .toEqual([-117.468881, 49.246325, -116.479453, 50.180355]);
    for (const lake of ["wbk:328961697", "wbk:329459193"] as ItemId[])
      expect([lake, await source.waterBox(lake) === null]).toEqual([lake, false]);
  }, 300_000);
});
