/**
 * P11: START-UP OVER THE RELEASE — the function App.tsx runs (`bootRelease`), driven over a real release.
 *
 *   web (Tier 1, the fixture release)    a cold start fetches the manifest and paint, and NOTHING else, before the
 *                                        first coloured map; the first tap adds core; never a dev-path file.
 *   native (Tier 1, an in-memory disk)   first run installs every file with progress; an offline launch opens the held
 *                                        set; a newer release downloads only its changed files, in the background, and
 *                                        is used — and the old set pruned — at the next launch.
 *   the Vedder (Tier 2, the live release: RELEASE_DIR=data/release/r-…)
 *                                        opening gnis:3062 adds core + water + regs-2 (the loader's `opened()`), and a
 *                                        water in Region 5 then fetches regs-5 only. Without RELEASE_DIR it says so.
 */
import { describe, expect, it } from "vitest";
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { fetchVerified, type ItemId, type Manifest } from "@app/data";
import { bootRelease, heldPath, openRegsFor, releaseTiles, type ReleaseFs, type ReleasePlatform }
  from "@app/data/release";
import { releaseCache } from "../packages/data/src/release/drivers/cache.web";
import { releaseSource } from "../packages/data/src/release/drivers/source.web";
import { releaseStatus, statusOf } from "../packages/ui/src/release";
import { buildFixtureRelease, nodeOpener, releaseFetch } from "../conformance/src/fixtureRelease";

const BASE = "https://r2.test/release/";
const DEV_TILES = { atlas: "dev:atlas.pmtiles", basemap: "dev:basemap.pmtiles", outside: "dev:bc_outside.geojson" };
const DEV_PATH = /province\.sqlite|status_index\.bin|atlas\.meta\.json|bundle\.sqlite/;

/** The web half, for real (sql.js aside: node:sqlite opens the same verified bytes). */
const web: ReleasePlatform = {
  releaseOpener: () => nodeOpener(),
  releaseCache: () => releaseCache(),
  releaseSource: (m, at) => releaseSource(m, at),
  device: () => null,
};

describe("web: a cold start is the manifest and paint, then each file on first need", () => {
  it("fetches manifest + paint before the first coloured map, and nothing from the dev path", async () => {
    const fx = buildFixtureRelease(null), log: string[] = [];
    const m = JSON.parse(await (await releaseFetch(BASE, fx.dir, [])(`${BASE}manifest.json`)).text()) as Manifest;
    const stages: string[] = [];
    const b = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                                  platform: web, fetch: releaseFetch(BASE, fx.dir, log), onStage: (s) => stages.push(s) });
    expect(stages).toEqual(["manifest", "paint"]);
    expect(log).toEqual([`${BASE}manifest.json`, `${BASE}${m.files.paint!.url}`]);
    expect(b.loader.opened()).toEqual(["paint"]);
    // the map's status is paint's, decoded already: colouring fetches nothing more
    const status = await statusOf(releaseStatus(b.loader));
    expect(status).toBe(b.paint.status);
    expect(log.length).toBe(2);
    // the first tap: core, and only core
    expect(await b.source.itemForSection(0 as never)).not.toBeUndefined();
    expect(log).toEqual([`${BASE}manifest.json`, `${BASE}${m.files.paint!.url}`, `${BASE}${m.files.core!.url}`]);
    expect(log.filter((u) => DEV_PATH.test(u))).toEqual([]);
    // no tile version folder in the fixture: the map keeps the development endpoints
    expect(releaseTiles(b, DEV_TILES)).toEqual(DEV_TILES);
  });

  it("a release that names its tiles and mask is drawn from them", async () => {
    const fx = buildFixtureRelease(null);
    const b = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                                  platform: web, fetch: releaseFetch(BASE, fx.dir, []) });
    const named = { ...b, manifest: { ...b.manifest, tiles: { ...b.manifest.tiles,
      current: { id: "v1", url: "tiles/v1/atlas-v1.pmtiles", bytes: 1, sha256: "x" },
      mask: { url: "tiles/v1/bc_outside.geojson", bytes: 1, sha256: "y" } } } };
    expect(releaseTiles(named, DEV_TILES)).toEqual({
      atlas: `${BASE}tiles/v1/atlas-v1.pmtiles`, basemap: DEV_TILES.basemap, outside: `${BASE}tiles/v1/bc_outside.geojson` });
  });
});

function memFs(): ReleaseFs & { files: Map<string, Uint8Array> } {
  const files = new Map<string, Uint8Array>();
  return {
    files,
    exists: async (p) => files.has(p) || [...files.keys()].some((k) => k.startsWith(p + "/")),
    read: async (p) => { const b = files.get(p); if (!b) throw new Error(`no ${p}`); return b; },
    write: async (p, b) => { files.set(p, b); },
    rename: async (a, b) => { files.set(b, files.get(a)!); files.delete(a); },
    remove: async (p) => { for (const k of [...files.keys()]) if (k === p || k.startsWith(p + "/")) files.delete(k); },
  };
}

/** The phone, over an in-memory disk: files are read from the held set, as source.native.ts does. */
function phone(fs: ReturnType<typeof memFs>): ReleasePlatform {
  return {
    releaseOpener: () => nodeOpener(),
    releaseCache: async () => ({ kind: "none", get: async () => null, put: async () => {} }),
    releaseSource: (m, at) => ({
      bytes: async (name) => fs.read(heldPath(at.held!, m.files[name]!.url)),
    }),
    device: (f) => ({
      fs, download: (url, e) => fetchVerified(url, e, undefined, f),
      toDisk: async () => { throw new Error("no tiles in the fixture"); },
      dirUri: (p) => `file:///doc/${p}`, fileUri: (p) => `file:///doc/${p}`,
    }),
  };
}
const offline = (async () => { throw new TypeError("Network request failed"); }) as unknown as typeof fetch;

describe("native: always offline, updated by diff, swapped at the next launch", () => {
  it("installs on first run, opens offline, downloads only what changed, and prunes the old set after", async () => {
    const r1 = buildFixtureRelease(null);
    // a second release: three lake stations more, which changes core (item.gauged) and conditions, not paint
    const r2 = buildFixtureRelease(null, (db) => {
      db.prepare("INSERT INTO lake_gauge VALUES ('wbk:329177889', '08MH103')").run();
    });
    const fs = memFs(), log: string[] = [], stages: string[] = [], progress: string[] = [];
    const boot = (f: typeof fetch) => bootRelease({
      manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "", platform: phone(fs), fetch: f,
      onStage: (s) => stages.push(s), onProgress: (p) => progress.push(`${p.file} ${p.done}/${p.total}`),
      manifestTimeoutMs: 1000 });

    const first = await boot(releaseFetch(BASE, r1.dir, log));
    expect(stages).toEqual(["manifest", "install", "paint"]);
    expect(progress).toEqual(["conditions 1/5", "core 2/5", "flow-area 3/5", "near 4/5", "paint 5/5"]);
    expect(log.length).toBe(6);                                       // the manifest + every file, once
    expect(first.held.release).toBe(first.manifest.data_release);
    expect(await first.update).toBeNull();

    // an offline launch: the held set, no network
    stages.length = 0;
    const again = await boot(offline);
    expect(stages).toEqual(["manifest", "paint"]);
    expect(again.held).toEqual(first.held);
    expect(await again.source.water("gnis:8634" as ItemId)).not.toBeNull();

    // a newer release: this launch stays on the held set; the update fetches only the changed files
    log.length = 0;
    const m2 = JSON.parse(await (await releaseFetch(BASE, r2.dir, [])(`${BASE}manifest.json`)).text()) as Manifest;
    const changed = Object.keys(m2.files).filter((k) => m2.files[k]!.sha256 !== first.manifest.files[k]!.sha256).sort();
    expect(changed).toEqual(["conditions", "core"]);
    const third = await boot(releaseFetch(BASE, r2.dir, log));
    expect(third.held.release).toBe(first.held.release);
    const next = await third.update;
    expect(next!.release).toBe(m2.data_release);
    expect(log).toEqual([`${BASE}manifest.json`, ...changed.map((k) => `${BASE}${m2.files[k]!.url}`)]);

    // the next launch uses the new set, and only then is the old one deleted
    expect([...fs.files.keys()].some((k) => k.startsWith(`release/${first.held.release}/`))).toBe(true);
    const fourth = await boot(offline);
    expect(fourth.held.release).toBe(m2.data_release);
    expect([...fs.files.keys()].some((k) => k.startsWith(`release/${first.held.release}/`))).toBe(false);
    expect(fourth.locate("tiles/x/atlas-x.pmtiles")).toBe("file:///doc/release/tiles/x/atlas-x.pmtiles");
  });

  it("a first run with no network says so, rather than opening nothing", async () => {
    await expect(bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                               platform: phone(memFs()), fetch: offline, manifestTimeoutMs: 1000 }))
      .rejects.toMatchObject({ firstRunOffline: true });
  });
});

const LIVE = process.env.RELEASE_DIR ? resolve(__dirname, "../..", process.env.RELEASE_DIR) : null;
describe.skipIf(!LIVE || !existsSync(LIVE!))("the Vedder on the live release (Tier 2: RELEASE_DIR)", () => {
  it("opening the Vedder adds core + water + regs-2; a Region 5 water then adds regs-5 only", async () => {
    const log: string[] = [];
    const b = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                                  platform: web, fetch: releaseFetch(BASE, LIVE!, log) });
    expect(log.map((u) => u.split("/").pop()!.split("-")[0])).toEqual(["manifest.json", "paint"]);
    expect((await openRegsFor(b.loader, "gnis:3062" as ItemId)).length).toBeGreaterThan(0);
    expect(b.loader.opened()).toEqual(["paint", "core", "water", "regs-2"]);
    const water = await b.loader.db("water"), core = await b.loader.db("core");
    const five = await water.get(
      "SELECT p.item_ord FROM part p JOIN akey_region a ON a.akey = p.akey WHERE a.region = '5' AND p.akey NOT IN " +
      "(SELECT akey FROM akey_region WHERE region <> '5') ORDER BY p.item_ord LIMIT 1");
    const item = await core.get("SELECT item_id FROM item WHERE ord = ?", Number(five!.item_ord));
    await openRegsFor(b.loader, String(item!.item_id) as ItemId);
    expect(b.loader.opened()).toEqual(["paint", "core", "water", "regs-2", "regs-5"]);
    expect(log.filter((u) => DEV_PATH.test(u))).toEqual([]);
  });
});
