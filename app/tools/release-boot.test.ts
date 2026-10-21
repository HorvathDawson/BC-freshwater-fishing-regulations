/**
 * P11: START-UP OVER THE RELEASE — the function App.tsx runs (`bootRelease`), driven over a real release.
 *
 *   web (Tier 1, the fixture release)    `bootRelease` and the map's status fetch the manifest and paint, and NOTHING
 *                                        else; the first tap adds core; never a dev-path file. This does NOT mount the
 *                                        Shell. Of what the Shell reads on the map tab, the counts wait for a screen that
 *                                        shows them (`ui/src/datafacts.render.test.tsx`); the flow standings
 *                                        (`usePanelStandings`) do NOT wait — once the map reports its visible sections
 *                                        and the feed is up they open flow-area, core and conditions. So "manifest +
 *                                        paint only" holds up to the first viewport report, not past it (unmeasured in
 *                                        a mounted Shell).
 *   native (Tier 1, an in-memory disk)   first run installs every file with progress; an offline launch opens the held
 *                                        set; a newer release downloads only its changed files, in the background, and
 *                                        is used — and the old set pruned — at the next launch. A tile-only release
 *                                        (same data_release) is an update too, and its old tile folder is pruned; a new
 *                                        set that will not open is rolled back to the one it replaced; a launch killed
 *                                        right after the swap still prunes.
 *   the Vedder (Tier 2, the live release: RELEASE_DIR=data/release/r-…)
 *                                        opening gnis:3062 adds core + water + regs-2 (the loader's `opened()`), and a
 *                                        water in Region 5 then fetches regs-5 only. Without RELEASE_DIR it says so.
 */
import { describe, expect, it } from "vitest";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fetchVerified, sha256Hex, type ItemId, type Manifest } from "@app/data";
import { bootRelease, heldPath, POINTER, releaseTiles, type ReleaseFs, type ReleasePlatform }
  from "@app/data/release";
import { releaseCache } from "../packages/data/src/release/drivers/cache.web";
import { releaseSource } from "../packages/data/src/release/drivers/source.web";
import { releaseStatus, statusOf } from "../packages/ui/src/release";
import { buildFixtureRelease, nodeOpener, releaseFetch, type FixtureRelease } from "../conformance/src/fixtureRelease";

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

/** A tile version in the fixture release's own sid space ("<handles16>.<tilebuild12>"). */
function tileVersion(fx: FixtureRelease, build: string): string {
  const m = JSON.parse(readFileSync(join(fx.dir, "manifest.json"), "utf8")) as Manifest;
  return `${m.tiles.compatible[0]!.slice(0, -1)}${build}`;
}

/**
 * The fixture release with a tile version folder (`release/tiles/<v>/`: atlas + mask, as `to_release` writes it), served
 * by a manifest that names it. Returns the folder `releaseFetch` serves that manifest from (its data urls are the
 * release's own).
 */
function withTiles(fx: FixtureRelease, v: string): string {
  const folder = join(fx.root, "tiles", v);
  mkdirSync(folder, { recursive: true });
  const ref = (name: string, body: string) => {
    const b = new TextEncoder().encode(body);
    writeFileSync(join(folder, name), b);
    return { url: `tiles/${v}/${name}`, bytes: b.length, sha256: sha256Hex(b) };
  };
  const m = JSON.parse(readFileSync(join(fx.dir, "manifest.json"), "utf8")) as Manifest;
  m.tiles.current = { id: v, ...ref(`atlas-${v}.pmtiles`, `atlas ${v}`) };
  m.tiles.mask = ref("bc_outside.geojson", `mask ${v}`);
  const at = join(fx.root, `manifest-${v}`);
  mkdirSync(at, { recursive: true });
  writeFileSync(join(at, "manifest.json"), JSON.stringify(m));
  return at;
}

const tileFiles = (fs: ReturnType<typeof memFs>, v: string) =>
  [...fs.files.keys()].filter((k) => k.startsWith(`release/tiles/${v}/`)).sort();

/** The phone, over an in-memory disk: files are read from the held set, as source.native.ts does. */
function phone(fs: ReturnType<typeof memFs>, disk: ReleaseFs = fs): ReleasePlatform {
  return {
    releaseOpener: () => nodeOpener(),
    releaseCache: async () => ({ kind: "none", get: async () => null, put: async () => {} }),
    releaseSource: (m, at) => ({
      bytes: async (name) => fs.read(heldPath(at.held!, m.files[name]!.url)),
    }),
    device: (f) => ({
      fs: disk, download: (url, e) => fetchVerified(url, e, undefined, f),
      // the tiles stream to disk on a phone; here they are small, so a verified download written in one piece
      toDisk: async (url, path, e, onBytes) => {
        const b = await fetchVerified(url, e, undefined, f);
        await disk.write(path, b);
        onBytes?.(b.length);
      },
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

  it("a TILE-ONLY release (same data_release) is installed, drawn at the next launch, and the old tiles pruned after",
    async () => {
      const r1 = buildFixtureRelease(null);
      const v1 = tileVersion(r1, "aaaaaaaaaaaa"), v2 = tileVersion(r1, "bbbbbbbbbbbb");
      const at1 = withTiles(r1, v1), at2 = withTiles(r1, v2);
      const fs = memFs(), log: string[] = [];
      const boot = (f: typeof fetch) => bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE,
                                                       wasmBase: "", platform: phone(fs), fetch: f,
                                                       manifestTimeoutMs: 1000 });
      const first = await boot(releaseFetch(BASE, at1, []));
      expect(first.held.tiles).toBe(v1);
      expect(fs.files.has(`release/tiles/${v1}/atlas-${v1}.pmtiles`)).toBe(true);

      // the same data, new tiles: this launch keeps v1; the update fetches the new tiles and mask, and no data file
      const second = await boot(releaseFetch(BASE, at2, log));
      expect(second.held.tiles).toBe(v1);
      const next = await second.update;
      expect(next).toMatchObject({ release: first.held.release, tiles: v2 });
      expect(log).toEqual([`${BASE}manifest.json`, `${BASE}tiles/${v2}/atlas-${v2}.pmtiles`,
                           `${BASE}tiles/${v2}/bc_outside.geojson`]);
      // the set it replaces is still whole until the new one has opened
      expect(tileFiles(fs, v1)).toEqual([`release/tiles/${v1}/atlas-${v1}.pmtiles`,
                                         `release/tiles/${v1}/bc_outside.geojson`]);

      const third = await boot(offline);
      expect(third.held.tiles).toBe(v2);
      expect(third.manifest.tiles.current!.id).toBe(v2);
      expect(releaseTiles(third, DEV_TILES).atlas).toBe(`file:///doc/release/tiles/${v2}/atlas-${v2}.pmtiles`);
      // the old tile version is gone; the new one and the (shared) data set stay
      expect(tileFiles(fs, v1)).toEqual([]);
      expect(tileFiles(fs, v2)).toEqual([`release/tiles/${v2}/atlas-${v2}.pmtiles`,
                                         `release/tiles/${v2}/bc_outside.geojson`]);
      expect(await third.source.water("gnis:8634" as ItemId)).not.toBeNull();
      expect(fs.files.has("release/previous")).toBe(false);
    });

  it("a new set that will not open is ROLLED BACK to the one it replaced, said, pruned, and not installed again",
    async () => {
      const r1 = buildFixtureRelease(null);
      const r2 = buildFixtureRelease(null, (db) => {
        db.prepare("INSERT INTO lake_gauge VALUES ('wbk:329177889', '08MH103')").run();
      });
      const fs = memFs();
      const boot = (f: typeof fetch) => bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE,
                                                       wasmBase: "", platform: phone(fs), fetch: f,
                                                       manifestTimeoutMs: 1000 });
      const first = await boot(releaseFetch(BASE, r1.dir, []));
      expect(first.rolledBack).toBeNull();
      const next = (await (await boot(releaseFetch(BASE, r2.dir, []))).update)!;
      const m2 = JSON.parse(readFileSync(join(r2.dir, "manifest.json"), "utf8")) as Manifest;
      // the new set's paint is damaged on disk (the install verified it; something broke it since)
      fs.files.set(heldPath(next, m2.files.paint!.url), new Uint8Array([1, 2, 3]));

      const back = await boot(offline);
      expect(back.held.release).toBe(first.held.release);
      expect(back.rolledBack).toEqual({ refused: m2.data_release, using: first.held.release,
                                        reason: expect.any(String) as string });
      expect(JSON.parse(new TextDecoder().decode(fs.files.get(POINTER)!)).release).toBe(first.held.release);
      expect([...fs.files.keys()].some((k) => k.startsWith(`release/${m2.data_release}/`))).toBe(false);
      expect(fs.files.has("release/previous")).toBe(false);

      // the next launch opens the old set as its own (nothing more to say), and the refused set is not fetched again
      const log: string[] = [];
      const after = await boot(releaseFetch(BASE, r2.dir, log));
      expect(after.held.release).toBe(first.held.release);
      expect(after.rolledBack).toBeNull();
      expect(await after.update).toBeNull();
      expect(log).toEqual([`${BASE}manifest.json`]);
    });

  it("a launch killed right after the swap still prunes the set it replaced (PREVIOUS is written before the swap)",
    async () => {
      const r1 = buildFixtureRelease(null);
      const r2 = buildFixtureRelease(null, (db) => {
        db.prepare("INSERT INTO lake_gauge VALUES ('wbk:329177889', '08MH103')").run();
      });
      const fs = memFs();
      const boot = (f: typeof fetch, disk: ReleaseFs = fs) => bootRelease({
        manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
        platform: phone(fs, disk), fetch: f, manifestTimeoutMs: 1000 });
      const first = await boot(releaseFetch(BASE, r1.dir, []));
      // the swap lands, and the app dies before anything after it runs
      const dying: ReleaseFs = { ...fs, rename: async (a, b) => {
        await fs.rename(a, b);
        if (b === POINTER) throw new Error("killed");
      } };
      await expect((await boot(releaseFetch(BASE, r2.dir, []), dying)).update).rejects.toThrow("killed");
      const again = await boot(offline);
      expect(again.held.release).not.toBe(first.held.release);
      expect([...fs.files.keys()].some((k) => k.startsWith(`release/${first.held.release}/`))).toBe(false);
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
    // through the source the Shell is handed (`Booted.source`): the read a water's page makes
    expect((await b.source.regulationKeys("gnis:3062" as ItemId)).length).toBeGreaterThan(0);
    expect(b.loader.opened()).toEqual(["paint", "core", "water", "regs-2"]);
    expect(log.length).toBe(5);                                        // fetched, not only opened: one each
    const water = await b.loader.db("water"), core = await b.loader.db("core");
    const five = await water.get(
      "SELECT p.item_ord FROM part p JOIN akey_region a ON a.akey = p.akey WHERE a.region = '5' AND p.akey NOT IN " +
      "(SELECT akey FROM akey_region WHERE region <> '5') ORDER BY p.item_ord LIMIT 1");
    const item = await core.get("SELECT item_id FROM item WHERE ord = ?", Number(five!.item_ord));
    await b.source.regulationKeys(String(item!.item_id) as ItemId);
    expect(b.loader.opened()).toEqual(["paint", "core", "water", "regs-2", "regs-5"]);
    expect(log.length).toBe(6);
    expect(log.filter((u) => DEV_PATH.test(u))).toEqual([]);
  });
});
