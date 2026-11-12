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
 *                                        (same data_release) is an update too, and its old tile folder is pruned; a tile
 *                                        rebuild that keeps its assets id still gets the assets folder under the new
 *                                        tile version (copied from the held one); a new
 *                                        set that will not open is rolled back to the one it replaced; a launch killed
 *                                        right after the swap still prunes.
 *   the Vedder (Tier 2, the live release: RELEASE_DIR=data/release/r-…)
 *                                        opening gnis:3062 adds core + water + regs-2 (the loader's `opened()`), and a
 *                                        water in Region 5 then fetches regs-5 only. Without RELEASE_DIR it says so.
 */
import { describe, expect, it } from "vitest";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { gunzipSync } from "node:zlib";
import { fetchVerified, sha256Hex, type ItemId, type Manifest } from "@app/data";
import { assetsDigest, bootRelease, bootSaid, heldPath, POINTER, releaseTiles, type ReleaseFs, type ReleasePlatform }
  from "@app/data/release";
import { releaseCache } from "../packages/data/src/release/drivers/cache.web";
import { releaseSource } from "../packages/data/src/release/drivers/source.web";
import { releaseStatus, statusOf } from "../packages/ui/src/release";
import { fixtureRelease, namedRelease, nodeOpener, releaseFetch, type FixtureRelease } from "../conformance/src/fixtureRelease";

const BASE = "https://r2.test/release/";
// (T404) the environment's map assets folder is required: the map never falls back to another environment's host
const DEV_TILES = { atlas: "dev:atlas.pmtiles", basemap: "dev:basemap.pmtiles", outside: "dev:bc_outside.geojson",
                    assets: "dev:map-assets/v1/" };
const DEV_PATH = /province\.sqlite|status_index\.bin|atlas\.meta\.json|bundle\.sqlite/;

/** The web half, for real (sql.js aside: node:sqlite opens the same verified bytes). */
const web: ReleasePlatform = {
  releaseOpener: () => nodeOpener(),
  releaseCache: () => releaseCache(),
  releaseSource: (m, at) => releaseSource(m, at),
  device: () => null,
};

describe("web: a cold start is the manifest and paint, then each file on first need", () => {
  it("fetches manifest + paint before the first coloured map, and nothing else", async () => {
    const fx = fixtureRelease("plain"), log: string[] = [];
    const m = JSON.parse(await (await releaseFetch(BASE, fx.dir, [])(`${BASE}manifest.json`)).text()) as Manifest;
    const stages: string[] = [];
    const b = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                                  platform: web, fetch: releaseFetch(BASE, fx.dir, log), onStage: (s) => stages.push(s) });
    expect(stages).toEqual(["manifest", "paint"]);
    expect(log).toEqual([`${BASE}manifest.json`, `${BASE}${m.files.paint!.url}`]);
    expect(b.loader.opened()).toEqual(["paint"]);
    // the map's status is paint's, decoded already: colouring fetches nothing more
    const status = await statusOf(releaseStatus(b.loader));
    expect(status).toBe(b.paint!.status);
    expect(log.length).toBe(2);
    // the first tap: core, and only core
    expect(await b.source.itemForSection(0 as never)).not.toBeUndefined();
    expect(log).toEqual([`${BASE}manifest.json`, `${BASE}${m.files.paint!.url}`, `${BASE}${m.files.core!.url}`]);
    expect(log.filter((u) => DEV_PATH.test(u))).toEqual([]);
    // no tile version folder in the fixture: the map keeps the development endpoints
    expect(releaseTiles(b, DEV_TILES)).toEqual(DEV_TILES);
  });

  it("(T256) a Regulations-first start (paintFirst false) draws before paint: the manifest alone; paint on the map's ask", async () => {
    const fx = fixtureRelease("plain"), log: string[] = [];
    const m = JSON.parse(await (await releaseFetch(BASE, fx.dir, [])(`${BASE}manifest.json`)).text()) as Manifest;
    const stages: string[] = [];
    const b = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "", paintFirst: false,
                                  platform: web, fetch: releaseFetch(BASE, fx.dir, log), onStage: (s) => stages.push(s) });
    expect(stages).toEqual(["manifest"]);
    expect(log).toEqual([`${BASE}manifest.json`]);
    expect(b.paint).toBeNull();
    expect(b.loader.opened()).toEqual([]);
    // the map's status asks for it when shown (`releaseStatus` → `loader.paint()`)
    await b.loader.paint();
    expect(log).toEqual([`${BASE}manifest.json`, `${BASE}${m.files.paint!.url}`]);
  });

  it("(T304) a water page's region focuses the boot BEFORE the first read: the landing's core read is `core-2`, not the whole core", async () => {
    const fx = fixtureRelease("plain");
    // the fixture has no slices: `core-2` here is the whole core re-stamped as a slice (it covers every key it holds)
    const m0 = JSON.parse(readFileSync(join(fx.dir, "manifest.json"), "utf8")) as Manifest;
    const core = m0.files.core!, tmp = join(fx.root, "core-2.sqlite");
    writeFileSync(tmp, gunzipSync(readFileSync(join(fx.dir, "..", core.url))));
    const sq = new DatabaseSync(tmp);
    sq.exec("UPDATE meta SET v = 'core-slice/1' WHERE k = 'format'");
    sq.close();
    const raw = readFileSync(tmp), url = `${m0.data_release}/core-2.sqlite`;
    writeFileSync(join(fx.dir, "..", url), raw);
    const slice = { ...core, url, bytes: raw.length, sha256: sha256Hex(new Uint8Array(raw)), format: "core-slice/1",
                    slice_of: "core", region: "2" };
    const sliced = (log: string[]): typeof fetch => {
      const f = releaseFetch(BASE, fx.dir, log);
      return (async (u: RequestInfo | URL, i?: RequestInit) => {
        if (!String(u).endsWith("manifest.json")) return f(u, i);
        const m = JSON.parse(await (await f(u, i)).text()) as Manifest;
        m.files["core-2" as never] = slice as never;
        return new Response(JSON.stringify(m), { headers: { "content-type": "application/json" } });
      }) as typeof fetch;
    };
    const boot = async (focus: "2" | null) => {
      const names: string[] = [];
      const platform: ReleasePlatform = { ...web, releaseSource: (m, at) => {
        const s = releaseSource(m, at);
        return Object.assign(Object.create(s) as typeof s, { bytes: (n: never, o?: { ahead?: boolean }) => { names.push(n); return s.bytes(n, o); } });
      } };
      const b = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "", paintFirst: false,
                                    ...(focus ? { focus } : {}), platform, fetch: sliced([]) });
      return { b, names };
    };
    const plain = await boot(null);
    const id = String((await (await plain.b.loader.db("core")).get("SELECT item_id FROM item ORDER BY ord LIMIT 1"))!.item_id);
    const READ = "SELECT ord FROM item WHERE item_id = ?";
    const whole = await plain.b.loader.routed("core").get(READ, id);
    expect(plain.names).toEqual(["core"]);   // MUTATION PIN: no focus → the whole core (what a landing read before T304)
    const landed = await boot("2");
    expect(landed.names).toEqual([]);        // focusing reads nothing
    expect(await landed.b.loader.routed("core").get(READ, id)).toEqual(whole);
    expect(landed.names).toEqual(["core-2"]);
  });

  it("a release that names its tiles and mask is drawn from them", async () => {
    const fx = fixtureRelease("plain");
    const b = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                                  platform: web, fetch: releaseFetch(BASE, fx.dir, []) });
    const named = { ...b, manifest: { ...b.manifest, tiles: { ...b.manifest.tiles,
      current: { id: "v1", url: "tiles/v1/atlas-v1.pmtiles", bytes: 1, sha256: "x" },
      mask: { url: "tiles/v1/bc_outside.geojson", bytes: 1, sha256: "y" } } } };
    expect(releaseTiles(named, DEV_TILES)).toEqual({
      atlas: `${BASE}tiles/v1/atlas-v1.pmtiles`, basemap: DEV_TILES.basemap, outside: `${BASE}tiles/v1/bc_outside.geojson`,
      assets: DEV_TILES.assets });
    // the map assets (glyphs + sprites) from the release url on the web
    const assets = { ...named, manifest: { ...named.manifest, tiles: { ...named.manifest.tiles,
      assets: { id: "map-assets-1", url: "tiles/v1/map-assets-1/", files_sha256: "z" } } } };
    expect(releaseTiles(assets, DEV_TILES)).toEqual({
      atlas: `${BASE}tiles/v1/atlas-v1.pmtiles`, basemap: DEV_TILES.basemap, outside: `${BASE}tiles/v1/bc_outside.geojson`,
      assets: `${BASE}tiles/v1/map-assets-1/` });
  });

  it("N2: a release that names its map assets gives the map their folder (glyphs + sprites, no CDN)", async () => {
    const fx = fixtureRelease("plain");
    const b = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                                  platform: web, fetch: releaseFetch(BASE, fx.dir, []) });
    const named = { ...b, manifest: { ...b.manifest, tiles: { ...b.manifest.tiles,
      current: { id: "v1", url: "tiles/v1/atlas-v1.pmtiles", bytes: 1, sha256: "x" },
      assets: { id: "map-assets-1", url: "tiles/v1/map-assets-1/", files_sha256: "z", pin: "p" } } } };
    expect(releaseTiles(named as never, DEV_TILES)).toEqual({
      atlas: `${BASE}tiles/v1/atlas-v1.pmtiles`, basemap: DEV_TILES.basemap, outside: DEV_TILES.outside,
      assets: `${BASE}tiles/v1/map-assets-1/` });
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
function withTiles(fx: FixtureRelease, v: string, assets?: { id: string; body: Record<string, string> }): string {
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
  if (assets) {
    // the map assets folder as assets.py writes it: the files, and assets.json listing their sha256
    const files: Record<string, string> = {};
    for (const [rel, body] of Object.entries(assets.body)) {
      const b = new TextEncoder().encode(body);
      mkdirSync(join(folder, assets.id, rel, ".."), { recursive: true });
      writeFileSync(join(folder, assets.id, rel), b);
      files[rel] = sha256Hex(b);
    }
    writeFileSync(join(folder, assets.id, "assets.json"), JSON.stringify({ id: assets.id, pin: "p", files }));
    m.tiles.assets = { id: assets.id, url: `tiles/${v}/${assets.id}/`, files_sha256: assetsDigest(files), pin: "p" };
  }
  const at = join(fx.root, `manifest-${v}-${assets?.id ?? "none"}`);
  mkdirSync(at, { recursive: true });
  writeFileSync(join(at, "manifest.json"), JSON.stringify(m));
  return at;
}

const tileFiles = (fs: ReturnType<typeof memFs>, v: string) =>
  [...fs.files.keys()].filter((k) => k.startsWith(`release/tiles/${v}/`)).sort();

/**
 * The phone, over an in-memory disk: files are read from the held set, as source.native.ts does. `failOpen`: the next
 * that many SQLite opens throw it (a transient failure: the opener, not the bytes).
 */
function phone(fs: ReturnType<typeof memFs>, disk: ReleaseFs = fs,
               failOpen: { error: Error; times: number } | null = null): ReleasePlatform {
  return {
    releaseOpener: () => {
      const open = nodeOpener();
      return async (name, bytes) => {
        if (failOpen && failOpen.times > 0) { failOpen.times--; throw failOpen.error; }
        return open(name, bytes);
      };
    },
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
  /** A fixture release's data_release id, as its manifest states it. */
  const idOf = async (r: FixtureRelease): Promise<string> =>
    (JSON.parse(await (await releaseFetch(BASE, r.dir, [])(`${BASE}manifest.json`)).text()) as Manifest).data_release;

  it("installs on first run, opens offline, downloads only what changed, and prunes the old set after", async () => {
    const r1 = fixtureRelease("plain");
    // a second release: one lake station more, which changes core (item.gauged) and conditions, not paint
    const r2 = fixtureRelease("lakeGauged");
    const fs = memFs(), log: string[] = [], stages: string[] = [], progress: string[] = [];
    const boot = (f: typeof fetch) => bootRelease({
      manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "", platform: phone(fs), fetch: f,
      onStage: (s) => stages.push(s), onProgress: (p) => progress.push(p.setup ? `setup ${p.setup.step} ${p.setup.done}/${p.setup.of}` : `${p.file} ${p.done}/${p.total}`),
      manifestTimeoutMs: 1000 });

    const first = await boot(releaseFetch(BASE, r1.dir, log));
    expect(stages).toEqual(["manifest", "install", "paint"]);
    // (T272) then the second stage: the set opened in steps (manifest, paint, core)
    expect(progress).toEqual(["access 1/6", "conditions 2/6", "core 3/6", "flow-area 4/6", "near 5/6", "paint 6/6",
                              "setup open 0/3", "setup open 1/3", "setup open 2/3", "setup open 3/3"]);
    expect(log.length).toBe(7);                                       // the manifest + every file, once
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

  it("(T585 F12) a set staged earlier is APPLIED by the cold start that opens it — said once — and web and phone end on one data_release",
    async () => {
      const r1 = fixtureRelease("plain"), r2 = fixtureRelease("lakeGauged");
      const id1 = await idOf(r1), id2 = await idOf(r2);
      expect(id2).not.toBe(id1);
      const fs = memFs();
      // as the app boots: the update is the update service's (`deferUpdate`), staged through `Booted.stage`
      const boot = (f: typeof fetch, keepReplaced?: boolean) => bootRelease({
        manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "", platform: phone(fs), fetch: f,
        manifestTimeoutMs: 1000, deferUpdate: true, ...(keepReplaced ? { keepReplaced } : {}) });
      const said = (b: Awaited<ReturnType<typeof boot>>, generation = 0, before: typeof b.held | null = null) =>
        bootSaid({ generation, booted: b, before });

      // a first install is not an update
      const first = await boot(releaseFetch(BASE, r1.dir, []));
      expect([first.applied, said(first)]).toEqual([null, { kind: "nothing" }]);
      // nor is opening the same set again
      expect((await boot(offline)).applied).toBeNull();

      // the server names r2: the session stays on r1 and stages r2 beside it; the app is then closed before any safe moment
      const session = await boot(releaseFetch(BASE, r2.dir, []));
      expect(session.manifest.data_release).toBe(id1);
      expect(await session.stage(session.latest!)).toMatchObject({ kind: "staged", held: { release: id2 } });

      // THE COLD START opens r2: it is the boot that applied it, and says so (the service is told, the reader toasted)
      const cold = await boot(offline);
      expect(cold.manifest.data_release).toBe(id2);
      expect(cold.applied).toEqual({ from: id1 });
      expect(said(cold)).toEqual({ kind: "applied", release: id2, from: id1 });
      // … once: the next launch on the same set says nothing
      const again = await boot(offline);
      expect([again.manifest.data_release, again.applied, said(again)]).toEqual([id2, null, { kind: "nothing" }]);

      // THE WEB, asked the same manifest, is on the same release: once staging is done the two platforms agree
      const onWeb = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                                        platform: web, fetch: releaseFetch(BASE, r2.dir, []) });
      expect([onWeb.manifest.data_release, cold.manifest.data_release]).toEqual([id2, id2]);
      expect(onWeb.applied).toBeNull();
      // the web's in-session apply is known by the set it held before (it keeps none between visits)
      const before = { ...onWeb.held, release: id1 };
      expect(bootSaid({ generation: 1, booted: onWeb, before })).toEqual({ kind: "applied", release: id2, from: id1 });
      expect(bootSaid({ generation: 1, booted: onWeb, before: onWeb.held })).toEqual({ kind: "nothing" });
    });

  it("(T585 F12) an IN-SESSION apply is said by the boot that made it, and not again by the next cold start", async () => {
    const r1 = fixtureRelease("plain"), r2 = fixtureRelease("lakeGauged");
    const id1 = await idOf(r1), id2 = await idOf(r2);
    const fs = memFs();
    const boot = (f: typeof fetch, keepReplaced?: boolean) => bootRelease({
      manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "", platform: phone(fs), fetch: f,
      manifestTimeoutMs: 1000, deferUpdate: true, ...(keepReplaced ? { keepReplaced } : {}) });
    await boot(releaseFetch(BASE, r1.dir, []));
    const session = await boot(releaseFetch(BASE, r2.dir, []));
    await session.stage(session.latest!);
    // the app's apply (generation 1, the replaced set kept for the map on screen)
    const applied = await boot(releaseFetch(BASE, r2.dir, []), true);
    expect(bootSaid({ generation: 1, booted: applied, before: session.held }))
      .toEqual({ kind: "applied", release: id2, from: id1 });
    // a re-boot onto the set already in use (an apply asked with nothing new) is never "Updated to"
    const same = await boot(releaseFetch(BASE, r2.dir, []), true);
    expect(bootSaid({ generation: 2, booted: same, before: applied.held })).toEqual({ kind: "nothing" });
    // and the next cold start, which prunes what the apply kept, says nothing either
    const cold = await boot(offline);
    expect([cold.manifest.data_release, cold.applied]).toEqual([id2, null]);
  });

  it("a TILE-ONLY release (same data_release) is installed, drawn at the next launch, and the old tiles pruned after",
    async () => {
      const r1 = fixtureRelease("plain");
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

  it("(T418) an update APPLIED IN SESSION keeps the replaced tiles (the map on screen reads them) until the next launch",
    async () => {
      const r1 = fixtureRelease("plain");
      const v1 = tileVersion(r1, "aaaaaaaaaaaa"), v2 = tileVersion(r1, "bbbbbbbbbbbb"), v3 = tileVersion(r1, "cccccccccccc");
      const at1 = withTiles(r1, v1), at2 = withTiles(r1, v2), at3 = withTiles(r1, v3);
      const fs = memFs();
      const boot = (f: typeof fetch, keepReplaced?: boolean) => bootRelease({
        manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "", platform: phone(fs), fetch: f,
        manifestTimeoutMs: 1000, ...(keepReplaced ? { keepReplaced, deferUpdate: true } : {}) });
      await boot(releaseFetch(BASE, at1, []));
      expect(await (await boot(releaseFetch(BASE, at2, []))).update).toMatchObject({ tiles: v2 });

      // the app applies it while v1 is drawn (the 0.5.0 phone, r-4e70 → r-5a59): v2 opens, v1 stays whole (the server
      // already names v3; the update service stages it later — `deferUpdate`, as the app boots)
      const applied = await boot(releaseFetch(BASE, at3, []), true);
      expect(applied.held.tiles).toBe(v2);
      expect(tileFiles(fs, v1)).toEqual([`release/tiles/${v1}/atlas-${v1}.pmtiles`, `release/tiles/${v1}/bc_outside.geojson`]);
      expect(fs.files.has("release/previous")).toBe(true);

      // the next update staged in that session: v2 has been drawn since, so v1 goes first, then v3 is installed
      const staged = await applied.stage(applied.latest!);
      expect(staged).toMatchObject({ kind: "staged", held: { tiles: v3 } });
      expect(tileFiles(fs, v1)).toEqual([]);
      expect(tileFiles(fs, v2)).toHaveLength(2);

      // and the next launch prunes what it replaced, as before (nothing on screen yet)
      const next = await boot(offline);
      expect(next.held.tiles).toBe(v3);
      expect(tileFiles(fs, v2)).toEqual([]);
      expect(fs.files.has("release/previous")).toBe(false);
    });

  it("(T418) an in-session apply's kept set is pruned by the next LAUNCH when nothing else was staged", async () => {
    const r1 = fixtureRelease("plain");
    const v1 = tileVersion(r1, "aaaaaaaaaaaa"), v2 = tileVersion(r1, "bbbbbbbbbbbb");
    const fs = memFs();
    const boot = (f: typeof fetch, keepReplaced?: boolean) => bootRelease({
      manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "", platform: phone(fs), fetch: f,
      manifestTimeoutMs: 1000, ...(keepReplaced ? { keepReplaced } : {}) });
    await boot(releaseFetch(BASE, withTiles(r1, v1), []));
    await (await boot(releaseFetch(BASE, withTiles(r1, v2), []))).update;
    await boot(offline, true);
    expect(tileFiles(fs, v1)).toHaveLength(2);
    const next = await boot(offline);
    expect(next.held.tiles).toBe(v2);
    expect(tileFiles(fs, v1)).toEqual([]);
    expect(fs.files.has("release/previous")).toBe(false);
  });

  const GLYPHS = "fonts/Noto Sans Regular/0-255.pbf", SPRITE = "sprites/v4/light.png";
  const assetFiles = (fs: ReturnType<typeof memFs>, v: string, id: string) =>
    [...fs.files.keys()].filter((k) => k.startsWith(`release/tiles/${v}/${id}/`)).sort();

  it("MAP ASSETS are installed (every file verified) and the map reads its glyphs and sprites from the installed folder",
    async () => {
      const r1 = fixtureRelease("plain");
      const v1 = tileVersion(r1, "aaaaaaaaaaaa");
      const at = withTiles(r1, v1, { id: "map-assets-1", body: { [GLYPHS]: "glyphs", [SPRITE]: "png" } });
      const fs = memFs(), log: string[] = [];
      const first = await bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                                        platform: phone(fs), fetch: releaseFetch(BASE, at, log), manifestTimeoutMs: 1000 });
      expect(first.held.assets).toBe("map-assets-1");
      expect(assetFiles(fs, v1, "map-assets-1")).toEqual([
        `release/tiles/${v1}/map-assets-1/assets.json`,
        `release/tiles/${v1}/map-assets-1/${GLYPHS}`,
        `release/tiles/${v1}/map-assets-1/${SPRITE}`,
      ]);
      expect(new TextDecoder().decode(fs.files.get(`release/tiles/${v1}/map-assets-1/${GLYPHS}`))).toBe("glyphs");
      expect(log.filter((u) => u.includes("map-assets-1"))).toEqual([
        `${BASE}tiles/${v1}/map-assets-1/assets.json`,
        `${BASE}tiles/${v1}/map-assets-1/fonts/Noto%20Sans%20Regular/0-255.pbf`,
        `${BASE}tiles/${v1}/map-assets-1/${SPRITE}`,
      ]);
      expect(releaseTiles(first, DEV_TILES)).toEqual({
        atlas: `file:///doc/release/tiles/${v1}/atlas-${v1}.pmtiles`, basemap: DEV_TILES.basemap,
        outside: `file:///doc/release/tiles/${v1}/bc_outside.geojson`,
        assets: `file:///doc/release/tiles/${v1}/map-assets-1/` });
    });

  it("an ASSETS-ONLY release fetches only the new assets folder, and the old one is pruned at the next launch",
    async () => {
      const r1 = fixtureRelease("plain");
      const v1 = tileVersion(r1, "aaaaaaaaaaaa");
      const at1 = withTiles(r1, v1, { id: "map-assets-1", body: { [GLYPHS]: "glyphs", [SPRITE]: "png" } });
      const at2 = withTiles(r1, v1, { id: "map-assets-2", body: { [GLYPHS]: "glyphs", [SPRITE]: "png 2" } });
      const fs = memFs(), log: string[] = [];
      const boot = (f: typeof fetch) => bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE,
                                                       wasmBase: "", platform: phone(fs), fetch: f,
                                                       manifestTimeoutMs: 1000 });
      await boot(releaseFetch(BASE, at1, []));
      const second = await boot(releaseFetch(BASE, at2, log));
      const next = await second.update;
      expect(next).toMatchObject({ tiles: v1, assets: "map-assets-2" });
      // the unchanged glyphs are copied from the held folder; only the list and the changed sprite are fetched
      expect(log).toEqual([`${BASE}manifest.json`, `${BASE}tiles/${v1}/map-assets-2/assets.json`,
                           `${BASE}tiles/${v1}/map-assets-2/${SPRITE}`]);
      expect(assetFiles(fs, v1, "map-assets-1").length).toBe(3);

      const third = await boot(offline);
      expect(third.held.assets).toBe("map-assets-2");
      expect(releaseTiles(third, DEV_TILES).assets).toBe(`file:///doc/release/tiles/${v1}/map-assets-2/`);
      expect(assetFiles(fs, v1, "map-assets-1")).toEqual([]);
      expect(assetFiles(fs, v1, "map-assets-2")).toEqual([
        `release/tiles/${v1}/map-assets-2/assets.json`,
        `release/tiles/${v1}/map-assets-2/${GLYPHS}`,
        `release/tiles/${v1}/map-assets-2/${SPRITE}`,
      ]);
      expect(tileFiles(fs, v1)).toContain(`release/tiles/${v1}/atlas-${v1}.pmtiles`);
    });

  it("a TILE REBUILD that keeps its assets id still makes the assets folder whole under the new tile version",
    async () => {
      // every tile rebuild is this: new tiles, same map-assets-1, which lives under the tile version's folder
      const r1 = fixtureRelease("plain");
      const v1 = tileVersion(r1, "aaaaaaaaaaaa"), v2 = tileVersion(r1, "bbbbbbbbbbbb");
      const body = { [GLYPHS]: "glyphs", [SPRITE]: "png" };
      const at1 = withTiles(r1, v1, { id: "map-assets-1", body });
      const at2 = withTiles(r1, v2, { id: "map-assets-1", body });
      const fs = memFs(), log: string[] = [];
      const boot = (f: typeof fetch) => bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE,
                                                       wasmBase: "", platform: phone(fs), fetch: f,
                                                       manifestTimeoutMs: 1000 });
      await boot(releaseFetch(BASE, at1, []));
      const second = await boot(releaseFetch(BASE, at2, log));
      const next = await second.update;
      expect(next).toMatchObject({ tiles: v2, assets: "map-assets-1" });
      // the new tile folder has the assets before the swap: the list is fetched, the files copied from the held folder
      expect(assetFiles(fs, v2, "map-assets-1")).toEqual([
        `release/tiles/${v2}/map-assets-1/assets.json`,
        `release/tiles/${v2}/map-assets-1/${GLYPHS}`,
        `release/tiles/${v2}/map-assets-1/${SPRITE}`,
      ]);
      expect(log.filter((u) => u.includes("map-assets-1"))).toEqual([`${BASE}tiles/${v2}/map-assets-1/assets.json`]);

      const third = await boot(offline);
      expect(third.held.tiles).toBe(v2);
      expect(releaseTiles(third, DEV_TILES).assets).toBe(`file:///doc/release/tiles/${v2}/map-assets-1/`);
      expect(new TextDecoder().decode(fs.files.get(`release/tiles/${v2}/map-assets-1/${GLYPHS}`))).toBe("glyphs");
      // the old tile version, its assets with it, is pruned
      expect(assetFiles(fs, v1, "map-assets-1")).toEqual([]);
    });

  it("an update whose assets are not the manifest's is refused before the swap: the held set stays in force",
    async () => {
      const r1 = fixtureRelease("plain");
      const v1 = tileVersion(r1, "aaaaaaaaaaaa");
      const at1 = withTiles(r1, v1, { id: "map-assets-1", body: { [GLYPHS]: "glyphs", [SPRITE]: "png" } });
      const at2 = withTiles(r1, v1, { id: "map-assets-2", body: { [GLYPHS]: "glyphs", [SPRITE]: "png 2" } });
      // served bytes changed after the list was written
      writeFileSync(join(r1.root, "tiles", v1, "map-assets-2", SPRITE), "tampered");
      const fs = memFs();
      const boot = (f: typeof fetch) => bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE,
                                                       wasmBase: "", platform: phone(fs), fetch: f,
                                                       manifestTimeoutMs: 1000 });
      const first = await boot(releaseFetch(BASE, at1, []));
      await expect((await boot(releaseFetch(BASE, at2, []))).update).rejects.toThrow(/map-assets-2\/sprites\/v4\/light\.png: sha256/);
      expect(JSON.parse(new TextDecoder().decode(fs.files.get(POINTER)!))).toEqual(first.held);
    });

  it("a new set that will not open is ROLLED BACK to the one it replaced, said, pruned, and not installed again",
    async () => {
      const r1 = fixtureRelease("plain");
      const r2 = fixtureRelease("lakeGauged");
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
      // a decode failure of verified bytes is a VERIFIED defect: refused for good
      expect(back.rolledBack).toEqual({ refused: m2.data_release, using: first.held.release,
                                        reason: expect.any(String) as string, retry: false });
      expect(JSON.parse(new TextDecoder().decode(fs.files.get(POINTER)!)).release).toBe(first.held.release);
      expect([...fs.files.keys()].some((k) => k.startsWith(`release/${m2.data_release}/`))).toBe(false);
      expect(fs.files.has("release/previous")).toBe(false);
      // (T555 item 1, T585 F12) a rollback applied NOTHING: never "Updated to"; an in-session apply names the refused release
      expect(back.applied).toBeNull();
      expect(bootSaid({ generation: 0, booted: back, before: null })).toEqual({ kind: "nothing" });
      expect(bootSaid({ generation: 1, booted: back, before: first.held }))
        .toEqual({ kind: "not-applied", refused: m2.data_release, reason: back.rolledBack!.reason });
      expect(bootSaid({ generation: 1, booted: back, before: first.held, applyError: new Error("x") })).toEqual({ kind: "nothing" });

      // the next launch opens the old set as its own (nothing more to say), and the refused set is not fetched again
      const log: string[] = [];
      const after = await boot(releaseFetch(BASE, r2.dir, log));
      expect(after.held.release).toBe(first.held.release);
      expect(after.rolledBack).toBeNull();
      expect(await after.update).toBeNull();
      expect(log).toEqual([`${BASE}manifest.json`]);
      // (T555) staging the refused release is a NAMED outcome (never a bare "nothing"); the set in use is "in-use"
      expect(await after.stage(m2)).toEqual({ kind: "refused", release: m2.data_release });
      expect(await after.stage(after.manifest)).toEqual({ kind: "in-use" });
    });

  /** r1 installed, then r2 installed by an update (not yet used): the state a launch that opens r2 starts from. */
  async function updated() {
    const r1 = fixtureRelease("plain");
    const r2 = fixtureRelease("lakeGauged");
    const fs = memFs();
    const boot = (f: typeof fetch, failOpen: { error: Error; times: number } | null = null) => bootRelease({
      manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "", platform: phone(fs, fs, failOpen),
      fetch: f, manifestTimeoutMs: 1000 });
    const first = await boot(releaseFetch(BASE, r1.dir, []));
    const next = (await (await boot(releaseFetch(BASE, r2.dir, []))).update)!;
    const m2 = JSON.parse(readFileSync(join(r2.dir, "manifest.json"), "utf8")) as Manifest;
    return { r2, fs, boot, first, next, m2 };
  }
  const pointerOf = (fs: ReturnType<typeof memFs>) =>
    JSON.parse(new TextDecoder().decode(fs.files.get(POINTER)!)) as { release: string };
  const holds = (fs: ReturnType<typeof memFs>, release: string) =>
    [...fs.files.keys()].some((k) => k.startsWith(`release/${release}/`));

  it("a TRANSIENT failure opening the new set rolls back for this launch only: nothing refused, nothing pruned, retried",
    async () => {
      const { r2, fs, boot, first, m2 } = await updated();
      // the opener fails (not the bytes): this launch goes back to r1, but r2 stays the pointer's, whole, unrefused
      const log: string[] = [];
      const back = await boot(releaseFetch(BASE, r2.dir, log), { error: new Error("database is locked"), times: 1 });
      expect(back.held.release).toBe(first.held.release);
      expect(back.rolledBack).toEqual({ refused: m2.data_release, using: first.held.release,
                                        reason: "database is locked", retry: true });
      expect(pointerOf(fs).release).toBe(m2.data_release);
      expect(fs.files.has("release/refused")).toBe(false);
      expect(fs.files.has("release/previous")).toBe(true);
      expect(holds(fs, m2.data_release)).toBe(true);
      expect(holds(fs, first.held.release!)).toBe(true);
      // the set it will retry is not downloaded again
      expect(await back.update).toBeNull();
      expect(log).toEqual([`${BASE}manifest.json`]);

      // the next launch opens r2 after all, and only then prunes r1
      const again = await boot(offline);
      expect(again.held.release).toBe(m2.data_release);
      expect(again.rolledBack).toBeNull();
      expect(holds(fs, first.held.release!)).toBe(false);
      expect(fs.files.has("release/previous")).toBe(false);
    });

  it("an open failure that is not classed is refused only when the set is PROVEN damaged (its bytes fail their sha)",
    async () => {
      const { fs, boot, first, next, m2 } = await updated();
      // core on disk is no longer the bytes the install verified: the opener fails, and the sha proves why
      fs.files.set(heldPath(next, m2.files.core!.url), new TextEncoder().encode("not a database"));
      const back = await boot(offline);
      expect(back.held.release).toBe(first.held.release);
      expect(back.rolledBack).toMatchObject({ refused: m2.data_release, using: first.held.release, retry: false });
      expect(pointerOf(fs).release).toBe(first.held.release);
      expect(fs.files.has("release/refused")).toBe(true);
      expect(holds(fs, m2.data_release)).toBe(false);
    });

  it("a launch killed right after the swap still prunes the set it replaced (PREVIOUS is written before the swap)",
    async () => {
      const r1 = fixtureRelease("plain");
      const r2 = fixtureRelease("lakeGauged");
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

  /** (T403) The held set made an OLDER APP's: its kept manifest names a format this app does not read (salmon/3 then). */
  const makeOld = async (fs: ReturnType<typeof memFs>, h: { manifest?: string }) => {
    const path = h.manifest!;
    const m = JSON.parse(new TextDecoder().decode(await fs.read(path))) as Manifest;
    m.files.access = { ...m.files.access!, format: "access/0" };
    await fs.write(path, new TextEncoder().encode(JSON.stringify(m)));
  };

  it("(T403) a held set this app cannot read is never opened: the manifest's set is installed over it first, then pruned",
    async () => {
      const r1 = fixtureRelease("plain"), r2 = fixtureRelease("lakeGauged");
      const fs = memFs(), stages: string[] = [];
      const boot = (f: typeof fetch) => bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                                                       platform: phone(fs), fetch: f, manifestTimeoutMs: 1000,
                                                       onStage: (s) => stages.push(s) });
      const first = await boot(releaseFetch(BASE, r1.dir, []));
      await makeOld(fs, first.held);
      // offline: nothing this app reads is on the phone, so the start fails and says it needs a connection
      await expect(boot(offline)).rejects.toMatchObject({ firstRunOffline: true, message: expect.stringContaining("access is access/0") });
      // online: installed BEFORE the map, as a first run is (not in the background for the next launch)
      stages.length = 0;
      const b = await boot(releaseFetch(BASE, r2.dir, []));
      expect(stages).toEqual(["manifest", "install", "paint"]);
      expect(b.held.release).not.toBe(first.held.release);
      expect(b.rolledBack).toBeNull();
      expect(await b.update).toBeNull();
      expect(await b.source.water("gnis:8634" as ItemId)).not.toBeNull();
      // the old set is gone once the new one has opened
      expect([...fs.files.keys()].some((k) => k.startsWith(`release/${first.held.release}/`))).toBe(false);
      expect(fs.files.has("release/previous")).toBe(false);
    });

  it("(T403) a manifest this app cannot read is never installed: a first run fails saying why, an update is refused",
    async () => {
      const r1 = fixtureRelease("plain"), r2 = fixtureRelease("lakeGauged");
      const m2 = JSON.parse(readFileSync(join(r2.dir, "manifest.json"), "utf8")) as Manifest;
      m2.files.salmon = { ...m2.files.core!, format: "salmon/3" };
      const bad = join(r2.root, "manifest-salmon3");
      mkdirSync(bad, { recursive: true });
      writeFileSync(join(bad, "manifest.json"), JSON.stringify(m2));
      const at = (dir: string) => releaseFetch(BASE, dir, []);
      const boot = (fs: ReturnType<typeof memFs>, f: typeof fetch) => bootRelease({
        manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "", platform: phone(fs), fetch: f,
        manifestTimeoutMs: 1000 });
      await expect(boot(memFs(), at(bad))).rejects.toThrow("salmon is salmon/3, not salmon/4");
      const fs = memFs();
      const first = await boot(fs, at(r1.dir));
      const second = await boot(fs, at(bad));
      expect(second.held).toEqual(first.held);
      await expect(second.update).rejects.toThrow("salmon is salmon/3");
      // (T555) an outcome, not a throw: the update service names it (an old app meeting a release it cannot read)
      await expect(second.stage(m2)).resolves.toMatchObject({ kind: "unreadable", reason: expect.stringContaining("is not one this app reads") });
    });

  it("a first run with no network says so, rather than opening nothing", async () => {
    await expect(bootRelease({ manifestUrl: `${BASE}manifest.json`, releaseBase: BASE, wasmBase: "",
                               platform: phone(memFs()), fetch: offline, manifestTimeoutMs: 1000 }))
      .rejects.toMatchObject({ firstRunOffline: true });
  });
});

const LIVE = namedRelease();   // (T667) the release this Tier 2 run named ($RELEASE_DIR), through the one helper
describe.skipIf(!LIVE || !existsSync(LIVE!))("the Vedder on the live release (Tier 2: RELEASE_DIR)", () => {
  it("the live manifest's assets digest is the one the app computes over the folder's assets.json", () => {
    const m = JSON.parse(readFileSync(join(LIVE!, "manifest.json"), "utf8")) as Manifest;
    const a = m.tiles.assets;
    if (!a) return;                                                      // a release that names no assets yet
    const list = JSON.parse(readFileSync(join(LIVE!, "..", a.url, "assets.json"), "utf8")) as
      { files: Record<string, string> };
    expect(assetsDigest(list.files)).toBe(a.files_sha256);
  });

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
