/**
 * T193-T198 — THE BUILD: a release folder (or the committed fixture release) in, a static site out. `tools/build-seo.mjs` is
 * the command line; the test calls `buildAll` and `writeSite` over the 43-water fixture release into a gitignored dist folder,
 * never a full province build.
 *
 * (T269) A WATER PAGE IS THE APP: the web export's own index.html (`--app`, the export built for this release and host) with
 * the water's regs pane prerendered by the app's components (pane.tsx), handed to the app once its own pane is up.
 *
 *   node app/tools/build-seo.mjs --release-dir <folder> --app <web export> [--out app/dist/seo]
 *                                [--slug-map app/seo/slug-map.json --update-slugs] [--previous <snapshot.json>] [--date YYYY-MM-DD]
 *   node app/tools/build-seo.mjs --fixture [--app <web export>]
 *   node app/tools/build-seo.mjs --release-dir <folder> --snapshot-only   (no pages: only what the NEXT release is diffed against)
 *   node app/tools/build-seo.mjs --release-dir <folder> --snapshot-only --keep-pages   (T490: upgrade a kept snapshot in place)
 *
 * The snapshot (each water's rules hash + each page's lastmod) is kept between releases in app/dist/seo-snapshots/
 * <release>.json; a build without `--previous` diffs against the newest kept snapshot of ANOTHER release (the /changes page
 * and sitemap lastmod need it). `--snapshot-only` writes one for a release that was never built (its pages map is empty, so
 * no lastmod is carried from it).
 */
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, readdirSync, renameSync, rmSync, statSync, writeFileSync, existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { openRegsStore } from "@app/data/regs";
import { openRegsFixture, openReleaseDir, regsFixtureDir } from "@app/data/release/node";
import { bcNow } from "@app/core";
import type { ItemId } from "@app/data";
import { makeReleaseSource } from "@app/data/release";
import type { AdiposeImages, FishImages } from "@app/ui-native";
import { readAppExport, type AppExport } from "./appExport";
import { webFonts } from "./webFonts";
import { BUDGET, checkBudget, lighthouseConfig, measure, type Measured } from "./budget";
import { FACES_REQUIRED, fontFaceCss, paneCss as paneCss_, PaneRenderer, type PaneCssParts } from "./pane";
import { PanePool } from "./panePool";
import type { WaterModel } from "./model";
import { appDocument } from "./prerender";
import { drawSharePngs } from "./share";
import { withFavicon } from "./favicon";
import { assetLinksFile, headersFile, hostRules, hostWorker, redirectsFile } from "./hosting";
import type { Site } from "./html";
import { legacyRedirects, legacyWorker, type Crosswalk, type LegacyBuilt } from "./legacy";
import { buildDayOf, type BuildDay } from "./model";
import { buildSite, changesOf, rulesSha, type Snapshot, type SiteBuild, type WaterMade } from "./site";
import { listWaterOrds, rulesKey } from "./model";
import { gzipSync } from "node:zlib";
import { EMPTY_SLUG_MAP, type SlugMap } from "./slug";
import { readGaugeRows, type GaugeRows } from "./gauges";

const APP = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
export const DEFAULT_OUT = join(APP, "dist/seo");
export const SLUG_MAP_FILE = join(APP, "seo/slug-map.json");
export const CROSSWALK_FILE = join(APP, "seo/legacy-crosswalk.json");
const SNAPSHOT_DIR = join(APP, "dist/seo-snapshots");

/** The newest kept snapshot of a release other than `release` (by its as-of date, then file time), or null. */
function previousSnapshot(dir: string, release: string): string | null {
  if (!existsSync(dir)) return null;
  const kept = readdirSync(dir).filter((f) => f.endsWith(".json") && f !== `${release}.json`).map((f) => {
    const file = join(dir, f), snap = JSON.parse(readFileSync(file, "utf8")) as Snapshot;
    return { file, asOf: snap.asOf, t: statSync(file).mtimeMs };
  });
  kept.sort((a, b) => b.asOf.localeCompare(a.asOf) || b.t - a.t);
  return kept[0]?.file ?? null;
}

export interface Config extends Site { readonly alsoHosts: readonly string[] }
export const readConfig = (): Config => JSON.parse(readFileSync(join(APP, "seo/site.json"), "utf8")) as Config;

export interface BuildArgs {
  readonly loader: ReturnType<typeof openRegsFixture>;
  readonly config?: Config; readonly on: BuildDay;
  readonly slugMap?: SlugMap; readonly previous?: Snapshot | null;
  /** the v1 site's indexed paths and the items that hold them (legacy-crosswalk.json); none = no v1 redirects */
  readonly crosswalk?: Crosswalk;
  readonly hubSplitAt?: number;
  /** (T269) the app the water pages are: its web export (template, faces, pictures); absent → a bare page (tests) */
  readonly app?: AppExport;
  readonly images?: { readonly fish: FishImages; readonly adipose: AdiposeImages };
  /**
   * (T279) the render threads: at most 4 (heavy jobs), each writing its pages compressed under `out` (`compressed.ts`);
   * absent → rendered here, kept in `files` (a test's few)
   */
  readonly threads?: { readonly n: number; readonly releaseDir: string; readonly appDir: string; readonly out: string };
  readonly only?: ReadonlySet<string>;
  /** no water page is rendered (a stub stands in): for `--snapshot-only`, whose output is the snapshot alone */
  readonly bare?: boolean;
  /** (T487) the gauges, given (a test over the regs fixture, which carries no conditions file); absent: read from the release */
  readonly gauges?: GaugeRows | null;
}
/** The app document a test builds into when no web export is given: the export's shape, no script. */
export const BARE_APP = `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8" /><title>canifishthis</title>` +
  `<style id="expo-reset">html,body{height:100%}body{overflow:hidden}#root{display:flex;height:100%;flex:1}</style></head>` +
  `<body><noscript>You need to enable JavaScript to run this app.</noscript><div id="root"></div></body></html>`;
const sha = (s: string) => createHash("sha256").update(s).digest("hex").slice(0, 12);
/** bumped when the pane stylesheet's makeup changes without its inputs changing */
const PANE_CSS_VERSION = 2;   // 2 (T409): the faces as WOFF2
/** (T279) the water pages as stored: raw, gzip and brotli totals (the upload is the two compressed sets) */
function compressedTotals(b: SiteBuild): { pages: number; rawBytes: number; gzipBytes: number; brBytes: number } {
  let pages = 0, rawBytes = 0, gzipBytes = 0, brBytes = 0;
  for (const p of b.pages) {
    if (p.kind !== "water") continue;
    const s = p.size as typeof p.size & { rawBytes?: number; brBytes?: number };
    pages++; rawBytes += s.rawBytes ?? 0; gzipBytes += s.gzipBytes; brBytes += s.brBytes ?? 0;
  }
  return { pages, rawBytes, gzipBytes, brBytes };
}
export interface Built {
  readonly legacy: LegacyBuilt | null; readonly build: SiteBuild; readonly files: ReadonlyMap<string, string>;
  /** (T406) the binary files: each water's share card, `share/<slug>.png` */
  readonly binary: ReadonlyMap<string, Uint8Array>;
  readonly measured: Measured; readonly problems: readonly string[];
}

/** Everything the site is, in memory: pages, sitemap, robots, hosting config, the budget and its Lighthouse config. */
export async function buildAll(a: BuildArgs): Promise<Built> {
  const config = a.config ?? readConfig();
  const store = await openRegsStore(a.loader);
  // the water pages: the app's document, its pane prerendered; one stylesheet for all of them, named before any is made
  // (a page is written as soon as it is made) by what decides its content: the release, the app, the day
  const template = a.app?.template ?? BARE_APP;
  // (T409) the faces as WOFF2 at /assets/fonts/ (webFonts.ts), checked against the app's own TTF
  const web = a.app ? webFonts(a.app, FACES_REQUIRED) : null;
  const fonts = fontFaceCss((f) => (a.app?.font(f) ? web!.url(f) : null));
  const paneHref = `/assets/pane.${sha(`${PANE_CSS_VERSION}|${store.meta.release}|${template}|${a.on.iso}`)}.css`;
  const preload = ["Archivo_400Regular", "BricolageGrotesque_800ExtraBold"].map((f) => web?.url(f)).filter((u): u is string => !!u);
  const doc = { critical: PaneRenderer.critical(), cssHref: paneHref, preload };
  let waterPage: (m: WaterModel, head: string, file: string) => Promise<WaterMade>;
  let cssParts: () => Promise<PaneCssParts[]>;
  let pool: PanePool | null = null;
  if (a.bare) {
    waterPage = async (m) => ({ html: null, size: { path: m.id, gzipBytes: 0, blocking: false, critical: "" }, problems: [] });
    cssParts = async () => [];
  } else if (a.threads) {
    pool = new PanePool(Math.min(4, Math.max(1, a.threads.n)), { ...a.threads, origin: config.origin, doc });
    waterPage = (m, head, file) => pool!.page(m.id, m.focus, head, file);
    cssParts = () => pool!.css();
  } else {
    const panes = new PaneRenderer({ source: makeReleaseSource(a.loader), loader: a.loader,
                                     ...(a.images ? { fishImages: a.images.fish, adiposeImages: a.images.adipose } : {}) });
    // one render at a time: a render owns the hooks' answers (site.ts asks for every page at once)
    let chain: Promise<unknown> = Promise.resolve();
    waterPage = (m, head) => {
      const p = chain.then(async () => ({ html: appDocument(template, { ...doc, head, item: m.id, region: m.focus, pane: await panes.water(m.id as ItemId) }) }));
      chain = p.catch(() => {});
      return p;
    };
    cssParts = async () => [panes.parts()];
  }
  let build: SiteBuild, parts: PaneCssParts[];
  // (T487) the gauges and the waters each serves (null: a release with no conditions file, the regs fixture)
  const gaugeRows = a.gauges !== undefined ? a.gauges : await readGaugeRows(a.loader);
  try {
    build = await buildSite({
      store, site: config, release: { id: store.meta.release, validUntil: a.loader.manifest.regulations.valid_until },
      on: a.on, slugMap: a.slugMap ?? EMPTY_SLUG_MAP, previous: a.previous ?? null, ...(a.hubSplitAt ? { hubSplitAt: a.hubSplitAt } : {}),
      waterPage, ...(a.only ? { only: a.only } : {}), gauges: gaugeRows,
    });
    parts = await cssParts();
  } finally { await pool?.close(); }
  const files = new Map(build.files);
  // (T406) each water's card drawn as the PNG its head names; its SVG source is not shipped
  const binary = new Map<string, Uint8Array>(), toDraw: [string, string][] = [];
  for (const [name, svg] of build.files) {
    if (!/^share\/[^/]+\.svg$/.test(name)) continue;
    files.delete(name);
    if (!a.bare) toDraw.push([name.replace(/\.svg$/, ".png"), svg]);
  }
  // drawn with a turn between cards (share.ts: a synchronous loop held every card's native pixmap, 59 GB)
  await drawSharePngs(toDraw, (name, png) => binary.set(name, png));
  for (const [name, bytes] of web?.files ?? []) binary.set(name, bytes);
  const paneCss = paneCss_(fonts.css, parts);
  const unmerged = parts.flatMap((p) => p.unmerged);
  files.set(paneHref.slice(1), paneCss);
  files.set("_headers", headersFile());
  files.set(".well-known/assetlinks.json", assetLinksFile());
  withFavicon(files);
  files.set("_redirects", redirectsFile(build.removed, build.regions));
  files.set("redirects.json", `${JSON.stringify(hostRules(config, config.alsoHosts), null, 1)}\n`);
  files.set("host-worker.js", hostWorker(config, build.removed, build.regions));
  let legacy: LegacyBuilt | null = null;
  if (a.crosswalk) {
    const pathOfItem = new Map(build.models.map((m) => [m.id, `/water/${build.slugMap.waters[m.id]!.slug}`]));
    legacy = legacyRedirects(a.crosswalk, pathOfItem, new Set(build.regions));
    files.set("legacy-redirects.json", `${JSON.stringify(legacy.map)}\n`);
    files.set("_worker.js", legacyWorker(legacy.map, undefined, Object.fromEntries(build.gauges.map((g) => [g.station, g.path]))));
  }
  const sizes = build.pages.map((p) => p.size);
  const measured = measure(sizes, [files.get(build.cssHref.slice(1))!, paneCss]);
  const problems = checkBudget(measured, sizes);
  const cards = [...binary].filter(([n]) => n.startsWith("share/")).map(([, b]) => b.length);
  const share = { files: cards.length, bytes: cards.reduce((x, y) => x + y, 0), largest: Math.max(0, ...cards) };
  if (share.largest > BUDGET.sharePngBytes) problems.push(`a share card is ${share.largest} B, over ${BUDGET.sharePngBytes}`);
  if (web) problems.push(...web.problems);
  if (gaugeRows && !a.only && !build.gauges.length) problems.push("the release's conditions file names no gauge that serves a water with a page (no /gauge pages)");
  const noTtf = a.app ? FACES_REQUIRED.filter((f) => !a.app!.font(f)) : [];
  if (noTtf.length) problems.push(`the app's web export has no file for the face(s) ${noTtf.join(", ")}`);
  if (unmerged.length) problems.push(`${unmerged.length} pane(s) whose light and dark renders differ beyond colour, shipped twice: ${unmerged.slice(0, 5).join(", ")}`);
  // the page count, the sitemap's count and the bundle's waters with rules are one number
  const listed = [...files.get("sitemap-1.xml")!.matchAll(/<loc>/g)].length - 1;   // minus the app's own "/"
  if (listed !== build.pages.length) problems.push(`sitemap lists ${listed} pages, ${build.pages.length} were built`);
  const waters = build.pages.filter((p) => p.kind === "water").length;
  if (waters !== build.models.length) problems.push(`${waters} water pages for ${build.models.length} waters with rules`);
  const sample = ["/about", ...build.pages.filter((p) => p.kind === "region").slice(0, 1).map((p) => p.path),
                  ...build.pages.filter((p) => p.kind === "water").slice(0, 3).map((p) => p.path)];
  files.set("lighthouserc.json", `${JSON.stringify(lighthouseConfig(sample.map((p) => `${p.slice(1)}/index.html`)), null, 1)}\n`);
  // uniqueness and length report: what Search Console's duplicate / "title too long" findings would otherwise tell us later
  const titles = build.pages.map((p) => p.title), descs = build.pages.map((p) => p.description);
  const quality = {
    pages: build.pages.length,
    uniqueTitles: new Set(titles).size, uniqueDescriptions: new Set(descs).size,
    titlesOver60: titles.filter((t) => t.length > 60).length, descriptionsOver155: descs.filter((t) => t.length > 155).length,
    emptyTitleOrDescription: titles.filter((t) => !t).length + descs.filter((t) => !t).length,
  };
  if (quality.uniqueTitles !== quality.pages || quality.uniqueDescriptions !== quality.pages) problems.push(`titles/descriptions are not unique: ${JSON.stringify(quality)}`);
  files.set("budget.json", `${JSON.stringify({ budget: BUDGET, measured, quality, share, themeVariables: new Set(parts.flatMap((p) => p.vars)).size,
    ...(a.threads ? { compressed: compressedTotals(build) } : {}), problems }, null, 1)}\n`);
  return { build, files, binary, measured, problems, legacy };
}

const outDir = (out: string): string => {
  const abs = resolve(out);
  if (!/[\\/]dist[\\/]/.test(`${abs}/`)) throw new Error(`seo: refusing to empty ${abs}: the output folder must be inside a dist folder`);
  return abs;
};
const put = (root: string, name: string, text: string | Uint8Array) => {
  const to = join(root, name);
  mkdirSync(dirname(to), { recursive: true });
  writeFileSync(to, text);
};

/** Write `files` under `out`, which is emptied first and must sit in a `dist` folder (it is deleted). */
export function writeSite(out: string, files: ReadonlyMap<string, string>, binary: ReadonlyMap<string, Uint8Array> = new Map()): void {
  const abs = outDir(out);
  rmSync(abs, { recursive: true, force: true });
  for (const [name, text] of files) put(abs, name, text);
  for (const [name, bytes] of binary) put(abs, name, bytes);
}

const opt = (args: readonly string[], k: string): string | null => { const i = args.indexOf(k); return i >= 0 ? (args[i + 1] ?? null) : null; };

/**
 * The command line: build, gate, write. Throws on a failed gate. Called by tools/build-seo.mjs through vite-node (a dynamic
 * load knip cannot follow). The water pages are written as they are made, into `<out>.partial`, which becomes `<out>` only
 * when every gate has passed — a failed build never leaves a half site where the deploy looks.
 * @public
 */
export async function main(args: readonly string[]): Promise<void> {
  const dir = opt(args, "--release-dir"), fixture = args.includes("--fixture"), appDir = opt(args, "--app");
  if (dir && args.includes("--snapshot-only")) return snapshotOnly(dir, args);
  if (dir && args.includes("--check-changes")) return void (await checkChanges(dir, opt(args, "--previous")));
  if ((!dir && !fixture) || (dir && !appDir)) throw new Error("usage: build-seo.mjs --release-dir <folder> --app <web export> | --fixture [--app <web export>]  [--out DIR] [--slug-map FILE --update-slugs] [--previous snapshot.json] [--date YYYY-MM-DD]");
  const loader = fixture ? openRegsFixture() : openReleaseDir(resolve(dir!));
  const date = opt(args, "--date"), now = date ? new Date(`${date}T12:00:00`) : bcNow();   // (T551) the B.C. day, not the build machine's
  const slugFile = resolve(opt(args, "--slug-map") ?? SLUG_MAP_FILE);
  const prevFile = opt(args, "--previous") ?? (fixture ? null : previousSnapshot(SNAPSHOT_DIR, loader.release));
  // (T529) the /changes budget, checked in seconds before the ~15 min page build
  if (dir && prevFile) await checkChanges(dir, prevFile);
  const app = appDir ? readAppExport(appDir) : undefined;
  const out = outDir(opt(args, "--out") ?? DEFAULT_OUT), partial = `${out}.partial`;
  rmSync(partial, { recursive: true, force: true });
  const started = Date.now();
  // (T279) with the app's export, the water pages are rendered on threads (at most 4) and written compressed into `partial`
  const threads = app ? { n: Number(opt(args, "--threads") ?? 4), releaseDir: dir ? resolve(dir) : regsFixtureDir(), appDir: resolve(appDir!), out: partial } : undefined;
  const built = await buildAll({
    loader, on: buildDayOf(now),
    slugMap: existsSync(slugFile) ? (JSON.parse(readFileSync(slugFile, "utf8")) as SlugMap) : EMPTY_SLUG_MAP,
    ...(existsSync(CROSSWALK_FILE) ? { crosswalk: JSON.parse(readFileSync(CROSSWALK_FILE, "utf8")) as Crosswalk } : {}),
    previous: prevFile ? (JSON.parse(readFileSync(resolve(prevFile), "utf8")) as Snapshot) : null,
    ...(app ? { app } : {}),
    // a sample to look at (`--only gnis:1,gnis:2`): never a site to publish (the slug map is not updated from it)
    ...(opt(args, "--only") ? { only: new Set(opt(args, "--only")!.split(",")) } : {}),
    ...(threads ? { threads } : {}),
  });
  if (built.problems.length) throw new Error(`seo: ${built.problems.length} budget/consistency problem(s):\n${built.problems.join("\n")}`);
  for (const [name, text] of built.files) put(partial, name, text);
  for (const [name, bytes] of built.binary) put(partial, name, bytes);
  rmSync(out, { recursive: true, force: true });
  renameSync(partial, out);
  if (args.includes("--update-slugs") && !args.includes("--only")) writeFileSync(slugFile, built.files.get("slug-map.json")!);
  // kept for the next release's diff (a sample build is never kept)
  if (!fixture && !args.includes("--only")) put(SNAPSHOT_DIR, `${built.build.snapshot.release}.json`, built.files.get("snapshot.json")!);
  console.log(`seo: ${built.build.pages.length} pages (${built.build.models.length} waters), largest ${built.measured.largest.gzipBytes} B gzip, ` +
    `p95 ${built.measured.p95GzipBytes} B gzip, css ${built.measured.cssGzipBytes} B gzip, ${Math.round((Date.now() - started) / 1000)} s` +
    (threads ? ` on ${Math.min(4, threads.n)} threads; stored ${JSON.stringify(JSON.parse(built.files.get("budget.json")!).compressed)}` : "") +
    `; diffed against ${prevFile ?? "no previous snapshot"}` +
    `; slug steps ${JSON.stringify(built.build.slugSteps)}` + (built.legacy ? `; v1 redirects ${JSON.stringify(built.legacy.counts)}` : ""));
}

/**
 * (T529, fast-fail) `--check-changes`: the /changes page's water list against the previous snapshot, from the rules keys alone
 * (seconds), measured as the page lists it (its rows, gzip) plus a page shell, and refused over the HTML budget — caught
 * 2026-11-09: T517's re-cut made /changes 180 KB gzip, found only after a 15 min SEO build. Never trimmed to fit: an honest
 * list that is too big is a question for the owner (how /changes summarises), not a silent cut.
 */
const CHANGES_SHELL_GZIP = 6 * 1024;
async function checkChanges(dir: string, prevArg: string | null): Promise<void> {
  const loader = openReleaseDir(resolve(dir)), store = await openRegsStore(loader);
  const prevFile = prevArg ?? previousSnapshot(SNAPSHOT_DIR, loader.release);
  if (!prevFile) { console.log(`seo: --check-changes: no kept snapshot of another release; /changes is not built`); return; }
  const prev = JSON.parse(readFileSync(resolve(prevFile), "utf8")) as Snapshot;
  if (Object.values(prev.waters).some((w) => typeof w.rules !== "string"))
    throw new Error(`seo: the previous snapshot (${prev.release}) has no rules hash (made before T490); rewrite it with --snapshot-only --keep-pages`);
  const ids: string[] = [], names = new Map<string, string>(), rules = new Map<string, string>();
  for (const ord of await listWaterOrds(store)) {
    const it = await store.item(ord);
    ids.push(it.id); names.set(it.id, it.name); rules.set(it.id, rulesSha(await rulesKey(store, ord)));
  }
  const { added, changed, gone } = changesOf(prev.waters, ids, rules);
  const li = (id: string, name: string) => `<li><a href="/water/${id}">${name}</a></li>`;
  const rows = [...changed].map((id) => li(id, names.get(id)!)).join("") + [...added].map((id) => li(id, names.get(id)!)).join("") +
    gone.map((id) => `<li>${prev.waters[id]!.name}</li>`).join("");
  const est = gzipSync(rows).length + CHANGES_SHELL_GZIP;
  const said = `${changed.size} changed, ${added.size} new, ${gone.length} gone vs ${prev.release}; /changes ≈ ${est} B gzip (budget ${BUDGET.htmlGzipBytes})`;
  if (est > BUDGET.htmlGzipBytes) {
    const sample = [...changed].slice(0, 8).map((id) => names.get(id)).join(", ");
    throw new Error(`seo: --check-changes: ${said} — over budget. The list is the honest one (rules key: what applies where); ` +
      `ask the owner how /changes should summarise, never trim it. First changed: ${sample}`);
  }
  console.log(`seo: --check-changes: ${said} — ok`);
}

/** `--snapshot-only`: the snapshot of a release no page was built for (rules hashes; no page lastmods), kept for the next diff. */
async function snapshotOnly(dir: string, args: readonly string[]): Promise<void> {
  const date = opt(args, "--date"), now = date ? new Date(`${date}T12:00:00`) : bcNow();   // (T551) the B.C. day, not the build machine's
  const slugFile = resolve(opt(args, "--slug-map") ?? SLUG_MAP_FILE);
  const built = await buildAll({ loader: openReleaseDir(resolve(dir)), on: buildDayOf(now), bare: true,
    slugMap: existsSync(slugFile) ? (JSON.parse(readFileSync(slugFile, "utf8")) as SlugMap) : EMPTY_SLUG_MAP });
  // (T490) `--keep-pages`: rewrite a kept snapshot with today's water hashes, carrying its as-of day and its pages' lastmods (an older snapshot
  // format, upgraded without losing when each page last moved)
  const kept = join(SNAPSHOT_DIR, `${built.build.snapshot.release}.json`);
  if (args.includes("--keep-pages") && !existsSync(kept)) throw new Error(`seo: --keep-pages: no kept snapshot ${kept}`);
  const was = args.includes("--keep-pages") ? (JSON.parse(readFileSync(kept, "utf8")) as Snapshot) : null;
  const pages = was?.pages ?? {};
  const snap = { ...built.build.snapshot, ...(was ? { asOf: was.asOf } : {}), pages };
  put(SNAPSHOT_DIR, `${snap.release}.json`, `${JSON.stringify(snap, null, 1)}\n`);
  console.log(`seo: snapshot of ${snap.release} (${Object.keys(snap.waters).length} waters, ${Object.keys(pages).length} pages kept) -> ${join(SNAPSHOT_DIR, `${snap.release}.json`)}`);
}
