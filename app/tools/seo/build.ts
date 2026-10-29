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
 */
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, renameSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { openRegsStore } from "@app/data/regs";
import { openRegsFixture, openReleaseDir, regsFixtureDir } from "@app/data/release/node";
import type { ItemId } from "@app/data";
import { makeReleaseSource } from "@app/data/release";
import type { AdiposeImages, FishImages } from "@app/ui-native";
import { readAppExport, type AppExport } from "./appExport";
import { BUDGET, checkBudget, lighthouseConfig, measure, type Measured } from "./budget";
import { fontFaceCss, paneCss as paneCss_, PaneRenderer, type PaneCssParts } from "./pane";
import { PanePool } from "./panePool";
import type { WaterModel } from "./model";
import { appDocument } from "./prerender";
import { headersFile, hostRules, redirectsFile } from "./hosting";
import type { Site } from "./html";
import { legacyRedirects, legacyWorker, type Crosswalk, type LegacyBuilt } from "./legacy";
import { buildDayOf, type BuildDay } from "./model";
import { buildSite, type Snapshot, type SiteBuild, type WaterMade } from "./site";
import { EMPTY_SLUG_MAP, type SlugMap } from "./slug";

const APP = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
export const DEFAULT_OUT = join(APP, "dist/seo");
export const SLUG_MAP_FILE = join(APP, "seo/slug-map.json");
export const CROSSWALK_FILE = join(APP, "seo/legacy-crosswalk.json");

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
}
/** The app document a test builds into when no web export is given: the export's shape, no script. */
export const BARE_APP = `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8" /><title>canifishthis</title>` +
  `<style id="expo-reset">html,body{height:100%}body{overflow:hidden}#root{display:flex;height:100%;flex:1}</style></head>` +
  `<body><noscript>You need to enable JavaScript to run this app.</noscript><div id="root"></div></body></html>`;
const sha = (s: string) => createHash("sha256").update(s).digest("hex").slice(0, 12);
/** bumped when the pane stylesheet's makeup changes without its inputs changing */
const PANE_CSS_VERSION = 1;
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
export interface Built { readonly legacy: LegacyBuilt | null; readonly build: SiteBuild; readonly files: ReadonlyMap<string, string>; readonly measured: Measured; readonly problems: readonly string[] }

/** Everything the site is, in memory: pages, sitemap, robots, hosting config, the budget and its Lighthouse config. */
export async function buildAll(a: BuildArgs): Promise<Built> {
  const config = a.config ?? readConfig();
  const store = await openRegsStore(a.loader);
  // the water pages: the app's document, its pane prerendered; one stylesheet for all of them, named before any is made
  // (a page is written as soon as it is made) by what decides its content: the release, the app, the day
  const template = a.app?.template ?? BARE_APP;
  const fonts = fontFaceCss((f) => a.app?.font(f) ?? null);
  const paneHref = `/assets/pane.${sha(`${PANE_CSS_VERSION}|${store.meta.release}|${template}|${a.on.iso}`)}.css`;
  const preload = ["Archivo_400Regular", "BricolageGrotesque_800ExtraBold"].map((f) => a.app?.font(f)).filter((u): u is string => !!u);
  const doc = { critical: PaneRenderer.critical(), cssHref: paneHref, preload };
  let waterPage: (m: WaterModel, head: string, file: string) => Promise<WaterMade>;
  let cssParts: () => Promise<PaneCssParts[]>;
  let pool: PanePool | null = null;
  if (a.threads) {
    pool = new PanePool(Math.min(4, Math.max(1, a.threads.n)), { ...a.threads, origin: config.origin, doc });
    waterPage = (m, head, file) => pool!.page(m.id, head, file);
    cssParts = () => pool!.css();
  } else {
    const panes = new PaneRenderer({ source: makeReleaseSource(a.loader), loader: a.loader,
                                     ...(a.images ? { fishImages: a.images.fish, adiposeImages: a.images.adipose } : {}) });
    // one render at a time: a render owns the hooks' answers (site.ts asks for every page at once)
    let chain: Promise<unknown> = Promise.resolve();
    waterPage = (m, head) => {
      const p = chain.then(async () => ({ html: appDocument(template, { ...doc, head, item: m.id, pane: await panes.water(m.id as ItemId) }) }));
      chain = p.catch(() => {});
      return p;
    };
    cssParts = async () => [panes.parts()];
  }
  let build: SiteBuild, parts: PaneCssParts[];
  try {
    build = await buildSite({
      store, site: config, release: { id: store.meta.release, validUntil: a.loader.manifest.regulations.valid_until },
      on: a.on, slugMap: a.slugMap ?? EMPTY_SLUG_MAP, previous: a.previous ?? null, ...(a.hubSplitAt ? { hubSplitAt: a.hubSplitAt } : {}),
      waterPage, ...(a.only ? { only: a.only } : {}),
    });
    parts = await cssParts();
  } finally { await pool?.close(); }
  const files = new Map(build.files);
  const paneCss = paneCss_(fonts.css, parts);
  const unmerged = parts.flatMap((p) => p.unmerged);
  files.set(paneHref.slice(1), paneCss);
  files.set("_headers", headersFile());
  files.set("_redirects", redirectsFile(build.removed));
  files.set("redirects.json", `${JSON.stringify(hostRules(config, config.alsoHosts), null, 1)}\n`);
  let legacy: LegacyBuilt | null = null;
  if (a.crosswalk) {
    const pathOfItem = new Map(build.models.map((m) => [m.id, `/water/${build.slugMap.waters[m.id]!.slug}`]));
    legacy = legacyRedirects(a.crosswalk, pathOfItem, new Set(build.regions));
    files.set("legacy-redirects.json", `${JSON.stringify(legacy.map)}\n`);
    files.set("_worker.js", legacyWorker(legacy.map));
  }
  const sizes = build.pages.map((p) => p.size);
  const measured = measure(sizes, [files.get(build.cssHref.slice(1))!, paneCss]);
  const problems = checkBudget(measured, sizes);
  if (a.app && fonts.missing.length) problems.push(`the app's web export has no file for the face(s) ${fonts.missing.join(", ")}`);
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
  files.set("budget.json", `${JSON.stringify({ budget: BUDGET, measured, quality, themeVariables: new Set(parts.flatMap((p) => p.vars)).size,
    ...(a.threads ? { compressed: compressedTotals(build) } : {}), problems }, null, 1)}\n`);
  return { build, files, measured, problems, legacy };
}

const outDir = (out: string): string => {
  const abs = resolve(out);
  if (!/[\\/]dist[\\/]/.test(`${abs}/`)) throw new Error(`seo: refusing to empty ${abs}: the output folder must be inside a dist folder`);
  return abs;
};
const put = (root: string, name: string, text: string) => {
  const to = join(root, name);
  mkdirSync(dirname(to), { recursive: true });
  writeFileSync(to, text);
};

/** Write `files` under `out`, which is emptied first and must sit in a `dist` folder (it is deleted). */
export function writeSite(out: string, files: ReadonlyMap<string, string>): void {
  const abs = outDir(out);
  rmSync(abs, { recursive: true, force: true });
  for (const [name, text] of files) put(abs, name, text);
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
  if ((!dir && !fixture) || (dir && !appDir)) throw new Error("usage: build-seo.mjs --release-dir <folder> --app <web export> | --fixture [--app <web export>]  [--out DIR] [--slug-map FILE --update-slugs] [--previous snapshot.json] [--date YYYY-MM-DD]");
  const loader = fixture ? openRegsFixture() : openReleaseDir(resolve(dir!));
  const date = opt(args, "--date"), now = date ? new Date(`${date}T12:00:00`) : new Date();
  const slugFile = resolve(opt(args, "--slug-map") ?? SLUG_MAP_FILE);
  const prevFile = opt(args, "--previous");
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
  rmSync(out, { recursive: true, force: true });
  renameSync(partial, out);
  if (args.includes("--update-slugs") && !args.includes("--only")) writeFileSync(slugFile, built.files.get("slug-map.json")!);
  console.log(`seo: ${built.build.pages.length} pages (${built.build.models.length} waters), largest ${built.measured.largest.gzipBytes} B gzip, ` +
    `p95 ${built.measured.p95GzipBytes} B gzip, css ${built.measured.cssGzipBytes} B gzip, ${Math.round((Date.now() - started) / 1000)} s` +
    (threads ? ` on ${Math.min(4, threads.n)} threads; stored ${JSON.stringify(JSON.parse(built.files.get("budget.json")!).compressed)}` : "") +
    `; slug steps ${JSON.stringify(built.build.slugSteps)}` + (built.legacy ? `; v1 redirects ${JSON.stringify(built.legacy.counts)}` : ""));
}
