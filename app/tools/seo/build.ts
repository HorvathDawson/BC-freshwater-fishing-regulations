/**
 * T193-T198 — THE BUILD: a release folder (or the committed fixture release) in, a static site out. `tools/build-seo.mjs` is
 * the command line; the test calls `buildAll` and `writeSite` over the 43-water fixture release into a gitignored dist folder,
 * never a full province build.
 *
 *   node app/tools/build-seo.mjs --release-dir <folder> [--out app/dist/seo] [--slug-map app/seo/slug-map.json --update-slugs]
 *                                [--previous <snapshot.json>] [--date YYYY-MM-DD] | --fixture
 */
import { mkdirSync, readFileSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { openRegsStore } from "@app/data/regs";
import { openRegsFixture, openReleaseDir } from "@app/data/release/node";
import { BUDGET, checkBudget, lighthouseConfig, measure, type Measured } from "./budget";
import { headersFile, hostRules, redirectsFile } from "./hosting";
import type { Site } from "./html";
import { legacyRedirects, legacyWorker, type Crosswalk, type LegacyBuilt } from "./legacy";
import { buildDayOf, type BuildDay } from "./model";
import { buildSite, type Snapshot, type SiteBuild } from "./site";
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
}
export interface Built { readonly legacy: LegacyBuilt | null; readonly build: SiteBuild; readonly files: ReadonlyMap<string, string>; readonly measured: Measured; readonly problems: readonly string[] }

/** Everything the site is, in memory: pages, sitemap, robots, hosting config, the budget and its Lighthouse config. */
export async function buildAll(a: BuildArgs): Promise<Built> {
  const config = a.config ?? readConfig();
  const store = await openRegsStore(a.loader);
  const build = await buildSite({
    store, site: config, release: { id: store.meta.release, validUntil: a.loader.manifest.regulations.valid_until },
    on: a.on, slugMap: a.slugMap ?? EMPTY_SLUG_MAP, previous: a.previous ?? null, ...(a.hubSplitAt ? { hubSplitAt: a.hubSplitAt } : {}),
  });
  const files = new Map(build.files);
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
  const htmlPaths = [...files.keys()].filter((k) => k.endsWith(".html") && k !== "404.html");
  const measured = measure(files, htmlPaths, build.cssHref.slice(1));
  const problems = checkBudget(measured, files, htmlPaths);
  // the page count, the sitemap's count and the bundle's waters with rules are one number
  const listed = [...files.get("sitemap-1.xml")!.matchAll(/<loc>/g)].length - 1;   // minus the app's own "/"
  if (listed !== build.pages.length) problems.push(`sitemap lists ${listed} pages, ${build.pages.length} were built`);
  const waters = build.pages.filter((p) => p.kind === "water").length;
  if (waters !== build.models.length) problems.push(`${waters} water pages for ${build.models.length} waters with rules`);
  const sample = ["/about", ...build.pages.filter((p) => p.kind === "region").slice(0, 1).map((p) => p.path),
                  ...build.pages.filter((p) => p.kind === "water").slice(0, 3).map((p) => p.path)];
  files.set("lighthouserc.json", `${JSON.stringify(lighthouseConfig(sample.map((p) => `${p.slice(1)}/index.html`)), null, 1)}\n`);
  // uniqueness and length report: what Search Console's duplicate / "title too long" findings would otherwise tell us later
  const tag = (re: RegExp) => build.pages.map((p) => re.exec(p.html)?.[1] ?? "");
  const titles = tag(/<title>([^<]*)<\/title>/), descs = tag(/<meta name="description" content="([^"]*)">/);
  const quality = {
    pages: build.pages.length,
    uniqueTitles: new Set(titles).size, uniqueDescriptions: new Set(descs).size,
    titlesOver60: titles.filter((t) => t.length > 60).length, descriptionsOver155: descs.filter((t) => t.length > 155).length,
    emptyTitleOrDescription: titles.filter((t) => !t).length + descs.filter((t) => !t).length,
  };
  if (quality.uniqueTitles !== quality.pages || quality.uniqueDescriptions !== quality.pages) problems.push(`titles/descriptions are not unique: ${JSON.stringify(quality)}`);
  files.set("budget.json", `${JSON.stringify({ budget: BUDGET, measured, quality, problems }, null, 1)}\n`);
  return { build, files, measured, problems, legacy };
}

/** Write `files` under `out`, which is emptied first and must sit in a `dist` folder (it is deleted). */
export function writeSite(out: string, files: ReadonlyMap<string, string>): void {
  const abs = resolve(out);
  if (!/[\\/]dist[\\/]/.test(`${abs}/`)) throw new Error(`seo: refusing to empty ${abs}: the output folder must be inside a dist folder`);
  rmSync(abs, { recursive: true, force: true });
  for (const [name, text] of files) {
    const to = join(abs, name);
    mkdirSync(dirname(to), { recursive: true });
    writeFileSync(to, text);
  }
}

const opt = (args: readonly string[], k: string): string | null => { const i = args.indexOf(k); return i >= 0 ? (args[i + 1] ?? null) : null; };

/**
 * The command line: build, gate, write. Throws on a failed gate. Called by tools/build-seo.mjs through vite's SSR loader
 * (a dynamic load knip cannot follow).
 * @public
 */
export async function main(args: readonly string[]): Promise<void> {
  const dir = opt(args, "--release-dir"), fixture = args.includes("--fixture");
  if (!dir && !fixture) throw new Error("usage: build-seo.mjs --release-dir <folder> | --fixture  [--out DIR] [--slug-map FILE --update-slugs] [--previous snapshot.json] [--date YYYY-MM-DD]");
  const loader = fixture ? openRegsFixture() : openReleaseDir(resolve(dir!));
  const date = opt(args, "--date"), now = date ? new Date(`${date}T12:00:00`) : new Date();
  const slugFile = resolve(opt(args, "--slug-map") ?? SLUG_MAP_FILE);
  const prevFile = opt(args, "--previous");
  const built = await buildAll({
    loader, on: buildDayOf(now),
    slugMap: existsSync(slugFile) ? (JSON.parse(readFileSync(slugFile, "utf8")) as SlugMap) : EMPTY_SLUG_MAP,
    ...(existsSync(CROSSWALK_FILE) ? { crosswalk: JSON.parse(readFileSync(CROSSWALK_FILE, "utf8")) as Crosswalk } : {}),
    previous: prevFile ? (JSON.parse(readFileSync(resolve(prevFile), "utf8")) as Snapshot) : null,
  });
  if (built.problems.length) throw new Error(`seo: ${built.problems.length} budget/consistency problem(s):\n${built.problems.join("\n")}`);
  writeSite(opt(args, "--out") ?? DEFAULT_OUT, built.files);
  if (args.includes("--update-slugs")) writeFileSync(slugFile, built.files.get("slug-map.json")!);
  console.log(`seo: ${built.build.pages.length} pages (${built.build.models.length} waters), p95 ${built.measured.p95GzipBytes} B gzip, css ${built.measured.cssGzipBytes} B gzip` +
    (built.legacy ? `; v1 redirects ${JSON.stringify(built.legacy.counts)}` : ""));
}
