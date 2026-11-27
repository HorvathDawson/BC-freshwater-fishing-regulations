/**
 * (T1252) THE WEB MAP'S WORKER FILES — placed by metro.config.js when Metro starts (the dev server, `expo export`), as the
 * version stamp is. maplibre-gl 6 is ES modules only and runs its tile work in a worker it starts from a separate file
 * (`dist/maplibre-gl-worker.mjs`); a bundler cannot hand it that file, so the web map names it (`setWorkerUrl`,
 * packages/map/src/web/ready.ts `glWorkerUrl`) and this copies it from the installed package into `public/`, which Expo
 * serves at the site's root in development and copies into every web export.
 *
 * WITH WHAT IT IMPORTS: some 6.x releases split the worker and the page over a shared chunk (6.4's worker imports
 * `./maplibre-gl-shared.mjs`), others ship it whole (6.19), so the worker's relative imports are copied beside it.
 *
 * IN A FOLDER NAMED BY GL'S VERSION (`maplibre-gl-<version>/`): the page asks for the worker of its own GL, so a CDN's
 * cached worker of another release is never paired with a new page. Other versions' folders are removed. The copy is
 * generated, never committed (`.gitignore`). A native bundle never reads `public/`; the copy is harmless there.
 * Plain CommonJS: Metro's config is loaded by Node before any TypeScript exists.
 */
const { copyFileSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync } = require("node:fs");
const path = require("node:path");

const PREFIX = "maplibre-gl-";
const WORKER = "maplibre-gl-worker.mjs";

/** The folder in `public/` for a GL version — the path `glWorkerUrl` asks for is `/<this>/maplibre-gl-worker.mjs`. */
function glWorkerDir(version) {
  return `${PREFIX}${version}`;
}

/** The files of GL's `dist/` the worker needs: itself and, transitively, every relative module it imports. */
function glWorkerFiles(dist) {
  const need = [WORKER];
  for (let i = 0; i < need.length; i++) {
    const file = path.join(dist, need[i]);
    if (!existsSync(file)) throw new Error(`gl-worker: ${file} is missing: the web map would draw no tile`);
    const src = readFileSync(file, "utf8");
    for (const m of src.matchAll(/(?:\bfrom|\bimport)\s*["']\.\/([^"']+)["']/g))
      if (!need.includes(m[1])) need.push(m[1]);
  }
  return need;
}

/** Copy the installed GL's worker files into `<projectRoot>/public/maplibre-gl-<version>/`; returns that folder. */
function placeGlWorker(projectRoot) {
  // the GL the web map's package resolves (`packages/map` declares maplibre-gl; this app does not import a map SDK)
  const mapPkg = path.resolve(projectRoot, "../../packages/map");
  const gl = path.dirname(require.resolve("maplibre-gl/package.json", { paths: [mapPkg] }));
  const { version } = JSON.parse(readFileSync(path.join(gl, "package.json"), "utf8"));
  const dist = path.join(gl, "dist");
  const files = glWorkerFiles(dist);
  const pub = path.join(projectRoot, "public");
  const dir = glWorkerDir(version);
  if (existsSync(pub))
    for (const f of readdirSync(pub)) if (f.startsWith(PREFIX) && f !== dir) rmSync(path.join(pub, f), { recursive: true });
  mkdirSync(path.join(pub, dir), { recursive: true });
  for (const f of files) copyFileSync(path.join(dist, f), path.join(pub, dir, f));
  return path.join(pub, dir);
}

module.exports = { glWorkerDir, glWorkerFiles, placeGlWorker };
