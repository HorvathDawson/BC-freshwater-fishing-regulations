#!/usr/bin/env node
/**
 * (T352) SOURCE MAPS, KEPT PER BUILD. A crash's stack names minified frames (`index-<hash>.js:2:2257` on
 * the web, `index.android.bundle:1:2384728` from Hermes); the maps that read them back are kept on this machine under
 * data/sourcemaps/<key>/ (gitignored; nothing uploads data/sourcemaps), one folder per build, keyed like a crash's `app`:
 * "0.5.0 (build 8, 9813980, 2026-11-04)" → `0.5.0-b8-9813980`.
 *
 *   node app/tools/sourcemaps.mjs keep-web <export dir>
 *        copy every *.map of a web export (`expo export --source-maps`) into data/sourcemaps/<key>/web/. (T1008, owner
 *        2026-11-22: "we should serve them? why not?") THE WEB'S MAPS ARE SERVED: the maps and the bundles'
 *        `sourceMappingURL` line stay in the export, so the site carries them (same cache rules as the bundle: `/_expo/static/*`)
 *        and any browser's tools read real file and line names; a browser fetches a map only with its tools open. First
 *        `checkServedMaps`: every served .js over 100 KB names its map, the map is there, parses and carries the bundle's
 *        debug id, and names only files the repository tracks or node_modules holds (the repository is public: a map shows
 *        nothing GitHub does not). The key is app.json's version + build and HEAD's short sha, as the version stamp puts
 *        them in the bundle. No map found, or a check failed: it fails.
 *   node app/tools/sourcemaps.mjs keep-android <index.android.bundle.map> <key>
 *        copy the Hermes map of a release build (deploy/staging/build-apk.sh) to data/sourcemaps/<key>/android/.
 *   node app/tools/sourcemaps.mjs keep-r8 <mapping.txt> <key>
 *        (T749, T1003) copy R8's mapping.txt of a release build (R8 is always on) to data/sourcemaps/<key>/android/r8-mapping.txt.
 *        `symbolicate` below does NOT read it: our own crash store holds no Java/Kotlin frames (JS stacks are Hermes's;
 *        the exit-info module sends Android's exit reason, never a trace). The same mapping goes to Play with the AAB
 *        (play-upload.py --mapping), where Android vitals reads Java crashes and ANRs back; this copy is for a Java stack
 *        read by hand (Play Console, logcat): `$ANDROID_HOME/cmdline-tools/latest/bin/retrace <r8-mapping.txt> <stack.txt>`.
 *        Missing or empty: it FAILS, so a shrunk build whose names cannot be read back is never kept as if it could.
 *   node app/tools/sourcemaps.mjs symbolicate [<crash json> | -]
 *        a crash-store kind (or its `sample`) from a file or stdin: each frame of its stack with the source position
 *        it came from. No folder for its build: it fails, naming the folders there are.
 *
 * No dependency: the mappings are decoded here (base64 VLQ, source map v3).
 */
import { execSync } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, readdirSync, readFileSync, statSync } from "node:fs";
import { basename, dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
export const ROOT = join(REPO, "data", "sourcemaps");

/** "0.5.0 (build 8, 9813980, 2026-11-04)" → "0.5.0-b8-9813980" (null: not a version line with a build and a sha). */
export function keyOf(app) {
  const m = /^(\S+) \(build (\d+), ([0-9a-f]{7,40})\b/.exec(String(app ?? ""));
  return m ? `${m[1]}-b${m[2]}-${m[3].slice(0, 7)}` : null;
}

/** The key of the build in this checkout: app.json's version + build, HEAD's short sha. */
export function checkoutKey(repo = REPO) {
  const app = JSON.parse(readFileSync(join(repo, "app/apps/mobile/app.json"), "utf8")).expo;
  const sha = execSync("git rev-parse --short=7 HEAD", { cwd: repo }).toString().trim();
  return `${app.version}-b${app.android.versionCode}-${sha}`;
}

const B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const DIGIT = new Map([...B64].map((c, i) => [c, i]));

/** A v3 map's `mappings` → per generated line, its segments [genCol, src, line, col, name?] (absolute, 0-based). */
export function decodeMappings(mappings) {
  const lines = [];
  let src = 0, line = 0, col = 0, name = 0;
  for (const text of mappings.split(";")) {
    const segs = [];
    let gen = 0;
    for (const s of text ? text.split(",") : []) {
      const v = [];
      let shift = 0, acc = 0;
      for (const c of s) {
        const d = DIGIT.get(c);
        if (d === undefined) throw new Error(`sourcemaps: "${c}" is not base64 VLQ`);
        acc += (d & 31) << shift;
        if (d & 32) { shift += 5; continue; }
        v.push(acc & 1 ? -(acc >>> 1) : acc >>> 1);
        acc = 0; shift = 0;
      }
      gen += v[0];
      if (v.length >= 4) {
        src += v[1]; line += v[2]; col += v[3];
        if (v.length >= 5) name += v[4];
        segs.push(v.length >= 5 ? [gen, src, line, col, name] : [gen, src, line, col]);
      }
    }
    lines.push(segs);
  }
  return lines;
}

/** Where a generated position (line 1-based, column 0-based) came from, or null (no segment at or before it). */
export function originalFor(map, decoded, line, column) {
  const segs = decoded[line - 1] ?? [];
  let lo = 0, hi = segs.length - 1, at = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (segs[mid][0] <= column) { at = mid; lo = mid + 1; } else hi = mid - 1;
  }
  if (at < 0) return null;
  const s = segs[at];
  return { source: map.sources[s[1]], line: s[2] + 1, column: s[3], name: s.length > 4 ? map.names[s[4]] : null };
}

/** One stack line's file, line and column: "at f (address at index.android.bundle:1:2384728)", "f@…/x.js:2:10", "at x.js:2:10". */
export function frameOf(text) {
  const m = /([^\s()@]+):(\d+):(\d+)\)?\s*$/.exec(text);
  return m ? { file: basename(m[1]), line: Number(m[2]), column: Number(m[3]) } : null;
}

function* walk(dir) {
  for (const n of readdirSync(dir)) {
    const p = join(dir, n);
    if (statSync(p).isDirectory()) yield* walk(p); else yield p;
  }
}

/**
 * Symbolicate a crash sample's stack from `root/<key>/`. Web (V8) columns are 1-based; Hermes's are bytecode offsets,
 * read as the composed map's 0-based columns. A frame whose file has no map, or no segment, is said as such.
 */
export function symbolicate(sample, root = ROOT) {
  const key = keyOf(sample.app);
  if (!key) throw new Error(`sourcemaps: "${sample.app}" names no build (version, build, sha)`);
  const dir = join(root, key);
  if (!existsSync(dir)) {
    const have = existsSync(root) ? readdirSync(root).sort() : [];
    throw new Error(`sourcemaps: no maps kept for ${key} (${dir}); kept: ${have.join(", ") || "none"}`);
  }
  const files = [...walk(dir)].filter((p) => p.endsWith(".map"));
  const maps = new Map();
  const load = (file) => {
    const p = files.find((f) => basename(f) === `${file}.map`);
    if (!p) return null;
    if (!maps.has(p)) {
      const map = JSON.parse(readFileSync(p, "utf8"));
      if (map.sections) throw new Error(`sourcemaps: ${p} is an indexed map (sections), not read here`);
      maps.set(p, { map, decoded: decodeMappings(map.mappings) });
    }
    return maps.get(p);
  };
  const hermes = !String(sample.platform ?? "").startsWith("web");
  return String(sample.stack ?? "").split("\n").filter(Boolean).map((text) => {
    const f = frameOf(text);
    if (!f) return `${text}`;
    const m = load(f.file);
    if (!m) return `${text}    [no map for ${f.file}]`;
    const o = originalFor(m.map, m.decoded, f.line, hermes ? f.column : f.column - 1);
    if (!o) return `${text}    [no mapping]`;
    return `${text}\n      → ${o.source}:${o.line}:${o.column + 1}${o.name ? ` (${o.name})` : ""}`;
  });
}

/** (T1008) the bundles a map is required for: a served .js at least this big */
export const MAP_REQUIRED_BYTES = 100 * 1024;
/** (T1008) a bundler's own module, not a file (Metro's prelude, its `\0polyfill:` modules) */
const VIRTUAL = (s) => s === "__prelude__" || s.startsWith("\0");

/**
 * (T1008) WHAT IS WRONG WITH THE MAPS A WEB EXPORT SERVES: a .js over `MAP_REQUIRED_BYTES` with no `sourceMappingURL`, or one
 * naming a map that is not there or does not parse, or whose debug id is not the bundle's (`file`, where a map has one, must
 * be the bundle's name); a map naming a source the repository does not track and node_modules does not hold. `appRoot` is
 * the folder the bundler's sources are relative to (app/); `tracked(path)` says whether git tracks a path under it.
 */
export function checkServedMaps(exportDir, appRoot, tracked) {
  const bad = [];
  for (const js of [...walk(exportDir)].filter((p) => p.endsWith(".js")).sort()) {
    const body = readFileSync(js, "utf8"), at = relative(exportDir, js);
    const url = /^\/\/# sourceMappingURL=(\S+)\s*$/m.exec(body)?.[1];
    if (!url) { if (statSync(js).size >= MAP_REQUIRED_BYTES) bad.push(`${at}: no sourceMappingURL`); continue; }
    if (/^[a-z]+:/i.test(url)) { bad.push(`${at}: its map is not a file the site serves (${url.slice(0, 40)})`); continue; }
    const mapPath = url.startsWith("/") ? join(exportDir, url) : join(dirname(js), url);
    if (!existsSync(mapPath)) { bad.push(`${at}: names ${url}, which the export does not have`); continue; }
    let map;
    try { map = JSON.parse(readFileSync(mapPath, "utf8")); } catch (e) { bad.push(`${at}: its map does not parse (${String(e)})`); continue; }
    if (map.version !== 3) bad.push(`${at}: its map is not a v3 map`);
    const id = /^\/\/# debugId=(\S+)\s*$/m.exec(body)?.[1] ?? null;
    if ((map.debugId ?? null) !== id) bad.push(`${at}: its map's debug id ${map.debugId ?? "(none)"} is not the bundle's ${id ?? "(none)"}`);
    if (map.file !== undefined && basename(map.file) !== basename(js)) bad.push(`${at}: its map is for ${map.file}`);
    const sources = map.sections ? map.sections.flatMap((x) => x.map?.sources ?? []) : (map.sources ?? []);
    const stray = [...new Set(sources)].filter((s) => !VIRTUAL(s) && !(/(^|\/)node_modules\//.test(s) && existsSync(join(appRoot, s))) &&
                                               !tracked(s.replace(/^\/+/, "")));
    if (stray.length) bad.push(`${at}: its map names ${stray.length} file(s) the repository does not track: ${stray.slice(0, 5).join(", ")}`);
  }
  return bad;
}

/** The paths git tracks under `dir` (relative to it): `checkServedMaps`'s "in the repository". */
export function trackedUnder(dir) {
  const list = execSync("git ls-files -z", { cwd: dir, maxBuffer: 256 * 1024 * 1024 }).toString().split("\0").filter(Boolean);
  const set = new Set(list);
  return (p) => set.has(p);
}

/** Copy a web export's maps under `root/<key>/web/` (they stay in the export: served, T1008), once they pass `checkServedMaps`. Returns the copies. */
export function keepWeb(exportDir, key, root = ROOT, appRoot = join(REPO, "app"), tracked = trackedUnder(join(REPO, "app"))) {
  const maps = [...walk(exportDir)].filter((p) => p.endsWith(".map"));
  if (!maps.length) throw new Error(`sourcemaps: no .map under ${exportDir} (exported without --source-maps?)`);
  const bad = checkServedMaps(exportDir, appRoot, tracked);
  if (bad.length) throw new Error(`sourcemaps: the export's maps cannot be served:\n${bad.join("\n")}`);
  const out = [];
  for (const p of maps) {
    const to = join(root, key, "web", relative(exportDir, p));
    mkdirSync(dirname(to), { recursive: true });
    copyFileSync(p, to);
    out.push(to);
  }
  return out;
}

/** Copy a Hermes map to `root/<key>/android/`. */
export function keepAndroid(mapPath, key, root = ROOT) {
  if (!existsSync(mapPath)) throw new Error(`sourcemaps: no Hermes map at ${mapPath}`);
  const to = join(root, key, "android", basename(mapPath));
  mkdirSync(dirname(to), { recursive: true });
  copyFileSync(mapPath, to);
  return to;
}

/** (T749) Copy R8's mapping.txt to `root/<key>/android/r8-mapping.txt`; refuses a missing or empty one. */
export function keepR8(mappingPath, key, root = ROOT) {
  if (!existsSync(mappingPath)) throw new Error(`sourcemaps: no R8 mapping at ${mappingPath} (a release build has R8 on, T1003: did R8 run?)`);
  if (statSync(mappingPath).size === 0) throw new Error(`sourcemaps: the R8 mapping at ${mappingPath} is empty`);
  const to = join(root, key, "android", "r8-mapping.txt");
  mkdirSync(dirname(to), { recursive: true });
  copyFileSync(mappingPath, to);
  return to;
}

function main(argv) {
  const [cmd, a, b] = argv;
  if (cmd === "keep-web" && a) {
    const key = checkoutKey();
    for (const p of keepWeb(resolve(a), key)) console.log(`kept ${relative(REPO, p)}`);
  } else if (cmd === "keep-android" && a && b) {
    console.log(`kept ${relative(REPO, keepAndroid(resolve(a), b))}`);
  } else if (cmd === "keep-r8" && a && b) {
    console.log(`kept ${relative(REPO, keepR8(resolve(a), b))}`);
  } else if (cmd === "symbolicate") {
    const text = !a || a === "-" ? readFileSync(0, "utf8") : readFileSync(a, "utf8");
    const j = JSON.parse(text);
    const sample = j.sample ?? j;
    console.log(`${sample.name ?? "Error"}: ${sample.message ?? ""}  [${sample.app}, ${sample.platform}]`);
    for (const l of symbolicate(sample)) console.log(l);
  } else {
    console.error("usage: sourcemaps.mjs keep-web <export dir> | keep-android <map> <key> | keep-r8 <mapping.txt> <key> | symbolicate [<crash json> | -]");
    process.exit(2);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { main(process.argv.slice(2)); } catch (e) { console.error(e instanceof Error ? e.message : String(e)); process.exit(1); }
}
