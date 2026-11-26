#!/usr/bin/env node
/**
 * T1163 — the BUILT Info.plist declares what the app config declares. Run by the TestFlight build script after the archive:
 *   node app/tools/check-built-plist.mjs <path to the built Info.plist>
 * The expectation is never typed here: it is what the app config produces once its plugins have run
 * (`expo config --type introspect`: the background modes `expo-background-task` / `expo-notifications` add, the task
 * identifiers, every `NS...UsageDescription` string). FAILS (exit 1), naming each one, when the built file lacks a declared
 * background mode, task identifier or usage string, or carries a different usage string. A mode the build has beyond the
 * config is not a failure here (the config is the floor), but is printed.
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const MOBILE = fileURLToPath(new URL("../apps/mobile/", import.meta.url));

/**
 * What the tests (and any caller that must not see the machine) run `expo config` with: only what a process needs to run at
 * all, never the caller's build variables (`app.config.js` reads many; as `r8-config.test.ts` `envFor`).
 */
export function minimalEnv() {
  const env = {};
  for (const k of ["PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "USER", "SHELL"]) if (process.env[k] !== undefined) env[k] = process.env[k];
  return env;
}

/**
 * A project folder holding the app's config (symlinks) and NO `ios/` or `android/` of its own: `expo config` reads an
 * existing native folder when there is one, and a developer's machine may have one. Returns its path (the caller removes it).
 */
export function bareProject() {
  const dir = mkdtempSync(join(tmpdir(), "t1163-project-"));
  for (const f of ["app.json", "app.config.js", "package.json", "plugins", "assets", "modules"]) symlinkSync(join(MOBILE, f), join(dir, f));
  symlinkSync(join(MOBILE, "../../node_modules"), join(dir, "node_modules"));   // the workspace's hoisted plugins
  return dir;
}

/**
 * pure: `{ modes, identifiers, strings }` from the config's `ios.infoPlist`. THROWS when there is nothing to check (no modes,
 * no usage strings, or `processing` declared without a task identifier): a check with nothing to compare must never pass.
 */
export function expectedFromInfoPlist(ip) {
  if (!ip || !Array.isArray(ip.UIBackgroundModes) || ip.UIBackgroundModes.length === 0) {
    throw new Error("the app config declares no ios.infoPlist.UIBackgroundModes: nothing to check");
  }
  const strings = Object.fromEntries(Object.entries(ip).filter(([k, v]) => /UsageDescription$/.test(k) && typeof v === "string"));
  if (Object.keys(strings).length === 0) throw new Error("the app config declares no usage strings: nothing to check");
  const identifiers = ip.BGTaskSchedulerPermittedIdentifiers;
  if (ip.UIBackgroundModes.includes("processing") && (!Array.isArray(identifiers) || identifiers.length === 0)) {
    throw new Error('the app config declares background mode "processing" but no BGTaskSchedulerPermittedIdentifiers');
  }
  return { modes: ip.UIBackgroundModes, identifiers: identifiers ?? [], strings };
}

/** The Info.plist the app config yields, plugins run (`expo config --type introspect`): see `expectedFromInfoPlist`. */
export function expectedFromConfig(env = process.env, cwd = MOBILE) {
  const out = execFileSync(join(MOBILE, "../../node_modules/.bin/expo"), ["config", "--type", "introspect", "--json"],
    { cwd, env, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"], maxBuffer: 64 * 1024 * 1024 });
  return expectedFromInfoPlist(JSON.parse(out)?.ios?.infoPlist);
}

/** Parse an XML plist (dict, array, string, true, false, integer, real) to a JS value. Binary plists go through plutil first. */
export function parsePlist(text) {
  const tok = /<(\/?)(dict|array|key|string|integer|real|date|data)>|<(true|false)\s*\/>|<(dict|array)\s*\/>|<string\s*\/>|([^<]+)/g;
  const unesc = (s) => s.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, "&");
  if (!text.includes("<plist")) throw new Error("not a plist: no <plist> element");
  const body = text.slice(text.indexOf("<plist"));
  const stack = [];
  let root, key = null, cur = null;
  const put = (v) => {
    if (Array.isArray(cur)) cur.push(v); else if (cur) { cur[key] = v; key = null; } else root = v;
  };
  let m, open = null, buf = "";
  tok.lastIndex = body.indexOf(">") + 1;
  while ((m = tok.exec(body))) {
    const [, close, tag, bool, emptyc, textNode] = m;
    if (textNode !== undefined) { buf += textNode; continue; }
    if (bool) { put(bool === "true"); continue; }
    if (emptyc) { put(emptyc === "dict" ? {} : []); continue; }
    if (!tag) { put(""); continue; }
    if (!close && (tag === "dict" || tag === "array")) { const v = tag === "dict" ? {} : []; put(v); stack.push([cur, key]); cur = v; key = null; continue; }
    if (close && (tag === "dict" || tag === "array")) { [cur, key] = stack.pop(); continue; }
    if (!close) { open = tag; buf = ""; continue; }
    if (open === "key") key = unesc(buf.trim()); else if (open === "integer" || open === "real") put(Number(buf)); else put(unesc(buf));
    open = null; buf = "";
  }
  if (root === null || typeof root !== "object" || Array.isArray(root)) throw new Error("not a plist: the top level is not a dict");
  return root;
}

export function readPlist(file) {
  const raw = readFileSync(file);
  const text = raw.subarray(0, 6).toString() === "bplist"
    ? execFileSync("plutil", ["-convert", "xml1", "-o", "-", file], { encoding: "utf8" }) : raw.toString("utf8");
  return parsePlist(text);
}

/** pure: the failures (one line each, naming the missing thing) of `built` against `expected` */
export function judge(expected, built) {
  const fails = [];
  const modes = Array.isArray(built.UIBackgroundModes) ? built.UIBackgroundModes : [];
  for (const m of expected.modes) if (!modes.includes(m)) fails.push(`UIBackgroundModes lacks "${m}"`);
  const ids = Array.isArray(built.BGTaskSchedulerPermittedIdentifiers) ? built.BGTaskSchedulerPermittedIdentifiers : [];
  for (const i of expected.identifiers) if (!ids.includes(i)) fails.push(`BGTaskSchedulerPermittedIdentifiers lacks "${i}"`);
  for (const [k, v] of Object.entries(expected.strings)) {
    if (typeof built[k] !== "string" || built[k].trim() === "") fails.push(`${k} is missing`);
    else if (built[k] !== v) fails.push(`${k} differs from the app config: "${built[k]}"`);
  }
  return fails;
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  const file = process.argv[2];
  if (!file) { console.error("usage: check-built-plist.mjs <built Info.plist>"); process.exit(2); }
  try {
    const expected = expectedFromConfig();
    const built = readPlist(file);
    const fails = judge(expected, built);
    for (const m of (built.UIBackgroundModes ?? [])) if (!expected.modes.includes(m)) console.log(`NOTE built mode "${m}" is not in the app config`);
    for (const f of fails) console.error(`FAIL ${file}: ${f}`);
    if (fails.length) process.exit(1);
    console.log(`built plist ok: modes ${expected.modes.join(", ")}; ${Object.keys(expected.strings).length} usage strings`);
  } catch (e) {
    console.error(`FAIL ${file}: ${e instanceof Error ? e.message : String(e)}`);
    process.exit(1);
  }
}
