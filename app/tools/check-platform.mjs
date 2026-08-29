#!/usr/bin/env node
/**
 * "Works on one platform but not the other" — the failures that are mechanically
 * detectable, checked before anyone opens two devices.
 *
 * 1. PLATFORM VARIANT SETS. React Native resolves `x.ios.tsx` / `x.android.tsx` /
 *    `x.native.ts` / `x.web.ts` automatically. That is exactly how a feature quietly
 *    ships on one platform only: someone adds `foo.web.ts`, never writes
 *    `foo.native.ts`, and the native app silently falls back or crashes at runtime.
 *    A variant must come as a COMPLETE set.
 *
 * 2. IDENTICAL EXPORT SURFACES. Even a complete set diverges if one side exports a
 *    helper the other does not — callers compile on one platform and fail on the other.
 *
 * 3. BANNED APIS IN SHARED CODE. Things that exist in react-native-web but not React
 *    Native, or vice versa. Shared phone components must use neither.
 *
 * Not detectable here, and still needs a real device: gesture feel, font metrics,
 * map SDK rendering. See README.
 */
import { readFileSync, readdirSync, statSync, existsSync } from "node:fs";
import { join, relative, basename, dirname } from "node:path";

const root = new URL("..", import.meta.url).pathname;

/** Groups that must be complete when any member exists. */
const VARIANT_SETS = [
  ["ios", "android"],      // if you special-case one OS you must consider the other
  ["native", "web"],       // RNW split
];

/** APIs unavailable or behaviourally different on the other side of RNW. */
const BANNED_IN_SHARED = [
  ["Alert.alert", "not available in react-native-web — use a shared dialog component"],
  ["PermissionsAndroid", "Android-only; guard behind a platform variant file"],
  ["document.", "DOM is absent in React Native"],
  ["window.localStorage", "absent in React Native — use the storage abstraction"],
  ["navigator.geolocation", "differs across RN/RNW — go through a variant file"],
];

const walk = (dir, out = []) => {
  for (const n of readdirSync(dir)) {
    if (n === "node_modules" || n === "dist") continue;
    const p = join(dir, n);
    statSync(p).isDirectory() ? walk(p, out) : /\.(ts|tsx)$/.test(p) && out.push(p);
  }
  return out;
};

const SHARED = ["packages/ui", "packages/ui-native", "packages/core", "packages/map"];
let failed = 0;
const fail = (m) => { console.error("✗ " + m); failed++; };

// --- 1 + 2: variant sets ---
const seen = new Map();   // "dir/base|group" -> {tag: path}
for (const scope of [...SHARED, "apps/mobile", "apps/web"]) {
  const dir = join(root, scope);
  if (!existsSync(dir)) continue;
  for (const file of walk(dir)) {
    const m = basename(file).match(/^(.+?)\.(ios|android|native|web)\.tsx?$/);
    if (!m) continue;
    const [, stem, tag] = m;
    for (const group of VARIANT_SETS) {
      if (!group.includes(tag)) continue;
      const key = `${dirname(file)}/${stem}|${group.join("+")}`;
      if (!seen.has(key)) seen.set(key, { group, stem, dir: dirname(file), have: {} });
      seen.get(key).have[tag] = file;
    }
  }
}

const exportsOf = (file) => {
  const src = readFileSync(file, "utf8");
  const names = new Set();
  for (const m of src.matchAll(/export\s+(?:async\s+)?(?:function|const|let|class|type|interface|enum)\s+([A-Za-z0-9_$]+)/g))
    names.add(m[1]);
  for (const m of src.matchAll(/export\s*\{([^}]*)\}/g))
    for (const part of m[1].split(","))
      names.add((part.split(/\sas\s/).pop() ?? part).trim());
  names.delete("");
  return names;
};

for (const { group, stem, dir, have } of seen.values()) {
  const missing = group.filter((t) => !have[t]);
  if (missing.length) {
    fail(`${relative(root, dir)}/${stem}: has ${Object.keys(have).join(", ")} but no ` +
         `${missing.join(", ")} variant.\n    A variant set must be complete, or the feature ` +
         `silently exists on one platform only.`);
    continue;
  }
  const surfaces = group.map((t) => [t, exportsOf(have[t])]);
  const [, first] = surfaces[0];
  for (const [tag, names] of surfaces.slice(1)) {
    const only = [...first].filter((n) => !names.has(n));
    const extra = [...names].filter((n) => !first.has(n));
    if (only.length || extra.length)
      fail(`${relative(root, dir)}/${stem}: export surfaces differ between ` +
           `${surfaces[0][0]} and ${tag}.\n` +
           (only.length ? `    only in ${surfaces[0][0]}: ${only.join(", ")}\n` : "") +
           (extra.length ? `    only in ${tag}: ${extra.join(", ")}\n` : "") +
           `    A caller that compiles on one platform must compile on the other.`);
  }
}

// --- 3: banned APIs in shared code ---
for (const scope of SHARED) {
  const dir = join(root, scope);
  if (!existsSync(dir)) continue;
  for (const file of walk(dir)) {
    if (/\.(ios|android|native|web)\.tsx?$/.test(file)) continue;   // variants may
    if (/\.test\.tsx?$/.test(file)) continue;
    const src = readFileSync(file, "utf8");
    for (const [needle, why] of BANNED_IN_SHARED)
      if (src.includes(needle))
        fail(`${relative(root, file)}: uses "${needle}" in shared code — ${why}`);
  }
}

if (failed) { console.error(`\n${failed} platform-parity problem(s).`); process.exit(1); }
console.log(`✓ platform parity: ${seen.size} variant set(s) complete, shared code clean`);
