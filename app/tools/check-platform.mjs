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
 * 4. NO INLINE PLATFORM BRANCH (T410). Code that differs by platform lives in a platform file (`foo.web.ts` +
 *    `foo.native.ts`, typed by one `foo.d.ts`): the bundler picks one, so the other platform's code is not even in
 *    the bundle. `Platform.OS` and `typeof window / document / navigator` in a shared file are refused, except where
 *    INLINE_ALLOWED says why — a one-value difference (a size, a flag, a word), at most the count given.
 *    `Platform.select` is the other one-value form and is not counted. (app/README.md "Web and native code".)
 *
 * 5. THE MAP'S PLATFORM FOLDERS (T436). `packages/map/src/web/` and `native/` are one platform's code without a suffix:
 *    only that folder and that platform's `*.web.*` / `*.native.*` files may import from it.
 *
 * Not detectable here, and still needs a real device: gesture feel, font metrics,
 * map SDK rendering. See README.
 */
import { readFileSync, readdirSync, statSync, existsSync } from "node:fs";
import { join, relative, basename, dirname } from "node:path";

// The app root by default. `CHECK_ROOT` points the gate at another tree: its own tests write their fixtures into a
// scratch copy OUTSIDE the app, never into packages/ where every other tree-scanning test walks (T156: a `_plat.ts`
// written here was deleted under check-colours / one-speaker mid-walk, ENOENT, in a parallel run).
const root = process.env.CHECK_ROOT ? process.env.CHECK_ROOT.replace(/\/?$/, "/") : new URL("..", import.meta.url).pathname;

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
  /*
   * react-native-web 0.21 DROPPED `accessibilityState`. It does not warn and it does not
   * throw — the prop is ignored and the element renders with no aria attribute at all. Ten
   * components in this package used it, so every radio group, tab and disabled button in
   * the app was unreadable to a screen reader, and nothing failed: the state was also
   * carried by a background colour, and every test looked at the DOM the way a sighted
   * reader looks at the screen.
   *
   * The ARIA props work on BOTH targets — RN >= 0.71 and RNW >= 0.19 — so they are the one
   * spelling that is not a platform difference:
   *
   *     accessibilityState={{ checked }}   ->  aria-checked={...}      (role="radio")
   *     accessibilityState={{ selected }}  ->  aria-selected={...}     (role="tab")
   *     accessibilityState={{ disabled }}  ->  aria-disabled={...}
   */
  ["accessibilityState",
   "ignored by react-native-web >= 0.21 — use aria-checked / aria-selected / aria-disabled"],
];

const walk = (dir, out = []) => {
  for (const n of readdirSync(dir)) {
    if (n === "node_modules" || n === "dist") continue;
    const p = join(dir, n);
    // the generated native projects (`expo prebuild`, gitignored): not our source, and their Pods hold dangling links
    if (/apps\/mobile\/(ios|android)$/.test(p)) continue;
    statSync(p).isDirectory() ? walk(p, out) : /\.(ts|tsx)$/.test(p) && out.push(p);
  }
  return out;
};

const SHARED = ["packages/ui", "packages/ui-native", "packages/core", "packages/map"];
let failed = 0;
const fail = (m) => { console.error("✗ " + m); failed++; };

// --- 1 + 2: variant sets ---
const seen = new Map();   // "dir/base|group" -> {tag: path}
// packages/data is scanned for PAIRS only (not for banned APIs): its drivers — the bundle engine, the spots store,
// the release adapters (P11) — are the .native/.web pairs that most easily ship on one platform only.
for (const scope of [...SHARED, "packages/data", "apps/mobile"]) {
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

/**
 * Block and line comments out, string literals left alone.
 *
 * Deliberately the same shape as the stripper in `check-boundaries.mjs`: both gates read
 * source as text and both were fooled by prose. String literals are KEPT because a banned
 * API named in a string is usually a real dynamic call.
 */
function stripComments(src) {
  let out = "";
  let i = 0;
  while (i < src.length) {
    const c = src[i], d = src[i + 1];
    if (c === "/" && d === "/") { while (i < src.length && src[i] !== "\n") i++; continue; }
    if (c === "/" && d === "*") {
      i += 2;
      while (i < src.length && !(src[i] === "*" && src[i + 1] === "/")) i++;
      i += 2;
      continue;
    }
    if (c === '"' || c === "'" || c === "`") {
      const q = c;
      out += src[i++];
      while (i < src.length && src[i] !== q) {
        if (src[i] === "\\") out += src[i++];
        out += src[i++];
      }
      out += src[i++] ?? "";
      continue;
    }
    out += src[i++];
  }
  return out;
}

// --- 3: banned APIs in shared code ---
for (const scope of SHARED) {
  const dir = join(root, scope);
  if (!existsSync(dir)) continue;
  for (const file of walk(dir)) {
    if (/\.(ios|android|native|web)\.tsx?$/.test(file)) continue;   // variants may
    if (/\.test\.tsx?$/.test(file)) continue;
    // COMMENTS OUT FIRST. The most useful comment a shared file can carry is the one
    // saying WHY it does not reach for the banned API — `hatch.ts` explains that
    // `document.createElement("canvas")` is a browser call this package may not make, and
    // that sentence was reported as the violation it exists to prevent. A gate that fails
    // on its own explanation teaches people to delete the explanation.
    const src = stripComments(readFileSync(file, "utf8"));
    for (const [needle, why] of BANNED_IN_SHARED)
      if (src.includes(needle))
        fail(`${relative(root, file)}: uses "${needle}" in shared code — ${why}`);
  }
}

// --- 4: inline platform branches (T410) ---
/**
 * The files that may still read the platform inline, each with how many reads and why. Every one is a ONE-VALUE
 * difference: a size, a flag that gates a call or a row, a word in a report. Logic that differs (a listener, a gesture,
 * an effect, a loader) goes into a `.web` / `.native` pair instead — never a new line here to make the gate pass.
 * A count above is refused (a new branch); a count below must be lowered here (the list stays exact).
 */
const INLINE_ALLOWED = {
  "packages/ui-native/src/MapRail.tsx": [2, "two sizes: the rail clears MapLibre's web zoom stack (84 / 10 on the web, 8 / 14 on a phone)"],
  "packages/ui-native/src/MiniMap.tsx": [1, "one style value: the browser's `touchAction` on the minimap"],
  "packages/ui-native/src/NetBanner.tsx": [1, "a flag: the phone's banner says loading and takes no press"],
  "packages/ui-native/src/Shell.tsx": [1, "a flag: the web preloads its later parts when idle (the phone has them at once)"],
  "packages/ui-native/src/Walkthrough.tsx": [1, "a word: which guide (web / app) the steps are written for"],
  "packages/ui-native/src/WebSpotsNotice.tsx": [1, "a flag: the web shows the notice in place of spots"],
  "packages/ui-native/src/menu/HelpPage.tsx": [1, "a word: which help (web / app) is shown"],
  "packages/ui-native/src/menu/SettingsPage.tsx": [1, "a flag: the low-bandwidth row is a website setting (moved from MoreScreen, T703)"],
  "packages/ui-native/src/motion.ts": [1, "a flag: the native animation driver (none on the web)"],
  "packages/ui-native/src/regs/RegsScreen.tsx": [1, "a flag: the dev-only debug hook (`RegsDebug`) is web-only"],
  "packages/ui-native/src/regs/openBook.ts": [1, "a value: the os handed to core's `bookUrl` (a parameter's default, tests pass it)"],
  "packages/ui-native/src/shell/BrowseHost.tsx": [2, "two flags: the mobile-data setting and the Map tab's regs read-ahead are the phone's"],
  "packages/ui-native/src/shell/ShellSheets.tsx": [1, "a word: the offline-maps page's platform (web / phone)"],
  "packages/ui-native/src/shell/useShellNav.ts": [1, "a value: the os handed to `useEdgeBack` (the iOS edge swipe)"],
  "apps/mobile/src/App.tsx": [2, "a flag (`WEB`: version headers, the mobile-data gate, the release gate, the clipboard, the cache note; the Shell's `prefetch`); (T436) the platform line is ui-native's `platformLine`"],
  "apps/mobile/src/config.ts": [1, "a flag: `APP_ENV.web`, the one place config reads the platform"],
};
const INLINE = /Platform\.OS\b|typeof\s+(?:window|document|navigator)\b/g;
for (const scope of [...SHARED, "packages/data", "apps/mobile"]) {
  const dir = join(root, scope);
  if (!existsSync(dir)) continue;
  for (const file of walk(dir)) {
    if (/\.(ios|android|native|web)\.tsx?$/.test(file) || /\.test\.tsx?$/.test(file) || /\.d\.ts$/.test(file)) continue;
    const rel = relative(root, file);
    const n = (stripComments(readFileSync(file, "utf8")).match(INLINE) ?? []).length;
    const allowed = INLINE_ALLOWED[rel]?.[0] ?? 0;
    if (n > allowed)
      fail(`${rel}: ${n} inline platform check(s) (\`Platform.OS\` / \`typeof window|document|navigator\`), ${allowed} allowed.\n` +
           `    Code that differs by platform goes in a .web.ts / .native.ts pair (app/README.md "Web and native code");\n` +
           `    a one-value difference may use Platform.select.`);
    else if (n < allowed)
      fail(`${rel}: ${n} inline platform check(s), INLINE_ALLOWED says ${allowed} — lower it in tools/check-platform.mjs.`);
  }
}
// a line for a file that is gone is stale (not checked over a scratch tree, CHECK_ROOT, which holds only its fixtures)
if (!process.env.CHECK_ROOT)
  for (const rel of Object.keys(INLINE_ALLOWED))
    if (!existsSync(join(root, rel))) fail(`${rel}: listed in INLINE_ALLOWED but not there — remove its line.`);

// --- 5: the map's platform folders (T436) ---
// `packages/map/src/web/` and `native/` hold one platform's code without a `.web` / `.native` suffix, so rule 1 cannot
// see them. Only that platform may reach in: a file inside the folder, or a `.web.*` / `.native.*` file. A shared file
// importing `./web/x` would ship the browser's code to the phone (or the reverse); tests are exempt.
for (const side of ["web", "native"]) {
  const dir = join(root, "packages/map/src");
  if (!existsSync(dir)) continue;
  const into = new RegExp(`from\\s+["'](?:\\./|(?:\\.\\./)+)${side}/`);
  for (const file of walk(dir)) {
    const rel = relative(dir, file);
    if (/\.test\.tsx?$/.test(file) || rel.startsWith(`${side}/`) || new RegExp(`\\.${side}\\.tsx?$`).test(file)) continue;
    if (into.test(stripComments(readFileSync(file, "utf8"))))
      fail(`packages/map/src/${rel}: imports from map/src/${side}/ — only ${side}/ itself and *.${side}.* files may (T436)`);
  }
}

if (failed) { console.error(`\n${failed} platform-parity problem(s).`); process.exit(1); }
console.log(`✓ platform parity: ${seen.size} variant set(s) complete, shared code clean, ` +
            `inline platform checks only where INLINE_ALLOWED says (${Object.keys(INLINE_ALLOWED).length} files)`);
