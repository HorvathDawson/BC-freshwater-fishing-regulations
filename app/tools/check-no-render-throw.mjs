#!/usr/bin/env node
/**
 * (T1164) NO `throw` IN A RENDER PATH. A throw while a component draws, or a hook or an effect runs, is caught only by a
 * boundary (`ui-native` `ScreenBoundary`): the screen, page or panel is lost, the fault reported. Better never to throw
 * there: a state that can be reached is DRAWN (a line, `ErrorState`), a state that cannot be reached is made unreachable
 * by the types.
 *
 * WHAT IT FLAGS (typescript compiler API, no new dependency), a `throw` statement whose nearest enclosing function is:
 *   - a React component (a capital name: `function Foo`, `const Foo = () =>`, a `memo` / `forwardRef` of one, an anonymous
 *     `export default function` in a .tsx file), a hook (`useX`), or, in a class extending `Component` / `PureComponent`,
 *     its `render`, `constructor` or `getDerivedStateFromProps`;
 *   - a callback React runs while drawing: `useMemo`, a `useState` / `useReducer` initialiser or reducer;
 *   - an EFFECT callback: `useEffect` / `useLayoutEffect` / `useInsertionEffect`. React hands a throw in an effect to the
 *     nearest boundary too, so the gate treats it as a render path. (Its cleanup function, returned from it, is not looked at.)
 *   - an array-method callback (`map`, `filter`, `flatMap`, `reduce`, `forEach`, `some`, `every`, `find`, `sort`, …) inside one
 *     of the above: it runs while the component draws;
 *   - a plain helper function declared in the same file (lowercase, not `async`, not named `onX`) that one of the above
 *     CALLS by its name, directly or through other such helpers (like Shell's own `screen`, `layered`, `withGuide`).
 * WHAT IT SKIPS: an event handler (an `on*` prop's arrow, or a helper named `onX`), `useCallback`, `.then` callbacks, anything
 * `async`, a function only handed on as a value (called by name from nowhere in a render path).
 * WHAT IT CANNOT SEE: a throw in a function in ANOTHER file that a component calls (core's presenters, a hook's helper), a
 * call through a variable, a property or a callback prop, a throw in a render-prop function child. It is a net for the
 * written-out case in the file, not a proof of no throw.
 *
 * What is left is listed in ALLOWED: each `file::function` with how many throws it holds and why. A new throw, or a second
 * one in a listed function, fails here: draw the state instead, or, only when it is a programmer-error assertion that the
 * boundary should catch, list it with its count and reason.
 *
 *   node tools/check-no-render-throw.mjs [--list]
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const APP = join(dirname(fileURLToPath(import.meta.url)), "..");
const ROOTS = ["packages/ui/src", "packages/ui-native/src", "packages/map/src", "apps/mobile/src"];
const SKIP = /\.test\.tsx?$|\.d\.ts$|\.generated\.tsx?$|\.fixture\.ts$|\/test\//;

/** The hooks whose callback (argument 0) React runs while drawing or as an effect. */
const RENDER_CALLBACK = new Set(["useMemo", "useState", "useReducer", "useEffect", "useLayoutEffect", "useInsertionEffect"]);
const ARRAY_METHODS = new Set(["map", "filter", "flatMap", "reduce", "reduceRight", "forEach", "some", "every", "find", "findIndex",
  "findLast", "findLastIndex", "sort", "toSorted"]);
const CLASS_METHODS = new Set(["render", "constructor", "getDerivedStateFromProps"]);

/** Each allowed function: `file::function` -> how many throws it holds, and why. */
export const ALLOWED = {
  // (T810) an assertion of a contract the host keeps and a test pins: a page that draws a trout table is always handed its
  // sources (`WaterViews` / `troutSourcesOf`). Drawing a table without its Source line would be a silent fallback (W46), so
  // the fault is left to the screen's boundary: the regulations view says it could not be drawn, and it is reported.
  "packages/ui-native/src/regs/KeepTab.tsx::KeepTab": { count: 1, why: "a trout table with no sources is a programming error (the host's contract)" },
  // the wider gate (helpers called from a render path, effects) found these; each is a contract assertion left to the boundary:
  "packages/ui/src/frameView.ts::middleOf": { count: 1, why: "a part with no part_geo row is a data-contract defect; never its first section said as its middle" },
  "packages/ui/src/waterMap.ts::waterMapView": { count: 2, why: "a state outside its group (a part, a mark) is a programming error: never clamped to a neighbour" },
  "packages/ui/src/waterMap.ts::waterMapStartIx": { count: 1, why: "a part not in its group is thrown, never clamped to the first" },
  "packages/ui-native/src/regs/AdiposeArt.tsx::adiposeArt": { count: 1, why: "a palette no theme wears has no printed hatch drawing: thrown, not drawn in guessed colours (pnpm adipose:build)" },
  "packages/map/src/web/useMapInstance.ts::useMapInstance>useEffect": { count: 1, why: "a map constructor failure that is not 'no WebGL' is a defect, thrown on to the boundary (no-WebGL is said on the map)" },
};

const isComponentName = (n) => /^[A-Z]/.test(n);
const isHookName = (n) => /^use[A-Z0-9]/.test(n);
const isAsync = (fn) => !!fn.modifiers?.some((m) => m.kind === ts.SyntaxKind.AsyncKeyword);
const isFn = (n) => ts.isFunctionDeclaration(n) || ts.isFunctionExpression(n) || ts.isArrowFunction(n) || ts.isMethodDeclaration(n)
  || ts.isGetAccessor(n) || ts.isSetAccessor(n) || ts.isConstructorDeclaration(n);

/** The name a function answers to, looking through `memo(...)` / `forwardRef(...)` wrappers. */
function nameOf(fn) {
  if (ts.isConstructorDeclaration(fn)) return "constructor";
  if (fn.name && (ts.isIdentifier(fn.name))) return fn.name.text;
  let p = fn.parent;
  while (p && ts.isCallExpression(p) && ((ts.isIdentifier(p.expression) && /^(memo|forwardRef)$/.test(p.expression.text))
    || (ts.isPropertyAccessExpression(p.expression) && /^(memo|forwardRef)$/.test(p.expression.name.text)))) p = p.parent;
  if (p && ts.isVariableDeclaration(p) && ts.isIdentifier(p.name)) return p.name.text;
  return null;
}

const isReactClass = (c) => ts.isClassLike(c) && !!c.heritageClauses?.some((h) => /\b(Pure)?Component\b/.test(h.getText()));
const nearestFn = (n) => { let f = n.parent; while (f && !isFn(f)) f = f.parent; return f ?? null; };

/** A render path on its own account: a component, a hook, a React class's lifecycle, a default-exported anonymous function. */
function ownLabel(fn, tsx) {
  if (isAsync(fn)) return null;
  if ((ts.isMethodDeclaration(fn) || ts.isConstructorDeclaration(fn)) && isReactClass(fn.parent)) {
    const m = ts.isConstructorDeclaration(fn) ? "constructor" : fn.name.getText();
    return CLASS_METHODS.has(m) ? `${fn.parent.name?.text ?? "class"}.${m}` : null;
  }
  const name = nameOf(fn);
  if (name && (isComponentName(name) || isHookName(name))) return name;
  if (!name && tsx && !isFn(fn.parent) && ts.isFunctionDeclaration(fn) && fn.modifiers?.some((m) => m.kind === ts.SyntaxKind.DefaultKeyword)) return "default";
  if (!name && tsx && ts.isExportAssignment(fn.parent)) return "default";
  return null;
}

/** Every render-path `throw` in one source text: `{ at: "file::function", line }`. */
export function renderThrows(file, text) {
  const tsx = file.endsWith("x");
  const sf = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, tsx ? ts.ScriptKind.TSX : ts.ScriptKind.TS);
  const fns = [], calls = new Map(), throws = [];
  const visit = (n) => {
    if (isFn(n)) fns.push(n);
    if (ts.isCallExpression(n) && ts.isIdentifier(n.expression)) {
      const list = calls.get(n.expression.text) ?? [];
      list.push(nearestFn(n));
      calls.set(n.expression.text, list);
    }
    if (ts.isThrowStatement(n)) throws.push(n);
    ts.forEachChild(n, visit);
  };
  visit(sf);

  const R = new Map();
  const callbackLabel = (fn) => {
    if (isAsync(fn)) return null;
    const call = fn.parent;
    if (!call || !ts.isCallExpression(call)) return null;
    const outer = nearestFn(call);
    const outerLabel = outer && R.get(outer);
    if (!outerLabel) return null;
    if (ts.isIdentifier(call.expression) && RENDER_CALLBACK.has(call.expression.text) && call.arguments[0] === fn) return `${outerLabel}>${call.expression.text}`;
    if (ts.isPropertyAccessExpression(call.expression) && ARRAY_METHODS.has(call.expression.name.text) && call.arguments[0] === fn) return `${outerLabel}>${call.expression.name.text}`;
    return null;
  };
  const helperLabel = (fn) => {
    if (isAsync(fn) || ts.isMethodDeclaration(fn)) return null;
    const name = nameOf(fn);
    if (!name || isComponentName(name) || isHookName(name) || /^on[A-Z]/.test(name)) return null;
    return (calls.get(name) ?? []).some((e) => e && e !== fn && R.has(e)) ? name : null;
  };
  // a hook's own callback is a render path where the enclosing function is one: found in rounds until nothing new
  for (let changed = true; changed;) {
    changed = false;
    for (const fn of fns) {
      if (R.has(fn)) continue;
      const label = ownLabel(fn, tsx) ?? callbackLabel(fn) ?? helperLabel(fn);
      if (label) { R.set(fn, label); changed = true; }
    }
  }
  return throws.flatMap((t) => {
    const fn = nearestFn(t);
    const label = fn && R.get(fn);
    return label ? [{ at: `${file}::${label}`, line: sf.getLineAndCharacterOfPosition(t.getStart()).line + 1 }] : [];
  });
}

function sources(dir, out = []) {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (/apps\/mobile\/(ios|android)$/.test(p)) continue;
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.tsx?$/.test(f) && !SKIP.test(p)) out.push(p);
  }
  return out;
}

/** The verdict on a list of found throws against an allow-list: new ones, wrong counts, stale entries. */
export function judge(found, allowed = ALLOWED) {
  const counts = {};
  for (const f of found) counts[f.at] = (counts[f.at] ?? 0) + 1;
  const problems = [];
  for (const f of found) if (!(f.at in allowed)) problems.push(`${f.at}:${f.line}: a throw in a render path (a fault ends that screen; draw the state instead, or list it in ALLOWED with its count and reason)`);
  for (const [k, v] of Object.entries(allowed)) {
    if (!(k in counts)) problems.push(`${k}: listed in ALLOWED but no longer throws; remove it`);
    else if (counts[k] !== v.count) problems.push(`${k}: ${counts[k]} throw(s) found, ALLOWED says ${v.count}; a new one is not listed (or one was fixed: lower the count)`);
  }
  return problems;
}

/** The scan over the app: every render-path throw found, and the problems (new throw, wrong count, stale entry). */
export function scan() {
  const found = [];
  for (const root of ROOTS) for (const p of sources(join(APP, root))) found.push(...renderThrows(relative(APP, p), readFileSync(p, "utf8")));
  return { found, problems: judge(found) };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { found, problems } = scan();
  if (process.argv.includes("--list")) { for (const f of found) console.log(`${f.at}:${f.line}`); process.exit(0); }
  for (const m of problems) console.error(m);
  if (problems.length) process.exit(1);
  console.log(`no-render-throw: ${found.length} listed render-path throw(s) in ${Object.keys(ALLOWED).length} function(s), no new ones`);
}
