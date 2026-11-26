#!/usr/bin/env node
/**
 * (T1164) NO `throw` IN A RENDER PATH. A throw while a component draws, or a hook runs, is caught only by the screen's
 * error boundary (`ui-native` `ScreenBoundary`): the screen is lost, the fault reported. Better never to throw there:
 * a state that can be reached is DRAWN (a line, `ErrorState`), a state that cannot be reached is made unreachable by the types.
 *
 * WHAT IT DETECTS (typescript compiler API, no new dependency): a `throw` statement in the BODY of a React component
 * (a function named with a capital: `function Foo`, `const Foo = () =>`, a `forwardRef`/`memo` of one), a hook (named
 * `useX`), a class `render`, or in a callback React runs while drawing (`useMemo`, a `useState` / `useReducer` initialiser
 * or reducer), in `packages/ui` hooks and every `packages/ui-native`, `packages/map` and `apps/mobile` source file.
 * WHAT IT DOES NOT: a throw in a function a component CALLS (core's presenters, a helper), which can still reach the
 * boundary; a throw in an event handler, an effect or a promise (not drawing: reported where caught, AGENTS W46); a
 * throw behind a dynamic call. It is a net for the direct, written-out case, not a proof of no throw.
 *
 * What is left is listed in ALLOWED: each site by file and function, with its reason. A new throw fails here: draw the
 * state instead, or, only when it is a programmer-error assertion that the boundary should catch, list it with its reason.
 *
 *   node tools/check-no-render-throw.mjs
 */
import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const APP = join(dirname(fileURLToPath(import.meta.url)), "..");
const ROOTS = ["packages/ui/src", "packages/ui-native/src", "packages/map/src", "apps/mobile/src"];
const SKIP = /\.test\.tsx?$|\.d\.ts$|\.generated\.tsx?$|\.fixture\.ts$|\/test\//;

/** The render-time callbacks: the hook, and which argument is called while drawing. */
const RENDER_CALLBACK = { useMemo: 0, useState: 0, useReducer: 0 };

/**
 * Each allowed throw: `file::function` -> why. One site today (the count is pinned by `check-no-render-throw.test.ts`).
 */
export const ALLOWED = {
  // (T810) an assertion of a contract the host keeps and a test pins: a page that draws a trout table is always handed its
  // sources (`WaterViews` / `troutSourcesOf`). Drawing a table without its Source line would be a silent fallback (W46), so
  // the fault is left to the screen's boundary: the regulations view says it could not be drawn, and it is reported.
  "packages/ui-native/src/regs/KeepTab.tsx::KeepTab": "a trout table with no sources is a programming error (the host's contract)",
};

const isComponentName = (n) => /^[A-Z]/.test(n);
const isHookName = (n) => /^use[A-Z0-9]/.test(n);

/** The name a function expression/arrow/declaration answers to, looking through `memo(...)` / `forwardRef(...)` wrappers. */
function nameOf(fn) {
  if (fn.name && ts.isIdentifier(fn.name)) return fn.name.text;
  let p = fn.parent;
  while (p && ts.isCallExpression(p) && ts.isIdentifier(p.expression) && /^(memo|forwardRef)$/.test(p.expression.text)) p = p.parent;
  if (p && ts.isCallExpression(p) && ts.isPropertyAccessExpression(p.expression) && /^(memo|forwardRef)$/.test(p.expression.name.text)) p = p.parent;
  if (p && ts.isVariableDeclaration(p) && ts.isIdentifier(p.name)) return p.name.text;
  return null;
}

const isFn = (n) => ts.isFunctionDeclaration(n) || ts.isFunctionExpression(n) || ts.isArrowFunction(n) || ts.isMethodDeclaration(n)
  || ts.isGetAccessor(n) || ts.isSetAccessor(n) || ts.isConstructorDeclaration(n);

/** What the nearest enclosing function of a node is, as a render path: its label, or null when it is not one. */
function renderPath(fn) {
  if (ts.isMethodDeclaration(fn) && fn.name.getText() === "render" && ts.isClassLike(fn.parent)) return `${fn.parent.name?.text ?? "class"}.render`;
  const name = nameOf(fn);
  if (name && (isComponentName(name) || isHookName(name)) && !fn.modifiers?.some((m) => m.kind === ts.SyntaxKind.AsyncKeyword)) return name;
  // a callback React runs while drawing: `useMemo(() => …)`, `useState(() => …)`, `useReducer((s, a) => …)`
  const call = fn.parent;
  if (call && ts.isCallExpression(call) && ts.isIdentifier(call.expression) && call.expression.text in RENDER_CALLBACK
      && call.arguments[RENDER_CALLBACK[call.expression.text]] === fn) {
    let up = call.parent;
    while (up && !isFn(up)) up = up.parent;
    const outer = up && renderPath(up);
    return outer ? `${outer}>${call.expression.text}` : null;
  }
  return null;
}

/** Every render-path `throw` in one source text: `{ at: "file::function", line }`. */
export function renderThrows(file, text) {
  const sf = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, file.endsWith("x") ? ts.ScriptKind.TSX : ts.ScriptKind.TS);
  const out = [];
  const visit = (node) => {
    if (ts.isThrowStatement(node)) {
      let fn = node.parent;
      while (fn && !isFn(fn)) fn = fn.parent;
      const label = fn ? renderPath(fn) : null;
      if (label) out.push({ at: `${file}::${label}`, line: sf.getLineAndCharacterOfPosition(node.getStart()).line + 1 });
    }
    ts.forEachChild(node, visit);
  };
  visit(sf);
  return out;
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

/** The scan over the app: every render-path throw found, and the verdict against ALLOWED. */
export function scan() {
  const found = [];
  for (const root of ROOTS) for (const p of sources(join(APP, root))) {
    const rel = relative(APP, p);
    // packages/ui holds hooks and plain functions: only its `useX` hooks are render paths (its .ts has no components)
    found.push(...renderThrows(rel, readFileSync(p, "utf8")));
  }
  const counts = {};
  for (const f of found) counts[f.at] = (counts[f.at] ?? 0) + 1;
  const fresh = found.filter((f) => !(f.at in ALLOWED));
  const stale = Object.keys(ALLOWED).filter((k) => !(k in counts));
  return { found, fresh, stale };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { found, fresh, stale } = scan();
  if (process.argv.includes("--list")) { for (const f of found) console.log(`${f.at}:${f.line}`); process.exit(0); }
  for (const f of fresh) console.error(`${f.at}:${f.line}: a throw in a render path (a screen's fault ends that screen; draw the state instead, or list it in ALLOWED with its reason)`);
  for (const k of stale) console.error(`${k}: listed in ALLOWED but no longer throws; remove it`);
  if (fresh.length || stale.length) process.exit(1);
  console.log(`no-render-throw: ${found.length} listed render-path throw(s), no new ones`);
}
