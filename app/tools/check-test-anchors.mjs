#!/usr/bin/env node
/**
 * (T667 work item 4) TEST ANCHORS, the app half (the pipeline half is `pipeline/tests/anchors.py`, same rule).
 *
 * A test stands on ONE of four anchors (T667-audit §3): (a) data it builds itself, (b) a frozen committed fixture (the
 * three `fixtureRelease(...)` releases, `makeFixtureSource`), (c) THE RELEASE UNDER TEST through the allowed helpers,
 * (d) a generated expectations file read through its reader (`taps()`, `hydrate()`, `pageOracle()`). This scanner finds the test files
 * that stand on something else, or that name a POSITION of one build. It walks the TypeScript syntax tree, so a comment
 * is never an offender: only code is.
 *
 * THE ALLOWED HELPERS
 *   the release under test   `liveRelease()` / `liveReleaseAt()` (`conformance/src/fixtureRelease.ts`): $RELEASE_DIR —
 *                            which the release command sets to its CANDIDATE — else the release LATEST names
 *   the regs fixture release `openRegsFixture()` / `openRegsFixtureWithLines()` / `regsFixtureDir()` /
 *                            `regsFixtureLines()` (`@app/data/release/node`), re-cut by `tools/regs-recut/sync.mjs`
 *   a committed data file    `repoData("curated/waters/cuts.json")` (`conformance/src/repoData.ts`, node-side tests only): the
 *                            path `data/curated/…`, checked to exist — the name carries no `data/` prefix, so it is no
 *                            `path` read; a typed "data/curated/…" string still is one
 *   names, not numbers       `sectionNames(loader)` (same module): `.stream(key, metre)`, `.lake(item, part)`,
 *                            `.part(item, lo, hi)` / `.partAt(item, key, metre)` (a part by its ends / by a place)
 *
 * WHAT IS AN OFFENDER (every `*.test.ts|tsx|mjs` under app/)
 *
 * READS, in any test file — the live tree reached without a helper:
 *   path        a string that IS a path into the repo's data: it starts (after any `../`) with
 *               `data/release|generated|curated|source`, standing alone or handed to `join` / `resolve` after a repo
 *               root (`REPO`, `ROOT`, `REPO_ROOT`, `__dirname`). After any other first argument
 *               (`join(tmp, "data/release")`) it is the test's own tree and is fine; so is a test's title.
 *   pointer     the string "LATEST" or "CANDIDATE" in a file that also has a `path` read: which release is under test
 *               is the helper's answer.
 *   env         `process.env.RELEASE_DIR` (or `env.RELEASE_DIR`, `["RELEASE_DIR"]`) read in a test: the helper owns it.
 *
 * PINS, only in a TEST BLOCK THAT FOLLOWS A RELEASE, because a number in a test that builds its own data names that
 * data (`99_999 as SectionId` beside `makeFixtureSource` or a hand-made store is fine). A test block is a top-level
 * `describe` / `it` / `test` statement. It follows a release when it has a read above, names `regs-release` /
 * `section-lines.json` / `fixtures/golden`, or uses (outside a type) a release-following reader — `FOLLOWS`: the
 * helpers above, `parityStore`, ...; `taps` / `hydrate` / `cases` / ... imported from the golden modules — or a
 * module-level name bound from one (`const store = openRegsStore(openRegsFixture())`, a helper function that opens it,
 * a `let` a top-level `beforeAll` fills), however many steps away. A module-level constant is a pin when a following
 * block uses it. A file wrapped in one `describe` is one block.
 *   raw-section a section handle typed as a number: `<n> as SectionId` (also inside an array cast to it),
 *               `sectionLine(<n>)`, or `<n>` as the value of `sid` / `tapSid` / `sectionId` / `handle`. 0 is "no
 *               section", never a name.
 *   raw-part    a part typed as a number: `<n>` as the value of `partIx` / `part_ix`, the number after a water id
 *               (`f("gnis:2936", 2)`), or `.lake(item, <n>)`. Part 0 is a water's first part in river order (its mouth
 *               end, or its only part) and names that end; any other number counts the cuts below it, which move.
 *               NOT one: the stream key in the helper's own call `<names>.partAt("<water id>", <stream key>, <metre>)`
 *               — a METHOD call of exactly three arguments with the water id FIRST. The number after the water id
 *               there is a blue line, a name. `@app/core`'s function `partAt(g, "<water id>", <part>)` (the water id
 *               second, no receiver) does take a part number and is still one.
 *   in-text     (T680, r-483b295de57c: "panel for 578 at 49.1" failed only in CI, after the push) a section or part
 *               number TYPED INTO AN EXPECTED STRING where the file's own template puts one: a template
 *               `…panel for ${section} at …` (the slot's expression ends in `section` / `sectionId` / `sid` / `tapSid`
 *               / `handle`, or `…Sid`: a section; `part` / `partIx` / `part_ix`: a part) makes the string
 *               "panel for 578 at" a raw-section (a raw-part for a part slot). The text before the slot must hold a
 *               letter and 3+ characters, and the number is the whole run of digits there; a slot that ENDS the template
 *               must end the string ("regs on gnis:8634 part 3" is one; a label "…, part 1 of 5 · …" is not); 0 names
 *               nothing. Write the
 *               expected string from the name instead: `panel for ${await names.stream(key, metre)} at …`.
 *   release-id  a string holding a real-looking release id `r-<12 hex>` (5+ different hex digits). In ANY file: the id
 *               of a committed fixture release typed out, where `FIXTURE_RELEASES` / `regsFixtureDir()` names it.
 *   digest      a string that IS a build digest: 12-64 hex digits (6+ different, letters and digits both), or a tiles
 *               id `<16 hex>.<12 hex>`. A counting run (`0123456789abcdef`) is invented and fine, and so is a tiles id
 *               whose two halves are both counting runs (`0123456789abcdef.0123456789ab`).
 *
 * Not scanned here (later T667 slices): literal counts on live data (D3), golden file reads past `taps()`.
 *
 * `sectionNames.part` / `.partAt` name a part; `regsFixtureLines().unnamed` is the fixture's unnamed reach, read from its own file.
 *
 * THE GUARD IS ON (2026-11-11, owner D4: every offender fails, no baseline, no list of excused files). `pnpm anchors`
 * (in `pnpm check` / `check:fast`) and `scripts/fast-fail.sh` run it.
 *   node tools/check-test-anchors.mjs            exit 1 while one offender is left: every hit and what to write instead
 *                                                (`--strict` is accepted and means the same; `--json`: the hits as JSON)
 *   node tools/check-test-anchors.mjs --report   the counts only, exit 0 (`--list`: + every hit). For reading: no check
 *                                                may use it.
 * A rule that is wrong for a case is changed HERE, with a test for the accepted case and for the near-miss that must
 * still fail; a hit is never reworded past the scanner.
 */
import { execFileSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const APP = join(dirname(fileURLToPath(import.meta.url)), "..");

export const READ_KINDS = ["path", "pointer", "env"];
export const PIN_KINDS = ["raw-section", "raw-part", "release-id", "digest"];
export const KINDS = [...READ_KINDS, ...PIN_KINDS];

/** names that make the code using them release-following, wherever they come from */
const FOLLOWS = new Set(["openRegsFixture", "openRegsFixtureWithLines", "regsFixtureDir", "regsFixtureLines",
  "REGS_FIXTURE_ROOT", "sectionNames", "liveRelease", "liveReleaseAt", "provinceStoreOf", "parityStore", "paritySource",
  "openPaired", "releasePairing"]);
/** imported names that do so only from the golden / oracle / pointer modules (the names are common words) */
const FOLLOWS_FROM = new Set(["taps", "tapsOnce", "hydrate", "pageOracle", "requestOf", "cases", "latest", "latestId"]);
const FROM = /golden|oracles|latestRelease|regs\/testing/;
const FOLLOWS_TEXT = /regs-release|section-lines\.json|fixtures\/golden/;

const DATA_PATH = /^(?:\.\.\/)*(?:\.\/)?data\/(?:release|generated|curated|source)(?:\/|$)/;
const REPO_ROOTS = new Set(["REPO", "ROOT", "REPO_ROOT", "__dirname"]);
const TITLED = new Set(["it", "test", "describe"]);
const SECTION_KEYS = new Set(["sid", "tapSid", "sectionId", "handle"]);
const PART_KEYS = new Set(["partIx", "part_ix"]);
const WATER_ID = /^(?:gnis|wbk|fwa|blk|area|zp):\S+$/;
const RID = /\br-([0-9a-f]{12})\b/g;
const HEX = /^[0-9a-f]{12,64}$/;
const TILES = /^[0-9a-f]{16}\.[0-9a-f]{12}$/;
const RUN = "0123456789abcdef".repeat(5);

/** what to write instead, per kind (printed with a failure) */
export const INSTEAD = {
  path: `the release under test: liveRelease() / namedRelease() (conformance/src/fixtureRelease.ts); a committed data file: repoData("curated/…") (conformance/src/repoData.ts); the regs fixture: openRegsFixture()`,
  pointer: `liveRelease() answers which release is under test (the candidate during a release, else LATEST)`,
  env: `namedRelease() (the release the run named) or liveRelease(): the helper reads RELEASE_DIR`,
  "raw-section": `name the section: await sectionNames(loader).stream(<stream key>, <start metre>) (a lake: .lake(item, part))`,
  "raw-part": `name the part: await sectionNames(loader).part(item, <lo end>, <hi end>), or .partAt(item, <stream key>, <metre>)`,
  "release-id": `regsFixtureDir() / FIXTURE_RELEASES name a committed release; the live one is liveRelease().id; an invented id is r-000000000001`,
  digest: `read it from the release (its manifest / store meta); an invented one is a counting run: "0123456789abcdef"`,
};

/** The release ids in `text` that look real (5+ different hex digits). */
export function realReleaseIds(text) {
  return [...text.matchAll(RID)].filter((m) => new Set(m[1]).size >= 5).map((m) => m[0]);
}

const countingRun = (text) => RUN.includes(text) || RUN.includes([...text].reverse().join(""));

export function isDigest(text) {
  if (TILES.test(text)) return new Set(text).size >= 6 && !text.split(".").every(countingRun);
  if (!HEX.test(text) || countingRun(text)) return false;
  return new Set(text).size >= 6 && /[0-9]/.test(text) && /[a-f]/.test(text);
}

/** The ids of the committed fixture releases (`packages/data/dev/fixture-release/r-*`, `dev/regs-release/r-*`). */
export function committedReleaseIds(app = APP) {
  const out = [];
  for (const d of ["packages/data/dev/fixture-release", "packages/data/dev/regs-release"]) {
    const p = join(app, d);
    if (existsSync(p)) out.push(...readdirSync(p).filter((n) => /^r-[0-9a-f]{12}$/.test(n)));
  }
  return out;
}

const num = (n) => ts.isNumericLiteral(n) || (ts.isPrefixUnaryExpression(n) && ts.isNumericLiteral(n.operand));
/** A number used as a NAME: any but 0 (section 0 is "no section"; part 0 is a water's first part in river order). */
const named = (n) => num(n) && Number(n.getText().replace(/_/g, "")) !== 0;
/** A naming number, or an array holding one (what a cast to SectionId covers). */
function holdsNumber(n) {
  if (named(n)) return true;
  if (ts.isParenthesizedExpression(n) || ts.isAsExpression(n)) return holdsNumber(n.expression);
  if (ts.isArrayLiteralExpression(n)) return n.elements.some(holdsNumber);
  return false;
}
const callee = (c) => ts.isIdentifier(c.expression) ? c.expression.text
  : ts.isPropertyAccessExpression(c.expression) ? c.expression.name.text : null;
const text = (n) => ts.isStringLiteralLike(n) ? n.text : ts.isTemplateHead(n) || ts.isTemplateMiddle(n) || ts.isTemplateTail(n) ? n.text : null;
const keyName = (p) => ts.isIdentifier(p.name) || ts.isStringLiteralLike(p.name) ? p.name.text : null;

/** The root name of a call chain: `describe.skipIf(x)("…", f)` -> "describe". */
function rootName(e) {
  for (;;) {
    if (ts.isIdentifier(e)) return e.text;
    if (ts.isCallExpression(e) || ts.isPropertyAccessExpression(e)) e = e.expression;
    else return null;
  }
}
const isBlock = (st) => ts.isExpressionStatement(st) && ts.isCallExpression(st.expression) && TITLED.has(rootName(st.expression) ?? "");

/** Names a statement binds: its declarations (destructured too), its function / class, and what it assigns. */
function boundNames(st) {
  const out = new Set();
  const bind = (n) => {
    if (ts.isIdentifier(n)) out.add(n.text);
    else if (ts.isObjectBindingPattern(n) || ts.isArrayBindingPattern(n)) n.elements.forEach((e) => ts.isBindingElement(e) && bind(e.name));
  };
  if (ts.isVariableStatement(st)) st.declarationList.declarations.forEach((d) => bind(d.name));
  else if ((ts.isFunctionDeclaration(st) || ts.isClassDeclaration(st)) && st.name) out.add(st.name.text);
  const assigned = (n) => {
    if (ts.isBinaryExpression(n) && n.operatorToken.kind === ts.SyntaxKind.EqualsToken && ts.isIdentifier(n.left)) out.add(n.left.text);
    ts.forEachChild(n, assigned);
  };
  if (!ts.isVariableStatement(st)) assigned(st);
  return out;
}

const SECTION_SLOT = /^(?:section|sectionId|sid|tapSid|handle)$|Sid$/;
const PART_SLOT = /^(?:part|partIx|part_ix)$/;
const escapeRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/**
 * (T680 in-text) What the file's own templates put a section or part number into: `[RegExp, kind, template text]` per
 * slot. The regex finds the number typed where the slot goes, in any string (`(\d+)` is the number).
 */
export function slotPatterns(sf) {
  const out = [];
  const visit = (n) => {
    if (ts.isTemplateExpression(n)) {
      let before = n.head.text;
      for (const span of n.templateSpans) {
        const last = span.expression.getText(sf).split(/[.?!\s()]+/).filter(Boolean).pop() ?? "";
        const kind = SECTION_SLOT.test(last) ? "raw-section" : PART_SLOT.test(last) ? "raw-part" : null;
        if (kind && /[A-Za-z]/.test(before) && before.trim().length >= 3) {
          const after = span.literal.text;
          // a slot that ends the template ends the string it makes: "… part 3" is one, "part 1 of 5 · …" (a label) is not
          out.push([new RegExp(`${escapeRe(before)}(\\d[\\d_]*)${after ? escapeRe(after) : "$"}`), kind,
            n.getText(sf).replace(/\s+/g, " ").slice(0, 60)]);
        }
        before = span.literal.text;
      }
    }
    ts.forEachChild(n, visit);
  };
  visit(sf);
  return out;
}

/**
 * Every offender of one test file: `[line, kind, what]`, in line order. `committed`: the committed fixture releases' ids
 * (an id typed out is an offender in any file).
 */
export function scanSource(source, name = "x.test.tsx", committed = []) {
  const sf = ts.createSourceFile(name, source, ts.ScriptTarget.Latest, true,
    /\.tsx$/.test(name) ? ts.ScriptKind.TSX : /\.mjs$/.test(name) ? ts.ScriptKind.JS : ts.ScriptKind.TS);
  const reads = [], anywhere = [], pointers = [];
  const taint = new Set(FOLLOWS);
  const slots = slotPatterns(sf);
  const line =(n) => sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1;
  /** per top-level statement: the identifiers it uses outside types, whether it reads the release by text, its pins */
  const stmts = sf.statements.map((st) => ({ st, ids: new Set(), reads: false, pins: [], block: isBlock(st) }));
  let cur = null;
  const visit = (n) => {
    if (ts.isImportDeclaration(n)) {
      const from = ts.isStringLiteral(n.moduleSpecifier) ? n.moduleSpecifier.text : "", named = n.importClause?.namedBindings;
      for (const el of named && ts.isNamedImports(named) ? named.elements : []) {
        const id = (el.propertyName ?? el.name).text;
        if (FOLLOWS.has(id) || (FOLLOWS_FROM.has(id) && FROM.test(from))) taint.add(el.name.text);
      }
      return;
    }
    if (ts.isTypeNode(n)) return;                                    // `typeof openRegsFixture` in a type reads nothing
    if (ts.isIdentifier(n)) cur.ids.add(n.text);
    const s = text(n);
    if (s !== null) {
      if (FOLLOWS_TEXT.test(s)) cur.reads = true;
      const p = n.parent;
      const titled = ts.isCallExpression(p) && p.arguments[0] === n && TITLED.has(rootName(p) ?? "");
      if (!titled) {
        if (DATA_PATH.test(s)) {
          const joined = ts.isCallExpression(p) && ["join", "resolve"].includes(callee(p) ?? "") && p.arguments[0] !== n;
          const first = joined ? p.arguments[0] : null;
          const own = first && !(ts.isIdentifier(first) && REPO_ROOTS.has(first.text)) &&
            !(ts.isStringLiteralLike(first) && /^(\.\.\/|\.\.$)/.test(first.text));
          if (!own) { reads.push([line(n), "path", s.slice(0, 60)]); cur.reads = true; }
        }
        if (s === "LATEST" || s === "CANDIDATE") pointers.push([line(n), "pointer", s]);
        const rids = realReleaseIds(s);
        for (const r of rids) (committed.includes(r) ? anywhere : cur.pins).push([line(n), "release-id", r]);
        if (!rids.length && isDigest(s)) cur.pins.push([line(n), "digest", s.slice(0, 16)]);
        for (const [re, kind, tpl] of slots) {
          const m = re.exec(s);
          if (m && Number(m[1].replace(/_/g, "")) !== 0) cur.pins.push([line(n), kind, `"${m[0]}" typed where ${tpl} puts it`]);
        }
      }
    }
    if ((ts.isPropertyAccessExpression(n) && n.name.text === "RELEASE_DIR") ||
        (ts.isElementAccessExpression(n) && ts.isStringLiteralLike(n.argumentExpression) && n.argumentExpression.text === "RELEASE_DIR")) {
      if (/\benv$/.test(n.expression.getText(sf))) { reads.push([line(n), "env", "RELEASE_DIR"]); cur.reads = true; }
    }
    if (ts.isAsExpression(n) && /\bSectionId\b/.test(n.type.getText(sf)) && holdsNumber(n.expression))
      cur.pins.push([line(n), "raw-section", n.getText(sf).replace(/\s+/g, " ").slice(0, 50)]);
    if (ts.isCallExpression(n)) {
      const f = callee(n), a = n.arguments;
      if (f === "sectionLine" && a[0] && named(a[0])) cur.pins.push([line(n), "raw-section", `sectionLine(${a[0].getText(sf)})`]);
      if (f === "lake" && a[1] && named(a[1])) cur.pins.push([line(n), "raw-part", `lake(…, ${a[1].getText(sf)})`]);
      // `<names>.partAt("<water id>", <stream key>, <metre>)`: the helper's call, the number is a blue line (header)
      const byPlace = f === "partAt" && ts.isPropertyAccessExpression(n.expression) && a.length === 3 &&
        ts.isStringLiteralLike(a[0]) && WATER_ID.test(a[0].text);
      if (f !== "lake" && !byPlace) a.forEach((x, i) => {
        if (i > 0 && named(x) && ts.isStringLiteralLike(a[i - 1]) && WATER_ID.test(a[i - 1].text))
          cur.pins.push([line(x), "raw-part", `"${a[i - 1].text}", ${x.getText(sf)}`]);
      });
    }
    if (ts.isPropertyAssignment(n) && named(n.initializer)) {
      const k = keyName(n);
      if (k && SECTION_KEYS.has(k)) cur.pins.push([line(n), "raw-section", n.getText(sf).slice(0, 50)]);
      if (k && PART_KEYS.has(k)) cur.pins.push([line(n), "raw-part", n.getText(sf).slice(0, 50)]);
    }
    ts.forEachChild(n, visit);
  };
  for (const x of stmts) { cur = x; visit(x.st); }
  if (reads.some((r) => r[1] === "path")) reads.push(...pointers);

  // which statements follow the release: they read it, or use a name bound by a module-level statement that does
  const follows = (x) => x.reads || [...x.ids].some((i) => taint.has(i));
  for (let grew = true; grew;) {
    grew = false;
    for (const x of stmts) if (!x.block && follows(x)) for (const b of boundNames(x.st)) if (!taint.has(b)) { taint.add(b); grew = true; }
  }
  const blocks = stmts.filter((x) => x.block && follows(x));
  const pins = [];
  for (const x of stmts) {
    if (!x.pins.length) continue;
    const used = () => { const b = boundNames(x.st); return blocks.some((k) => [...b].some((i) => k.ids.has(i))); };
    if (follows(x) || (!x.block && used())) pins.push(...x.pins);
  }
  const out = [...reads, ...anywhere, ...pins];
  const seen = new Set();
  return out.sort((a, b) => a[0] - b[0] || KINDS.indexOf(a[1]) - KINDS.indexOf(b[1]))
    .filter((h) => { const k = h.join("|"); return seen.has(k) ? false : (seen.add(k), true); });
}

/** The app's test files, app-relative: tracked and new ones (git's own ignore rules: no node_modules, no build output). */
export function testFiles(app = APP) {
  return execFileSync("git", ["ls-files", "--cached", "--others", "--exclude-standard", "--", "*.test.ts", "*.test.tsx", "*.test.mjs"],
    { cwd: app, encoding: "utf8", maxBuffer: 1 << 26 }).split("\n").filter((f) => f && existsSync(join(app, f))).sort();
}

/** `{file: hits}` for every offending test file (files with no hit are left out). */
export function scan(app = APP, files = testFiles(app)) {
  const committed = committedReleaseIds(app), out = {};
  for (const f of files) {
    const hits = scanSource(readFileSync(join(app, f), "utf8"), f, committed);
    if (hits.length) out[f] = hits;
  }
  return out;
}

/** `{kind: [files, hits]}` in `KINDS` order. */
export function totals(found) {
  const all = Object.values(found);
  return Object.fromEntries(KINDS.map((k) => [k, [all.filter((h) => h.some((x) => x[1] === k)).length,
    all.reduce((n, h) => n + h.filter((x) => x[1] === k).length, 0)]]));
}

export function report(found, nFiles) {
  return [`test anchors (app): ${Object.keys(found).length} of ${nFiles} test files offend`,
    ...Object.entries(totals(found)).map(([k, [f, h]]) => `  ${k.padEnd(12)} ${String(f).padStart(4)} files ${String(h).padStart(5)} hits`)].join("\n");
}

/** The guard is on: every run fails on an offender. `--report` (counts only, exit 0) is for reading. */
export const strict = (argv = []) => !argv.includes("--report");

/** What a failing run says after the hits: what to write instead for each kind hit. */
export function advice(found) {
  const hit = KINDS.filter((k) => Object.values(found).some((h) => h.some((x) => x[1] === k)));
  return [`test anchors: ${Object.keys(found).length} test file(s) off their anchor (T667, AGENTS W46). A test stands on data it builds, ` +
    `a frozen fixture, or the release under test through the helpers. Instead:`,
    ...hit.map((k) => `  ${k}: ${INSTEAD[k]}`),
    `  the whole rule: the header of app/tools/check-test-anchors.mjs. If the rule is wrong for your case, change it there ` +
    `with a test; never reword the hit past the scanner, and there is no list of excused files.`].join("\n");
}

export function main(argv, app = APP, log = console.log) {
  const files = testFiles(app), found = scan(app, files), fail = strict(argv) && Object.keys(found).length > 0;
  if (argv.includes("--json")) log(JSON.stringify(found, null, 1));
  else {
    log(report(found, files.length));
    if (argv.includes("--list") || fail)
      for (const [f, hits] of Object.entries(found)) for (const [l, k, w] of hits) log(`${f}:${l}  ${k}: ${w}`);
  }
  if (fail) log(advice(found));
  return fail ? 1 : 0;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) process.exit(main(process.argv.slice(2)));
