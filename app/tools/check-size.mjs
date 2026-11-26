#!/usr/bin/env node
/**
 * (T1081) THE SIZE GUARD: the docs stay lean. `check-tickets.mjs` runs it (so docs-ci, `pnpm tickets` and the pre-push
 * hook all do); `node app/tools/check-size.mjs --measure` prints today's sizes and the exception maps they would need
 * (the one-line way to re-set a budget when a cut lands).
 *
 * Every number is in BUDGETS, with its reason; the exception maps below it name each file allowed to be bigger, with its
 * own budget. A failure says the file, its size, the budget and what to do: cut it, or raise the budget in the same PR
 * with a reason. One `git ls-files` and the markdown files read: no more.
 *
 * `checkSize` is pure (the test feeds it a made-up tree); the CLI at the end reads the repo and git.
 */
import { execFileSync } from "node:child_process";
import { readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { KEPT_ASSETS, TICKETS_DIR, assetIds, readWorkTree } from "./ledger.mjs";

/** The docs folder (the planning pages, runbooks, restart pages). The tickets folder (ledger.mjs) sits inside it today. */
export const DOCS_DIR = "handoff/PLAN";
/** The trees the totals count: every tracked path under one of these (a file under both counts once). */
export const TREE = [DOCS_DIR, TICKETS_DIR];

/** Every limit, in one place: [value, why]. A limit is raised in the PR that needs it, with the reason in this table. */
export const BUDGETS = {
  // required reading for a new chat: it reads all of these before it starts, so each is a cost paid by every chat
  readme:       [80, "the repo's front page: what it is and where to go next"],
  startHere:    [30, "a new chat's first page: it points, it holds nothing"],
  agents:       [450, "the rules every chat reads in full before it works"],
  status:       [60, "the release state: the top entry is what a chat needs, older entries are cut"],
  restart:      [60, "one short page per live chat, deleted when the chat ends"],
  decisions:    [650, "the decision log a chat reads before it changes a decided thing"],
  // the rest of the tree
  subReadme:    [40, "a package's README says what it is and where its parts are; detail lives in the code and its tests"],
  ticket:       [150, "an open ticket: the ask, the plan, the owner block; a long history is cut into closed.tsv's archive"],
  anyMarkdown:  [700, "past this no one reads a markdown file whole (generated / reference files are listed below)"],
  // the docs tree (handoff/ today; docs/ + tickets/ after the rename): set at today's value + 10%, re-set with --measure
  treeLines:    [41836, "tracked markdown lines in the docs tree"],
  treeFiles:    [1676, "tracked files in the docs tree"],
  treePictureBytes: [174437113, "bytes of tracked png/jpg/webp/gif in the docs tree (pictures live in git forever)"],
};

/** README.md files under app/ or pipeline/ allowed to pass BUDGETS.subReadme: path -> its own budget (today's size + 10%). */
export const README_LONG = {
  "app/README.md": 88,
  "app/tools/maplibre-android/README.md": 74,
  "app/tools/maplibre-android/archive-t420/upstream/README.md": 57,
  "app/tools/testbed/README.md": 153,
  "pipeline/README.md": 140,
  "pipeline/atlas/waters/added_lakes/README.md": 114,
  "pipeline/atlas/waters/added_streams/README.md": 201,
  "pipeline/regs/dfo_salmon/README.md": 72,
};

/** TEMPORARY: open tickets over BUDGETS.ticket today, each held at its size on 2026-11-25 so it can only shrink. The ticket
 *  cut (T1081) takes them off this list as it shortens them; an entry that is gone or under the cap fails ("take it off"). */
export const TICKET_OVER = {
  "T1029-one-bank-stripe.md": 399,
  "T1031-sources-inline-species-tabs.md": 151,
  "T1039-gauge-page-app-chart.md": 164,
  "T1041-faster-ci-feedback.md": 322,
  "T1053-walk-stops-at-head-lake.md": 306,
  "T1072-store-screenshots-and-marketing-content.md": 158,
  "T234-push-notifications.md": 216,
  "T240-production-launch-checklist.md": 242,
  "T319-store-screenshots-auto.md": 190,
  "T360-app-memory-footprint.md": 226,
  "T634-lake-tab-depth-and-stocking.md": 256,
  "T667-tests-that-grow.md": 456,
  "T678-tidal-in-the-build.md": 243,
  "T679-decisions-in-their-own-stage.md": 322,
  "T708-missing-cuts-audit-fixes.md": 337,
  "T723-ios-screens-since-1105.md": 242,
  "T737-web-first-tap-waits-for-prefetch.md": 318,
  "T739-push-wake-ups.md": 536,
  "T741-faster-lighter-both-platforms.md": 353,
  "T746-applying-new-data-shows-progress.md": 179,
  "T760-first-install-waits-on-a-system-download-that-never-moves.md": 255,
  "T810-trout-table-build-from-the-pipeline-to-the-screen.md": 587,
  "T993-smoother-release-run.md": 276,
};

/** Markdown files allowed over BUDGETS.anyMarkdown: path -> its own budget (today's size + 10%). */
export const MARKDOWN_LONG = {
  "handoff/PLAN/app-integration/T1140-files/multi-part-waters.md": 840,   // 763
  "handoff/PLAN/app-integration/T678-progress.md": 1059,   // 962
  "handoff/PLAN/design/trout-table-contract.md": 889,   // 808
  "pipeline/regs/parsing/prompts/CATALOGUE_PARSE_PROMPT.md": 777,   // 706
};

const PICTURE = /\.(png|jpe?g|webp|gif)$/i;
const MARKDOWN = /\.md$/i;
const base = (p) => p.slice(p.lastIndexOf("/") + 1);
const under = (p, dir) => p.startsWith(`${dir}/`);
/** The lines of a document, as the other guards count them (the trailing newline is no line). */
export const lineCount = (text) => text.replace(/\n+$/, "").split("\n").length;

const FIX = "cut it, or raise the budget in the same PR with a reason";
const over = (path, size, unit, max, why) => `${path}: ${size} ${unit}, budget ${max} (${why}): ${FIX}`;

/** The folders that must each hold a README.md: app packages, app/conformance, app/tools, and each pipeline package. */
export function packageDirs(paths) {
  const out = new Set(["app/conformance", "app/tools"]);
  for (const p of paths) {
    const m = p.match(/^(app\/(?:apps|packages|workers)\/[^/]+)\/./) ?? p.match(/^(pipeline\/[^/]+)\/__init__\.py$/);
    if (m) out.add(m[1]);
  }
  return [...out].sort();
}

/**
 * Every way the docs are over their budgets, as one sorted list (empty = lean).
 *
 * @param {{ files: { path: string, lines?: number, bytes?: number }[], closedIds: Iterable<string>, kept?: Record<string, string> }} input
 *   files — every tracked path; `lines` for each markdown file, `bytes` for each picture under the docs tree;
 *   closedIds — the ids of closed.tsv; kept — KEPT_ASSETS (picture folders a document or a test still reads);
 *   readmeLong, ticketOver, markdownLong — the exception maps (the tests pass their own)
 */
export function checkSize({ files, closedIds, kept = KEPT_ASSETS, readmeLong = README_LONG, ticketOver = TICKET_OVER, markdownLong = MARKDOWN_LONG }) {
  if (!Array.isArray(files) || !files.length) throw new Error("checkSize needs the tracked files: never \"no files to check\"");
  const problems = [];
  const byPath = new Map(files.map((f) => [f.path, f]));
  const lines = (p) => byPath.get(p)?.lines ?? 0;
  const [B, why] = [(k) => BUDGETS[k][0], (k) => BUDGETS[k][1]];

  // required reading
  const reading = [["README.md", "readme"], [`${DOCS_DIR}/START-HERE.md`, "startHere"], ["AGENTS.md", "agents"],
                   [`${DOCS_DIR}/STATUS.md`, "status"], [`${DOCS_DIR}/app-integration/decisions.md`, "decisions"]];
  for (const [p, k] of reading) {
    if (!byPath.has(p)) problems.push(`${p}: not tracked, but its budget (${k}) names it: fix the path in BUDGETS' reading list`);
    else if (lines(p) > B(k)) problems.push(over(p, lines(p), "lines", B(k), why(k)));
  }
  for (const f of files)
    if (under(f.path, `${DOCS_DIR}/restart`) && MARKDOWN.test(f.path) && f.lines > B("restart"))
      problems.push(over(f.path, f.lines, "lines", B("restart"), why("restart")));

  // a README under app/ or pipeline/ is short unless it is listed; a package has one
  for (const f of files) {
    if (base(f.path) !== "README.md" || !/^(app|pipeline)\//.test(f.path)) continue;
    const max = readmeLong[f.path] ?? B("subReadme");
    if (f.lines > max) problems.push(over(f.path, f.lines, "lines", max, why("subReadme")));
  }
  for (const [p, max] of Object.entries(readmeLong)) {
    if (!byPath.has(p)) problems.push(`README_LONG lists ${p}, which is not tracked: take it off the list`);
    else if (lines(p) <= B("subReadme")) problems.push(`README_LONG lists ${p}, now ${lines(p)} lines (within ${B("subReadme")}): take it off the list`);
    else if (max < B("subReadme")) problems.push(`README_LONG lists ${p} at ${max}, under the general budget ${B("subReadme")}: take it off the list`);
  }
  for (const dir of packageDirs(files.map((f) => f.path)))
    if (!byPath.has(`${dir}/README.md`)) problems.push(`${dir}: no README.md: every app package and pipeline package says what it is in a README.md (at most ${B("subReadme")} lines)`);

  // an open ticket is short; the ones over today are held where they are
  const tickets = files.filter((f) => f.path.startsWith(`${TICKETS_DIR}/`) && /^T\d+[a-z]?-.*\.md$/.test(base(f.path)) && f.path.split("/").length === TICKETS_DIR.split("/").length + 1);
  for (const t of tickets) {
    const max = ticketOver[base(t.path)] ?? B("ticket");
    if (t.lines > max) problems.push(over(t.path, t.lines, "lines", max, why("ticket")));
  }
  for (const [name, max] of Object.entries(ticketOver)) {
    const t = tickets.find((x) => base(x.path) === name);
    if (!t) problems.push(`TICKET_OVER lists ${name}, which is not an open ticket file: take it off the list`);
    else if (t.lines <= B("ticket")) problems.push(`TICKET_OVER lists ${name}, now ${t.lines} lines (within ${B("ticket")}): take it off the list`);
    else if (max > t.lines && max !== B("ticket")) problems.push(`TICKET_OVER holds ${name} at ${max} but it is ${t.lines} lines: lower the entry to ${t.lines} (it only shrinks)`);
  }

  // no markdown file anywhere is huge unless it is listed
  for (const f of files) {
    if (!MARKDOWN.test(f.path)) continue;
    const max = markdownLong[f.path] ?? B("anyMarkdown");
    if (f.lines > max) problems.push(over(f.path, f.lines, "lines", max, why("anyMarkdown")));
  }
  for (const [p, max] of Object.entries(markdownLong)) {
    if (!byPath.has(p)) problems.push(`MARKDOWN_LONG lists ${p}, which is not tracked: take it off the list`);
    else if (lines(p) <= B("anyMarkdown")) problems.push(`MARKDOWN_LONG lists ${p}, now ${lines(p)} lines (within ${B("anyMarkdown")}): take it off the list`);
    else if (max < B("anyMarkdown")) problems.push(`MARKDOWN_LONG lists ${p} at ${max}, under the general budget ${B("anyMarkdown")}: take it off the list`);
  }

  // totals for the docs tree
  const tree = files.filter((f) => TREE.some((d) => under(f.path, d)));
  const mdLines = tree.reduce((n, f) => n + (MARKDOWN.test(f.path) ? f.lines ?? 0 : 0), 0);
  const pics = tree.reduce((n, f) => n + (PICTURE.test(f.path) ? f.bytes ?? 0 : 0), 0);
  if (mdLines > B("treeLines")) problems.push(over(`${TREE.join(" + ")}`, mdLines, "tracked markdown lines", B("treeLines"), why("treeLines")));
  if (tree.length > B("treeFiles")) problems.push(over(`${TREE.join(" + ")}`, tree.length, "tracked files", B("treeFiles"), why("treeFiles")));
  if (pics > B("treePictureBytes")) problems.push(over(`${TREE.join(" + ")}`, pics, "bytes of pictures", B("treePictureBytes"), why("treePictureBytes")));

  // a ticket-keyed picture/data folder of a closed ticket is deleted with the ticket (ledger.mjs --archive), unless kept
  const closed = new Set(closedIds);
  const seen = new Set();
  for (const f of files) {
    const segs = f.path.split("/");
    for (let i = 0; i < segs.length - 1; i++) {
      const owners = assetIds(segs[i]);
      if (!owners || !owners.every((id) => closed.has(id))) continue;
      const dir = segs.slice(0, i + 1).join("/");
      const key = under(dir, DOCS_DIR) ? dir.slice(DOCS_DIR.length + 1) : dir;
      if (seen.has(dir) || kept[key]) continue;
      seen.add(dir);
      problems.push(`${dir}: its ticket ${owners.join(", ")} is closed (closed.tsv): delete the folder (its pictures are in git history), or list it in ledger.mjs KEPT_ASSETS with the reader that needs it`);
    }
  }
  return [...new Set(problems)].sort();
}

/** The repo's tracked files, each with the size the budgets need (markdown: lines; a picture under the docs tree: bytes). */
export function readTree(repoRoot) {
  const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("GIT_")));
  const listed = execFileSync("git", ["-C", repoRoot, "ls-files", "-z"], { encoding: "utf8", env, maxBuffer: 1 << 28 }).split("\0").filter(Boolean);
  const files = [];
  for (const path of listed) {
    const full = join(repoRoot, path);
    try {
      if (MARKDOWN.test(path)) files.push({ path, lines: lineCount(readFileSync(full, "utf8")) });
      else if (PICTURE.test(path) && TREE.some((d) => under(path, d))) files.push({ path, bytes: statSync(full).size });
      else files.push({ path });
    } catch (e) {
      if (e.code === "ENOENT") continue;   // tracked but deleted in the working tree: not a file to count
      throw e;
    }
  }
  return files;
}

/** The guard on the real repo. */
export function checkRepo(repoRoot) {
  const closedIds = readWorkTree(repoRoot).closed.map((r) => r.id);
  return checkSize({ files: readTree(repoRoot), closedIds });
}

/** Today's sizes, as the numbers and maps to paste into BUDGETS / the exception maps (+10% headroom on the totals). */
export function measure(repoRoot) {
  const files = readTree(repoRoot);
  const tree = files.filter((f) => TREE.some((d) => under(f.path, d)));
  const mdLines = tree.reduce((n, f) => n + (MARKDOWN.test(f.path) ? f.lines : 0), 0);
  const pics = tree.reduce((n, f) => n + (PICTURE.test(f.path) ? f.bytes : 0), 0);
  const plus = (n) => Math.ceil(n * 1.1);
  const out = [`treeLines: [${plus(mdLines)}, …]   // today ${mdLines}`, `treeFiles: [${plus(tree.length)}, …]   // today ${tree.length}`,
               `treePictureBytes: [${plus(pics)}, …]   // today ${pics}`, "README_LONG = {"];
  for (const f of files.filter((f) => base(f.path) === "README.md" && /^(app|pipeline)\//.test(f.path) && f.lines > BUDGETS.subReadme[0]))
    out.push(`  "${f.path}": ${plus(f.lines)},   // ${f.lines}`);
  out.push("}", "TICKET_OVER = {");
  for (const f of files.filter((f) => f.path.startsWith(`${TICKETS_DIR}/`) && /^T\d+[a-z]?-.*\.md$/.test(base(f.path)) && f.lines > BUDGETS.ticket[0]))
    out.push(`  "${base(f.path)}": ${f.lines},`);
  out.push("}", "MARKDOWN_LONG = {");
  for (const f of files.filter((f) => MARKDOWN.test(f.path) && f.lines > BUDGETS.anyMarkdown[0])) out.push(`  "${f.path}": ${plus(f.lines)},   // ${f.lines}`);
  out.push("}");
  return out.join("\n");
}

const isMain = process.argv[1] && new URL(import.meta.url).pathname === process.argv[1];
if (isMain) {
  const repoRoot = new URL("../..", import.meta.url).pathname;
  if (process.argv.includes("--measure")) console.log(measure(repoRoot));
  else {
    const problems = checkRepo(repoRoot);
    if (problems.length) { console.error(`✗ the docs are over their size budgets (T1081):\n  ${problems.join("\n  ")}`); process.exit(1); }
    console.log("✓ the docs are within their size budgets");
  }
}
