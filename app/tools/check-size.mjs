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
import { KEPT_ASSETS, KEPT_READERS, TICKETS_DIR, assetIds, readWorkTree } from "./ledger.mjs";

/** The docs folder (the planning pages, runbooks, restart pages). The tickets folder (ledger.mjs) sits inside it today. */
export const DOCS_DIR = "docs";
/** (T1193) The pages written for agent chats: START-HERE, STATUS, the restart pages, the roadmap, the inbox. */
export const AGENTS_DIR = `${DOCS_DIR}/agents`;
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
  // (owner 2026-11-25, the second cut) the standards for every page outside the tickets folder
  page:         [150, "one page per subject, read in ten minutes: cut it to what is built, or split it by subject"],
  // (T1144, 2026-11-26; the owner confirms) the restart pages are NOT in this total: each live chat adds one, so with about
  // 112 lines of room the third chat to save its state failed docs-ci and its push the night credits ran short. They keep
  // their own cap (`restart`, 60 lines each) and are deleted when their chat ends.
  readingLines: [11705, "every markdown line outside the tickets folder, the look-up files (LOOKUP) and the restart pages: what a person reads to know the repo"],   // +15 (T1189: the Province list read from the PDF); +9 (T1200: eight feedback-log rows and their count line, a closed tester's email); +77 (T1195: the Province list's 17 new entries, the batch 3 rulings, four feedback-log rows); +11 (T1195: the late rulings, the Snehumption entry); +1 (T1167: the in-season banner window, one decision line); +1 (T1177: RULINGS E29); +2 (T1177: his release of PR #330 and the away rule); +6 (T1177: RULINGS L27-L29 and Q31, the owner's P7 / O1 rulings); +3 (T1177: his Blue Lead, T1243 and Strathcona-Westmin answers); +3 (T1193: the default-tier line in AGENTS 18 +2, the README's test line +1); +6 (T1222: secrets.md says where run-time settings come from now that no .env is read); +177 (T1193: docs/README and every tool named in the three tool READMEs); +1 (T1193: the moved-page pointer at docs/START-HERE.md); +1 (T1243: RULINGS Q32, the annual quota under its row's release); +3 (T1193: the README's five everyday things with the make verbs and the test tier line, the scripts/README verb table); +79 (T1191: docs batch 5, the new section of OWNER-RULINGS.md, the owner's rulings of the night of 11-26/27); +2 (T1199: ruling E30 and its superseded row E8); +1 (T1199: the waters README row for area_edge_calls.json); +10 (T1177: RULINGS T19, AGENTS 15, GOTCHAS 6, the tributary walk); +12 (T1177: his 11-27 walk answers in OWNER-RULINGS, the gotcha lines, AGENTS 63); +1 (T1251: RULINGS T20, the most upstream row named); +10 (T1250: RULINGS S14 and S15, the owner's three steelhead answers in OWNER-RULINGS, AGENTS 54's Pacific-stream line, the barrier list's README row); +6 (T1250: RULINGS S12a and OWNER-RULINGS, his over-50 sheet answers and the Kitimat and Regions 1-2 picks); +8 (T1250: OWNER-RULINGS, the three under-closure questions he checked)
  // the docs tree (docs/ today; docs/ + tickets/ after the rename): set at today's value + 10%, re-set with --measure
  treeLines:    [25537, "tracked markdown lines in the docs tree (19,850 on 2026-11-25 after the second cut; the room is for new tickets, the pages are held by readingLines)"],   // +39 (T1222: main stood at 22962; its ticket and the secrets.md section took the rest of the room); +23 (T1192 PR 4: the ticket's PR 4 notes, its found-not-moved list and the review's gaps)   // re-set with --measure (today 23215 + 10%; T1191: docs batch 5 adds a net 171 markdown lines, the OWNER-RULINGS section 79, T1246 46, the ticket notes)
  treeFiles:    [1600, "tracked files in the docs tree"],
  treePictureBytes: [140908411, "bytes of tracked png/jpg/webp/gif in the docs tree (pictures live in git forever)"],   // -16,751,824 (T1193, owner 2026-11-26): the 110 pictures of tickets/shots (24.4 MB) deleted; set to the measured 128,098,555 + 10%
};

/** README.md files under app/ or pipeline/ allowed to pass BUDGETS.subReadme: path -> its own budget (today's size + 10%). */
export const README_LONG = {
  "app/README.md": 84,
  "app/tools/README.md": 96,   // 87 (T1193: every top-level runnable named, grouped, one line each)
  "app/tools/maplibre-android/README.md": 54,
  "app/tools/testbed/README.md": 92,
  "pipeline/README.md": 83,
  "pipeline/atlas/waters/added_lakes/README.md": 56,
  "pipeline/atlas/waters/added_streams/README.md": 122,
  "pipeline/regs/dfo_salmon/README.md": 109,   // 99: the design page was merged into it (the second cut)
};

/** TEMPORARY: open tickets over BUDGETS.ticket today, each held at its size on 2026-11-25 so it can only shrink. The ticket
 *  cut (T1081) takes them off this list as it shortens them; an entry that is gone or under the cap fails ("take it off"). */
export const TICKET_OVER = {
  "T1029-one-bank-stripe.md": 399,
  "T1041-faster-ci-feedback.md": 324,
  "T1053-walk-stops-at-head-lake.md": 306,
  "T1072-store-screenshots-and-marketing-content.md": 165,
};

/** Markdown files allowed over BUDGETS.anyMarkdown: path -> its own budget (today's size + 10%). */
export const MARKDOWN_LONG = {
  "docs/app-integration/T1140-files/multi-part-waters.md": 840,   // 763
  "pipeline/regs/parsing/prompts/CATALOGUE_PARSE_PROMPT.md": 777,   // 706
};

/** Markdown outside the tickets folder allowed over BUDGETS.page: path -> its own budget (its size when listed). A page
 *  leaves the list when it is cut; a NEW long page needs an entry here, in its PR, with the reason beside it. */
export const PAGE_LONG = {
  "pipeline/docs/RULINGS.md": 467,                                        // the owner's rulings on how the book is read: his words, never shortened (+1, T1177: E29, the audit's per-water extent rulings; +6, T1177: L27-L29 and Q31; +1, T1243: Q32; +2, T1199: E30, E8 superseded); +1 (T1177: T19, the tributary walk); +1 (T1251: T20); +2, T1250: S14 Pacific streams, S15 a water's rainbow cap
  "docs/OWNER-RULINGS.md": 391,                                   // his standing rulings, in his words, never shortened (T1177: audit batch 2); like RULINGS.md; +50 (T1195: audit batch 3, the evening's rulings); +9 (T1195: the rulings made after the first draft); +2 (T1177: his release of PR #330 and the away rule); +3 (T1177: Blue Lead, T1243, Strathcona-Westmin); +79 (T1191: docs batch 5, the owner's answers of the night of 11-26/27, five return batches and the single answers); +6 (T1177: his 11-27 walk answers); +5 (T1250: "Pacific streams now", Buckley Creek, the Columbia and Skagit, the over-50 words)
  "pipeline/docs/GOTCHAS.md": 219,                                 // the gotchas (T1177: the audit rulings' entries); the Province's list is its own page; +5 (T1177: the tributary walk, item 6); +4 (T1177: Kootenay's reading, the own stream, Upper Arrow)
  "docs/agents/ROADMAP-after-0.9.0.md": 300,                              // the planning chat's live page (PAGE_ROOM: a cap, not its size)
  "docs/play-production-access/questionnaire-draft.md": 173,       // the Play production access record
  "docs/repo-move/OUTSIDE-GIT.md": 241,                            // the repo-move chat's record, until that chat ends
  "docs/repo-move/PREP.md": 262,                                   // the same
  // held as they are while 0.10.0 ships from them (planning chat 2026-11-25): one release.md + a short by-hand page after its merge back
  "docs/runbooks/release-by-hand.md": 495,   // +2 (T1161): "lid open, on power", the line that would have saved 0.10.0 its 8.3 h asleep
  "docs/runbooks/release-gates-explained.md": 366,
  "docs/runbooks/release.md": 219,   // +3 (T1161): the same line, with the battery refusal and its flag
  // long files in an open ticket's folder (the T1140 one is in MARKDOWN_LONG)
  "docs/app-integration/T1053-shots/head-lakes-before-after.md": 272,
  "docs/app-integration/T1108-files/handoff-2026-11-25-offline-satellite-and-reversible-engine.md": 209,
};

/** PAGE_LONG entries that are a cap with room under it, not the page's size (a live page another chat keeps). */
export const PAGE_ROOM = new Set(["docs/agents/ROADMAP-after-0.9.0.md"]);

/** Look-up files: text a program reads or writes, or the book's own words. Not counted in BUDGETS.readingLines and not
 *  held to BUDGETS.page (anyMarkdown still holds them). A prefix ending in "/" is a folder. */
export const LOOKUP = [
  "CHANGELOG.md",                              // written by the release from the commits
  "tools/curation-review/API.md",                    // generated: tools/curation-review/api_routes.py --write
  "data/LICENSE-ODbL.md",                      // a licence text
  "data/curated/regulations/reference/",       // the book's words, read by the parser
  "pipeline/regs/parsing/prompts/",            // the parser's prompts
  "pipeline/tests/fixtures/",                  // fixtures
];
const isLookup = (p) => LOOKUP.some((l) => (l.endsWith("/") ? p.startsWith(l) : p === l));
/** A live chat's restart page: held by BUDGETS.restart each, left out of BUDGETS.readingLines (see there). */
const isRestartPage = (p) => p.startsWith(`${AGENTS_DIR}/restart/`) && /\.md$/i.test(p);

/** A page named for a day (…-2026-11-15.md) is a record of that day: history, which git keeps. */
const DATED_NAME = /(^|[-_./])20\d\d-\d\d-\d\d[^/]*(\/|\.md$)/;
/** A ticket's file in ITS folder (Tnnn-files/, Tnnn-shots/) is that ticket's: it is held to the page budget, counted in the
 *  reading total and deleted with the folder when the ticket closes (below). What is refused is a ticket-keyed page that
 *  is NOT in such a folder (Tnnn-findings.md loose in a docs folder, a folder named t796-…): it outlives its ticket.
 *  path -> why it stays, for the rare one that must. */
export const WORKING_PAGES = {
};
const TICKET_FOLDER = /(^|\/)T\d+[a-z]?(-\d+)?-(files|shots)\//;
const TICKET_KEYED = /(^|\/)T\d+[a-z]?-[^/]*(\/|\.md$)/i;

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
export function checkSize({ files, closedIds, kept = KEPT_ASSETS, readmeLong = README_LONG, ticketOver = TICKET_OVER, markdownLong = MARKDOWN_LONG,
                            pageLong = PAGE_LONG, workingPages = WORKING_PAGES, readingBudget = BUDGETS.readingLines[0] }) {
  if (!Array.isArray(files) || !files.length) throw new Error("checkSize needs the tracked files: never \"no files to check\"");
  const problems = [];
  const byPath = new Map(files.map((f) => [f.path, f]));
  const lines = (p) => byPath.get(p)?.lines ?? 0;
  const [B, why] = [(k) => BUDGETS[k][0], (k) => BUDGETS[k][1]];

  // required reading
  const reading = [["README.md", "readme"], [`${AGENTS_DIR}/START-HERE.md`, "startHere"], ["AGENTS.md", "agents"],
                   [`${AGENTS_DIR}/STATUS.md`, "status"], [`${DOCS_DIR}/app-integration/decisions.md`, "decisions"]];
  for (const [p, k] of reading) {
    if (!byPath.has(p)) problems.push(`${p}: not tracked, but its budget (${k}) names it: fix the path in BUDGETS' reading list`);
    else if (lines(p) > B(k)) problems.push(over(p, lines(p), "lines", B(k), why(k)));
  }
  for (const f of files)
    if (under(f.path, `${AGENTS_DIR}/restart`) && MARKDOWN.test(f.path) && f.lines > B("restart"))
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

  // (the second cut) the standards outside the tickets folder: one page per subject, no dated pages, no ticket working pages
  const pages = files.filter((f) => MARKDOWN.test(f.path) && !under(f.path, TICKETS_DIR) && !isLookup(f.path));
  const ownBudget = new Set([...reading.map(([p]) => p), ...Object.keys(readmeLong)]);
  for (const f of pages) {
    if (!ownBudget.has(f.path) && !(f.path in markdownLong)) {   // a MARKDOWN_LONG file holds its budget there
      const max = pageLong[f.path] ?? B("page");
      if (f.lines > max) problems.push(over(f.path, f.lines, "lines", max, why("page")));
    }
    if (!under(f.path, DOCS_DIR)) continue;
    const rel = f.path.slice(DOCS_DIR.length + 1);
    if (TICKET_FOLDER.test(rel)) continue;   // a ticket's own file, in its folder
    if (DATED_NAME.test(`/${rel}`) && !workingPages[f.path]) problems.push(`${f.path}: a page named for a day is history: fold what still holds into its subject's page and delete it (git keeps it)`);
    if (TICKET_KEYED.test(rel) && !workingPages[f.path])
      problems.push(`${f.path}: a ticket's page outside its ticket's folder: put it in Tnnn-files/ (it goes when the ticket closes), fold it into the ticket, or list it in WORKING_PAGES with why it stays`);
  }
  for (const [p, max] of Object.entries(pageLong)) {
    if (!byPath.has(p)) problems.push(`PAGE_LONG lists ${p}, which is not tracked: take it off the list`);
    else if (lines(p) <= B("page")) problems.push(`PAGE_LONG lists ${p}, now ${lines(p)} lines (within ${B("page")}): take it off the list`);
    else if (max > lines(p) && !PAGE_ROOM.has(p)) problems.push(`PAGE_LONG holds ${p} at ${max} but it is ${lines(p)} lines: lower the entry to ${lines(p)} (it only shrinks)`);
  }
  for (const p of Object.keys(workingPages)) if (!byPath.has(p)) problems.push(`WORKING_PAGES lists ${p}, which is not tracked: take it off the list`);
  const readingLines = pages.reduce((n, f) => n + (isRestartPage(f.path) ? 0 : f.lines ?? 0), 0);
  if (readingLines > readingBudget) problems.push(over("markdown outside the tickets folder", readingLines, "lines", readingBudget, why("readingLines")));

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

/**
 * (T1081) KEPT_ASSETS cannot rot: every entry is a tracked folder or file, has a named reader, and each reader is tracked and
 * still names the asset (its last path segment). Pure: `read(path)` returns a file's text.
 */
export function keptProblems({ paths, kept = KEPT_ASSETS, readers = KEPT_READERS, read }) {
  const problems = [];
  const have = new Set(paths);
  for (const key of Object.keys(kept)) {
    // ledger.mjs keys by the path under the docs folder; the tickets folder is top-level, its keys are repo paths
    const full = under(key, TICKETS_DIR) ? key : `${DOCS_DIR}/${key}`;
    if (!paths.some((p) => p === full || p.startsWith(`${full}/`))) problems.push(`KEPT_ASSETS lists ${key}, which is not tracked: take it off the list`);
    const who = readers[key];
    if (!who?.length) { problems.push(`KEPT_ASSETS lists ${key} with no KEPT_READERS entry: name the file that still reads it, or take it off the list`); continue; }
    for (const r of who) {
      if (!have.has(r)) problems.push(`KEPT_READERS names ${r} for ${key}, which is not tracked: fix the reader, or delete the asset and take it off KEPT_ASSETS`);
      else if (!read(r).includes(base(key))) problems.push(`${r} no longer names ${base(key)} (KEPT_ASSETS): delete the asset and take it off the list, or restore the citation`);
    }
  }
  for (const key of Object.keys(readers)) if (!kept[key]) problems.push(`KEPT_READERS lists ${key}, which is not in KEPT_ASSETS: take it off`);
  return problems;
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
  const files = readTree(repoRoot);
  const kept = keptProblems({ paths: files.map((f) => f.path), read: (p) => readFileSync(join(repoRoot, p), "utf8") });
  return [...checkSize({ files, closedIds }), ...kept].sort();
}

/** Today's sizes, as the numbers and maps to paste into BUDGETS / the exception maps (+10% headroom on the totals). */
export function measure(repoRoot) {
  const files = readTree(repoRoot);
  const tree = files.filter((f) => TREE.some((d) => under(f.path, d)));
  const mdLines = tree.reduce((n, f) => n + (MARKDOWN.test(f.path) ? f.lines : 0), 0);
  const pics = tree.reduce((n, f) => n + (PICTURE.test(f.path) ? f.bytes : 0), 0);
  const plus = (n) => Math.ceil(n * 1.1);
  const pages = files.filter((f) => MARKDOWN.test(f.path) && !under(f.path, TICKETS_DIR) && !isLookup(f.path));
  const reading = pages.reduce((n, f) => n + (isRestartPage(f.path) ? 0 : f.lines), 0);
  const own = new Set(["README.md", "AGENTS.md", `${AGENTS_DIR}/START-HERE.md`, `${AGENTS_DIR}/STATUS.md`, `${DOCS_DIR}/app-integration/decisions.md`]);
  const long = pages.filter((f) => f.lines > BUDGETS.page[0] && !own.has(f.path) && !(f.path in MARKDOWN_LONG) && !(base(f.path) === "README.md" && /^(app|pipeline)\//.test(f.path)));
  const out = [`readingLines: [${Math.ceil(reading * 1.05)}, …]   // today ${reading}`, "PAGE_LONG = {", ...long.map((f) => `  "${f.path}": ${PAGE_ROOM.has(f.path) ? PAGE_LONG[f.path] ?? f.lines : f.lines},   // + the reason it stays long`), "}", `treeLines: [${plus(mdLines)}, …]   // today ${mdLines}`, `treeFiles: [${plus(tree.length)}, …]   // today ${tree.length}`,
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
