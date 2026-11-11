#!/usr/bin/env node
/**
 * T84 — the ticket ledger stays whole.
 *
 * Tickets are the sole source of future work (owner 2026-10-22), so a ticket that drops out of the
 * ledger, or a ledger row with no ticket behind it, is work that silently stops existing. This fails
 * `pnpm check` when:
 *
 *   1. a `tickets/T*.md` file has no `Status:` line in its first lines, or one whose value is not a
 *      status the README defines;
 *   2. a ticket file is missing from the README ledger table, or a ledger row has no ticket file, or two files share an id
 *      (or a row is repeated);
 *   3. a T-number named in a commit subject (since the tickets folder began) is not in the ledger;
 *   4. a ticket is DONE — in its file or in its ledger row — with no commit hash;
 *   5. (T437) a ticket file's leading status keyword differs from its ledger row's. Twelve had drifted (OPEN in the
 *      file, POSTPONED in the ledger; PARTIAL vs "DONE locally"): a lane closes the file AND the row in one commit.
 *   6. (T539) an IN REVIEW or PARTIAL status names its step: `owner: OWNER-TODO "<text>"` (the text must exist in
 *      OWNER-TODO.md) or `left (Claude): …`; one whose only step is Claude's is IN PROGRESS, not IN REVIEW. Rows that
 *      predate the rule are on `NO_STEP_YET` (a list that only shrinks: a listed ticket that now complies fails until it
 *      is taken off, and a new ticket can never be added).
 *   7. (T539) the docs guard, `checkDocs`: STATUS.md starts with its title; every repo path a backticked token cites in
 *      START-HERE, the restart page and OWNER-TODO exists, and every path a runbook (handoff/PLAN/runbooks/*.md) cites,
 *      in backticks or in a command block (T628); the restart page holds no state (no release id, no build
 *      number: those live only in the STATUS top entry); AGENTS rule ids are unique.
 *
 * `checkLedger` and `checkDocs` are pure (the test feeds it mutated copies); the CLI below reads the repo and git.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

/** The repo-relative tickets folder. */
export const TICKETS_DIR = "handoff/PLAN/app-integration/tickets";

/** The `Status:` line must sit within this many lines of the top (title, blank, Status). */
export const STATUS_WITHIN = 6;

/** The status keywords README.md "How the system works" defines. Longest first: IN PROGRESS before a prefix match. */
export const STATUSES = ["IN PROGRESS", "IN REVIEW", "BLOCKED", "POSTPONED", "PARTIAL", "DROPPED", "OPEN", "DONE"];

/**
 * Tickets whose IN REVIEW / PARTIAL status line predates the T539 rule and names no step yet. THIS LIST ONLY SHRINKS: the
 * lane that closes or re-words one removes its id here (a listed id that complies now fails the guard).
 */
export const NO_STEP_YET = [
  "T88", "T178", "T261", "T292", "T310", "T360", "T390", "T401", "T404", "T409",
  "T410", "T461", "T485", "T496", "T498", "T508", "T532", "T543", "T544", "T546",
  "T547", "T548", "T550", "T551", "T552", "T553", "T558", "T559", "T561", "T562",
  "T563", "T564", "T565", "T566", "T567", "T568", "T569", "T570", "T571", "T572",
  "T573", "T581", "T582", "T583", "T584", "T586", "T589", "T590", "T594", "T595",
  "T596", "T598", "T604", "T607", "T609", "T610", "T620", "T621", "T622", "T624",
  "T632", "T636",
];

const ID = /^T\d+[a-z]?/;
const HASH = /\b[0-9a-f]{7,40}\b/;
// not after a word character or a hyphen: "Q-T57a" is an owner-question label, not ticket T57a
const T_IN_SUBJECT = /(?<![\w-])T\d+[a-z]?\b/g;

/** The leading status keyword of a status text, or null. */
export function statusKeyword(text) {
  const t = text.trim();
  return STATUSES.find((s) => t.startsWith(s)) ?? null;
}

/** The ticket id a file name carries (`T15b-caveats.md` → `T15b`), or null. */
export function idOfFile(name) {
  return name.match(ID)?.[0] ?? null;
}

const OWNER_STEP = /owner:\s*OWNER-TODO\s+"([^"]+)"/g;
const CLAUDE_STEP = /left \(Claude\):\s*\S/;

/** What is wrong with an IN REVIEW / PARTIAL status's step (T539); `ignoreList` skips the NO_STEP_YET allowance. */
function stepProblems(id, kw, value, ownerTodo, ignoreList = false) {
  if (!ignoreList && NO_STEP_YET.includes(id)) return [];
  const owners = [...value.matchAll(OWNER_STEP)].map((m) => m[1]);
  const out = [];
  for (const text of owners)
    if (!(ownerTodo ?? "").includes(text)) out.push(`${id}: Status cites OWNER-TODO "${text}", which OWNER-TODO.md does not contain`);
  const claude = CLAUDE_STEP.test(value);
  if (!owners.length && !claude)
    out.push(`${id}: ${kw} names no step: add \`owner: OWNER-TODO "<text>"\` or \`left (Claude): …\` to the Status line`);
  else if (!owners.length && kw === "IN REVIEW") out.push(`${id}: IN REVIEW but the only step is Claude's (left (Claude)): that is IN PROGRESS`);
  return out;
}

/** The ledger table under `## Ledger`: one `{ id, title, status, commits, left }` per row, in order. */
export function ledgerRows(readme) {
  const lines = readme.split("\n");
  const start = lines.findIndex((l) => /^##\s+Ledger\b/.test(l));
  if (start < 0) return [];
  const rows = [];
  for (const line of lines.slice(start + 1)) {
    if (/^##\s/.test(line)) break;
    const cells = line.split("|").slice(1, -1).map((c) => c.trim());
    if (cells.length < 5 || !ID.test(cells[0]) || cells[0] !== idOfFile(cells[0])) continue;
    const [id, title, status, commits, left] = cells;
    rows.push({ id, title, status, commits, left });
  }
  return rows;
}

/**
 * Every way the ledger is not whole, as one sorted list of plain messages (empty = whole).
 *
 * @param {{ tickets: { name: string, text: string }[], readme: string, subjects: string[] }} input
 *   tickets — every `T*.md` in the folder (name + contents); readme — README.md; subjects — commit
 *   subjects since the tickets folder began; ownerTodo — handoff/PLAN/OWNER-TODO.md (the texts a status may cite).
 */
export function checkLedger({ tickets, readme, subjects, ownerTodo }) {
  const problems = [];
  for (const { name, text } of tickets) {
    const id = idOfFile(name);
    if (!id) { problems.push(`${name}: file name does not start with a ticket id (Tnn-slug.md)`); continue; }
    const head = text.split("\n").slice(0, STATUS_WITHIN);
    const line = head.find((l) => /^Status:/.test(l));
    if (!line) { problems.push(`${id}: no "Status:" line in the first ${STATUS_WITHIN} lines of ${name}`); continue; }
    const value = line.replace(/^Status:\s*/, "");
    const kw = statusKeyword(value);
    if (!kw) { problems.push(`${id}: Status "${value}" is not one of ${STATUSES.join(", ")}`); continue; }
    if (kw === "DONE" && !HASH.test(value)) problems.push(`${id}: Status DONE names no commit hash in ${name}`);
    if (kw === "IN REVIEW" || kw === "PARTIAL") problems.push(...stepProblems(id, kw, value, ownerTodo));
  }
  for (const id of NO_STEP_YET) {
    const t = tickets.find((x) => idOfFile(x.name) === id);
    const v = t?.text.split("\n").slice(0, STATUS_WITHIN).find((l) => /^Status:/.test(l))?.replace(/^Status:\s*/, "");
    const kw = v ? statusKeyword(v) : null;
    if (!t) problems.push(`${id}: on NO_STEP_YET but has no ticket file (take it off the list)`);
    else if (kw !== "IN REVIEW" && kw !== "PARTIAL") problems.push(`${id}: on NO_STEP_YET but is ${kw} (take it off the list)`);
    else if (!stepProblems(id, kw, v, ownerTodo, true).length) problems.push(`${id}: on NO_STEP_YET but its status now names its step (take it off the list)`);
  }

  const fileKw = new Map();
  for (const { name, text } of tickets) {
    const line = text.split("\n").slice(0, STATUS_WITHIN).find((l) => /^Status:/.test(l));
    const kw = line ? statusKeyword(line.replace(/^Status:\s*/, "")) : null;
    if (kw && idOfFile(name)) fileKw.set(idOfFile(name), kw);
  }

  const rows = ledgerRows(readme);
  const seen = new Set();
  for (const row of rows) {
    if (seen.has(row.id)) problems.push(`${row.id}: ledger row appears more than once`);
    seen.add(row.id);
    const kw = statusKeyword(row.status);
    if (!kw) problems.push(`${row.id}: ledger status "${row.status}" is not one of ${STATUSES.join(", ")}`);
    if (kw === "DONE" && !HASH.test(row.commits)) problems.push(`${row.id}: ledger row is DONE with no commit hash`);
    const fk = fileKw.get(row.id);
    if (kw && fk && kw !== fk) problems.push(`${row.id}: the ticket file says ${fk}, its ledger row ${kw} (close both together)`);
  }

  const namesById = new Map();
  for (const { name } of tickets) {
    const id = idOfFile(name);
    if (id) namesById.set(id, [...(namesById.get(id) ?? []), name]);
  }
  for (const [id, names] of namesById)
    if (names.length > 1) problems.push(`${id}: more than one ticket file (${[...names].sort().join(", ")})`);

  const files = new Set(namesById.keys());
  for (const id of files) if (!seen.has(id)) problems.push(`${id}: ticket file has no ledger row in README.md`);
  for (const id of seen) if (!files.has(id)) problems.push(`${id}: ledger row has no ticket file`);

  const named = new Set(subjects.flatMap((s) => s.match(T_IN_SUBJECT) ?? []));
  for (const id of named) if (!seen.has(id)) problems.push(`${id}: named in a commit subject but missing from the ledger`);

  return [...new Set(problems)].sort();
}

const RUNBOOKS_DIR = "handoff/PLAN/runbooks";
/** Paths a runbook's own steps CREATE (so they do not exist until it is followed), per runbook. A listed path that
 *  exists, or that its runbook no longer cites, fails: the list only holds what it must. */
export const RUNBOOK_MAKES = {
  "annual-synopsis.md": ["data/source/fishing_synopsis.2025-27.pdf"],   // step 1: `cp` keeps last year's book under this name
};
const CITED_ROOTS = /^(app|pipeline|data|deploy|scripts|handoff|curation-review|\.github)\//;
const TOP_FILES = /^(AGENTS|CLAUDE)\.md$/;

/** The repo paths a document cites in backticks (globs, placeholders and `path:line` / `path#anchor` tails are cut). */
export function citedPaths(text) {
  const out = new Set();
  for (const m of text.matchAll(/`([^`\s]+)`/g)) {
    const tok = m[1];
    if (/[*<>{}$|()=]/.test(tok)) continue;
    const p = tok.replace(/[:#].*$/, "").replace(/[.,;]+$/, "");
    if (CITED_ROOTS.test(p) || TOP_FILES.test(p)) out.add(p);
  }
  return [...out].sort();
}

/**
 * The repo paths a RUNBOOK cites (T628): the backticked ones, and every path inside its fenced command blocks — a
 * runbook's paths are mostly arguments of commands (`OLD=data/curated/...`, `cp ... deploy/x.sh`), which no backtick
 * holds. A token with a variable, glob or placeholder is not a path.
 */
export function runbookPaths(text) {
  const out = new Set();
  const prose = [];
  text.split(/^```.*$/m).forEach((part, i) => {
    if (i % 2 === 0) { prose.push(part); return; }
    for (const tok of part.split(/[\s"'=;,]+/)) {
      if (/[*<>{}$|()\[\]`]/.test(tok)) continue;
      const p = tok.replace(/[:#].*$/, "").replace(/[.]+$/, "");
      if (CITED_ROOTS.test(p) || TOP_FILES.test(p)) out.add(p);
    }
  });
  for (const p of citedPaths(prose.join("\n"))) out.add(p);
  return [...out].sort();
}

/** AGENTS rule ids (`12.`, `20a.`, `A1.`, `W45.`) at the start of a line, with the lines they sit on. */
export function ruleIds(agents) {
  const ids = [];
  agents.split("\n").forEach((l, i) => { const m = l.match(/^([A-Z]?\d+[a-z]?)\.\s/); if (m) ids.push({ id: m[1], line: i + 1 }); });
  return ids;
}

/**
 * Every way the handoff docs are wrong (T539), as a sorted list (empty = fine). Pure: `exists` / `isIgnored` answer for a
 * repo path (a git-ignored output such as app/dist/... is not required to exist).
 *
 * `runbooks` (T628): every `handoff/PLAN/runbooks/*.md`, read with `runbookPaths` (its command blocks too).
 *
 * @param {{ status: string, startHere: string, restartName: string, restart: string, ownerTodo: string, agents: string,
 *           runbooks: { name: string, text: string }[],
 *           exists: (p: string) => boolean, isIgnored: (p: string) => boolean }} input
 */
export function checkDocs({ status, startHere, restartName, restart, ownerTodo, agents, runbooks, exists, isIgnored }) {
  const problems = [];
  if (!/^# /.test(status)) problems.push(`STATUS.md must start with its title ("# …"), not: ${status.split("\n")[0].slice(0, 60)}`);
  for (const [name, text] of [["START-HERE.md", startHere], [restartName || "the restart page", restart], ["OWNER-TODO.md", ownerTodo]])
    for (const p of citedPaths(text)) if (!exists(p) && !isIgnored(p)) problems.push(`${name} cites ${p}, which does not exist`);
  if (!Array.isArray(runbooks) || !runbooks.length) problems.push(`no runbook was read (${RUNBOOKS_DIR}/*.md): the dead-path guard covers them (T628)`);
  else {
    for (const { name, text } of runbooks) {
      const makes = RUNBOOK_MAKES[name] ?? [];
      const cited = runbookPaths(text);
      for (const p of cited) if (!exists(p) && !isIgnored(p) && !makes.includes(p)) problems.push(`${RUNBOOKS_DIR}/${name} cites ${p}, which does not exist`);
      for (const p of makes) if (exists(p) || !cited.includes(p)) problems.push(`RUNBOOK_MAKES lists ${p} for ${name}, which ${exists(p) ? "now exists" : "the runbook no longer cites"}: take it off the list`);
    }
    for (const name of Object.keys(RUNBOOK_MAKES)) if (!runbooks.some((r) => r.name === name)) problems.push(`RUNBOOK_MAKES names ${name}, which is not a runbook`);
  }
  if (!restartName) problems.push("START-HERE.md names no restart page (handoff/PLAN/NEXT-SESSION-*.md)");
  for (const m of restart.matchAll(/\br-[0-9a-f]{12}\b/g)) problems.push(`${restartName} holds state: release id ${m[0]} (it lives only in the STATUS top entry)`);
  for (const m of restart.matchAll(/\b[Bb]uild \d+\b/g)) problems.push(`${restartName} holds state: "${m[0]}" (the Play build lives only in the STATUS top entry)`);
  const seen = new Map();
  for (const { id, line } of ruleIds(agents)) {
    if (seen.has(id)) problems.push(`AGENTS.md: rule id ${id} is used at lines ${seen.get(id)} and ${line}`);
    else seen.set(id, line);
  }
  return [...new Set(problems)].sort();
}

/** The repo's real inputs: the tickets folder, its README, and commit subjects since the folder began. */
export function readRepo(repoRoot) {
  const dir = join(repoRoot, TICKETS_DIR);
  const tickets = readdirSync(dir)
    .filter((n) => /^T.*\.md$/.test(n))
    .sort()
    .map((name) => ({ name, text: readFileSync(join(dir, name), "utf8") }));
  const readme = readFileSync(join(dir, "README.md"), "utf8");
  const git = (...args) => execFileSync("git", ["-C", repoRoot, ...args], { encoding: "utf8" });
  // Subjects before the tickets folder used T-numbers of an older plan (e.g. "T3a"); they are not tickets.
  const firsts = git("log", "--diff-filter=A", "--format=%H", "--", TICKETS_DIR).trim().split("\n").filter(Boolean);
  if (!firsts.length) throw new Error(`no commit adds ${TICKETS_DIR}; cannot scope commit subjects`);
  const first = firsts[firsts.length - 1];
  const subjects = git("log", "--format=%s", "HEAD", "--not", `${first}^@`).split("\n").filter(Boolean);
  const plan = join(repoRoot, "handoff/PLAN");
  const ownerTodo = readFileSync(join(plan, "OWNER-TODO.md"), "utf8");
  return { tickets, readme, subjects, ownerTodo };
}

/** The docs checkLedger's sibling reads: real files, real existence, real git-ignore. */
export function readDocs(repoRoot) {
  const plan = join(repoRoot, "handoff/PLAN");
  const startHere = readFileSync(join(plan, "START-HERE.md"), "utf8");
  const restartName = startHere.match(/handoff\/PLAN\/(NEXT-SESSION-[\w.-]+\.md)/)?.[1] ?? "";
  const restart = restartName && existsSync(join(plan, restartName)) ? readFileSync(join(plan, restartName), "utf8") : "";
  if (restartName && !restart) throw new Error(`START-HERE.md names ${restartName}, which does not exist`);
  // git answers 0 (ignored) or 1 (not); anything else is git itself failing, said aloud so a runner that cannot ask
  // does not read as "every ignored path is a dead path". GIT_* from a caller's environment must not point git elsewhere.
  const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("GIT_")));
  const isIgnored = (p) => {
    const got = spawnSync("git", ["-C", repoRoot, "check-ignore", "-q", p], { env, encoding: "utf8" });
    if (got.status === 0) return true;
    if (got.status === 1) return false;
    throw new Error(`git check-ignore ${p}: status ${got.status}${got.error ? ` (${got.error.message})` : ""} ${got.stderr ?? ""}`.trim());
  };
  const runbooks = readdirSync(join(repoRoot, RUNBOOKS_DIR)).filter((n) => n.endsWith(".md")).sort()
    .map((name) => ({ name, text: readFileSync(join(repoRoot, RUNBOOKS_DIR, name), "utf8") }));
  return { status: readFileSync(join(plan, "STATUS.md"), "utf8"), startHere, restartName, restart, runbooks,
           ownerTodo: readFileSync(join(plan, "OWNER-TODO.md"), "utf8"), agents: readFileSync(join(repoRoot, "AGENTS.md"), "utf8"),
           exists: (p) => existsSync(join(repoRoot, p)), isIgnored };
}

const isMain = process.argv[1] && new URL(import.meta.url).pathname === process.argv[1];
if (isMain) {
  const repoRoot = new URL("../..", import.meta.url).pathname;
  const problems = checkLedger(readRepo(repoRoot));
  const docProblems = checkDocs(readDocs(repoRoot));
  if (problems.length) {
    console.error(`✗ the ticket ledger is not whole (${TICKETS_DIR}/README.md):\n  ` + problems.join("\n  ") +
      "\n\n  Every ticket file needs a Status line and a ledger row; DONE needs its commit hashes; IN REVIEW / PARTIAL name their step.");
  }
  if (docProblems.length) console.error(`✗ the handoff docs are wrong (T539):\n  ` + docProblems.join("\n  "));
  if (problems.length || docProblems.length) process.exit(1);
  console.log(`✓ ticket ledger whole, handoff docs guard passes`);
}
