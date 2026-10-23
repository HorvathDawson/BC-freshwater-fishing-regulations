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
 *   2. a ticket file is missing from the README ledger table, or a ledger row has no ticket file
 *      (or a row is repeated);
 *   3. a T-number named in a commit subject (since the tickets folder began) is not in the ledger;
 *   4. a ticket is DONE — in its file or in its ledger row — with no commit hash.
 *
 * It does NOT require a row's status to equal its file's: a lane closes its ticket file and the merger updates the
 * ledger row afterwards, so the two legitimately differ between those commits.
 *
 * `checkLedger` is pure (the test feeds it mutated copies); the CLI below reads the repo and git.
 */
import { execFileSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

/** The repo-relative tickets folder. */
export const TICKETS_DIR = "handoff/PLAN/app-integration/tickets";

/** The `Status:` line must sit within this many lines of the top (title, blank, Status). */
export const STATUS_WITHIN = 6;

/** The status keywords README.md "How the system works" defines. Longest first: IN PROGRESS before a prefix match. */
export const STATUSES = ["IN PROGRESS", "BLOCKED", "POSTPONED", "PARTIAL", "DROPPED", "OPEN", "DONE"];

const ID = /^T\d+[a-z]?/;
const HASH = /\b[0-9a-f]{7,40}\b/;
const T_IN_SUBJECT = /\bT\d+[a-z]?\b/g;

/** The leading status keyword of a status text, or null. */
export function statusKeyword(text) {
  const t = text.trim();
  return STATUSES.find((s) => t.startsWith(s)) ?? null;
}

/** The ticket id a file name carries (`T15b-caveats.md` → `T15b`), or null. */
export function idOfFile(name) {
  return name.match(ID)?.[0] ?? null;
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
 *   subjects since the tickets folder began.
 */
export function checkLedger({ tickets, readme, subjects }) {
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
  }

  const rows = ledgerRows(readme);
  const seen = new Set();
  for (const row of rows) {
    if (seen.has(row.id)) problems.push(`${row.id}: ledger row appears more than once`);
    seen.add(row.id);
    const kw = statusKeyword(row.status);
    if (!kw) problems.push(`${row.id}: ledger status "${row.status}" is not one of ${STATUSES.join(", ")}`);
    if (kw === "DONE" && !HASH.test(row.commits)) problems.push(`${row.id}: ledger row is DONE with no commit hash`);
  }

  const files = new Set(tickets.map((t) => idOfFile(t.name)).filter(Boolean));
  for (const id of files) if (!seen.has(id)) problems.push(`${id}: ticket file has no ledger row in README.md`);
  for (const id of seen) if (!files.has(id)) problems.push(`${id}: ledger row has no ticket file`);

  const named = new Set(subjects.flatMap((s) => s.match(T_IN_SUBJECT) ?? []));
  for (const id of named) if (!seen.has(id)) problems.push(`${id}: named in a commit subject but missing from the ledger`);

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
  return { tickets, readme, subjects };
}

const isMain = process.argv[1] && new URL(import.meta.url).pathname === process.argv[1];
if (isMain) {
  const repoRoot = new URL("../..", import.meta.url).pathname;
  const problems = checkLedger(readRepo(repoRoot));
  if (problems.length) {
    console.error(`✗ the ticket ledger is not whole (${TICKETS_DIR}/README.md):\n  ` + problems.join("\n  ") +
      "\n\n  Every ticket file needs a Status line and a ledger row; DONE needs its commit hashes.");
    process.exit(1);
  }
  console.log(`✓ ticket ledger whole`);
}
