#!/usr/bin/env node
/**
 * (T1041) ONE FILE PER LEDGER ROW AND PER OWNER-TODO ITEM; THE TABLES ARE BUILT FROM THEM.
 *
 * On 2026-11-23/24 a dozen lanes each edited the same three files (the ledger table in `tickets/README.md`,
 * `tickets/DONE.md`, the 3,500-line `OWNER-TODO.md`) and nearly every PR hit a merge conflict there. Now:
 *
 *   - a ticket's ledger row lives in its own file `tickets/Tnnn-*.md`, as a one-row table right under its Status
 *     paragraph (`ROW_HEADER`, `ROW_SEP`, the row: the five cells the ledger always had);
 *   - an owner item lives in its own file `handoff/PLAN/owner-todo/<slug>.md`: `order: <number>`, a blank line, then the
 *     item, which is one `## ` section of the old OWNER-TODO.md, word for word; `_head.md` holds the page's head;
 *   - the combined views are BUILT, never committed (`handoff/PLAN/views/`, git-ignored): `LEDGER.md` (the rows in
 *     work), `DONE.md` (DONE / DROPPED rows), `OWNER-TODO.md` (every item, highest order first).
 *
 * Two PRs that touch different tickets or different owner items touch different files, so they cannot conflict.
 *
 *   node app/tools/ledger.mjs                 build the views (the post-checkout / post-merge hooks run it)
 *   node app/tools/ledger.mjs --check         exit 1 when a view on disk is missing or not what the source builds
 *   node app/tools/ledger.mjs --import        split the OLD files of this work tree (README.md's ledger, DONE.md,
 *                                             OWNER-TODO.md) into the per-item files; refuses unless the round trip
 *                                             (build(import(old)) = old, see `roundTrip`) holds; prints the counts
 *   node app/tools/ledger.mjs --resolve       during a merge of an old-style side with a new-style side: carry the old
 *                                             side's edits (rows, items, README text) into the per-item files
 *   node app/tools/ledger.mjs --merge <ref>   `git merge --no-commit <ref>`, then --resolve, the ticket guard, and the
 *                                             merge commit (stops, saying what, at the first conflict left)
 *
 * The pure functions (row block, views, split, import, round trip, carry) are what the tests feed; the CLI at the end
 * reads the repo and git.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

export const TICKETS_DIR = "handoff/PLAN/app-integration/tickets";
export const README = `${TICKETS_DIR}/README.md`;
/** The old closed-rows file (T540). It no longer exists: its rows are built into `views/DONE.md`. */
export const OLD_DONE = `${TICKETS_DIR}/DONE.md`;
export const OWNER_DIR = "handoff/PLAN/owner-todo";
/** The committed pointer page that used to hold every owner item. */
export const OWNER_PAGE = "handoff/PLAN/OWNER-TODO.md";
export const HEAD_FILE = "_head.md";
/** Files in OWNER_DIR that are not items. */
export const NOT_ITEMS = [HEAD_FILE, "README.md"];
export const VIEWS_DIR = "handoff/PLAN/views";
export const VIEWS = { ledger: `${VIEWS_DIR}/LEDGER.md`, done: `${VIEWS_DIR}/DONE.md`, owner: `${VIEWS_DIR}/OWNER-TODO.md` };

/** The row block's two fixed lines (the old ledger table's header, word for word). */
export const ROW_HEADER = "| id | title | status | commits | what's left |";
export const ROW_SEP = "|---|---|---|---|---|";
/** The `Status:` line sits within this many lines of a ticket's top (check-tickets.mjs STATUS_WITHIN). */
const STATUS_WITHIN = 6;
const ID = /^T\d+[a-z]?/;

/** The ticket id a file name carries (`T15b-caveats.md` -> `T15b`), or null. */
export const idOfFile = (name) => basename(name).match(ID)?.[0] ?? null;
/** DONE and DROPPED rows go to the DONE view; every other row is in work (check-tickets.mjs CLOSED). */
export const isClosed = (status) => ["DONE", "DROPPED"].some((k) => status.trim().startsWith(k));

/** A ledger table row's cells, read as the ledger guard always read them, or null when the line is not a row. */
export function parseRowLine(line) {
  if (typeof line !== "string") return null;
  const cells = line.split("|").slice(1, -1).map((c) => c.trim());
  if (cells.length < 5 || !ID.test(cells[0]) || cells[0] !== cells[0].match(ID)[0]) return null;
  const [id, title, status, commits, left] = cells;
  return { id, title, status, commits, left, line };
}

/** Every row of the table under a `## Ledger` heading (the old README.md / DONE.md, and the built views). */
export function tableRows(text) {
  const lines = text.split("\n");
  const start = lines.findIndex((l) => /^##\s+Ledger\b/.test(l));
  if (start < 0) return [];
  const rows = [];
  for (const line of lines.slice(start + 1)) {
    if (/^##\s/.test(line)) break;
    const row = parseRowLine(line);
    if (row) rows.push(row);
  }
  return rows;
}

/** Every row block in a ticket file: the header line's index, the line under it and the row line. */
export function rowBlocks(text) {
  const lines = text.split("\n");
  const out = [];
  lines.forEach((l, i) => { if (l === ROW_HEADER) out.push({ at: i, sep: lines[i + 1], line: lines[i + 2] }); });
  return out;
}

/**
 * A ticket file's row and what is wrong with its block (none = exactly one block, its separator, a row of its own id).
 * @returns {{ row: ReturnType<typeof parseRowLine>, problems: string[] }}
 */
export function ticketRow(name, text) {
  const id = idOfFile(name);
  const blocks = rowBlocks(text);
  if (!blocks.length) return { row: null, problems: [`${id}: ticket file has no ledger row (the one-row table under its Status line, T1041)`] };
  if (blocks.length > 1) return { row: null, problems: [`${id}: ledger row appears more than once (${name} holds ${blocks.length} row tables: keep one)`] };
  const [b] = blocks;
  if (b.sep !== ROW_SEP) return { row: null, problems: [`${id}: the row table in ${name} has no "${ROW_SEP}" line under its header`] };
  const row = parseRowLine(b.line);
  if (!row) return { row: null, problems: [`${id}: the row table in ${name} holds no row (a "| ${id} | title | status | commits | what's left |" line)`] };
  if (row.id !== id) return { row: null, problems: [`${id}: the row in ${name} is ${row.id}'s: a file holds its own row`] };
  return { row, problems: [] };
}

/** `text` with a row block put right under its Status paragraph: [..., status paragraph, "", header, sep, row, ...]. */
export function insertRow(text, line) {
  const lines = text.split("\n");
  const at = lines.slice(0, STATUS_WITHIN).findIndex((l) => /^Status:/.test(l));
  if (at < 0) throw new Error(`no "Status:" line in the first ${STATUS_WITHIN} lines: the row block goes under it`);
  let end = at + 1;
  while (end < lines.length && lines[end].trim() !== "") end++;
  lines.splice(end, 0, "", ROW_HEADER, ROW_SEP, line);
  return lines.join("\n");
}

/** `text` without its row block (the exact inverse of `insertRow`; the first block when there are more). */
export function stripRow(text) {
  const lines = text.split("\n");
  const h = lines.indexOf(ROW_HEADER);
  if (h < 0) return text;
  const from = h > 0 && lines[h - 1] === "" ? h - 1 : h;
  lines.splice(from, h + 3 - from);
  return lines.join("\n");
}

/** `text` with its row line set to `line` (in place when it has a block, else a new block under the Status line). */
export function setRow(text, line) {
  const lines = text.split("\n");
  const h = lines.indexOf(ROW_HEADER);
  if (h < 0) return insertRow(text, line);
  lines.splice(h + 2, 1, line);
  return lines.join("\n");
}

const idParts = (id) => { const m = id.match(/^T(\d+)([a-z]?)/); return [Number(m[1]), m[2]]; };
/** Ticket ids in number order (T9 < T10 < T10b < T11). */
export function byId(a, b) {
  const [na, sa] = idParts(a), [nb, sb] = idParts(b);
  return na - nb || sa.localeCompare(sb);
}

const banner = (what) =>
  `<!-- GENERATED by \`node app/tools/ledger.mjs\` from ${what} (T1041). Not committed: an edit here is lost, change the source. -->`;
const table = (rows) => [ROW_HEADER, ROW_SEP, ...rows.map((r) => r.line)].join("\n");

/**
 * The two ledger views from the ticket files (`{ name, text }[]`): every well-formed row once, in id order, in the view
 * its status keyword names. A file whose block is wrong is left out (the ticket guard names it).
 */
export function ledgerViews(tickets) {
  const rows = tickets.map((t) => ticketRow(t.name, t.text).row).filter(Boolean).sort((a, b) => byId(a.id, b.id));
  const open = rows.filter((r) => !isClosed(r.status)), closed = rows.filter((r) => isClosed(r.status));
  const ledger = [banner(`the row table in each ${TICKETS_DIR}/T*.md`), "# Ledger: the tickets in work", "",
    `${open.length} rows, in id order: every ticket that is not DONE or DROPPED. The closed ones (${closed.length}) are in`,
    "`DONE.md` beside this file. A row is edited in its ticket file, under the Status line (tickets/README.md says how).",
    "", "## Ledger", "", table(open), ""].join("\n");
  const done = [banner(`the row table in each ${TICKETS_DIR}/T*.md`), "# Closed tickets (DONE / DROPPED)", "",
    `${closed.length} rows, in id order. A reopened ticket's row moves to LEDGER.md by itself: its status says where.`,
    "", "## Ledger", "", table(closed), ""].join("\n");
  return { ledger, done, open: open.length, closed: closed.length };
}

const trimBlank = (lines) => { let e = lines.length; while (e > 0 && lines[e - 1].trim() === "") e--; return lines.slice(0, e); };

/** The old OWNER-TODO.md as its head (the lines before the first `## `) and one item per `## ` section, in page order. */
export function splitOwnerTodo(text) {
  const lines = text.split("\n");
  const first = lines.findIndex((l) => l.startsWith("## "));
  const head = trimBlank(first < 0 ? lines : lines.slice(0, first)).join("\n");
  const items = [];
  if (first >= 0) for (const l of lines.slice(first)) {
    if (l.startsWith("## ")) items.push([l]);
    else items[items.length - 1].push(l);
  }
  return { head, items: items.map((c) => ({ heading: c[0], text: trimBlank(c).join("\n") })) };
}

const ORDER = /^order: (-?\d+(?:\.\d+)?)\n\n/;
/** An item file's text from its order and its item text. */
export const itemFile = (order, text) => `order: ${order}\n\n${text}\n`;

/**
 * The owner to-do from its folder's files (`{ name, text }[]`, names without the folder): the head, the items (order,
 * heading, text) and what is wrong (a bad first line, no heading, two items in a file, a heading used twice, no head).
 */
export function readOwnerItems(files) {
  const problems = [], items = [];
  let head = null;
  for (const { name, text } of files) {
    const where = `${OWNER_DIR}/${name}`;
    if (name === HEAD_FILE) { head = text.replace(/\n+$/, ""); continue; }
    if (NOT_ITEMS.includes(name) || !name.endsWith(".md")) continue;
    const m = text.match(ORDER);
    if (!m) { problems.push(`${where}: the first line must be "order: <number>" and the second blank`); continue; }
    const body = text.slice(m[0].length).replace(/\n+$/, "");
    if (!body.startsWith("## ")) { problems.push(`${where}: the item must start with its "## " heading after the order line`); continue; }
    const more = body.split("\n").slice(1).filter((l) => l.startsWith("## "));
    if (more.length) { problems.push(`${where}: holds more than one item (${more[0]}): one file per item`); continue; }
    items.push({ name, order: Number(m[1]), heading: body.split("\n")[0], text: body });
  }
  if (head === null) problems.push(`${OWNER_DIR}/${HEAD_FILE} is missing: the owner to-do's head (title and how the page works)`);
  const seen = new Map();
  for (const i of items) {
    if (seen.has(i.heading)) problems.push(`${OWNER_DIR}: "${i.heading}" is the heading of ${seen.get(i.heading)} and ${i.name}: a heading names one item`);
    else seen.set(i.heading, i.name);
  }
  return { head: head ?? "", items, problems: problems.sort() };
}

/** Items in page order: highest `order` first, a tie by file name. */
export const pageOrder = (items) => [...items].sort((a, b) => b.order - a.order || a.name.localeCompare(b.name));

/** The owner to-do page text (no banner): the head, then every item, one blank line between. */
export function ownerTodoText(head, items) {
  return [head, ...pageOrder(items).map((i) => i.text)].join("\n\n") + "\n";
}

/** A file name for an item heading, unique among `taken` (lower-case, so no clash on a case-blind disk). */
export function itemName(heading, taken) {
  let s = heading.replace(/^##\s+/, "").toLowerCase().normalize("NFKD").replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  if (s.length > 60) s = s.slice(0, 60).replace(/-[^-]*$/, "") || s.slice(0, 60);
  if (!s || NOT_ITEMS.some((n) => n.toLowerCase() === `${s}.md`) || s.startsWith("_")) s = `item-${s}`.replace(/-$/, "");
  let name = `${s}.md`;
  for (let k = 2; taken.has(name); k++) name = `${s}-${k}.md`;
  taken.add(name);
  return name;
}

/** The `## Ledger` section of tickets/README.md once the rows live in the ticket files. */
export const LEDGER_POINTER = `## Ledger

**The rows are not kept here any more (T1041).** Each ticket's row lives in its own file, as a one-row table right
under its \`Status:\` line (same five cells as ever):

    | id | title | status | commits | what's left |
    |---|---|---|---|---|
    | Tnnn | the ticket's title | OPEN | — | what's left, a pointer (240 characters at most) |

\`node app/tools/ledger.mjs\` builds the tables into \`handoff/PLAN/views/LEDGER.md\` (every row not DONE / DROPPED) and
\`handoff/PLAN/views/DONE.md\` (the closed ones), git-ignored; the post-checkout and post-merge hooks rebuild them. So two
PRs that touch different tickets never conflict. A row's view follows its status keyword: nothing to move by hand.
\`node app/tools/check-tickets.mjs\` checks the rows in the ticket files, and refuses a row typed here.
`;

/** OWNER-TODO.md once the items live in their own files: a pointer, never an item (the ticket guard refuses a `## `). */
export const OWNER_PAGE_TEXT = `# Owner to-do: where it lives now (T1041)

The owner ticks items off in the Owner To-Do page: https://claude.ai/artifact/Lh2ougAwfxx1wNmCV18m8f (owner
2026-11-11). Every owner item is a row there AND a file in \`handoff/PLAN/owner-todo/\`, one file per item (it used to
be a section of this page; a dozen lanes editing this one page conflicted on nearly every PR).

- **Read it:** \`node app/tools/ledger.mjs\` builds the whole page, newest first, into \`handoff/PLAN/views/OWNER-TODO.md\`
  (git-ignored; the post-checkout and post-merge hooks rebuild it).
- **Add an item:** a new file \`handoff/PLAN/owner-todo/<date>-<what>.md\`: first line \`order: <number>\` (higher is nearer
  the top; one more than the current top puts it first), a blank line, then the item as a \`## \` heading and its text.
  A ticket's \`owner: OWNER-TODO "<text>"\` must quote text one of these files holds (the ticket guard checks it).
- **Change or close one:** edit its file; it keeps its history text. A Store checks line from \`pipeline reports\` is a new
  file of its own (\`<date>-store-checks.md\`), never an edit of an older one.
- The page's own head (title, how the page works) is \`handoff/PLAN/owner-todo/_head.md\`.

This page holds no item: the ticket guard fails on a \`## \` section here.
`;

/** `readme` with its `## Ledger` section (up to the next `## `) replaced by `section`. */
export function replaceLedgerSection(readme, section) {
  const lines = readme.split("\n");
  const start = lines.findIndex((l) => /^##\s+Ledger\b/.test(l));
  if (start < 0) throw new Error("README.md has no ## Ledger section");
  let end = start + 1;
  while (end < lines.length && !/^##\s/.test(lines[end])) end++;
  const tail = lines.slice(end);
  return [...lines.slice(0, start), ...section.replace(/\n+$/, "").split("\n"), ...(tail.length ? ["", ...tail] : [""])].join("\n");
}

/** The `## Ledger` section of a README (heading to the line before the next `## `). */
export function ledgerSection(readme) {
  const lines = readme.split("\n");
  const start = lines.findIndex((l) => /^##\s+Ledger\b/.test(l));
  if (start < 0) return null;
  let end = start + 1;
  while (end < lines.length && !/^##\s/.test(lines[end])) end++;
  return trimBlank(lines.slice(start, end)).join("\n") + "\n";
}

/**
 * Split the old files into the new ones. Pure.
 * @param {{ tickets: { name: string, text: string }[], readme: string, done: string, ownerTodo: string }} old
 * @returns {{ tickets: { name: string, text: string }[], ownerFiles: { name: string, text: string }[], readme: string,
 *            ownerPage: string, problems: string[] }}
 */
export function importOld(old) {
  const problems = [];
  const rows = new Map();
  for (const [file, text] of [["README.md", old.readme], ["DONE.md", old.done]])
    for (const r of tableRows(text)) {
      if (rows.has(r.id)) problems.push(`${r.id}: two old ledger rows (${rows.get(r.id).file} and ${file}): settle them before importing`);
      else rows.set(r.id, { ...r, file });
    }
  const fileIds = new Set();
  const tickets = old.tickets.map((t) => {
    const id = idOfFile(t.name);
    fileIds.add(id);
    const r = rows.get(id);
    if (rowBlocks(t.text).length) { problems.push(`${id}: ${t.name} already holds a row table (imported before?)`); return t; }
    if (!r) { problems.push(`${id}: ${t.name} has no row in the old ledger`); return t; }
    try { return { name: t.name, text: insertRow(t.text, r.line) }; }
    catch (e) { problems.push(`${id}: ${t.name}: ${e.message}`); return t; }
  });
  for (const id of rows.keys()) if (!fileIds.has(id)) problems.push(`${id}: old ledger row with no ticket file`);
  const { head, items } = splitOwnerTodo(old.ownerTodo);
  const taken = new Set();
  const ownerFiles = [{ name: HEAD_FILE, text: `${head}\n` },
    ...items.map((it, k) => ({ name: itemName(it.heading, taken), text: itemFile((items.length - k) * 10, it.text) }))];
  return { tickets, ownerFiles, readme: replaceLedgerSection(old.readme, LEDGER_POINTER), ownerPage: OWNER_PAGE_TEXT,
           problems: problems.sort() };
}

const nonBlank = (text) => text.split("\n").filter((l) => l.trim() !== "");

/**
 * THE ROUND TRIP, checked WITHOUT the import's own parser: the views built from the new files hold exactly the old
 * tables' rows (the raw lines, compared as sorted lists: order and the README/DONE split are normalised), every ticket
 * file minus its row block is its old text byte for byte, and the owner to-do built from the item files holds the old
 * page's non-blank lines in the old order (blank lines are normalised: one between items, none at an item's end).
 * @returns {{ problems: string[], counts: Record<string, number> }}
 */
export function roundTrip(old, out) {
  const problems = [];
  const oldReadme = tableRows(old.readme), oldDone = tableRows(old.done);
  const was = [...oldReadme, ...oldDone].map((r) => r.line).sort();
  const views = ledgerViews(out.tickets);
  const now = [...tableRows(views.ledger), ...tableRows(views.done)].map((r) => r.line).sort();
  const count = (xs) => xs.reduce((m, x) => m.set(x, (m.get(x) ?? 0) + 1), new Map());
  const cw = count(was), cn = count(now);
  for (const [l, n] of cw) if ((cn.get(l) ?? 0) < n) problems.push(`row dropped: ${l.slice(0, 100)}`);
  for (const [l, n] of cn) if ((cw.get(l) ?? 0) < n) problems.push(`row added or duplicated: ${l.slice(0, 100)}`);
  const before = new Map(old.tickets.map((t) => [t.name, t.text]));
  for (const t of out.tickets) if (stripRow(t.text) !== before.get(t.name)) problems.push(`${t.name}: its text changed beyond the row block`);
  if (out.tickets.length !== old.tickets.length) problems.push(`ticket files: ${old.tickets.length} in, ${out.tickets.length} out`);
  const owner = readOwnerItems(out.ownerFiles);
  problems.push(...owner.problems);
  const a = nonBlank(old.ownerTodo), b = nonBlank(ownerTodoText(owner.head, owner.items));
  const at = a.findIndex((l, i) => l !== b[i]);
  if (at >= 0 || a.length !== b.length)
    problems.push(`owner to-do differs at non-blank line ${at >= 0 ? at + 1 : Math.min(a.length, b.length) + 1}: ` +
      `"${(a[at] ?? "").slice(0, 60)}" became "${(b[at] ?? "").slice(0, 60)}" (${a.length} lines in, ${b.length} out)`);
  const sections = splitOwnerTodo(old.ownerTodo).items.length;
  return { problems, counts: {
    "rows in README.md": oldReadme.length, "rows in DONE.md": oldDone.length,
    "ticket files": out.tickets.length, "rows in the LEDGER view": views.open, "rows in the DONE view": views.closed,
    "closed rows that sat in README.md (now in the DONE view)": oldReadme.filter((r) => isClosed(r.status)).length,
    "owner sections in": sections, "owner item files out": owner.items.length,
    "owner non-blank lines in": a.length, "owner non-blank lines out": b.length } };
}

// ------------------------------------------------------------------------------------------- carrying old-style edits

const isTicketPath = (p) => p.startsWith(`${TICKETS_DIR}/`) && /^T[^/]*\.md$/.test(p.slice(TICKETS_DIR.length + 1));
const isOwnerPath = (p) => p.startsWith(`${OWNER_DIR}/`) && !p.slice(OWNER_DIR.length + 1).includes("/") && p.endsWith(".md");
/** The paths this tool owns: ticket files, the README, the old DONE.md, the owner page and the owner folder. */
export const isManaged = (p) => isTicketPath(p) || isOwnerPath(p) || p === README || p === OLD_DONE || p === OWNER_PAGE;
/** A tree (Map path -> text) is new-style when it holds the owner folder's head file. */
export const isNewStyle = (tree) => tree.has(`${OWNER_DIR}/${HEAD_FILE}`);

const conflictRow = (ours, theirs) => ["<<<<<<< new-style side", ours ?? "(no row)", "=======", theirs ?? "(no row)", ">>>>>>> old-style side"].join("\n");

/**
 * Carry what the OLD-style side changed since the base into the NEW-style side's files. Pure: three trees in (Map of
 * managed path -> text: `base` and `old` are old-style, `neu` new-style), the result out. `merge3(ours, base, theirs)`
 * is a 3-way text merge (`gitMerge3`). Per ticket file: its text merges 3-way with the row block taken out, and its row
 * is the old side's when that side changed it, else the new side's (both changed it, differently: a conflict). Per owner
 * item (keyed by its heading): the old side's edit, addition or removal is applied; both sides editing one item merge
 * 3-way. README.md: the old side's changes outside the ledger merge into the new README.
 * @returns {{ write: Map<string,string>, remove: Set<string>, conflicts: { path: string|null, why: string }[],
 *            carried: Record<string, number> }} — a conflict with a path has markers in that file; one without needs a look

 */
export function carry({ base, old, neu }, merge3) {
  const write = new Map(), remove = new Set(), conflicts = [];
  const carried = { "ticket files": 0, rows: 0, "owner items": 0, "README text": 0 };
  const rowsOf = (tree) => new Map([...tableRows(tree.get(README) ?? ""), ...tableRows(tree.get(OLD_DONE) ?? "")].map((r) => [r.id, r.line]));
  const rB = rowsOf(base), rO = rowsOf(old);

  const paths = new Set([...base.keys(), ...old.keys(), ...neu.keys()].filter(isTicketPath));
  for (const p of [...paths].sort()) {
    const tB = base.get(p), tO = old.get(p), tN = neu.get(p);
    const id = idOfFile(p);
    const rowB = rB.get(id), rowO = rO.get(id);
    if (tO === tB && rowO === rowB) continue;
    const sN = tN === undefined ? undefined : stripRow(tN);
    const rowN = tN === undefined ? undefined : rowBlocks(tN)[0]?.line;
    let text, clash = false;
    if (tO === tB) text = sN;
    else if (sN === tB || sN === tO) text = tO;
    else if (tO === undefined || sN === undefined) { text = tO ?? sN; clash = true; }   // removed on one side, edited on the other
    else ({ text, conflict: clash } = merge3(sN, tB ?? "", tO));
    if (text === undefined) {
      remove.add(p);
      if (rowO !== rowB) conflicts.push({ path: null, why: `${p}: its row changed on the old-style side, the file is gone on the new-style side (renamed?): carry the row by hand` });
      continue;
    }
    let row;
    if (rowO === rowB) row = rowN;
    else if (rowN === rowB || rowN === rowO) { row = rowO; if (rowO !== rowN) carried.rows++; }
    else { row = conflictRow(rowN, rowO); clash = true; }
    let out = text;
    if (row !== undefined) {
      try { out = setRow(text, row); }
      catch { out = `${text.replace(/\n*$/, "")}\n\n${ROW_HEADER}\n${ROW_SEP}\n${row}\n`; clash = true; }
    }
    if (out !== tN) { write.set(p, out); carried["ticket files"]++; }
    if (clash) conflicts.push({ path: p, why: `${p}: both sides changed it` });
  }

  // the owner to-do, item by item (keyed by heading)
  const sB = splitOwnerTodo(base.get(OWNER_PAGE) ?? ""), sO = splitOwnerTodo(old.get(OWNER_PAGE) ?? "");
  const nFiles = [...neu].filter(([p]) => isOwnerPath(p)).map(([p, text]) => ({ name: p.slice(OWNER_DIR.length + 1), text }));
  const n = readOwnerItems(nFiles);
  const mB = new Map(sB.items.map((i) => [i.heading, i.text])), mO = new Map(sO.items.map((i) => [i.heading, i.text]));
  const mN = new Map(n.items.map((i) => [i.heading, i]));
  const pathOf = (name) => `${OWNER_DIR}/${name}`;
  if (sO.head !== sB.head) {
    const hp = pathOf(HEAD_FILE);
    if (n.head === sB.head) write.set(hp, `${sO.head}\n`);
    else if (n.head !== sO.head) { const m = merge3(`${n.head}\n`, `${sB.head}\n`, `${sO.head}\n`); write.set(hp, m.text); if (m.conflict) conflicts.push({ path: hp, why: `${hp}: both sides changed the head` }); }
    carried["owner items"]++;
  }
  const taken = new Set(nFiles.map((f) => f.name));
  const orders = new Map(n.items.map((i) => [i.heading, i.order]));
  const oList = sO.items.map((i) => i.heading);
  const place = (k) => {
    let above, below;
    for (let i = k - 1; i >= 0 && above === undefined; i--) above = orders.get(oList[i]);
    for (let i = k + 1; i < oList.length && below === undefined; i++) below = orders.get(oList[i]);
    if (above !== undefined && below !== undefined) return (above + below) / 2;
    if (below !== undefined) return below + 10;
    if (above !== undefined) return above - 10;
    return 10;
  };
  const headings = [...new Set([...sB.items.map((i) => i.heading), ...oList])];
  // additions first, in the old side's page order, so a run of new items stacks in order
  for (const h of oList.filter((x) => !mB.has(x) && !mN.has(x))) {
    const order = place(oList.indexOf(h));
    orders.set(h, order);
    write.set(pathOf(itemName(h, taken)), itemFile(order, mO.get(h)));
    carried["owner items"]++;
  }
  for (const h of headings) {
    const b = mB.get(h), o = mO.get(h), nItem = mN.get(h);
    if (o === b || (b === undefined && nItem === undefined)) continue;
    carried["owner items"]++;
    if (o === undefined) {                                    // removed on the old side
      if (nItem && nItem.text === b) remove.add(pathOf(nItem.name));
      else if (nItem) conflicts.push({ path: null, why: `${pathOf(nItem.name)}: removed on the old-style side, edited on the new-style side (kept: remove it by hand if it should go)` });
      continue;
    }
    if (nItem === undefined) {                                // edited on the old side, removed on the new side
      conflicts.push({ path: null, why: `${OWNER_DIR}: "${h}" was edited on the old-style side and removed on the new-style side (not brought back)` });
      continue;
    }
    if (nItem.text === b || nItem.text === o) write.set(pathOf(nItem.name), itemFile(nItem.order, o));
    else {
      const m = merge3(`${nItem.text}\n`, `${b ?? ""}\n`, `${o}\n`);
      write.set(pathOf(nItem.name), `order: ${nItem.order}\n\n${m.text}`);
      if (m.conflict) conflicts.push({ path: pathOf(nItem.name), why: `${pathOf(nItem.name)}: both sides changed the item` });
    }
  }

  // README.md: the old side's edits outside the ledger; the old pages settle to the new side's
  const rmB = base.get(README), rmO = old.get(README), rmN = neu.get(README);
  if (rmO !== rmB && rmN !== undefined) {
    const sec = ledgerSection(rmN);
    const b2 = sec && rmB !== undefined && ledgerSection(rmB) ? replaceLedgerSection(rmB, sec) : rmB;
    const o2 = sec && rmO !== undefined && ledgerSection(rmO) ? replaceLedgerSection(rmO, sec) : rmO;
    if (o2 === b2 || o2 === undefined) write.set(README, rmN);
    else if (rmN === b2) { write.set(README, o2); carried["README text"]++; }
    else { const m = merge3(rmN, b2 ?? "", o2); write.set(README, m.text); carried["README text"]++; if (m.conflict) conflicts.push({ path: README, why: `${README}: both sides changed the same lines` }); }
  }
  if (!neu.has(OLD_DONE) && (old.has(OLD_DONE) || base.has(OLD_DONE))) remove.add(OLD_DONE);
  if (neu.has(OWNER_PAGE) && old.get(OWNER_PAGE) !== base.get(OWNER_PAGE)) write.set(OWNER_PAGE, neu.get(OWNER_PAGE));
  return { write, remove, conflicts, carried };
}

// --------------------------------------------------------------------------------------------------------- the repo

/** A 3-way text merge by `git merge-file` (ours, base, theirs); `conflict` when it left markers. */
export function gitMerge3(ours, base, theirs) {
  const dir = mkdtempSync(join(tmpdir(), "ledger-merge-"));
  try {
    const f = (n, t) => { const p = join(dir, n); writeFileSync(p, t); return p; };
    const args = ["merge-file", "-p", "-L", "new-style side", "-L", "base", "-L", "old-style side", f("a", ours), f("b", base), f("c", theirs)];
    const got = spawnSync("git", args, { encoding: "utf8", maxBuffer: 1 << 28 });
    if (got.error || got.status === null || got.status < 0 || got.status > 127)
      throw new Error(`git merge-file failed (status ${got.status}): ${got.error?.message ?? got.stderr}`);
    return { text: got.stdout, conflict: got.status > 0 };
  } finally { rmSync(dir, { recursive: true, force: true }); }
}

/** git without the caller's repository variables (a hook's GIT_DIR must not point this at another repository). */
const cleanEnv = () => Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("GIT_")));
const git = (repo, args, opts = {}) => execFileSync("git", ["-C", repo, ...args], { encoding: "utf8", env: cleanEnv(), maxBuffer: 1 << 30, ...opts });

/** The managed files of commit `ref` as a Map path -> text (one `git cat-file --batch` for all of them). */
export function treeAt(repo, ref) {
  const listed = git(repo, ["ls-tree", "-r", "-z", "--name-only", ref, "--", TICKETS_DIR, OWNER_DIR, OWNER_PAGE])
    .split("\0").filter((p) => p && isManaged(p));
  const tree = new Map();
  if (!listed.length) return tree;
  const out = execFileSync("git", ["-C", repo, "cat-file", "--batch"], { input: listed.map((p) => `${ref}:${p}`).join("\n") + "\n", env: cleanEnv(), maxBuffer: 1 << 30 });
  let at = 0;
  for (const p of listed) {
    const nl = out.indexOf(10, at);
    const header = out.subarray(at, nl).toString("utf8");
    const m = header.match(/^[0-9a-f]+ blob (\d+)$/);
    if (!m) throw new Error(`git cat-file ${ref}:${p}: ${header}`);
    const size = Number(m[1]);
    tree.set(p, out.subarray(nl + 1, nl + 1 + size).toString("utf8"));
    at = nl + 1 + size + 1;
  }
  return tree;
}

/** The work tree's tickets, owner items and pages, as the guard and the views read them. */
export function readWorkTree(repo) {
  const tdir = join(repo, TICKETS_DIR), odir = join(repo, OWNER_DIR);
  const tickets = readdirSync(tdir).filter((n) => /^T.*\.md$/.test(n)).sort().map((name) => ({ name, text: readFileSync(join(tdir, name), "utf8") }));
  const ownerFiles = existsSync(odir)
    ? readdirSync(odir).filter((n) => n.endsWith(".md")).sort().map((name) => ({ name, text: readFileSync(join(odir, name), "utf8") }))
    : null;
  return { tickets, ownerFiles };
}

/**
 * The owner to-do of a work tree: its text (head + items, as the view prints it, no banner) and what is wrong. A missing
 * folder is a problem, never an empty to-do.
 */
export function readOwnerTodo(repo) {
  const { ownerFiles } = readWorkTree(repo);
  if (!ownerFiles) return { text: "", items: [], problems: [`${OWNER_DIR}/ is missing: the owner items live there (T1041)`] };
  const o = readOwnerItems(ownerFiles);
  return { text: ownerTodoText(o.head, o.items), items: o.items, problems: o.problems };
}

/** The three views as they should be on disk. */
export function buildViews(repo) {
  const { tickets } = readWorkTree(repo);
  const lv = ledgerViews(tickets);
  const o = readOwnerTodo(repo);
  return {
    files: { [VIEWS.ledger]: lv.ledger, [VIEWS.done]: lv.done,
             [VIEWS.owner]: `${banner(`the files in ${OWNER_DIR}/`)}\n${o.text}` },
    summary: `${lv.open} rows in work, ${lv.closed} closed, ${o.items.length} owner items`,
    problems: [...tickets.flatMap((t) => ticketRow(t.name, t.text).problems), ...o.problems],
  };
}

function writeViews(repo) {
  const v = buildViews(repo);
  mkdirSync(join(repo, VIEWS_DIR), { recursive: true });
  for (const [p, text] of Object.entries(v.files)) writeFileSync(join(repo, p), text);
  return v;
}

function cliImport(repo) {
  const tdir = join(repo, TICKETS_DIR);
  if (existsSync(join(repo, OWNER_DIR, HEAD_FILE))) throw new Error(`${OWNER_DIR}/${HEAD_FILE} exists: this tree is imported already (after a merge with an old-style side: --resolve)`);
  const read = (p) => readFileSync(join(repo, p), "utf8");
  const old = { tickets: readWorkTree(repo).tickets, readme: read(README), done: read(OLD_DONE), ownerTodo: read(OWNER_PAGE) };
  const out = importOld(old);
  const rt = roundTrip(old, out);
  for (const [k, v] of Object.entries(rt.counts)) console.log(`  ${k}: ${v}`);
  const problems = [...out.problems, ...rt.problems];
  if (problems.length) throw new Error(`the import was NOT written:\n  ${problems.join("\n  ")}`);
  for (const t of out.tickets) writeFileSync(join(tdir, t.name), t.text);
  mkdirSync(join(repo, OWNER_DIR), { recursive: true });
  for (const f of out.ownerFiles) writeFileSync(join(repo, OWNER_DIR, f.name), f.text);
  writeFileSync(join(repo, README), out.readme);
  writeFileSync(join(repo, OWNER_PAGE), out.ownerPage);
  rmSync(join(repo, OLD_DONE));
  console.log(`✓ imported: round trip holds (rows compared as raw lines, sorted; owner to-do by its non-blank lines)`);
}

/** --resolve: the merge in progress has one old-style and one new-style side; carry the old side's edits. */
function cliResolve(repo) {
  const head = git(repo, ["rev-parse", "HEAD"]).trim();
  const mergeHead = spawnSync("git", ["-C", repo, "rev-parse", "-q", "--verify", "MERGE_HEAD"], { encoding: "utf8", env: cleanEnv() });
  if (mergeHead.status !== 0) throw new Error("no merge in progress (MERGE_HEAD): run `git merge <ref>` first, or `node app/tools/ledger.mjs --merge <ref>`");
  const theirs = mergeHead.stdout.trim();
  const baseRef = git(repo, ["merge-base", head, theirs]).trim();
  const tHead = treeAt(repo, head), tTheirs = treeAt(repo, theirs), tBase = treeAt(repo, baseRef);
  if (isNewStyle(tBase)) { console.log("both sides already keep one file per row (the base is new-style): nothing to carry"); return 0; }
  const [neu, old, newRef] = isNewStyle(tHead) && !isNewStyle(tTheirs) ? [tHead, tTheirs, head]
    : isNewStyle(tTheirs) && !isNewStyle(tHead) ? [tTheirs, tHead, theirs] : [null, null, null];
  if (!neu) { console.log("neither side keeps one file per row (T1041): nothing to carry"); return 0; }
  const res = carry({ base: tBase, old, neu }, gitMerge3);
  const stage = [], conflicted = new Set(res.conflicts.map((c) => c.path).filter(Boolean));
  for (const [p, text] of res.write) {
    mkdirSync(join(repo, p, ".."), { recursive: true });
    writeFileSync(join(repo, p), text);
    if (!conflicted.has(p)) stage.push(p);
  }
  // every managed path git left unmerged and the carry did not write is the new side's version
  const unmerged = git(repo, ["diff", "--name-only", "--diff-filter=U", "-z"]).split("\0").filter((p) => p && isManaged(p));
  for (const p of unmerged) if (!res.write.has(p) && !res.remove.has(p) && !conflicted.has(p)) {
    if (neu.has(p)) { writeFileSync(join(repo, p), neu.get(p)); stage.push(p); }
    else res.remove.add(p);
  }
  for (let i = 0; i < stage.length; i += 200) git(repo, ["add", "--", ...stage.slice(i, i + 200)]);
  for (const p of res.remove) git(repo, ["rm", "-q", "-f", "--ignore-unmatch", "--", p]);
  // a carried conflict is marked unmerged in the index, so `git commit` refuses it until it is settled
  for (const p of conflicted) {
    const blob = (ref) => spawnSync("git", ["-C", repo, "rev-parse", "-q", "--verify", `${ref}:${p}`], { encoding: "utf8", env: cleanEnv() }).stdout.trim();
    const info = [`0 ${"0".repeat(40)}\t${p}`];
    for (const [n, ref] of [[1, baseRef], [2, head], [3, theirs]]) { const b = blob(ref); if (b) info.push(`100644 ${b} ${n}\t${p}`); }
    if (info.length > 1) git(repo, ["update-index", "--index-info"], { input: info.join("\n") + "\n" });
  }
  console.log(`carried from the old-style side (${newRef === head ? "the branch being merged in" : "this branch"}): ` +
    Object.entries(res.carried).map(([k, v]) => `${v} ${k}`).join(", ") + `; ${res.remove.size} removed`);
  if (res.conflicts.length) {
    console.error(`✗ ${res.conflicts.length} left to settle by hand (a file named has markers: settle it, then git add it):\n  ` +
      res.conflicts.map((c) => c.why).join("\n  "));
    return 1;
  }
  return 0;
}

function cliMerge(repo, ref) {
  if (!ref) throw new Error("--merge needs the ref to merge, e.g. --merge origin/main");
  const m = spawnSync("git", ["-C", repo, "merge", "--no-ff", "--no-commit", ref], { encoding: "utf8", env: cleanEnv(), stdio: "inherit" });
  if (m.status !== 0 && m.status !== 1) throw new Error(`git merge ${ref} failed (status ${m.status})`);
  const merging = spawnSync("git", ["-C", repo, "rev-parse", "-q", "--verify", "MERGE_HEAD"], { env: cleanEnv() }).status === 0;
  if (!merging) { console.log(`nothing to merge (${ref} is already in)`); return 0; }
  if (cliResolve(repo) !== 0) return 1;
  const left = git(repo, ["diff", "--name-only", "--diff-filter=U"]).trim();
  if (left) { console.error(`✗ git left these to settle by hand (then git add them and commit):\n  ${left.split("\n").join("\n  ")}`); return 1; }
  writeViews(repo);
  const guard = spawnSync(process.execPath, [join(repo, "app/tools/check-tickets.mjs")], { cwd: repo, stdio: "inherit", env: cleanEnv() });
  if (guard.status !== 0) { console.error("✗ the ticket guard fails on the merged tree: the merge is NOT committed (fix, git add, commit)"); return 1; }
  const c = spawnSync("git", ["-C", repo, "commit", "--no-edit"], { stdio: "inherit", env: cleanEnv() });
  return c.status === 0 ? 0 : 1;
}

const isMain = process.argv[1] && new URL(import.meta.url).pathname === process.argv[1];
if (isMain) {
  const [flag, arg] = process.argv.slice(2);
  try {
    // the repository of the current directory (the post-checkout hook runs at its root, `pnpm` scripts in app/)
    const repo = git(process.cwd(), ["rev-parse", "--show-toplevel"]).trim();
    if (flag === "--import") { cliImport(repo); console.log(`views: ${writeViews(repo).summary}`); }
    else if (flag === "--resolve") { const rc = cliResolve(repo); if (rc === 0) writeViews(repo); process.exit(rc); }
    else if (flag === "--merge") process.exit(cliMerge(repo, arg));
    else if (flag === "--check") {
      const v = buildViews(repo);
      const stale = Object.entries(v.files).filter(([p, text]) => !existsSync(join(repo, p)) || readFileSync(join(repo, p), "utf8") !== text).map(([p]) => p);
      for (const p of v.problems) console.error(`✗ ${p}`);
      for (const p of stale) console.error(`✗ ${p} is missing or out of date: node app/tools/ledger.mjs builds it`);
      if (v.problems.length || stale.length) process.exit(1);
      console.log(`✓ views current (${v.summary})`);
    }
    else if (flag === undefined || flag === "--quiet") {
      const v = writeViews(repo);
      for (const p of v.problems) console.error(`✗ ${p}`);
      if (flag !== "--quiet") console.log(`views (${VIEWS_DIR}/): ${v.summary}`);
      if (v.problems.length) process.exit(1);
    }
    else throw new Error(`unknown flag ${flag} (none, --check, --import, --resolve, --merge <ref>)`);
  } catch (e) {
    console.error(`✗ ledger: ${e.message}`);
    process.exit(1);
  }
}
