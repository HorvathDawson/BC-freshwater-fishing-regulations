#!/usr/bin/env node
/**
 * (T1041, T1081) THE LEDGER ROWS AND THE OWNER'S OPEN ASKS, EACH IN ITS TICKET; THE PAGES ARE BUILT FROM THEM.
 *
 *   - a ticket's ledger row lives in its own file `tickets/Tnnn-*.md`, as a one-row table right under its Status
 *     paragraph (`ROW_HEADER`, `ROW_SEP`, the row: the five cells the ledger always had);
 *   - (T1081) anything only the owner can do or decide is an `## Owner` block in the ticket it belongs to: lines
 *     `- [ ] YYYY-MM-DD — <ask>` (open) and `- [x] YYYY-MM-DD — <ask> → "<his answer>" (YYYY-MM-DD)` (answered). An ask
 *     with no ticket yet is a `## <title>` section of `tickets/OWNER-UNFILED.md`, the same lines, until the planning chat
 *     numbers a ticket for it. There is no owner-todo folder any more;
 *   - the combined views are BUILT, never committed (`docs/views/`, git-ignored): `LEDGER.md` (the rows in
 *     work), `DONE.md` (DONE / DROPPED rows), `OWNER.md` (every open owner line, newest first, by ticket).
 *
 *   node app/tools/ledger.mjs                 build the views (the post-checkout / post-merge hooks run it)
 *   node app/tools/ledger.mjs --check         exit 1 when a view on disk is missing or not what the source builds
 *                                             (check-tickets.mjs prints it as a warning)
 *   node app/tools/ledger.mjs --claim         write `tickets/ids/Tnnn` (the number's claim) for each new ticket file
 *   node app/tools/ledger.mjs --next          print the next free ticket number (closed.tsv, claims, open files, and the claims
 *                                             on remote-tracking branches: a number once used is never reused)
 *   node app/tools/ledger.mjs --archive [Tnnn…]  (T1081) move the CLOSED tickets (DONE / DROPPED; or only those named) out
 *                                             of the tree: their rows go to `tickets/closed.tsv`, their text into
 *                                             `tickets/archive/closed-<year>.tar.gz`, then the files, claims and picture
 *                                             folders are deleted; `--dry` only says what it would do. A ticket with an
 *                                             open owner line is refused: answer or drop the line first
 *
 * The pure functions (row block, owner lines, views, archive) are what the tests feed; the CLI at the end reads the repo
 * and git.
 */
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { gunzipSync, gzipSync } from "node:zlib";
import { basename, join } from "node:path";

export const TICKETS_DIR = "tickets";
/** (T1081) Where the tickets folder sat before the rename to docs/ + tickets/: the folder's git history is under it, and a branch
 *  not yet rebased still holds its tickets and claims there. The rename script leaves a line marked as below alone. */
export const LEGACY_TICKETS_DIR = "handoff/PLAN/app-integration/tickets";   // rename_handoff: keep
export const README = `${TICKETS_DIR}/README.md`;
/** The old closed-rows file (T540). It no longer exists: its rows are built into `views/DONE.md`. */
export const OLD_DONE = `${TICKETS_DIR}/DONE.md`;
/** (T1081) Owner asks that have no ticket yet: `## <title>` sections of owner lines; the planning chat numbers them. */
export const OWNER_UNFILED = `${TICKETS_DIR}/OWNER-UNFILED.md`;
/** (T1081) Paths that must not exist again: the owner to-do page and folder were replaced by `## Owner` blocks. */
export const RETIRED_OWNER_PATHS = ["docs/OWNER-TODO.md", "docs/owner-todo"];
export const VIEWS_DIR = "docs/views";
export const VIEWS = { ledger: `${VIEWS_DIR}/LEDGER.md`, done: `${VIEWS_DIR}/DONE.md`, owner: `${VIEWS_DIR}/OWNER.md` };

/**
 * (T1081) CLOSED TICKETS LIVE IN ONE INDEX AND ONE ARCHIVE, NOT AS FILES. `closed.tsv`: one line per DONE / DROPPED ticket,
 * id order, tab-separated, no header: id, title, status, commits (the ledger row's cells), release (a `0.x.y` or
 * `r-<12 hex>` the row names, else empty), left (the row's last cell), archive (the tar's name: `closed-2026`), tokens (every
 * report id / crash hash / feedback id the ticket text cites, space-separated: `pipeline/reports.py` finds them here).
 * The full text of each closed ticket is in `archive/<archive>.tar.gz` as `<file name>`; read one with
 * `tar -xzOf tickets/archive/closed-2026.tar.gz T123-*.md`.
 */
export const CLOSED_TSV = `${TICKETS_DIR}/closed.tsv`;
export const ARCHIVE_DIR = `${TICKETS_DIR}/archive`;
export const archiveName = (year) => `closed-${year}`;
export const archivePath = (name) => `${ARCHIVE_DIR}/${name}.tar.gz`;
const TSV_COLUMNS = ["id", "title", "status", "commits", "release", "left", "archive", "tokens"];

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

// ------------------------------------------------------------------------------------------- (T1081) the closed index

/** The report ids / crash hashes (16 hex), TestFlight feedback ids and Play review ids a text cites (`pipeline/reports.py`). */
export function citeTokens(text) {
  const out = new Set();
  for (const m of text.matchAll(/(?<![0-9A-Za-z])[0-9a-f]{16}(?![0-9A-Za-z])/g)) out.add(m[0]);
  for (const m of text.matchAll(/(?<![0-9A-Za-z_:-])[A-Za-z0-9_-]{20,32}(?![0-9A-Za-z_-])/g))
    if (/[A-Z]/.test(m[0]) && /[a-z]/.test(m[0]) && /\d/.test(m[0])) out.add(m[0]);
  for (const m of text.matchAll(/(?<![0-9A-Za-z])gp:[A-Za-z0-9_-]{20,}/g)) out.add(m[0]);
  return [...out].sort();
}

/** The release a row names (`0.10.0`, or `r-<12 hex>`), else "". */
export const releaseOf = (row) => `${row.status} ${row.commits} ${row.left}`.match(/\br-[0-9a-f]{12}\b|(?<![\w.])0\.\d+\.\d+\b/)?.[0] ?? "";

const clean = (s) => String(s).replace(/[\t\r\n]+/g, " ").trim();

/** A closed.tsv line from a ledger row and its ticket text. */
export function tsvLine(row, text, archive) {
  return [row.id, row.title, row.status, row.commits, releaseOf(row), row.left, archive, citeTokens(text).join(" ")].map(clean).join("\t");
}

/**
 * closed.tsv read: one `{ id, title, status, commits, release, left, archive, tokens, line }` per line, and what is wrong
 * (a line with the wrong number of fields, an id out of order or twice, a status that is not DONE / DROPPED).
 */
export function parseTsv(text) {
  const rows = [], problems = [];
  const lines = text.split("\n");
  if (lines.at(-1) === "") lines.pop();
  else if (text !== "") problems.push(`${CLOSED_TSV}: the last line does not end with a newline`);
  lines.forEach((line, i) => {
    const f = line.split("\t");
    const at = `${CLOSED_TSV}:${i + 1}`;
    if (f.length !== TSV_COLUMNS.length) { problems.push(`${at}: ${f.length} fields, ${TSV_COLUMNS.length} wanted (${TSV_COLUMNS.join(", ")})`); return; }
    const r = Object.fromEntries(TSV_COLUMNS.map((k, j) => [k, f[j]]));
    if (!/^T\d+[a-z]?$/.test(r.id)) { problems.push(`${at}: "${r.id}" is not a ticket id`); return; }
    if (!isClosed(r.status)) problems.push(`${at}: ${r.id} is ${r.status || "(no status)"}: only DONE / DROPPED tickets are archived`);
    const prev = rows.at(-1);
    if (prev && byId(prev.id, r.id) >= 0) problems.push(`${at}: ${r.id} is ${prev.id === r.id ? "listed twice" : `out of order after ${prev.id}`} (id order, once each)`);
    rows.push({ ...r, line: rowLineOf(r) });
  });
  return { rows, problems };
}

/** The ledger row line a closed.tsv line stands for (the DONE view's row). */
export const rowLineOf = (r) => `| ${r.id} | ${r.title} | ${r.status} | ${r.commits} | ${r.left} |`;
export const tsvText = (lines) => lines.length ? lines.join("\n") + "\n" : "";

// ---- a deterministic tar.gz (ustar: sorted names, mtime 0, owner 0, mode 0644; gzip with no name and mtime 0)

const octal = (n, width) => n.toString(8).padStart(width - 1, "0") + "\0";
/** `{ name, text }[]` -> tar bytes, in name order. */
export function tarBytes(files) {
  const parts = [];
  for (const { name, text } of [...files].sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))) {
    const data = Buffer.from(text, "utf8");
    if (Buffer.byteLength(name) > 100) throw new Error(`tar: file name longer than 100 bytes: ${name}`);
    const h = Buffer.alloc(512);
    h.write(name, 0, "utf8");
    h.write("0000644\0", 100); h.write("0000000\0", 108); h.write("0000000\0", 116);
    h.write(octal(data.length, 12), 124); h.write(octal(0, 12), 136);
    h.write("        ", 148); h.write("0", 156); h.write("ustar\0", 257); h.write("00", 263);
    h.write("0000000\0", 329); h.write("0000000\0", 337);
    let sum = 0; for (const b of h) sum += b;
    h.write(octal(sum, 7), 148); h.write(" ", 155);
    parts.push(h, data, Buffer.alloc((512 - (data.length % 512)) % 512));
  }
  parts.push(Buffer.alloc(1024));
  return Buffer.concat(parts);
}
/** tar bytes -> `{ name, text }[]` (regular files only; a header that fails its checksum throws). */
export function tarFiles(buf) {
  const out = [];
  for (let at = 0; at + 512 <= buf.length;) {
    const h = buf.subarray(at, at + 512);
    if (h.every((b) => b === 0)) break;
    const name = h.subarray(0, 100).toString("utf8").replace(/\0.*$/s, "");
    const size = parseInt(h.subarray(124, 135).toString("ascii"), 8);
    const want = parseInt(h.subarray(148, 154).toString("ascii"), 8);
    let sum = 0; h.forEach((b, i) => { sum += i >= 148 && i < 156 ? 32 : b; });
    if (sum !== want) throw new Error(`tar: bad checksum at byte ${at}`);
    out.push({ name, text: buf.subarray(at + 512, at + 512 + size).toString("utf8") });
    at += 512 + Math.ceil(size / 512) * 512;
  }
  return out;
}
export const gzipTar = (files) => gzipSync(tarBytes(files), { level: 9 });
export const gunzipTar = (gz) => tarFiles(gunzipSync(gz));

/**
 * What archiving does, pure. `tickets`: the ticket files (`{ name, text }`); `tsv`: the rows already in closed.tsv; `tars`:
 * the archives that exist (`{ [archive name]: { name, text }[] }`); `ids`: only these (else every closed one); `year`.
 * @returns {{ tsvLines: string[], tars: Record<string, {name,text}[]>, moved: {name,id}[], problems: string[] }}
 */
export function planArchive({ tickets, tsv, tars, ids, year }) {
  const problems = [], moved = [];
  const archive = archiveName(year);
  const lines = new Map(tsv.map((r) => [r.id, TSV_COLUMNS.map((k) => r[k]).join("\t")]));
  const files = [...(tars[archive] ?? [])];
  for (const t of tickets) {
    const id = idOfFile(t.name);
    if (!id || (ids && !ids.includes(id))) continue;
    const { row, problems: p } = ticketRow(t.name, t.text);
    if (!row) { if (ids) problems.push(...p); continue; }
    if (!isClosed(row.status)) { if (ids) problems.push(`${id}: ${row.status}, not DONE / DROPPED: it is not archived`); continue; }
    if (lines.has(id)) { problems.push(`${id}: already in ${CLOSED_TSV}, and the file ${t.name} is still here`); continue; }
    const open = ownerAsks(t.text, id).asks.filter((a) => a.open);
    if (open.length) { problems.push(`${id}: ${open.length} open owner line(s) in its "## Owner" block ("${open[0].text.slice(0, 50)}"): answer them or take them out, then archive`); continue; }
    lines.set(id, tsvLine(row, t.text, archive));
    files.push({ name: t.name, text: t.text });
    moved.push({ name: t.name, id });
  }
  const names = new Set(), dup = files.filter((f) => names.size === names.add(f.name).size).map((f) => f.name);
  for (const n of dup) problems.push(`${archive}: ${n} is in the archive twice`);
  for (const id of ids ?? []) if (!lines.has(id) && !tickets.some((t) => idOfFile(t.name) === id)) problems.push(`${id}: no such closed ticket file`);
  const tsvLines = [...lines.entries()].sort(([a], [b]) => byId(a, b)).map(([, l]) => l);
  return { tsvLines, tars: { ...tars, [archive]: files }, moved, problems };
}

const banner = (what) =>
  `<!-- GENERATED by \`node app/tools/ledger.mjs\` from ${what} (T1041). Not committed: an edit here is lost, change the source. ` +
  "It may be STALE (a commit, reset or rebase the hooks did not see): `node app/tools/ledger.mjs --check` says, `node app/tools/ledger.mjs` rebuilds. -->";
const table = (rows) => [ROW_HEADER, ROW_SEP, ...rows.map((r) => r.line)].join("\n");

/**
 * The two ledger views from the ticket files (`{ name, text }[]`): every well-formed row once, in id order, in the view
 * its status keyword names. A file whose block is wrong is left out (the ticket guard names it).
 */
export function ledgerViews(tickets, archived = []) {
  const rows = tickets.map((t) => ticketRow(t.name, t.text).row).filter(Boolean);
  const open = rows.filter((r) => !isClosed(r.status)).sort((a, b) => byId(a.id, b.id));
  const own = new Set(rows.map((r) => r.id));
  // (T1081) the archived rows (closed.tsv) stand with the closed ones; a file of the same id wins (the guard names the clash)
  const closed = [...rows.filter((r) => isClosed(r.status)), ...archived.filter((r) => !own.has(r.id))].sort((a, b) => byId(a.id, b.id));
  const ledger = [banner(`the row table in each ${TICKETS_DIR}/T*.md`), "# Ledger: the tickets in work", "",
    `${open.length} rows, in id order: every ticket that is not DONE or DROPPED. The closed ones (${closed.length}) are in`,
    "`DONE.md` beside this file. A row is edited in its ticket file, under the Status line (tickets/README.md says how).",
    "", "## Ledger", "", table(open), ""].join("\n");
  const done = [banner(`the row table in each ${TICKETS_DIR}/T*.md and ${CLOSED_TSV}`), "# Closed tickets (DONE / DROPPED)", "",
    `${closed.length} rows, in id order (the archived ones, from ${CLOSED_TSV}: the full text is in ${ARCHIVE_DIR}/). A reopened ticket's row moves to LEDGER.md by itself: its status says where.`,
    "", "## Ledger", "", table(closed), ""].join("\n");
  return { ledger, done, open: open.length, closed: closed.length };
}

const trimBlank = (lines) => { let e = lines.length; while (e > 0 && lines[e - 1].trim() === "") e--; return lines.slice(0, e); };


// ------------------------------------------------------------------------------------------- (T1081) the owner's asks

const OWNER_HEADING = /^##\s+Owner\s*$/;
/** An owner line: `- [ ] 2026-11-25 — the ask` (open) or `- [x] 2026-11-25 — the ask → "his answer" (2026-11-26)` (answered). */
const OWNER_LINE = /^- \[( |x)\] (.*)$/;
const OWNER_DATED = /^(\d{4}-\d{2}-\d{2}) — (\S.*)$/;

/** The lines of one owner block (`lines` from `from` up to `to`, exclusive), each `{ open, date, text }`, and what is wrong. */
function parseOwnerRange(lines, from, to, where) {
  const asks = [], problems = [];
  for (let i = from; i < to; i++) {
    const l = lines[i];
    if (l.trim() === "") continue;
    const m = l.match(OWNER_LINE);
    if (!m) {
      if (/^ {2}\S/.test(l) && asks.length) { asks.at(-1).text += ` ${l.trim()}`; continue; }
      problems.push(`${where}: owner line "${l.slice(0, 50)}" must read "- [ ] YYYY-MM-DD — <ask>" (or "- [x] …"); a longer ask continues on lines indented two spaces`);
      continue;
    }
    const d = m[2].match(OWNER_DATED);
    if (!d || Number.isNaN(Date.parse(d[1]))) {
      problems.push(`${where}: ${m[1] === " " ? "an open" : "an answered"} owner line has no date: "${m[2].slice(0, 50)}" (write "- [${m[1]}] YYYY-MM-DD — <ask>")`);
      continue;
    }
    asks.push({ open: m[1] === " ", date: d[1], text: d[2] });
  }
  return { asks, problems };
}

/**
 * A ticket's `## Owner` block (the lines up to the next `## `): `{ asks: { open, date, text }[], problems }`. No block is
 * no asks. A second `## Owner` heading is a problem (one block per ticket).
 */
export function ownerAsks(text, where = "ticket") {
  const lines = text.split("\n");
  const heads = lines.flatMap((l, i) => (OWNER_HEADING.test(l) ? [i] : []));
  if (!heads.length) return { asks: [], problems: [] };
  const problems = heads.length > 1 ? [`${where}: more than one "## Owner" block (keep one)`] : [];
  let end = heads[0] + 1;
  while (end < lines.length && !/^##\s/.test(lines[end])) end++;
  const got = parseOwnerRange(lines, heads[0] + 1, end, where);
  return { asks: got.asks, problems: [...problems, ...got.problems] };
}

/** OWNER-UNFILED.md: `## <title>` sections of owner lines. `{ groups: { title, asks }[], problems }`. */
export function unfiledAsks(text) {
  const lines = text.split("\n");
  const heads = lines.flatMap((l, i) => (/^##\s+\S/.test(l) ? [i] : []));
  const groups = [], problems = [];
  heads.forEach((h, k) => {
    const to = k + 1 < heads.length ? heads[k + 1] : lines.length;
    const title = lines[h].replace(/^##\s+/, "").trim();
    const got = parseOwnerRange(lines, h + 1, to, `${basename(OWNER_UNFILED)} "${title.slice(0, 40)}"`);
    groups.push({ title, asks: got.asks });
    problems.push(...got.problems);
    if (!got.asks.length && !got.problems.length) problems.push(`${basename(OWNER_UNFILED)} "${title.slice(0, 40)}": a section holds no owner line`);
  });
  const stray = lines.slice(0, heads[0] ?? lines.length).filter((l) => /^- \[/.test(l));
  if (stray.length) problems.push(`${basename(OWNER_UNFILED)}: an owner line sits above the first "## <title>" section`);
  return { groups, problems };
}

const oneLine = (s, max = 220) => (s.length > max ? `${s.slice(0, max - 1).trimEnd()}…` : s);

/**
 * The OWNER view (a page, one short line per open ask), pure: `tickets` as `{ name, text }[]`, `unfiled` the text of
 * OWNER-UNFILED.md. Groups by ticket (id + title) and by unfiled title; the group with the newest open ask first, and in a
 * group the newest ask first; ties by id.
 */
export function ownerView(tickets, unfiled = "") {
  const groups = [];
  for (const t of tickets) {
    const id = idOfFile(t.name);
    if (!id) continue;
    const open = ownerAsks(t.text, id).asks.filter((a) => a.open);
    if (open.length) groups.push({ key: id, title: `${id} — ${ticketRow(t.name, t.text).row?.title ?? t.text.split("\n")[0].replace(/^#\s*/, "")}`, asks: open });
  }
  for (const g of unfiledAsks(unfiled).groups) {
    const open = g.asks.filter((a) => a.open);
    if (open.length) groups.push({ key: "~", title: `(no ticket yet) ${g.title}`, asks: open });
  }
  const newest = (g) => g.asks.reduce((m, a) => (a.date > m ? a.date : m), "");
  groups.sort((a, b) => (newest(a) < newest(b) ? 1 : newest(a) > newest(b) ? -1 : a.key === "~" || b.key === "~" ? (a.key === "~" ? 1 : -1) : byId(a.key, b.key)));
  const n = groups.reduce((s, g) => s + g.asks.length, 0);
  const body = groups.flatMap((g) => ["", `## ${oneLine(g.title, 110)}`, ...[...g.asks].sort((a, b) => (a.date < b.date ? 1 : a.date > b.date ? -1 : 0)).map((a) => `- ${a.date} — ${oneLine(a.text)}`)]);
  const text = [banner(`the "## Owner" block of each ${TICKETS_DIR}/T*.md and ${OWNER_UNFILED}`), "# Owner: what only you can do or decide", "",
    `${n} open asks in ${groups.length} places, newest first. Answer in chat: the chat writes your answer into that ticket's \`## Owner\` block`,
    "(tickets/README.md, \"Owner blocks\"). The full text of an ask, its options and pictures are in the ticket.", ...body, ""].join("\n");
  return { text, asks: n, groups: groups.length };
}

/** A merge-conflict marker line (`<<<<<<< …` / `>>>>>>> …`): a ticket or owner file holding one was `git add`ed unsettled. */
export const MARKER = /^(<<<<<<<|>>>>>>>)( |$)/m;

/**
 * (T1041) What is wrong with the number claims (`tickets/ids/Tnnn`, holding the ticket file's name): a ticket file with
 * no claim or a claim naming another file, a claim with no ticket file. `claims`: `{ name, text }[]` of that folder.
 */
export function claimProblems(tickets, claims, closedIds = new Set()) {
  const problems = [];
  const byId = new Map(claims.map((c) => [c.name, c.text]));
  const files = new Set(tickets.map((t) => t.name));
  for (const { name } of tickets) {
    const id = idOfFile(name);
    if (!id) continue;
    const c = byId.get(id);
    if (c === undefined) problems.push(`${id}: no number claim ${claimPath(id)} (node app/tools/ledger.mjs --claim writes it): two PRs that take one number must collide there`);
    else if (c !== `${name}\n`) problems.push(`${id}: ${claimPath(id)} claims "${c.trim()}", but the ticket file is ${name}: one number, one ticket`);
  }
  for (const { name, text } of claims) {
    if (closedIds.has(name)) { problems.push(`${claimPath(name)}: ${name} is in ${CLOSED_TSV} (archived): its number is kept there, delete the claim`); continue; }
    if (!files.has(text.trim()) || idOfFile(text.trim()) !== name) problems.push(`${claimPath(name)}: claims "${text.trim()}", which is not a ticket file of that number`);
  }
  for (const { name } of tickets) { const id = idOfFile(name); if (id && closedIds.has(id)) problems.push(`${id}: ${name} is still here but ${id} is in ${CLOSED_TSV}: a number is used once`); }
  return problems.sort();
}

/** The `## Ledger` section of tickets/README.md once the rows live in the ticket files. */
export const LEDGER_POINTER = `## Ledger

**The rows are not kept here any more (T1041).** Each ticket's row lives in its own file, as a one-row table right
under its \`Status:\` line (same five cells as ever):

    | id | title | status | commits | what's left |
    |---|---|---|---|---|
    | Tnnn | the ticket's title | OPEN | — | what's left, a pointer (240 characters at most) |

\`node app/tools/ledger.mjs\` builds the tables into \`docs/views/LEDGER.md\` (every row not DONE / DROPPED) and
\`docs/views/DONE.md\` (the closed ones), git-ignored; the git hooks rebuild them after a checkout, merge, commit
or rebase, but a view MAY BE STALE (another worktree, a reset): \`node app/tools/ledger.mjs --check\` says so and the
ticket guard warns. So two PRs that touch different tickets never conflict. A row's view follows its status keyword.
A NEW ticket also claims its number: \`node app/tools/ledger.mjs --claim\` writes \`tickets/ids/Tnnn\` (the file's name);
two PRs that take one number then collide there in git instead of both merging. A CLOSED ticket (DONE / DROPPED) is
archived (T1081): its row is a line of \`closed.tsv\` beside this file and its text is in \`archive/closed-<year>.tar.gz\`;
\`node app/tools/ledger.mjs --archive\` moves it, \`--next\` prints the next free number. \`node app/tools/check-tickets.mjs\`
checks the rows and claims, and refuses this section holding anything but this text.
`;

/** The `## Ledger` section of a README (heading to the line before the next `## `). */
export function ledgerSection(readme) {
  const lines = readme.split("\n");
  const start = lines.findIndex((l) => /^##\s+Ledger\b/.test(l));
  if (start < 0) return null;
  let end = start + 1;
  while (end < lines.length && !/^##\s/.test(lines[end])) end++;
  return trimBlank(lines.slice(start, end)).join("\n") + "\n";
}

/** (T1041) Where a ticket NUMBER is claimed: one file per number, holding the ticket file's name. Two branches that take
 *  the same number add the same path with different text, so git refuses to merge them as they are (an add/add conflict). */
export const CLAIMS_DIR = `${TICKETS_DIR}/ids`;
export const claimPath = (id) => `${CLAIMS_DIR}/${id}`;

/** git without the caller's repository variables (a hook's GIT_DIR must not point this at another repository). */
const cleanEnv = () => Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("GIT_")));
const git = (repo, args, opts = {}) => execFileSync("git", ["-C", repo, ...args], { encoding: "utf8", env: cleanEnv(), maxBuffer: 1 << 30, ...opts });

/** The work tree's tickets, owner file, claims and closed index, as the guard and the views read them. */
export function readWorkTree(repo) {
  const tdir = join(repo, TICKETS_DIR);
  const tickets = readdirSync(tdir).filter((n) => /^T.*\.md$/.test(n)).sort().map((name) => ({ name, text: readFileSync(join(tdir, name), "utf8") }));
  const cdir = join(repo, CLAIMS_DIR);
  const claims = existsSync(cdir) ? readdirSync(cdir).filter((n) => ID.test(n)).sort().map((name) => ({ name, text: readFileSync(join(cdir, name), "utf8") })) : [];
  const tsvPath = join(repo, CLOSED_TSV);
  const closed = existsSync(tsvPath) ? parseTsv(readFileSync(tsvPath, "utf8")) : { rows: [], problems: [] };
  const unfiledPath = join(repo, OWNER_UNFILED);
  const unfiled = existsSync(unfiledPath) ? readFileSync(unfiledPath, "utf8") : "";
  return { tickets, unfiled, claims, closed: closed.rows, closedProblems: closed.problems };
}

/** The three views as they should be on disk. */
export function buildViews(repo) {
  const { tickets, unfiled, closed, closedProblems } = readWorkTree(repo);
  const lv = ledgerViews(tickets, closed);
  const ov = ownerView(tickets, unfiled);
  return {
    files: { [VIEWS.ledger]: lv.ledger, [VIEWS.done]: lv.done, [VIEWS.owner]: ov.text },
    summary: `${lv.open} rows in work, ${lv.closed} closed (${closed.length} archived), ${ov.asks} open owner asks`,
    problems: [...tickets.flatMap((t) => ticketRow(t.name, t.text).problems), ...closedProblems,
               ...tickets.flatMap((t) => ownerAsks(t.text, idOfFile(t.name) ?? t.name).problems), ...unfiledAsks(unfiled).problems],
  };
}


/** (T1041) The views on disk that are missing or differ from what the source builds now (empty = current). */
export function staleViews(repo) {
  const v = buildViews(repo);
  return Object.entries(v.files).filter(([p, text]) => !existsSync(join(repo, p)) || readFileSync(join(repo, p), "utf8") !== text).map(([p]) => p);
}

/** --claim: write the number claim of every ticket file that has none; a claim naming another file is left, and said. */
function cliClaim(repo) {
  const { tickets, claims, closed } = readWorkTree(repo);
  const closedIds = new Set(closed.map((r) => r.id));
  const have = new Map(claims.map((c) => [c.name, c.text]));
  mkdirSync(join(repo, CLAIMS_DIR), { recursive: true });
  let made = 0;
  for (const { name } of tickets) {
    const id = idOfFile(name);
    if (!id || have.has(id) || closedIds.has(id)) continue;   // (T1081) a closed id is never claimed again: claimProblems says so
    writeFileSync(join(repo, claimPath(id)), `${name}\n`); have.set(id, `${name}\n`); made++;
  }
  const problems = claimProblems(tickets, [...have].map(([name, text]) => ({ name, text })), closedIds);
  console.log(`claims: ${made} written (${CLAIMS_DIR}/); git add them with their tickets`);
  for (const p of problems) console.error(`✗ ${p}`);
  return problems.length ? 1 : 0;
}

/**
 * (T1081) The number a new ticket takes: one past the highest number used anywhere: closed.tsv, the claims, the open files,
 * and the claims and files on every local / remote-tracking branch (a PR not merged yet has claimed its number there).
 * `names`: every path-or-name seen. Pure.
 */
export function nextNumber(names) {
  let top = 0;
  for (const n of names) { const m = basename(n).match(/^T(\d+)/); if (m) top = Math.max(top, Number(m[1])); }
  return top + 1;
}

function cliNext(repo) {
  const { tickets, claims, closed } = readWorkTree(repo);
  const names = [...tickets.map((t) => t.name), ...claims.map((c) => c.name), ...closed.map((r) => r.id)];
  const refs = git(repo, ["for-each-ref", "--format=%(refname)", "refs/heads", "refs/remotes"]).split("\n").filter((r) => r && !r.endsWith("/HEAD"));
  for (const ref of refs) names.push(...git(repo, ["ls-tree", "--name-only", ref, `${TICKETS_DIR}/`, `${CLAIMS_DIR}/`, `${LEGACY_TICKETS_DIR}/`, `${LEGACY_TICKETS_DIR}/ids/`]).split("\n").filter(Boolean));
  console.log(`T${nextNumber(names)}`);
  return 0;
}

/** Ticket-keyed picture / data entries in `dir` (`T123-shots`, `T123-files`, `T563-566-shots`, `T41-walk`, `T321-wording-baseline.txt`): the ids they belong to. */
export function assetIds(name) {
  const m = name.match(/^T(\d+)([a-z]?)(?:-(\d+))?-(?!\d)/);
  if (!m || name.endsWith(".md")) return null;
  const [, a, letter, b] = m;
  if (!b) return [`T${a}${letter}`];
  const out = [];
  for (let k = Number(a); k <= Number(b); k++) out.push(`T${k}`);
  return out;
}

/** Asset entries a surviving document or a running tool still reads: never deleted by --archive (name -> why). */
export const KEPT_ASSETS = {
  "tickets/T281-files": "tests (pipeline/tests/test_t100_area_caveat.py) read sub_lake_areas.json from it",
  "app-integration/T1047-shots": "the datum-change pictures: data/curated/gauges/datum_change_stations.json and GAUGE-DATUM-CHANGES.md name them",
  "app-integration/T244-shots": "the icon preview: store/listing-draft.md and open T447 name it",
  "tickets/T928-shots": "open T938 names its shot",
  "app-integration/T665-main-rivers-map.html": "open T677 and next-rebuild.md name the held-lines map",
};

/** KEPT_ASSETS key -> the tracked files that still name it (check-size.mjs fails when a key is gone or no reader names it). */
export const KEPT_READERS = {
  "tickets/T281-files": ["pipeline/tests/test_t100_area_caveat.py"],
  "app-integration/T1047-shots": ["data/curated/gauges/datum_change_stations.json", "pipeline/docs/GAUGE-DATUM-CHANGES.md"],
  "app-integration/T244-shots": ["docs/app-integration/store/listing-draft.md", `${TICKETS_DIR}/T447-artist-app-icon.md`],
  "tickets/T928-shots": [`${TICKETS_DIR}/T938-ios-browse-map-half-drawn-after-back.md`],
  "app-integration/T665-main-rivers-map.html": [`${TICKETS_DIR}/T677-next-rebuild-leftovers.md`, "docs/app-integration/next-rebuild.md"],
};

const ASSET_ROOTS = ["docs/app-integration", TICKETS_DIR];

/** What is wrong with an archive's bytes against the ticket files it must hold (empty = each one there, byte for byte). */
export function archiveProblems(files, gz) {
  const back = new Map(gunzipTar(gz).map((f) => [f.name, f.text]));
  return files.filter((t) => back.get(t.name) !== t.text).map((t) => `${t.name} is not in the archive byte for byte`);
}

function cliArchive(repo, rest) {
  const dry = rest.includes("--dry");
  const ids = rest.filter((a) => /^T\d+[a-z]?$/.test(a));
  const year = new Date().getFullYear();
  const { tickets, closed, claims } = readWorkTree(repo);
  const archive = archiveName(year);
  const apath = join(repo, archivePath(archive));
  const tars = existsSync(apath) ? { [archive]: gunzipTar(readFileSync(apath)) } : {};
  const plan = planArchive({ tickets, tsv: closed, tars, ids: ids.length ? ids : null, year });
  if (plan.problems.length) throw new Error(`nothing was archived:\n  ${plan.problems.join("\n  ")}`);
  const movedIds = new Set(plan.moved.map((m) => m.id));
  const assets = [], kept = [];
  for (const root of ASSET_ROOTS) {
    if (!existsSync(join(repo, root))) continue;
    for (const name of readdirSync(join(repo, root))) {
      const owners = assetIds(name);
      if (!owners) continue;
      const wasClosed = (id) => movedIds.has(id) || (closed.some((r) => r.id === id) && !tickets.some((t) => idOfFile(t.name) === id));
      if (!owners.every(wasClosed)) continue;
      const key = `${root.replace("docs/", "")}/${name}`;
      if (KEPT_ASSETS[key]) kept.push(`${key}: ${KEPT_ASSETS[key]}`); else assets.push(`${root}/${name}`);
    }
  }
  const claimFiles = claims.filter((c) => movedIds.has(c.name)).map((c) => claimPath(c.name));
  console.log(`archive ${archive}: ${plan.moved.length} ticket files, ${claimFiles.length} claims, ${assets.length} picture/data folders; kept: ${kept.length}`);
  for (const k of kept) console.log(`  kept ${k}`);
  if (dry || !plan.moved.length) return 0;
  // write, read back, compare byte for byte BEFORE anything is deleted
  const gz = gzipTar(plan.tars[archive]);
  mkdirSync(join(repo, ARCHIVE_DIR), { recursive: true });
  writeFileSync(apath, gz);
  writeFileSync(join(repo, CLOSED_TSV), tsvText(plan.tsvLines));
  const wrong = archiveProblems(tickets.filter((t) => movedIds.has(idOfFile(t.name))), readFileSync(apath));
  if (wrong.length) throw new Error(`${archivePath(archive)} does not read back: nothing was deleted:\n  ${wrong.join("\n  ")}`);
  if (parseTsv(readFileSync(join(repo, CLOSED_TSV), "utf8")).problems.length) throw new Error(`${CLOSED_TSV} does not read back clean: nothing was deleted`);
  for (const m of plan.moved) rmSync(join(repo, TICKETS_DIR, m.name));
  for (const c of claimFiles) rmSync(join(repo, c), { force: true });
  for (const a of assets) rmSync(join(repo, a), { recursive: true, force: true });
  console.log(`✓ archived ${plan.moved.length} tickets into ${archivePath(archive)} and ${CLOSED_TSV}; deleted their files, claims and folders`);
  return 0;
}

function writeViews(repo) {
  const v = buildViews(repo);
  mkdirSync(join(repo, VIEWS_DIR), { recursive: true });
  for (const [p, text] of Object.entries(v.files)) writeFileSync(join(repo, p), text);
  return v;
}

const isMain = process.argv[1] && new URL(import.meta.url).pathname === process.argv[1];
if (isMain) {
  const [flag] = process.argv.slice(2);
  try {
    // the repository of the current directory (the post-checkout hook runs at its root, `pnpm` scripts in app/)
    const repo = git(process.cwd(), ["rev-parse", "--show-toplevel"]).trim();
    if (flag === "--claim") process.exit(cliClaim(repo));
    else if (flag === "--next") process.exit(cliNext(repo));
    else if (flag === "--archive") { const rc = cliArchive(repo, process.argv.slice(3)); if (rc === 0 && !process.argv.includes("--dry")) console.log(`views: ${writeViews(repo).summary}`); process.exit(rc); }
    else if (flag === "--check") {
      const v = buildViews(repo);
      const stale = staleViews(repo);
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
    else throw new Error(`unknown flag ${flag} (none, --check, --claim, --next, --archive)`);
  } catch (e) {
    console.error(`✗ ledger: ${e.message}`);
    process.exit(1);
  }
}
