/**
 * T84 — the ticket ledger stays whole.
 *
 * `tools/check-tickets.mjs` is the guard (`pnpm tickets`, in `pnpm check`). Held two ways, because a guard that
 * finds nothing proves nothing on its own:
 *   1. the real tickets folder (each file holds its own row, T1041), owner items and commit subjects pass — and the
 *      guard actually SAW them;
 *   2. each rule fires on a mutated copy of the real ledger (row removed, Status removed, hash removed, …).
 */
import { describe, expect, it } from "vitest";
import { repoData } from "../conformance/src/repoData";
// A plain .mjs gate (it runs under node as `pnpm tickets`), imported for its functions.
import { CLOSED, RUNBOOK_MAKES, LEFT_CELL_MAX, checkDocs, checkLedger, citedPaths, idOfFile, ledgerRows, readDocs, readRepo, ruleIds, runbookPaths, statusKeyword } from "./check-tickets.mjs";
import { LEDGER_POINTER, OWNER_PAGE_TEXT, ROW_HEADER, ROW_SEP, ledgerViews, readOwnerItems, readWorkTree, setRow, stripRow, ticketRow } from "./ledger.mjs";

type Legacy = { readme: string; doneLedger: string | null; ownerPage: string };
type Input = { tickets: { name: string; text: string }[]; subjects: string[]; ownerTodo: string; ownerProblems: string[]; legacy: Legacy };
const repoRoot = new URL("../..", import.meta.url).pathname;
/** the repo as it is: every ticket file with its row (T1041), the owner items, the old places */
const repo = readRepo(repoRoot) as Input;
const real = repo;

const withTicket = (input: Input, id: string, edit: (text: string) => string): Input => ({
  ...input,
  tickets: input.tickets.map((t) => (idOfFile(t.name) === id ? { ...t, text: edit(t.text) } : t)),
});
/** The row line of ticket `id`, read from its own file. */
const rowOf = (input: Input, id: string) => ticketRow(`${id}-x.md`, input.tickets.find((t) => idOfFile(t.name) === id)!.text).row!.line as string;
/** `input` with ticket `id`'s row line set to `line` (in its file, T1041). */
const withRow = (input: Input, id: string, line: string) => withTicket(input, id, (t) => setRow(t, line));
/** A ledger row with its status cell replaced — independent of what the live row says today. */
const withStatus = (row: string, status: string) => {
  const cells = row.split("|");
  cells[3] = ` ${status} `;
  return cells.join("|");
};
/** The real tickets with no commit subjects: the row/file rules alone, not today's git history. */
const noGit: Input = { ...real, subjects: [] };

describe("the real ledger", () => {
  it("is whole", () => {
    expect(checkLedger(repo)).toEqual([]);
  });

  it("was actually read: every ticket file holds its own row, the views split it, and commit subjects were scanned", () => {
    const fileIds = repo.tickets.map((t) => idOfFile(t.name)).sort();
    const rows = repo.tickets.map((t) => ticketRow(t.name, t.text).row!);
    expect(rows.map((r) => r.id).sort()).toEqual(fileIds);
    // the split is real: the DONE view holds rows, every one closed, and the ledger view still holds the rows in work
    const v = ledgerViews(repo.tickets);
    const open = ledgerRows(v.ledger) as { status: string }[], closed = ledgerRows(v.done) as { status: string }[];
    expect(closed.length).toBeGreaterThan(100);
    expect(closed.filter((r) => !CLOSED.includes(statusKeyword(r.status)))).toEqual([]);
    expect(open.length).toBeGreaterThan(10);
    expect(open.filter((r) => CLOSED.includes(statusKeyword(r.status)))).toEqual([]);
    expect(open.length + closed.length).toBe(fileIds.length);
    expect(fileIds.length).toBeGreaterThan(100);
    expect(real.subjects.some((s) => /\bT84\b|\bT56\b/.test(s))).toBe(true);
    // Scoped to the tickets era: "T3a" (an older plan, before the tickets folder) is not scanned.
    expect(real.subjects.some((s) => /\bT3a\b/.test(s))).toBe(false);
  });
});

describe("the guard fires (mutation)", () => {
  it("on a removed ledger row", () => {
    expect(checkLedger(withTicket(noGit, "T84", stripRow))).toEqual(["T84: ticket file has no ledger row (the one-row table under its Status line, T1041)"]);
  });

  it("on a removed ledger row of a ticket commits name (both rules fire)", () => {
    expect(checkLedger({ ...withTicket(noGit, "T84", stripRow), subjects: ["T84: ticket-ledger guard"] })).toEqual([
      "T84: named in a commit subject but missing from the ledger",
      "T84: ticket file has no ledger row (the one-row table under its Status line, T1041)",
    ]);
  });

  it("on two ticket files with the same id", () => {
    const t84 = real.tickets.find((t) => idOfFile(t.name) === "T84")!;
    const input = { ...noGit, tickets: [...real.tickets, { name: "T84-another-slug.md", text: t84.text }] };
    expect(checkLedger(input)).toEqual([
      "T84: more than one ticket file (T84-another-slug.md, T84-ticket-ledger-guard.md)",
    ]);
  });

  it("on a removed Status line", () => {
    const input = withTicket(real, "T84", (t) => t.replace(/^Status:.*\n/m, ""));
    expect(checkLedger(input)).toEqual([
      'T84: no "Status:" line in the first 6 lines of T84-ticket-ledger-guard.md',
    ]);
  });

  it("on a Status line pushed below the top", () => {
    const input = withTicket(real, "T84", (t) => t.replace(/^(Status:.*\n)/m, "\n\n\n\n\n$1"));
    expect(checkLedger(input)).toEqual([
      'T84: no "Status:" line in the first 6 lines of T84-ticket-ledger-guard.md',
    ]);
  });

  it("on a ticket file removed while commits still name it", () => {
    const input = { ...real, tickets: real.tickets.filter((t) => idOfFile(t.name) !== "T84") };
    expect(checkLedger(input)).toEqual(["T84: named in a commit subject but missing from the ledger"]);
  });

  it("(T1041) on a row twice in one file, a row of another ticket, a block with no row", () => {
    const twice = withTicket(noGit, "T84", (t) => `${t}\n${ROW_HEADER}\n${ROW_SEP}\n${rowOf(noGit, "T84")}\n`);
    expect(checkLedger(twice)).toEqual(["T84: ledger row appears more than once (T84-ticket-ledger-guard.md holds 2 row tables: keep one)"]);
    expect(checkLedger(withRow(noGit, "T84", rowOf(noGit, "T02")))).toEqual(["T84: the row in T84-ticket-ledger-guard.md is T02's: a file holds its own row"]);
    expect(checkLedger(withRow(noGit, "T84", "| nothing |"))[0]).toMatch(/^T84: the row table in T84-ticket-ledger-guard\.md holds no row/);
  });

  it("on a T-number in a commit subject that the ledger lacks", () => {
    const input = { ...real, subjects: [...real.subjects, "T99999: work nobody ticketed"] };
    expect(checkLedger(input)).toEqual(["T99999: named in a commit subject but missing from the ledger"]);
  });

  it("does not read an owner-question label (Q-T57a) or other hyphen-joined id as a ticket", () => {
    expect(checkLedger({ ...noGit, subjects: ["T57: status DONE - results and Q-T57a", "see T45-shots"] })).toEqual([]);
  });

  it("on DONE with no commit hash, in the file and in the row", () => {
    const input = withTicket(withRow(real, "T02", "| T02 | blob codec (fflate dict-inflate + UTF-8 decode) | DONE | — | — |"),
      "T02", (t) => t.replace(/^Status:.*$/m, "Status: DONE."));
    expect(checkLedger(input)).toEqual([
      "T02: Status DONE names no commit hash in T02-blob-codec.md",
      "T02: ledger row is DONE with no commit hash",
    ]);
  });

  it("(T437) on a ticket file whose status differs from its ledger row's", () => {
    const input = withTicket(withRow(noGit, "T84", withStatus(rowOf(noGit, "T84"), "OPEN")), "T84", (t) => t.replace(/^Status:.*$/m, "Status: DONE — abc1234."));
    expect(checkLedger(input)).toEqual(["T84: the ticket file says DONE, its ledger row OPEN (close both together)"]);
  });

  it("on an unknown status", () => {
    expect(checkLedger(withRow(noGit, "T84", withStatus(rowOf(noGit, "T84"), "MAYBE")))).toEqual([
      'T84: ledger status "MAYBE" is not one of IN PROGRESS, IN REVIEW, BLOCKED, POSTPONED, PARTIAL, DROPPED, OPEN, DONE',
    ]);
  });
});

describe("statusKeyword", () => {
  it("(T437) knows exactly the statuses the README defines", async () => {
    const { STATUSES } = await import("./check-tickets.mjs");
    const defined = [...real.legacy.readme.matchAll(/^  - `([A-Z ]+?)(?: \(owner\))?` — /gm)].map((m) => m[1]);
    expect([...defined].sort()).toEqual([...STATUSES].sort());
  });

  it("reads the leading keyword of every status the README defines", () => {
    expect(["OPEN.", "IN PROGRESS — lane x", "PARTIAL — a", "BLOCKED (owner)", "POSTPONED — hold",
      "DONE — 40b2c67d", "DROPPED — why", "DONE (in T21)", "Open"].map(statusKeyword)).toEqual(
      ["OPEN", "IN PROGRESS", "PARTIAL", "BLOCKED", "POSTPONED", "DONE", "DROPPED", "DONE", null]);
  });
});

// ---- T1041: a row lives in its ticket file; the old places hold nothing (T540's split is the views' now) ----
describe("T1041 the old places hold no row and no item", () => {
  it("the old places must be given, never read as 'nothing old'", () => {
    const { legacy: _gone, ...without } = noGit;
    expect(() => checkLedger(without as Input)).toThrow(/needs legacy/);
    expect(() => checkLedger({ ...noGit, legacy: { ...noGit.legacy, doneLedger: undefined as unknown as null } })).toThrow(/needs legacy/);
  });
  it("the real README holds the pointer, no row; DONE.md is gone; OWNER-TODO.md is the pointer page", () => {
    expect(real.legacy.readme).toContain(LEDGER_POINTER.trim());
    expect(real.legacy.doneLedger).toBeNull();
    expect(real.legacy.ownerPage).toBe(OWNER_PAGE_TEXT);
  });
  it("a row typed into README.md the old way fails, naming the way to carry it", () => {
    const readme = real.legacy.readme.replace(LEDGER_POINTER.trim(), `## Ledger\n\n${ROW_HEADER}\n${ROW_SEP}\n${rowOf(noGit, "T84")}`);
    expect(checkLedger({ ...noGit, legacy: { ...noGit.legacy, readme } })).toEqual([
      "handoff/PLAN/app-integration/tickets/README.md holds 1 ledger row(s) (T84…): a row lives in its ticket file under the Status line (T1041); " +
      "`node app/tools/ledger.mjs --resolve` carries an old-style branch's rows"]);
  });
  it("a DONE.md that comes back fails, even an empty one", () => {
    expect(checkLedger({ ...noGit, legacy: { ...noGit.legacy, doneLedger: "" } })[0]).toMatch(/^handoff\/PLAN\/app-integration\/tickets\/DONE\.md exists/);
  });
  it("an owner item typed into OWNER-TODO.md fails", () => {
    const ownerPage = `${real.legacy.ownerPage}\n## 2026-11-25: a new item the old way\n- x\n`;
    expect(checkLedger({ ...noGit, legacy: { ...noGit.legacy, ownerPage } })).toEqual([
      'handoff/PLAN/OWNER-TODO.md holds a "## " section (## 2026-11-25: a new item the old way): an owner item is a file of its own in handoff/PLAN/owner-todo/ (T1041)']);
  });
  it("what is wrong with an owner item file fails the guard", () => {
    const files = readWorkTree(repoRoot).ownerFiles!;
    expect(readOwnerItems(files).problems).toEqual([]);
    const broken = readOwnerItems(files.map((f, i) => (i === 1 ? { ...f, text: f.text.replace(/^order: .*\n\n/, "") } : f)));
    expect(checkLedger({ ...noGit, ownerProblems: broken.problems })).toEqual([`handoff/PLAN/owner-todo/${files[1]!.name}: the first line must be "order: <number>" and the second blank`]);
  });
  it("(T540, now by construction) a row's view follows its status: T84 reopened leaves the DONE view by itself", () => {
    const reopened = withRow(noGit, "T84", withStatus(rowOf(noGit, "T84"), "OPEN"));
    const before = ledgerViews(noGit.tickets), after = ledgerViews(reopened.tickets);
    expect([ledgerRows(before.done).some((r: { id: string }) => r.id === "T84"), ledgerRows(after.done).some((r: { id: string }) => r.id === "T84")]).toEqual([true, false]);
    expect(ledgerRows(after.ledger).some((r: { id: string }) => r.id === "T84")).toBe(true);
  });
});

// ---- T539: a status names a step that can close; the docs guard ----
/** T84 with its status line and ledger row both set to `status` (the two keywords must agree). */
const withT84 = (status: string): Input => {
  const kw = ["IN REVIEW", "PARTIAL", "IN PROGRESS", "DONE"].find((k) => status.startsWith(k))!;
  return withTicket(withRow(noGit, "T84", withStatus(rowOf(noGit, "T84"), kw)), "T84", (t) => t.replace(/^Status:.*$/m, `Status: ${status}`));
};
/** a text the owner to-do holds that no ticket's Status cites today (removing it must name T84 alone) */
const OWNER_TEXT = [...real.ownerTodo.matchAll(/\*\*([^*"]{8,40})\*\*/g)].map((m) => m[1]!)
  .find((text) => !real.tickets.some((t) => t.text.split("\n").slice(0, 6).join("\n").includes(text)))!;

describe("T539 a status names its step", () => {
  it("the real ledger passes, with no grandfather list", () => {
    expect(checkLedger(real)).toEqual([]);
  });
  it("IN REVIEW with no step fails", () => {
    expect(checkLedger(withT84("IN REVIEW — abc1234"))).toEqual([
      'T84: IN REVIEW names no step: add `owner: OWNER-TODO "<text>"` or `left (Claude): …` to the Status line']);
  });
  it("PARTIAL with no step fails", () => {
    expect(checkLedger(withT84("PARTIAL — abc1234 half done"))[0]).toMatch(/T84: PARTIAL names no step/);
  });
  it("an owner-todo text that exists passes; one that is gone fails", () => {
    expect(OWNER_TEXT).toBeTruthy();
    expect(checkLedger(withT84(`IN REVIEW — abc1234; owner: OWNER-TODO "${OWNER_TEXT}"`))).toEqual([]);
    const gone = { ...withT84(`IN REVIEW — abc1234; owner: OWNER-TODO "${OWNER_TEXT}"`), ownerTodo: real.ownerTodo.split(OWNER_TEXT).join("x") };
    expect(checkLedger(gone)).toEqual([`T84: Status cites OWNER-TODO "${OWNER_TEXT}", which no owner-todo item holds (handoff/PLAN/owner-todo/)`]);
  });
  it("IN REVIEW whose only step is Claude's is IN PROGRESS; PARTIAL with Claude's step is fine", () => {
    expect(checkLedger(withT84("IN REVIEW — abc1234; left (Claude): run the emulator"))).toEqual([
      "T84: IN REVIEW but the only step is Claude's (left (Claude)): that is IN PROGRESS"]);
    expect(checkLedger(withT84("PARTIAL — abc1234; left (Claude): run the emulator"))).toEqual([]);
    expect(checkLedger(withT84("IN PROGRESS — lane x; left (Claude): run the emulator"))).toEqual([]);
  });
});

describe("T540 the ledger's last cell is capped", () => {
  it("a row over the cap fails, an open one and a closed one; the real ledger is within it", () => {
    expect(checkLedger(real)).toEqual([]);
    const long = "x".repeat(LEFT_CELL_MAX + 1);
    for (const id of ["T84", ledgerRows(ledgerViews(real.tickets).ledger)[0]!.id as string]) {
      const input = withRow(real, id, rowOf(real, id).replace(/\| [^|]*\|$/, `| ${long} |`));
      expect(checkLedger(input)).toEqual([`${id}: ledger row's last cell is ${long.length} characters (max ${LEFT_CELL_MAX}): keep the detail in the ticket file`]);
    }
  });
});

describe("T539 the docs guard", () => {
  const docs = readDocs(repoRoot) as Parameters<typeof checkDocs>[0] & Record<string, any>;
  const live = () => ({ ...docs });
  it("the real docs pass, and the guard actually read them", () => {
    expect(checkDocs(docs)).toEqual([]);
    expect(docs.restartName).toMatch(/^NEXT-SESSION-/);
    expect(citedPaths(docs.ownerTodo).length).toBeGreaterThan(5);
    expect(citedPaths(docs.startHere)).toContain("handoff/PLAN/app-integration/tickets/README.md");
    expect(ruleIds(docs.agents).length).toBeGreaterThan(60);
  });
  it("(T1041) the owner pointer page must be read", () => {
    const { ownerPage: _gone, ...without } = docs;
    expect(checkDocs(without as typeof docs)).toContain("no text was read for handoff/PLAN/OWNER-TODO.md (ownerPage): the dead-path guard covers it");
  });
  it("fails on stray lines above STATUS's title", () => {
    expect(checkDocs({ ...live(), status: "- left over\n# Where things stand\n" })[0]).toMatch(/must start with its title/);
  });
  it("fails on a dead path in each of the three documents", () => {
    for (const k of ["startHere", "restart", "ownerTodo", "ownerPage"] as const) {
      const out = checkDocs({ ...live(), [k]: `${docs[k]}\n see \`app/tools/does-not-exist.mjs\`` });
      expect(out.some((m: string) => /cites app\/tools\/does-not-exist\.mjs, which does not exist/.test(m))).toBe(true);
    }
  });
  it("a git-ignored output path may be absent; a glob or placeholder is not a path", () => {
    const base = { ...live(), restart: "`app/dist/gone` `app/tools/*.mjs` `data/release/<r-id>`", isIgnored: (p: string) => p.startsWith("app/dist/") || docs.isIgnored(p) };   // the other live docs keep the real answer: a
    // fresh clone (CI) has no CLAUDE.md and no gauge feeds folder, both git-ignored
    expect(checkDocs(base)).toEqual([]);
  });
  it("fails on state in the restart page (release id, build number)", () => {
    const out = checkDocs({ ...live(), restart: `${docs.restart}\nLATEST = r-0123456789ab; Build 99 on Play` });
    expect(out.filter((m: string) => /holds state/.test(m))).toHaveLength(2);
  });
  it("fails on a repeated AGENTS rule id", () => {
    const out = checkDocs({ ...live(), agents: `${docs.agents}\n5. **a rule that reuses a number**\n` });
    expect(out.some((m: string) => /rule id 5 is used at lines \d+ and \d+/.test(m))).toBe(true);
  });
});

describe("T628 the docs guard covers the runbooks", () => {
  const docs = readDocs(repoRoot) as Parameters<typeof checkDocs>[0] & Record<string, any>;
  const annual = () => docs.runbooks.find((r: { name: string }) => r.name === "annual-synopsis.md")!;
  const withAnnual = (text: string) => ({ ...docs, runbooks: docs.runbooks.map((r: { name: string }) => (r.name === "annual-synopsis.md" ? { ...r, text } : r)) });
  it("the real runbooks were read, command blocks included", () => {
    expect(docs.runbooks.map((r: { name: string }) => r.name)).toContain("release.md");
    const synopsis = repoData("curated/regulations/synopsis");                             // the committed folder, by name (T667)
    expect(runbookPaths(annual().text)).toContain(synopsis);                               // only ever inside a ```bash block
    expect(citedPaths(annual().text)).not.toContain(synopsis);
  });
  it("fails on a dead path in a command block (the T553 case: a renamed curated folder) and in backticks", () => {
    const block = checkDocs(withAnnual(`${annual().text}\n\`\`\`bash\nOLD=data/curated/regulations/entries/catalogue\ncp $W/book/region-*.json $OLD/\n\`\`\`\n`));
    expect(block).toEqual(["handoff/PLAN/runbooks/annual-synopsis.md cites data/curated/regulations/entries/catalogue, which does not exist"]);
    const tick = checkDocs(withAnnual(`${annual().text}\n see \`pipeline/tools/does-not-exist.py\``));
    expect(tick).toEqual(["handoff/PLAN/runbooks/annual-synopsis.md cites pipeline/tools/does-not-exist.py, which does not exist"]);
  });
  it("a variable, glob or placeholder in a command is not a path", () => {
    expect(runbookPaths("```bash\ncp $W/book/region-*.json data/release/<r-id>/x app/tools/check-tickets.mjs:12\n```")).toEqual(["app/tools/check-tickets.mjs"]);
  });
  it("fails when no runbook was read", () => {
    expect(checkDocs({ ...docs, runbooks: [] }).some((m: string) => /no runbook was read/.test(m))).toBe(true);
    const { runbooks: _none, ...without } = docs;
    expect(checkDocs(without as typeof docs).some((m: string) => /no runbook was read/.test(m))).toBe(true);
  });
  it("a path the runbook creates is allowed only while it is absent and still cited", () => {
    const made = RUNBOOK_MAKES["annual-synopsis.md"][0];
    expect(checkDocs({ ...docs, exists: (p: string) => p === made || docs.exists(p) }).some((m: string) => m.includes(`${made} for annual-synopsis.md, which now exists`))).toBe(true);
    expect(checkDocs(withAnnual(annual().text.split(made).join("x"))).some((m: string) => m.includes("the runbook no longer cites"))).toBe(true);
  });
});
