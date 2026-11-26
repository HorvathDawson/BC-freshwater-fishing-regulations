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
import { CLOSED, LEGACY_RESTART, RESTART_DIR, RESTART_MAX, RUNBOOK_MAKES, START_HERE_MAX, LEFT_CELL_MAX, checkDocs, checkLedger, citedPaths, idOfFile, ledgerRows, readDocs, readRepo, ruleIds, runbookPaths, statusKeyword } from "./check-tickets.mjs";
import { LEDGER_POINTER, ROW_HEADER, ROW_SEP, ledgerViews, readWorkTree, setRow, stripRow, ticketRow } from "./ledger.mjs";

type Legacy = { readme: string; doneLedger: string | null; retired: string[] };
type Closed = { id: string; title: string; status: string; commits: string; release: string; left: string; archive: string; tokens: string };
type Input = { tickets: { name: string; text: string }[]; subjects: string[]; unfiled: string; extraProblems: string[]; legacy: Legacy;
  claims: { name: string; text: string }[]; closed: Closed[] };
const repoRoot = new URL("../..", import.meta.url).pathname;
/** the repo as it is: every ticket file with its row (T1041), the owner items, the old places */
const repo = readRepo(repoRoot) as Input;
/**
 * (T1081) The closed tickets are archived: their files are gone, their lines are in closed.tsv. The mutation tests below
 * need a CLOSED ticket FILE to break (a DONE one with a hash), so `real` is the repo plus two made-up ones, T84 and T02,
 * with their claims, taken out of closed.tsv so the id is used once.
 */
const fixture = (id: string, slug: string, title: string) => ({
  name: `${id}-${slug}.md`,
  text: `# ${id} — ${title}\n\nStatus: DONE — abc1234.\n\n${ROW_HEADER}\n${ROW_SEP}\n| ${id} | ${title} | DONE | abc1234 | — |\n\nSource: made up.\n`,
});
const fixtures = [fixture("T84", "ticket-ledger-guard", "ticket ledger guard"), fixture("T02", "blob-codec", "blob codec (fflate dict-inflate + UTF-8 decode)")];
const real: Input = {
  ...repo,
  tickets: [...repo.tickets, ...fixtures].sort((a, b) => (a.name < b.name ? -1 : 1)),
  claims: [...repo.claims, ...fixtures.map((f) => ({ name: idOfFile(f.name)!, text: `${f.name}\n` }))],
  closed: repo.closed.filter((r) => !["T84", "T02"].includes(r.id)),
};

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
    expect(checkLedger(real)).toEqual([]);
  });

  it("was actually read: every ticket file holds its own row, the views split it, and commit subjects were scanned", () => {
    const fileIds = repo.tickets.map((t) => idOfFile(t.name)).sort();
    const rows = repo.tickets.map((t) => ticketRow(t.name, t.text).row!);
    expect(rows.map((r) => r.id).sort()).toEqual(fileIds);
    // (T1081) the closed ones are lines of closed.tsv, not files: none of them is both
    expect(repo.closed.length).toBeGreaterThan(100);
    expect(repo.closed.filter((r) => fileIds.includes(r.id))).toEqual([]);
    // the split is real: the DONE view holds rows, every one closed, and the ledger view still holds the rows in work
    const v = ledgerViews(repo.tickets, repo.closed);
    const open = ledgerRows(v.ledger) as { status: string }[], closed = ledgerRows(v.done) as { status: string }[];
    expect(closed.length).toBeGreaterThan(100);
    expect(closed.filter((r) => !CLOSED.includes(statusKeyword(r.status)))).toEqual([]);
    expect(open.length).toBeGreaterThan(10);
    expect(open.filter((r) => CLOSED.includes(statusKeyword(r.status)))).toEqual([]);
    expect(open.length + closed.length).toBe(fileIds.length + repo.closed.length);
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
      'T84: handoff/PLAN/app-integration/tickets/ids/T84 claims "T84-ticket-ledger-guard.md", but the ticket file is T84-another-slug.md: one number, one ticket',
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
    expect(checkLedger(input)).toEqual(["T84: named in a commit subject but missing from the ledger",
      'handoff/PLAN/app-integration/tickets/ids/T84: claims "T84-ticket-ledger-guard.md", which is not a ticket file of that number']);
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

// ---- T1081: a closed ticket is a line of closed.tsv (its text is in archive/*.tar.gz), no file, no claim ----
describe("T1081 archived tickets", () => {
  const line = (id: string, over: Partial<Closed> = {}): Closed => ({ id, title: "x", status: "DONE", commits: "abc1234", release: "", left: "—", archive: "closed-2026", tokens: "", ...over });
  const gone = (id: string) => ({ ...noGit, tickets: noGit.tickets.filter((t) => idOfFile(t.name) !== id), claims: noGit.claims.filter((c) => c.name !== id) });
  it("a Tnnn named in a commit subject is satisfied by a closed.tsv line, and by nothing else", () => {
    const input = { ...gone("T84"), subjects: ["T84: ticket-ledger guard"] };
    expect(checkLedger({ ...input, closed: [line("T84")] })).toEqual([]);
    expect(checkLedger({ ...input, closed: [] })).toEqual(["T84: named in a commit subject but missing from the ledger"]);
  });
  it("a number is used once: an id in closed.tsv with a file or a claim still here fails", () => {
    expect(checkLedger({ ...noGit, closed: [line("T84")] })).toEqual([
      "T84: T84-ticket-ledger-guard.md is still here but T84 is in handoff/PLAN/app-integration/tickets/closed.tsv: a number is used once",
      "handoff/PLAN/app-integration/tickets/ids/T84: T84 is in handoff/PLAN/app-integration/tickets/closed.tsv (archived): its number is kept there, delete the claim"]);
    const claimOnly = { ...gone("T84"), claims: noGit.claims };
    expect(checkLedger({ ...claimOnly, closed: [line("T84")] })).toEqual([
      "handoff/PLAN/app-integration/tickets/ids/T84: T84 is in handoff/PLAN/app-integration/tickets/closed.tsv (archived): its number is kept there, delete the claim"]);
  });
  it("a closed.tsv line is held to the closed rules: DONE / DROPPED only, a hash on DONE, a short last cell", () => {
    const input = gone("T84");
    expect(checkLedger({ ...input, closed: [line("T84", { status: "OPEN" })] })).toEqual([
      'T84: handoff/PLAN/app-integration/tickets/closed.tsv holds it as "OPEN": only DONE / DROPPED tickets are archived']);
    expect(checkLedger({ ...input, closed: [line("T84", { commits: "—" })] })).toEqual([
      "T84: handoff/PLAN/app-integration/tickets/closed.tsv row is DONE with no commit hash"]);
    expect(checkLedger({ ...input, closed: [line("T84", { status: "DROPPED", commits: "—" })] })).toEqual([]);
    expect(checkLedger({ ...input, closed: [line("T84", { left: "x".repeat(LEFT_CELL_MAX + 1) })] })).toEqual([
      `T84: handoff/PLAN/app-integration/tickets/closed.tsv row's last cell is ${LEFT_CELL_MAX + 1} characters (max ${LEFT_CELL_MAX})`]);
  });
  it("the closed lines must be given, never read as 'nothing archived'", () => {
    const { closed: _none, ...without } = noGit;
    expect(() => checkLedger(without as Input)).toThrow(/needs closed/);
  });
  it("the commit check sees the archived ids (ledgerIds)", async () => {
    const { ledgerIds } = await import("./check-tickets.mjs");
    const ids = ledgerIds(repoRoot) as Set<string>;
    expect(repo.closed.every((r) => ids.has(r.id))).toBe(true);
    expect(repo.tickets.every((t) => ids.has(idOfFile(t.name)!))).toBe(true);
  });
  it("the real closed.tsv reads clean: every ticket claimed once, in id order, with no file left", () => {
    expect(repo.extraProblems).toEqual([]);
    expect(new Set(repo.closed.map((r) => r.id)).size).toBe(repo.closed.length);
    expect(repo.closed.filter((r) => repo.claims.some((c) => c.name === r.id))).toEqual([]);
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
  it("the real README holds the pointer, no row; DONE.md is gone; the owner to-do page and folder are retired", () => {
    expect(real.legacy.readme).toContain(LEDGER_POINTER.trim());
    expect(real.legacy.doneLedger).toBeNull();
    expect(real.legacy.retired).toEqual([]);
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
  it("(T1081) the retired owner to-do page or folder coming back fails, naming the Owner block", () => {
    const out = checkLedger({ ...noGit, legacy: { ...noGit.legacy, retired: ["handoff/PLAN/OWNER-TODO.md", "handoff/PLAN/owner-todo"] } });
    expect(out).toHaveLength(2);
    expect(out[0]).toMatch(/^handoff\/PLAN\/OWNER-TODO\.md exists: it was retired \(T1081\): an owner ask is a line of the "## Owner" block/);
    expect(out[1]).toMatch(/^handoff\/PLAN\/owner-todo exists: it was retired/);
    expect(() => checkLedger({ ...noGit, legacy: { ...noGit.legacy, retired: undefined as unknown as string[] } })).toThrow(/needs legacy/);
  });
  it("a line added to README's ledger section fails", () => {
    const readme = real.legacy.readme.replace(LEDGER_POINTER.trim(), `${LEDGER_POINTER.trim()}\n- T1099 is next (typed here the old way)`);
    expect(checkLedger({ ...noGit, legacy: { ...noGit.legacy, readme } })).toEqual([
      'handoff/PLAN/app-integration/tickets/README.md: its "## Ledger" section is not the pointer text (ledger.mjs LEDGER_POINTER): a row lives in its ticket file (T1041), nothing is written there']);
  });
  it("(T1041) every ticket claims its number in tickets/ids/, and the claims must be given", () => {
    expect(real.claims.length).toBe(real.tickets.length);
    expect(checkLedger({ ...noGit, claims: real.claims.filter((c) => c.name !== "T84") })).toEqual([
      "T84: no number claim handoff/PLAN/app-integration/tickets/ids/T84 (node app/tools/ledger.mjs --claim writes it): two PRs that take one number must collide there"]);
    const { claims: _none, ...without } = noGit;
    expect(() => checkLedger(without as Input)).toThrow(/needs claims/);
  });
  it("(T1041) a conflict marker left in a ticket fails", () => {
    expect(checkLedger(withTicket(noGit, "T84", (t) => `${t}\n<<<<<<< new-style side\nx\n=======\ny\n>>>>>>> old-style side\n`))).toEqual([
      "T84: T84-ticket-ledger-guard.md holds a merge-conflict marker line (<<<<<<< / >>>>>>>): settle it"]);
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

describe("T539 a status names its step", () => {
  it("the real ledger passes, with no grandfather list", () => {
    expect(checkLedger(real)).toEqual([]);
  });
  it("IN REVIEW with no step fails", () => {
    expect(checkLedger(withT84("IN REVIEW — abc1234"))).toEqual([
      'T84: IN REVIEW names no step: add `owner: see ## Owner` (with an open line in the ticket\'s "## Owner" block) or `left (Claude): …` to the Status line']);
  });
  it("PARTIAL with no step fails", () => {
    expect(checkLedger(withT84("PARTIAL — abc1234 half done"))[0]).toMatch(/T84: PARTIAL names no step/);
  });
  it("(T1081) `owner: see ## Owner` needs an open line in the ticket's Owner block; an answered one or none fails", () => {
    const status = "IN REVIEW — abc1234; owner: see ## Owner";
    const owner = (...lines: string[]) => (t: string) => `${t.replace(/\n*$/, "\n")}\n## Owner\n\n${lines.join("\n")}\n`;
    expect(checkLedger(withTicket(withT84(status), "T84", owner("- [ ] 2026-11-25 — look at the shots")))).toEqual([]);
    const none = `T84: Status says \`owner: see ## Owner\` but the ticket's "## Owner" block holds no open line`;
    expect(checkLedger(withT84(status))).toEqual([none]);
    expect(checkLedger(withTicket(withT84(status), "T84", owner('- [x] 2026-11-25 — looked → "fine" (2026-11-26)')))).toEqual([none]);
    expect(checkLedger(withT84(`IN REVIEW — abc1234; owner: OWNER-TODO "x"`))[0]).toMatch(/^T84: Status cites OWNER-TODO, which is retired \(T1081\)/);
  });
  it("(T1081) an Owner block that does not parse fails: an undated open line, a stray line, a second block", () => {
    const add = (body: string) => withTicket(noGit, "T84", (t) => `${t.replace(/\n*$/, "\n")}\n${body}\n`);
    expect(checkLedger(add("## Owner\n\n- [ ] no date here"))).toEqual(
      ['T84: an open owner line has no date: "no date here" (write "- [ ] YYYY-MM-DD — <ask>")']);
    expect(checkLedger(add("## Owner\n\njust words"))[0]).toMatch(/^T84: owner line "just words" must read/);
    expect(checkLedger(add("## Owner\n- [x] 2026-11-25 — a → \"b\"\n\n## Owner\n- [x] 2026-11-25 — c"))).toEqual(['T84: more than one "## Owner" block (keep one)']);
    expect(checkLedger({ ...noGit, unfiled: "## Waiting\n- [ ] no date\n" })[0]).toMatch(/OWNER-UNFILED\.md "Waiting": an open owner line has no date/);
  });
  it("(T1081) a DONE ticket holds no open owner line; an answered one is fine", () => {
    const owner = (line: string) => (t: string) => `${t.replace(/\n*$/, "\n")}\n## Owner\n\n${line}\n`;
    expect(checkLedger(withTicket(noGit, "T84", owner("- [ ] 2026-11-25 — still waiting")))).toEqual(
      ['T84: DONE but its "## Owner" block holds 1 open line(s) ("still waiting"): answer or take them out (a closed ticket has no open owner line)']);
    expect(checkLedger(withTicket(noGit, "T84", owner('- [x] 2026-11-25 — waited → "ok" (2026-11-26)')))).toEqual([]);
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
    expect(docs.restartPages.map((r: { name: string }) => r.name)).toContain("cleanup.md");
    expect(docs.restartFiles).toEqual(expect.arrayContaining(LEGACY_RESTART));
    expect(citedPaths(docs.startHere)).toContain("handoff/PLAN/app-integration/tickets/README.md");
    expect(ruleIds(docs.agents).length).toBeGreaterThan(60);
  });
  it("(T1081) the restart pages must be given, never read as 'no restart pages'", () => {
    const { restartPages: _gone, ...without } = docs;
    expect(checkDocs(without as typeof docs)).toContain('no restart pages were read (restartPages, restartFiles): never "no restart pages to check" (T1081)');
  });
  it("fails on stray lines above STATUS's title", () => {
    expect(checkDocs({ ...live(), status: "- left over\n# Where things stand\n" })[0]).toMatch(/must start with its title/);
  });
  it("fails on a dead path in START-HERE and in a restart page", () => {
    const dead = "\n see `app/tools/does-not-exist.mjs`";
    expect(checkDocs({ ...live(), startHere: `${docs.startHere}${dead}` }).some((m: string) => /START-HERE\.md cites app\/tools\/does-not-exist\.mjs, which does not exist/.test(m))).toBe(true);
    const pages = docs.restartPages.map((r: { name: string; text: string }, i: number) => (i === 0 ? { ...r, text: `${r.text}${dead}` } : r));
    expect(checkDocs({ ...live(), restartPages: pages }).some((m: string) => /restart\/.*cites app\/tools\/does-not-exist\.mjs, which does not exist/.test(m))).toBe(true);
  });
  it("a git-ignored output path may be absent; a glob or placeholder is not a path", () => {
    const base = { ...live(), restartPages: [{ name: "cleanup.md", text: "`app/dist/gone` `app/tools/*.mjs` `data/release/<r-id>`" }], startHere: `${docs.startHere}`, isIgnored: (p: string) => p.startsWith("app/dist/") || docs.isIgnored(p) };   // the other live docs keep the real answer: a
    // fresh clone (CI) has no CLAUDE.md and no gauge feeds folder, both git-ignored
    expect(checkDocs(base)).toEqual([]);
  });
  it("fails on state in a restart page (release id, build number)", () => {
    const pages = docs.restartPages.map((r: { name: string; text: string }) => ({ ...r, text: `${r.text}\nLATEST = r-0123456789ab; Build 99 on Play` }));
    const out = checkDocs({ ...live(), restartPages: pages });
    expect(out.filter((m: string) => /holds state/.test(m))).toHaveLength(2 * pages.length);
  });
  it("(T1081) START-HERE links every restart page and nothing missing; each is short, so is START-HERE", () => {
    const names = docs.restartPages.map((r: { name: string }) => r.name);
    expect(names.length).toBeGreaterThan(0);
    for (const n of names) expect(docs.startHere).toContain(`${RESTART_DIR}/${n}`);
    const extra = [...docs.restartPages, { name: "unlisted.md", text: "# x\n" }];
    expect(checkDocs({ ...live(), restartPages: extra })).toEqual([`${RESTART_DIR}/unlisted.md is not linked from START-HERE.md (it lists every live chat's page; delete the page when its chat ends)`]);
    const ghost = `${docs.startHere}\n\`${RESTART_DIR}/ghost.md\``;
    expect(checkDocs({ ...live(), startHere: ghost.split("\n").slice(0, START_HERE_MAX - 1).join("\n") + `\n\`${RESTART_DIR}/ghost.md\`` })).toEqual([
      `START-HERE.md cites ${RESTART_DIR}/ghost.md, which does not exist`, `START-HERE.md links ${RESTART_DIR}/ghost.md, which does not exist`]);
    const long = docs.restartPages.map((r: { name: string; text: string }, i: number) => (i === 0 ? { ...r, text: `${r.text.replace(/\n+$/, "")}\n${"filler\n".repeat(RESTART_MAX)}` } : r));
    expect(checkDocs({ ...live(), restartPages: long })).toEqual([`${RESTART_DIR}/${names[0]} is ${long[0].text.replace(/\n+$/, '').split('\n').length} lines (max ${RESTART_MAX}): a restart page is short; the ticket and STATUS hold the detail`]);
    expect(checkDocs({ ...live(), startHere: `${docs.startHere}${"filler\n".repeat(START_HERE_MAX)}` })[0]).toMatch(new RegExp(`^START-HERE\\.md is \\d+ lines \\(max ${START_HERE_MAX}\\)`));
  });
  it("(T1081) a restart page outside restart/ fails unless it is one of the four named legacy pages", () => {
    expect(LEGACY_RESTART).toHaveLength(4);
    expect(checkDocs({ ...live(), restartFiles: [...docs.restartFiles, "handoff/PLAN/RESTART-new-chat.md"] })).toEqual([
      "handoff/PLAN/RESTART-new-chat.md is a restart page outside handoff/PLAN/restart/ (T1081): move it there, link it from START-HERE.md (LEGACY_RESTART names the only ones left where they are)"]);
    expect(checkDocs({ ...live(), restartFiles: [...docs.restartFiles, "handoff/PLAN/NEXT-SESSION-2026-11-25-x.md"] })[0]).toMatch(/NEXT-SESSION-2026-11-25-x\.md is a restart page outside/);
    expect(checkDocs({ ...live(), restartFiles: [...LEGACY_RESTART] })).toEqual([]);
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
  it("(T1081) a branch name (docs/T1081-cut: one segment, no dot) is not a cited path; a file under it still is", () => {
    // the rename makes `docs/` a repo root, where a cited branch would read as a dead path (found in the rehearsal)
    expect(citedPaths("branch `docs/T1081-cut`, `ci/T1081-size-guard`, and `scripts/fast-fail.sh`")).toEqual(["scripts/fast-fail.sh"]);
    expect(runbookPaths("```bash\ngit switch docs/T1081-cut\nbash scripts/fast-fail.sh\n```\n")).toEqual(["scripts/fast-fail.sh"]);
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
