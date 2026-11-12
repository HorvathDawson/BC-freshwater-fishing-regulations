/**
 * T84 — the ticket ledger stays whole.
 *
 * `tools/check-tickets.mjs` is the guard (`pnpm tickets`, in `pnpm check`). Held two ways, because a guard that
 * finds nothing proves nothing on its own:
 *   1. the real tickets folder, README ledger and commit subjects pass — and the guard actually SAW them;
 *   2. each rule fires on a mutated copy of the real ledger (row removed, Status removed, hash removed, …).
 */
import { describe, expect, it } from "vitest";
import { repoData } from "../conformance/src/repoData";
// A plain .mjs gate (it runs under node as `pnpm tickets`), imported for its functions.
import { CLOSED, DONE_LEDGER, NO_STEP_YET, RUNBOOK_MAKES, checkDocs, checkLedger, citedPaths, idOfFile, ledgerRows, readDocs, readRepo, ruleIds, runbookPaths, statusKeyword } from "./check-tickets.mjs";

type Input = { tickets: { name: string; text: string }[]; readme: string; doneLedger: string; subjects: string[]; ownerTodo: string };
const repoRoot = new URL("../..", import.meta.url).pathname;
/** the repo as it is: README.md (rows in work) + DONE.md (closed rows, T540) */
const repo = readRepo(repoRoot) as Input;
const rowIn = (text: string, id: string) => text.split("\n").find((l) => l.startsWith(`| ${id} |`));
/** `input` with the row of `id` in README.md, wherever it sits today (a closed row lives in DONE.md): the mutations
 *  below edit a README row, and must not depend on which rows have been moved */
const rowInReadme = (input: Input, id: string): Input => {
  const row = rowIn(input.doneLedger, id);
  if (!row) return input;
  const lines = input.readme.split("\n");
  const start = lines.findIndex((l) => /^##\s+Ledger\b/.test(l));
  const last = lines.findLastIndex((l, i) => i > start && i < lines.findIndex((x, j) => j > start && /^##\s/.test(x)) && l.startsWith("| T"));
  lines.splice(last + 1, 0, row);
  return { ...input, readme: lines.join("\n"), doneLedger: input.doneLedger.replace(row + "\n", "") };
};
const real = rowInReadme(rowInReadme(repo, "T84"), "T02");

const withTicket = (input: Input, id: string, edit: (text: string) => string): Input => ({
  ...input,
  tickets: input.tickets.map((t) => (idOfFile(t.name) === id ? { ...t, text: edit(t.text) } : t)),
});
const rowOf = (readme: string, id: string) => readme.split("\n").find((l) => l.startsWith(`| ${id} |`))!;
/** A ledger row with its status cell replaced — independent of what the live row says today. */
const withStatus = (row: string, status: string) => {
  const cells = row.split("|");
  cells[3] = ` ${status} `;
  return cells.join("|");
};
/** The real tickets + README with no commit subjects: the row/file rules alone, not today's git history. */
const noGit: Input = { ...real, subjects: [] };

describe("the real ledger", () => {
  it("is whole", () => {
    expect(checkLedger(repo)).toEqual([]);
    expect(checkLedger(real)).toEqual([]);
  });

  it("was actually read: every ticket file has a row in README.md or DONE.md, and commit subjects were scanned", () => {
    const fileIds = repo.tickets.map((t) => idOfFile(t.name)).sort();
    const open = ledgerRows(repo.readme) as { id: string; status: string }[], closed = ledgerRows(repo.doneLedger) as { id: string; status: string }[];
    expect([...open, ...closed].map((r) => r.id).sort()).toEqual(fileIds);
    // (T540) the split is real: DONE.md holds rows, every one closed, and README.md still holds the rows in work
    expect(closed.length).toBeGreaterThan(100);
    expect(closed.filter((r) => !CLOSED.includes(statusKeyword(r.status)))).toEqual([]);
    expect(open.some((r) => !CLOSED.includes(statusKeyword(r.status)))).toBe(true);
    expect(fileIds.length).toBeGreaterThan(100);
    expect(real.subjects.some((s) => /\bT84\b|\bT56\b/.test(s))).toBe(true);
    // Scoped to the tickets era: "T3a" (an older plan, before the tickets folder) is not scanned.
    expect(real.subjects.some((s) => /\bT3a\b/.test(s))).toBe(false);
  });
});

describe("the guard fires (mutation)", () => {
  it("on a removed ledger row", () => {
    const readme = real.readme.replace(rowOf(real.readme, "T84") + "\n", "");
    expect(checkLedger({ ...noGit, readme })).toEqual(["T84: ticket file has no ledger row in README.md or DONE.md"]);
  });

  it("on a removed ledger row of a ticket commits name (both rules fire)", () => {
    const readme = real.readme.replace(rowOf(real.readme, "T84") + "\n", "");
    expect(checkLedger({ ...noGit, readme, subjects: ["T84: ticket-ledger guard"] })).toEqual([
      "T84: named in a commit subject but missing from the ledger",
      "T84: ticket file has no ledger row in README.md or DONE.md",
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

  it("on a ticket file removed while its row stays", () => {
    const input = { ...real, tickets: real.tickets.filter((t) => idOfFile(t.name) !== "T84") };
    expect(checkLedger(input)).toEqual(["T84: ledger row has no ticket file"]);
  });

  it("on a T-number in a commit subject that the ledger lacks", () => {
    const input = { ...real, subjects: [...real.subjects, "T999: work nobody ticketed"] };
    expect(checkLedger(input)).toEqual(["T999: named in a commit subject but missing from the ledger"]);
  });

  it("does not read an owner-question label (Q-T57a) or other hyphen-joined id as a ticket", () => {
    expect(checkLedger({ ...noGit, subjects: ["T57: status DONE - results and Q-T57a", "see T45-shots"] })).toEqual([]);
  });

  it("on DONE with no commit hash, in the file and in the row", () => {
    const row = rowOf(real.readme, "T02");
    const readme = real.readme.replace(row, "| T02 | blob codec (fflate dict-inflate + UTF-8 decode) | DONE | — | — |");
    const input = withTicket({ ...real, readme }, "T02", (t) => t.replace(/^Status:.*$/m, "Status: DONE."));
    expect(checkLedger(input)).toEqual([
      "T02: Status DONE names no commit hash in T02-blob-codec.md",
      "T02: ledger row is DONE with no commit hash",
    ]);
  });

  it("(T437) on a ticket file whose status differs from its ledger row's", () => {
    const row = rowOf(real.readme, "T84");
    const readme = real.readme.replace(row, withStatus(row, "OPEN"));
    const input = withTicket({ ...noGit, readme }, "T84", (t) => t.replace(/^Status:.*$/m, "Status: DONE — abc1234."));
    expect(checkLedger(input)).toEqual(["T84: the ticket file says DONE, its ledger row OPEN (close both together)"]);
  });

  it("on a duplicated row and an unknown status", () => {
    const row = rowOf(real.readme, "T84");
    const readme = real.readme.replace(row, `${row}\n${withStatus(row, "MAYBE")}`);
    expect(checkLedger({ ...noGit, readme })).toEqual([
      'T84: ledger row appears more than once',
      'T84: ledger status "MAYBE" is not one of IN PROGRESS, IN REVIEW, BLOCKED, POSTPONED, PARTIAL, DROPPED, OPEN, DONE',
    ]);
  });
});

describe("statusKeyword", () => {
  it("(T437) knows exactly the statuses the README defines", async () => {
    const { STATUSES } = await import("./check-tickets.mjs");
    const defined = [...real.readme.matchAll(/^  - `([A-Z ]+?)(?: \(owner\))?` — /gm)].map((m) => m[1]);
    expect([...defined].sort()).toEqual([...STATUSES].sort());
  });

  it("reads the leading keyword of every status the README defines", () => {
    expect(["OPEN.", "IN PROGRESS — lane x", "PARTIAL — a", "BLOCKED (owner)", "POSTPONED — hold",
      "DONE — 40b2c67d", "DROPPED — why", "DONE (in T21)", "Open"].map(statusKeyword)).toEqual(
      ["OPEN", "IN PROGRESS", "PARTIAL", "BLOCKED", "POSTPONED", "DONE", "DROPPED", "DONE", null]);
  });
});
// ---- T540: the ledger is two files, README.md (in work) and DONE.md (closed) ----
describe("T540 closed rows live in DONE.md", () => {
  const row = rowIn(real.readme, "T84")!;
  /** T84's row moved from README.md to DONE.md */
  const moved: Input = { ...noGit, readme: noGit.readme.replace(row + "\n", ""), doneLedger: `${noGit.doneLedger.trimEnd()}\n${row}\n` };
  it("the file is named, and a missing one is an error, never 'no closed rows'", () => {
    expect(DONE_LEDGER).toBe("DONE.md");
    const { doneLedger: _gone, ...without } = noGit;
    expect(() => checkLedger(without)).toThrow(/needs doneLedger/);
  });
  it("a ticket whose row is only in DONE.md passes", () => {
    expect(rowIn(moved.readme, "T84")).toBeUndefined();
    expect(checkLedger(moved)).toEqual([]);
  });
  it("a row in neither file fails", () => {
    expect(checkLedger({ ...moved, doneLedger: noGit.doneLedger })).toEqual(["T84: ticket file has no ledger row in README.md or DONE.md"]);
  });
  it("an OPEN ticket in DONE.md fails, as does every status but DONE and DROPPED", () => {
    const hidden = (kw: string, status: string): Input => withTicket({ ...moved, doneLedger: moved.doneLedger.replace(row, withStatus(row, kw)) },
      "T84", (t) => t.replace(/^Status:.*$/m, `Status: ${status}`));
    expect(checkLedger(hidden("OPEN", "OPEN."))).toEqual(["T84: DONE.md holds only DONE or DROPPED rows, this one is OPEN (move it back to README.md)"]);
    expect(checkLedger(hidden("IN PROGRESS", "IN PROGRESS — left (Claude): x"))).toEqual(["T84: DONE.md holds only DONE or DROPPED rows, this one is IN PROGRESS (move it back to README.md)"]);
    expect(checkLedger(hidden("POSTPONED", "POSTPONED."))).toHaveLength(1);
    expect(checkLedger(hidden("DROPPED", "DROPPED — not needed."))).toEqual([]);
  });
  it("a row in both files fails", () => {
    expect(checkLedger({ ...noGit, doneLedger: moved.doneLedger })).toEqual(["T84: ledger row appears more than once (in README.md and in DONE.md: keep one)"]);
  });
  it("the file-vs-row status match and the DONE hash rule run on DONE.md rows too", () => {
    const open = withTicket(moved, "T84", (t) => t.replace(/^Status:.*$/m, "Status: OPEN."));
    expect(checkLedger(open)).toEqual(["T84: the ticket file says OPEN, its ledger row DONE (close both together)"]);
    const cells = row.split("|"); cells[4] = " — ";
    expect(checkLedger({ ...moved, doneLedger: moved.doneLedger.replace(row, cells.join("|")) })).toEqual(["T84: ledger row is DONE with no commit hash"]);
  });
  it("a DONE.md row with no ticket file fails; a commit subject naming a DONE.md ticket is in the ledger", () => {
    expect(checkLedger({ ...moved, tickets: moved.tickets.filter((t) => idOfFile(t.name) !== "T84") })).toEqual(["T84: ledger row has no ticket file"]);
    expect(checkLedger({ ...moved, subjects: ["T84: ticket-ledger guard"] })).toEqual([]);
  });
});

// ---- T539: a status names a step that can close; the docs guard ----
const T84_ROW = (input: Input) => rowOf(input.readme, "T84");
/** T84 with its status line and ledger row both set to `status` (the two keywords must agree). */
const withT84 = (status: string): Input => {
  const kw = ["IN REVIEW", "PARTIAL", "IN PROGRESS", "DONE"].find((k) => status.startsWith(k))!;
  const readme = noGit.readme.replace(T84_ROW(noGit), withStatus(T84_ROW(noGit), kw));
  return withTicket({ ...noGit, readme }, "T84", (t) => t.replace(/^Status:.*$/m, `Status: ${status}`));
};
/** a text OWNER-TODO holds that no ticket's Status cites today (removing it must name T84 alone) */
const OWNER_TEXT = [...real.ownerTodo.matchAll(/\*\*([^*]{8,40})\*\*/g)].map((m) => m[1]!)
  .find((text) => !real.tickets.some((t) => t.text.split("\n").slice(0, 6).join("\n").includes(text)))!;

describe("T539 a status names its step", () => {
  it("the real ledger passes, and the grandfather list is what is left unnamed", () => {
    expect(checkLedger(real)).toEqual([]);
    expect(NO_STEP_YET.length).toBeGreaterThan(0);
    expect(new Set(NO_STEP_YET).size).toBe(NO_STEP_YET.length);
  });
  it("IN REVIEW with no step fails", () => {
    expect(checkLedger(withT84("IN REVIEW — abc1234"))).toEqual([
      'T84: IN REVIEW names no step: add `owner: OWNER-TODO "<text>"` or `left (Claude): …` to the Status line']);
  });
  it("PARTIAL with no step fails", () => {
    expect(checkLedger(withT84("PARTIAL — abc1234 half done"))[0]).toMatch(/T84: PARTIAL names no step/);
  });
  it("an OWNER-TODO text that exists passes; one that is gone fails", () => {
    expect(OWNER_TEXT).toBeTruthy();
    expect(checkLedger(withT84(`IN REVIEW — abc1234; owner: OWNER-TODO "${OWNER_TEXT}"`))).toEqual([]);
    const gone = { ...withT84(`IN REVIEW — abc1234; owner: OWNER-TODO "${OWNER_TEXT}"`), ownerTodo: real.ownerTodo.split(OWNER_TEXT).join("x") };
    expect(checkLedger(gone)).toEqual([`T84: Status cites OWNER-TODO "${OWNER_TEXT}", which OWNER-TODO.md does not contain`]);
  });
  it("IN REVIEW whose only step is Claude's is IN PROGRESS; PARTIAL with Claude's step is fine", () => {
    expect(checkLedger(withT84("IN REVIEW — abc1234; left (Claude): run the emulator"))).toEqual([
      "T84: IN REVIEW but the only step is Claude's (left (Claude)): that is IN PROGRESS"]);
    expect(checkLedger(withT84("PARTIAL — abc1234; left (Claude): run the emulator"))).toEqual([]);
    expect(checkLedger(withT84("IN PROGRESS — lane x; left (Claude): run the emulator"))).toEqual([]);
  });
  it("a grandfathered id that now complies, or is no longer in review, must come off the list", () => {
    const id = NO_STEP_YET[0] as string;
    const fixed = withTicket(real, id, (t) => t.replace(/^Status: (IN REVIEW|PARTIAL)/m, `Status: $1 — left (Claude): x; owner: OWNER-TODO "${OWNER_TEXT}" —`));
    expect(checkLedger(fixed)).toEqual([`${id}: on NO_STEP_YET but its status now names its step (take it off the list)`]);
    const closed = withTicket(real, id, (t) => t.replace(/^Status:.*$/m, "Status: OPEN."));
    expect(checkLedger(closed).some((m: string) => m.startsWith(`${id}: on NO_STEP_YET but is`))).toBe(true);
  });
  it("a NEW ticket cannot hide behind the list", () => {
    expect(NO_STEP_YET).not.toContain("T84");
    expect(NO_STEP_YET).not.toContain("T534");
    expect(NO_STEP_YET).not.toContain("T539");
    expect(NO_STEP_YET).not.toContain("T542");
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
  it("fails on stray lines above STATUS's title", () => {
    expect(checkDocs({ ...live(), status: "- left over\n# Where things stand\n" })[0]).toMatch(/must start with its title/);
  });
  it("fails on a dead path in each of the three documents", () => {
    for (const k of ["startHere", "restart", "ownerTodo"] as const) {
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
