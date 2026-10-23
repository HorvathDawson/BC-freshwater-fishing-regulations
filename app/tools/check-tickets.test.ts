/**
 * T84 — the ticket ledger stays whole.
 *
 * `tools/check-tickets.mjs` is the guard (`pnpm tickets`, in `pnpm check`). Held two ways, because a guard that
 * finds nothing proves nothing on its own:
 *   1. the real tickets folder, README ledger and commit subjects pass — and the guard actually SAW them;
 *   2. each rule fires on a mutated copy of the real ledger (row removed, Status removed, hash removed, …).
 */
import { describe, expect, it } from "vitest";
// A plain .mjs gate (it runs under node as `pnpm tickets`), imported for its functions.
import { checkLedger, idOfFile, ledgerRows, readRepo, statusKeyword } from "./check-tickets.mjs";

type Input = { tickets: { name: string; text: string }[]; readme: string; subjects: string[] };
const repoRoot = new URL("../..", import.meta.url).pathname;
const real = readRepo(repoRoot) as Input;

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
    expect(checkLedger(real)).toEqual([]);
  });

  it("was actually read: every ticket file has a row, and commit subjects were scanned", () => {
    const fileIds = real.tickets.map((t) => idOfFile(t.name)).sort();
    const rowIds = (ledgerRows(real.readme) as { id: string }[]).map((r) => r.id).sort();
    expect(rowIds).toEqual(fileIds);
    expect(fileIds.length).toBeGreaterThan(100);
    expect(real.subjects.some((s) => /\bT84\b|\bT56\b/.test(s))).toBe(true);
    // Scoped to the tickets era: "T3a" (an older plan, before the tickets folder) is not scanned.
    expect(real.subjects.some((s) => /\bT3a\b/.test(s))).toBe(false);
  });
});

describe("the guard fires (mutation)", () => {
  it("on a removed ledger row", () => {
    const readme = real.readme.replace(rowOf(real.readme, "T84") + "\n", "");
    expect(checkLedger({ ...noGit, readme })).toEqual(["T84: ticket file has no ledger row in README.md"]);
  });

  it("on a removed ledger row of a ticket commits name (both rules fire)", () => {
    const readme = real.readme.replace(rowOf(real.readme, "T84") + "\n", "");
    expect(checkLedger({ ...noGit, readme, subjects: ["T84: ticket-ledger guard"] })).toEqual([
      "T84: named in a commit subject but missing from the ledger",
      "T84: ticket file has no ledger row in README.md",
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

  it("allows a ledger status that lags its ticket file (the lane closes the file, the merger the row)", () => {
    const input = withTicket(real, "T84", (t) => t.replace(/^Status:.*$/m, "Status: DONE — abc1234."));
    expect(checkLedger(input)).toEqual([]);
  });

  it("on a duplicated row and an unknown status", () => {
    const row = rowOf(real.readme, "T84");
    const readme = real.readme.replace(row, `${row}\n${withStatus(row, "MAYBE")}`);
    expect(checkLedger({ ...noGit, readme })).toEqual([
      'T84: ledger row appears more than once',
      'T84: ledger status "MAYBE" is not one of IN PROGRESS, BLOCKED, POSTPONED, PARTIAL, DROPPED, OPEN, DONE',
    ]);
  });
});

describe("statusKeyword", () => {
  it("reads the leading keyword of every status the README defines", () => {
    expect(["OPEN.", "IN PROGRESS — lane x", "PARTIAL — a", "BLOCKED (owner)", "POSTPONED — hold",
      "DONE — 40b2c67d", "DROPPED — why", "DONE (in T21)", "Open"].map(statusKeyword)).toEqual(
      ["OPEN", "IN PROGRESS", "PARTIAL", "BLOCKED", "POSTPONED", "DONE", "DROPPED", "DONE", null]);
  });
});
