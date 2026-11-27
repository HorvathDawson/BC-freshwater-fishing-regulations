/**
 * (T1041, T1081) THE LEDGER ROWS AND THE OWNER'S OPEN ASKS, EACH IN ITS TICKET: `tools/ledger.mjs`.
 *
 *   1. the row block: put under the Status paragraph, taken out again byte for byte, set in place; what is wrong with one;
 *   2. the views: id order, and a row's view follows its status keyword (T540's "DONE.md holds only DONE / DROPPED",
 *      now by construction);
 *   3. the owner blocks (T1081): `## Owner` lines parse, what is wrong with one, the OWNER view, and an archive that
 *      refuses a ticket with an open owner line;
 *   4. the number claims, the closed index and the archive.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
// A plain .mjs tool (it runs under node), imported for its functions.
import {
  ROW_HEADER, ROW_SEP, insertRow, ledgerViews, ownerAsks, ownerView, setRow, stripRow, tableRows, unfiledAsks,
  ticketRow, claimProblems, readWorkTree,
  CLOSED_TSV, KEPT_ASSETS, KEPT_READERS, archivePath, assetIds, archiveProblems, citeTokens, gunzipTar, gzipTar, nextNumber, parseTsv, planArchive, releaseOf, tarBytes, tarFiles, tsvLine,
} from "./ledger.mjs";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const SCRIPT = fileURLToPath(new URL("./ledger.mjs", import.meta.url));
const T = "tickets";
const cleanEnv = () => Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("GIT_")));

const row = (id: string, status: string, commits = "—", title = `title of ${id}`) => `| ${id} | ${title} | ${status} | ${commits} | — |`;
const ticket = (id: string, status: string, more = "Source: x.\n\n## Work\n\nsome work\n") => `# ${id} — a ticket\n\nStatus: ${status}\n\n${more}`;

describe("the row block", () => {
  const shapes = {
    "one Status line": "# T1 — x\n\nStatus: OPEN.\n\nSource: a.\n",
    "a Status paragraph": "# T1 — x\n\nStatus: PARTIAL — a,\n  b and c;\nmore of it\n\n## Work\nw\n",
    "the Status paragraph ends the file": "# T1 — x\n\nStatus: OPEN.",
    "the Status paragraph ends the file, with its newline": "# T1 — x\n\nStatus: OPEN.\n",
    "Status on line 5": "# T1 — x\n\nline\nline\nStatus: OPEN.\n\nrest\n",
  };
  it("goes right under the Status paragraph and comes out byte for byte", () => {
    for (const [what, text] of Object.entries(shapes)) {
      const withRow = insertRow(text, row("T1", "OPEN"));
      const lines = withRow.split("\n");
      const h = lines.indexOf(ROW_HEADER);
      expect([what, lines[h - 1], lines[h + 1], lines[h + 2]]).toEqual([what, "", ROW_SEP, row("T1", "OPEN")]);
      expect(lines[h - 2]!.trim(), what).not.toBe("");                        // the line above the blank is the paragraph
      expect([what, stripRow(withRow)]).toEqual([what, text]);
      expect(ticketRow("T1-x.md", withRow).problems).toEqual([]);
    }
    expect(() => insertRow("# T1\n\n\n\n\n\nStatus: OPEN.\n", "x")).toThrow(/no "Status:" line/);
  });
  it("is set in place, and only its row line changes", () => {
    const a = insertRow(shapes["a Status paragraph"], row("T1", "OPEN"));
    const b = setRow(a, row("T1", "DONE", "abc1234"));
    expect(a.split("\n").filter((l, i) => l !== b.split("\n")[i])).toEqual([row("T1", "OPEN")]);
    expect(ticketRow("T1-x.md", b).row?.status).toBe("DONE");
  });
  it("names what is wrong: none, two, no separator, no row, another ticket's row", () => {
    const ok = insertRow(ticket("T7", "OPEN."), row("T7", "OPEN"));
    expect(ticketRow("T7-x.md", ticket("T7", "OPEN.")).problems).toEqual(["T7: ticket file has no ledger row (the one-row table under its Status line, T1041)"]);
    expect(ticketRow("T7-x.md", `${ok}\n${ROW_HEADER}\n${ROW_SEP}\n${row("T7", "OPEN")}\n`).problems[0]).toMatch(/T7: ledger row appears more than once/);
    expect(ticketRow("T7-x.md", ok.replace(ROW_SEP, "|--|")).problems[0]).toMatch(/has no "\|---\|---\|---\|---\|---\|" line/);
    expect(ticketRow("T7-x.md", ok.replace(row("T7", "OPEN"), "nothing")).problems[0]).toMatch(/holds no row/);
    expect(ticketRow("T7-x.md", ok.replace(row("T7", "OPEN"), row("T8", "OPEN"))).problems).toEqual(["T7: the row in T7-x.md is T8's: a file holds its own row"]);
  });
});

describe("the ledger views", () => {
  const tickets = [["T10b", "OPEN"], ["T2", "DONE"], ["T10", "DROPPED — no"], ["T9", "POSTPONED"], ["T11", "DONE (in T21)"], ["T1", "IN REVIEW — x"]]
    .map(([id, st]) => ({ name: `${id}-x.md`, text: insertRow(ticket(id!, st!), row(id!, st!, "abc1234")) }));
  it("in id order, each row in the view its status keyword names, once", () => {
    const v = ledgerViews(tickets);
    expect(tableRows(v.ledger).map((r) => r.id)).toEqual(["T1", "T9", "T10b"]);
    expect(tableRows(v.done).map((r) => r.id)).toEqual(["T2", "T10", "T11"]);
    expect([v.open, v.closed]).toEqual([3, 3]);
  });
  it("(T540, by construction) no open status can land in the DONE view: every keyword checked", () => {
    for (const st of ["OPEN", "IN PROGRESS — l", "IN REVIEW — x", "BLOCKED (owner)", "POSTPONED", "PARTIAL — y"]) {
      const v = ledgerViews([{ name: "T5-x.md", text: insertRow(ticket("T5", st), row("T5", st)) }]);
      expect([st, tableRows(v.done).length, tableRows(v.ledger).length]).toEqual([st, 0, 1]);
    }
  });
  it("leaves out a file whose block is wrong (the guard names it), never guesses its row", () => {
    const bad = { name: "T3-x.md", text: ticket("T3", "OPEN.") };
    expect(tableRows(ledgerViews([...tickets, bad]).ledger).map((r) => r.id)).not.toContain("T3");
  });
});

const block = (...lines: string[]) => `# T7 — a ticket\n\nStatus: IN REVIEW — owner: see ## Owner\n\n## Work\n\nw\n\n## Owner\n\n${lines.join("\n")}\n\n## Acceptance\n\nfine\n`;

describe("the owner blocks (T1081)", () => {
  it("reads open and answered lines, a continuation line, and only the block (up to the next ## heading)", () => {
    const got = ownerAsks(block("- [ ] 2026-11-25 — first", "  continued", "- [x] 2026-11-24 — second → \"yes\" (2026-11-25)"), "T7");
    expect(got).toEqual({ problems: [], asks: [
      { open: true, date: "2026-11-25", text: "first continued" },
      { open: false, date: "2026-11-24", text: "second → \"yes\" (2026-11-25)" }] });
    expect(ownerAsks("# T\n\n## Owner answers 2026-11-11\n- [ ] 2026-11-25 — not a block\n", "T7")).toEqual({ asks: [], problems: [] });
  });
  it("names what is wrong: an open line with no date, a bad date, a stray line, a second block", () => {
    expect(ownerAsks(block("- [ ] the ask with no date"), "T7").problems).toEqual(
      ['T7: an open owner line has no date: "the ask with no date" (write "- [ ] YYYY-MM-DD — <ask>")']);
    expect(ownerAsks(block("- [x] 2026-13-45 — month 13"), "T7").problems).toHaveLength(1);
    expect(ownerAsks(block("- [x] 2026-11-25 - hyphen, not a dash"), "T7").problems[0]).toMatch(/an answered owner line has no date/);
    expect(ownerAsks(block("a loose line"), "T7").problems[0]).toMatch(/must read "- \[ \] YYYY-MM-DD — <ask>"/);
    expect(ownerAsks(`${block("- [ ] 2026-11-25 — a")}\n## Owner\n- [ ] 2026-11-25 — b\n`, "T7").problems).toEqual(['T7: more than one "## Owner" block (keep one)']);
  });
  it("OWNER-UNFILED.md: sections of lines; an empty section, a line above the first section and a bad line are named", () => {
    const ok = unfiledAsks("# h\n\nintro\n\n## One\n- [ ] 2026-11-25 — a\n\n## Two\n- [x] 2026-11-20 — b → \"c\" (2026-11-21)\n");
    expect(ok.problems).toEqual([]);
    expect(ok.groups.map((g) => [g.title, g.asks.length])).toEqual([["One", 1], ["Two", 1]]);
    expect(unfiledAsks("## Empty\n\n## Bad\n- [ ] no date\n").problems.map((p) => p.replace(/ \(write.*$/, ""))).toEqual([
      'OWNER-UNFILED.md "Empty": a section holds no owner line', 'OWNER-UNFILED.md "Bad": an open owner line has no date: "no date"']);
    expect(unfiledAsks("- [ ] 2026-11-25 — above\n\n## One\n- [ ] 2026-11-25 — a\n").problems).toEqual(["OWNER-UNFILED.md: an owner line sits above the first \"## <title>\" section"]);
  });
  it("the OWNER view lists open lines only, newest group first and newest line first, one line each, unfiled after a tie", () => {
    const t = (id: string, ...lines: string[]) => ({ name: `${id}-x.md`, text: insertRow(block(...lines).replace("T7", id), row(id, "IN REVIEW — owner: see ## Owner")) });
    const long = "x".repeat(400);
    const v = ownerView([
      t("T2", "- [ ] 2026-11-20 — older ask", "- [ ] 2026-11-23 — newer ask", "- [x] 2026-11-25 — answered, hidden"),
      t("T10", `- [ ] 2026-11-24 — ${long}`),
      t("T3", "- [x] 2026-11-25 — nothing open here"),
      t("T1", "- [ ] 2026-11-23 — tie with T2, lower id"),
    ], "## Waiting on a number\n- [ ] 2026-11-23 — unfiled\n");
    const body = v.text.split("\n").filter((l) => l.startsWith("## ") || l.startsWith("- "));
    expect(body.map((l) => l.slice(0, 30))).toEqual([
      "## T10 — title of T10", `- 2026-11-24 — ${"x".repeat(15)}`,
      "## T1 — title of T1", "- 2026-11-23 — tie with T2, lo",
      "## T2 — title of T2", "- 2026-11-23 — newer ask", "- 2026-11-20 — older ask",
      "## (no ticket yet) Waiting on ", "- 2026-11-23 — unfiled"]);
    expect(body[1]!.length).toBeLessThan(240);                           // one line per ask, cut
    expect([v.asks, v.groups]).toEqual([5, 4]);
  });
  it("--archive refuses a ticket with an open owner line, and takes one whose lines are all answered", () => {
    const closed = (id: string, ...lines: string[]) => ({ name: `${id}-a.md`, text: insertRow(`# ${id} — t\n\nStatus: DONE — abc1234.\n\n## Owner\n\n${lines.join("\n")}\n`, row(id, "DONE", "abc1234")) });
    const open = planArchive({ tickets: [closed("T3", "- [ ] 2026-11-25 — still waiting")], tsv: [], tars: {}, ids: null, year: 2026 });
    expect(open.problems).toEqual(['T3: 1 open owner line(s) in its "## Owner" block ("still waiting"): answer them or take them out, then archive']);
    expect(open.moved).toEqual([]);
    const ok = planArchive({ tickets: [closed("T3", "- [x] 2026-11-25 — done → \"yes\" (2026-11-25)")], tsv: [], tars: {}, ids: null, year: 2026 });
    expect([ok.problems, ok.moved.map((m) => m.id)]).toEqual([[], ["T3"]]);
  });
  it("the CLI builds views/OWNER.md and --check says when it is stale", () => {
    const dir = mkdtempSync(join(tmpdir(), "owner-t1081-"));
    const td = join(dir, T);
    mkdirSync(join(td, "ids"), { recursive: true });
    execFileSync("git", ["-C", dir, "init", "-q"], { env: cleanEnv() });
    writeFileSync(join(td, "T7-a.md"), insertRow(block("- [ ] 2026-11-25 — please look"), row("T7", "IN REVIEW — owner: see ## Owner")));
    writeFileSync(join(td, "ids", "T7"), "T7-a.md\n");
    const run = (...a: string[]) => spawnSync(process.execPath, [SCRIPT, ...a], { cwd: dir, env: cleanEnv(), encoding: "utf8" });
    expect(run("--check").status).toBe(1);                                              // no views yet
    expect(run().status).toBe(0);
    const view = readFileSync(join(dir, "docs/views/OWNER.md"), "utf8");
    expect(view).toMatch(/1 open asks in 1 places/);
    expect(view).toMatch(/## T7 — title of T7\n- 2026-11-25 — please look/);
    expect(run("--check").status).toBe(0);
    writeFileSync(join(td, "T7-a.md"), readFileSync(join(td, "T7-a.md"), "utf8").replace("please look", "please look twice"));
    expect(run("--check").stderr).toMatch(/OWNER\.md is missing or out of date/);
    writeFileSync(join(td, "T7-a.md"), readFileSync(join(td, "T7-a.md"), "utf8").replace("2026-11-25 — please", "no date — please"));
    expect(run().stderr).toMatch(/an open owner line has no date/);
  });
});

describe("the number claims (T1041 review)", () => {
  const t = [{ name: "T5-a.md", text: "" }, { name: "T6-b.md", text: "" }];
  it("every ticket file has its claim naming it, every claim a ticket of its number", () => {
    expect(claimProblems(t, [{ name: "T5", text: "T5-a.md\n" }, { name: "T6", text: "T6-b.md\n" }])).toEqual([]);
    expect(claimProblems(t, [{ name: "T5", text: "T5-a.md\n" }])).toEqual([
      "T6: no number claim tickets/ids/T6 (node app/tools/ledger.mjs --claim writes it): two PRs that take one number must collide there"]);
    expect(claimProblems(t, [{ name: "T5", text: "T5-other.md\n" }, { name: "T6", text: "T6-b.md\n" }, { name: "T7", text: "T7-gone.md\n" }])).toEqual([
      'T5: tickets/ids/T5 claims "T5-other.md", but the ticket file is T5-a.md: one number, one ticket',
      'tickets/ids/T5: claims "T5-other.md", which is not a ticket file of that number',
      'tickets/ids/T7: claims "T7-gone.md", which is not a ticket file of that number']);
  });
});

// ---- T1081: closed tickets live in closed.tsv + one deterministic tar.gz; their files, claims and pictures are gone ----
describe("T1081 the closed index and the archive", () => {
  const done = (id: string, extra = "") => insertRow(ticket(id, "DONE — abc1234.", `Source: x ${extra}\n`), row(id, "DONE", "abc1234", `title ${id}`));
  const names = { T3: "T3-a.md", T4: "T4-b.md", T10: "T10-c.md" };
  it("citeTokens keeps what pipeline/reports.py matches (16 hex, TestFlight id, Play review id) and little else", () => {
    const text = "crash c920e3c59673b7ec, report 4d51dfb38c3ae4e9. sha 0123456789abcdef0123456789abcdef01234567; fb ACeTORpC4_17LbsatW-MECg; gp:AOqpTOGabc_def-1234567890xyz; the-slug-of-a-file-name-here";
    expect(citeTokens(text)).toEqual(["ACeTORpC4_17LbsatW-MECg", "4d51dfb38c3ae4e9", "c920e3c59673b7ec", "gp:AOqpTOGabc_def-1234567890xyz"].sort());
  });
  it("releaseOf reads a version or a release id the row names", () => {
    expect(releaseOf({ status: "DONE", commits: "abc1234 (0.10.0)", left: "—" })).toBe("0.10.0");
    expect(releaseOf({ status: "DONE", commits: "PR #12 r-71d09ad82e81", left: "—" })).toBe("r-71d09ad82e81");
    expect(releaseOf({ status: "DONE", commits: "abc1234 1.0.0.1.2", left: "—" })).toBe("");
  });
  it("the tar is deterministic, lists sorted names, and reads back byte for byte (also with the system tar)", () => {
    const files = [{ name: "T10-c.md", text: "ten ü\n" }, { name: "T3-a.md", text: "x".repeat(1000) }, { name: "T4-b.md", text: "" }];
    const a = gzipTar(files), b = gzipTar([...files].reverse());
    expect(a.equals(b)).toBe(true);
    expect(gunzipTar(a).map((f: { name: string }) => f.name)).toEqual(["T10-c.md", "T3-a.md", "T4-b.md"]);
    expect(gunzipTar(a)).toEqual([...files].sort((p, q) => (p.name < q.name ? -1 : 1)));
    expect(gzipTar(gunzipTar(a)).equals(a)).toBe(true);
    const dir = mkdtempSync(join(tmpdir(), "tar-t1081-"));
    writeFileSync(join(dir, "x.tar.gz"), a);
    expect(execFileSync("tar", ["-tzf", join(dir, "x.tar.gz")], { encoding: "utf8" }).trim().split("\n")).toEqual(["T10-c.md", "T3-a.md", "T4-b.md"]);
    expect(execFileSync("tar", ["-xzOf", join(dir, "x.tar.gz"), "T10-c.md"], { encoding: "utf8" })).toBe("ten ü\n");
    const raw = tarBytes(files);
    expect(raw.subarray(100, 124).toString("latin1")).toBe("0000644\0" + "0000000\0" + "0000000\0");        // mode, owner 0
    expect(raw.subarray(136, 148).toString("latin1")).toBe("00000000000\0");                                // mtime 0
    expect(archiveProblems(files, a)).toEqual([]);
    expect(archiveProblems([{ name: "T3-a.md", text: "x" }, { name: "T9-q.md", text: "" }], a)).toEqual([
      "T3-a.md is not in the archive byte for byte", "T9-q.md is not in the archive byte for byte"]);
    expect(() => tarFiles(Buffer.concat([tarBytes(files).subarray(0, 512).fill(65, 0, 3), Buffer.alloc(2048)]))).toThrow(/checksum/);
    expect(() => tarBytes([{ name: "x".repeat(101), text: "" }])).toThrow(/100 bytes/);
  });
  it("closed.tsv: a line reads back; out-of-order, repeated, short and non-closed lines are named", () => {
    const l = (id: string, status = "DONE") => [id, "t", status, "abc1234", "", "—", "closed-2026", ""].join("\t");
    expect(parseTsv(`${l("T3")}\n${l("T10")}\n`).problems).toEqual([]);
    expect(parseTsv(`${l("T10")}\n${l("T3")}\n`).problems).toEqual([`${CLOSED_TSV}:2: T3 is out of order after T10 (id order, once each)`]);
    expect(parseTsv(`${l("T3")}\n${l("T3")}\n`).problems).toEqual([`${CLOSED_TSV}:2: T3 is listed twice (id order, once each)`]);
    expect(parseTsv(`${l("T3", "OPEN")}\n`).problems[0]).toMatch(/only DONE \/ DROPPED/);
    expect(parseTsv("T3\tx\n").problems[0]).toMatch(/2 fields, 8 wanted/);
    expect(parseTsv(l("T3")).problems[0]).toMatch(/does not end with a newline/);
    expect(parseTsv(`${l("T3")}\n`).rows[0].line).toBe("| T3 | t | DONE | abc1234 | — |");
  });
  it("the DONE view takes the archived rows too, in id order, and a file of the same id wins", () => {
    const tsv = parseTsv(`${tsvLine(ticketRow(names.T3, done("T3")).row, done("T3"), "closed-2026")}\n`).rows;
    const open = { name: "T5-e.md", text: insertRow(ticket("T5", "OPEN."), row("T5", "OPEN")) };
    const v = ledgerViews([open], tsv);
    expect(tableRows(v.done).map((r: { id: string }) => r.id)).toEqual(["T3"]);
    expect(tableRows(v.ledger).map((r: { id: string }) => r.id)).toEqual(["T5"]);
    expect([v.open, v.closed]).toEqual([1, 1]);
  });
  it("planArchive moves the closed tickets only, sorts the lines, appends to the year's tar, and refuses a repeat", () => {
    const tickets = [names.T10, names.T3, names.T4].map((n) => {
      const id = n.split("-")[0]!;
      return { name: n, text: id === "T4" ? insertRow(ticket(id, "OPEN."), row(id, "OPEN")) : done(id, "ACeTORpC4_17LbsatW-MECg") };
    });
    const p = planArchive({ tickets, tsv: [], tars: {}, ids: null, year: 2026 });
    expect(p.problems).toEqual([]);
    expect(p.moved.map((m: { id: string }) => m.id).sort()).toEqual(["T10", "T3"]);
    expect(p.tsvLines.map((l: string) => l.split("\t")[0])).toEqual(["T3", "T10"]);
    expect(p.tsvLines[0]!.split("\t").at(-1)).toBe("ACeTORpC4_17LbsatW-MECg");
    expect(p.tars["closed-2026"]!.map((f: { name: string }) => f.name).sort()).toEqual(["T10-c.md", "T3-a.md"]);
    // a later pass adds to the same tar and the same tsv
    const rows = parseTsv(`${p.tsvLines.join("\n")}\n`).rows;
    const p2 = planArchive({ tickets: [{ name: "T20-z.md", text: done("T20") }], tsv: rows, tars: p.tars, ids: null, year: 2026 });
    expect(p2.tsvLines.map((l: string) => l.split("\t")[0])).toEqual(["T3", "T10", "T20"]);
    expect(p2.tars["closed-2026"]!.length).toBe(3);
    // the same ticket twice, or a named one that is open or missing
    expect(planArchive({ tickets: [tickets[1]!], tsv: rows, tars: p.tars, ids: null, year: 2026 }).problems[0]).toMatch(/T3: already in .*closed.tsv, and the file/);
    expect(planArchive({ tickets, tsv: [], tars: {}, ids: ["T4", "T99"], year: 2026 }).problems).toEqual([
      "T4: OPEN, not DONE / DROPPED: it is not archived", "T99: no such closed ticket file"]);
  });
  it("claimProblems: a claim or a file of an archived number fails, so a number is never used twice", () => {
    expect(claimProblems([], [{ name: "T9", text: "T9-a.md\n" }], new Set(["T9"]))).toEqual([
      "tickets/ids/T9: T9 is in tickets/closed.tsv (archived): its number is kept there, delete the claim"]);
    expect(claimProblems([{ name: "T9-a.md", text: "" }], [{ name: "T9", text: "T9-a.md\n" }], new Set(["T9"]))).toHaveLength(2);
    expect(claimProblems([], [], new Set(["T9"]))).toEqual([]);
  });
  it("nextNumber is one past the highest number anywhere (closed ids, claims, files, other branches' claims)", () => {
    expect(nextNumber(["T3", "T10-x.md", "T1141", "tickets/ids/T1150", "T15b-y.md"])).toBe(1151);
    expect(nextNumber([])).toBe(1);
  });
  it("assetIds: picture and data folders of a ticket, a range, never a ticket file", () => {
    expect(["T12-shots", "T41-polish-shots", "T41-walk", "T623-pending", "T563-566-shots", "T321-wording-baseline.txt", "T15b-files", "T1-x.md", "icons-shots", "T-1"].map(assetIds)).toEqual([
      ["T12"], ["T41"], ["T41"], ["T623"], ["T563", "T564", "T565", "T566"], ["T321"], ["T15b"], null, null, null]);
  });
  it("the real archive holds exactly the closed.tsv tickets: each file's row and tokens are what its line says", () => {
    const rows = readWorkTree(ROOT).closed as { id: string; title: string; status: string; commits: string; archive: string; tokens: string }[];
    expect(rows.length).toBeGreaterThan(800);
    const files = new Map<string, string>();
    for (const a of new Set(rows.map((r) => r.archive))) for (const f of gunzipTar(readFileSync(join(ROOT, archivePath(a)))) as { name: string; text: string }[]) files.set(f.name, f.text);
    expect(files.size).toBe(rows.length);
    for (const r of rows) {
      const [name, text] = [...files].find(([n]) => n.startsWith(`${r.id}-`)) ?? [];
      expect(name, r.id).toBeTruthy();
      const got = ticketRow(name!, text!).row!;
      expect([got.title, got.status, got.commits], r.id).toEqual([r.title, r.status, r.commits]);
      expect(citeTokens(text!).join(" "), r.id).toBe(r.tokens);
    }
  });
  it("--archive through the CLI: files, claims and picture folders go, the rest stays, a second run does nothing, the guard-read views build", () => {
    const dir = mkdtempSync(join(tmpdir(), "archive-t1081-"));
    const git = (...a: string[]) => execFileSync("git", ["-C", dir, ...a], { encoding: "utf8", env: cleanEnv() });
    git("init", "-q");
    const td = join(dir, T);
    mkdirSync(join(td, "ids"), { recursive: true });
    for (const [id, n, text] of [["T3", names.T3, done("T3")], ["T4", names.T4, insertRow(ticket("T4", "OPEN."), row("T4", "OPEN"))], ["T10", names.T10, done("T10")]] as const) {
      writeFileSync(join(td, n), text); writeFileSync(join(td, "ids", id), `${n}\n`);
    }
    for (const d of ["T3-shots", "T4-shots", "T10-files", "T11-shots"]) { mkdirSync(join(dir, "docs/app-integration", d), { recursive: true }); writeFileSync(join(dir, "docs/app-integration", d, "a.png"), "x"); }
    const run = (...a: string[]) => spawnSync(process.execPath, [SCRIPT, ...a], { cwd: dir, env: cleanEnv(), encoding: "utf8" });
    const dry = run("--archive", "--dry");
    expect([dry.status, dry.stdout]).toEqual([0, expect.stringMatching(/2 ticket files, 2 claims, 2 picture\/data folders/)]);
    expect(existsSync(join(td, names.T3))).toBe(true);                                      // a dry run deletes nothing
    const r = run("--archive");
    expect([r.status, r.stderr]).toEqual([0, ""]);
    expect(readdirSync(td).sort()).toEqual(["T4-b.md", "archive", "closed.tsv", "ids"]);
    expect(readdirSync(join(td, "ids"))).toEqual(["T4"]);
    expect(readdirSync(join(dir, "docs/app-integration")).sort()).toEqual(["T11-shots", "T4-shots"]);
    const year = new Date().getFullYear();
    expect(gunzipTar(readFileSync(join(dir, archivePath(`closed-${year}`)))).map((f: { name: string }) => f.name)).toEqual(["T10-c.md", "T3-a.md"]);
    expect(readFileSync(join(td, "closed.tsv"), "utf8").split("\n").map((l) => l.split("\t")[0])).toEqual(["T3", "T10", ""]);
    const bytes = readFileSync(join(dir, archivePath(`closed-${year}`)));
    const again = run("--archive");
    expect([again.status, again.stdout]).toEqual([0, expect.stringMatching(/0 ticket files/)]);
    expect(readFileSync(join(dir, archivePath(`closed-${year}`))).equals(bytes)).toBe(true);
    expect(run("--check").stdout).toMatch(/views current .*1 rows in work, 2 closed \(2 archived\)/);
    expect(run("--claim").stdout).toMatch(/0 written/);
    writeFileSync(join(td, "T3-again.md"), done("T3"));                                       // an archived number used again
    const bad = run("--claim");
    expect([bad.status, bad.stderr]).toEqual([1, expect.stringMatching(/T3: T3-again\.md is still here but T3 is in .*closed\.tsv/)]);
    expect(existsSync(join(td, "ids", "T3"))).toBe(false);                                   // and it wrote no claim for it
    expect(run("--next").stdout.trim()).toBe("T11");
    rmSync(join(td, "T3-again.md"));
    // a folder a running test reads is kept, and said
    mkdirSync(join(td, "T281-files")); writeFileSync(join(td, "T281-files", "sub_lake_areas.json"), "{}");
    writeFileSync(join(td, "T281-lake.md"), done("T281"));
    const kept = run("--archive");
    expect(kept.stdout).toMatch(/1 ticket files, 0 claims, 0 picture\/data folders; kept: 1/);
    expect(existsSync(join(td, "T281-files", "sub_lake_areas.json"))).toBe(true);
    // (review B1) every closed ticket's folder a surviving file still cites is in KEPT_ASSETS, so a real --archive spares it
    for (const k of Object.keys(KEPT_ASSETS)) expect(KEPT_READERS[k]?.length, k).toBeGreaterThan(0);
    mkdirSync(join(dir, "docs/app-integration/T244-shots"), { recursive: true }); writeFileSync(join(dir, "docs/app-integration/T244-shots/icons-preview.png"), "x");
    writeFileSync(join(td, "T244-icon.md"), done("T244"));
    const spared = run("--archive");
    expect(spared.stdout).toMatch(/0 picture\/data folders; kept: 2/);
    expect(existsSync(join(dir, "docs/app-integration/T244-shots/icons-preview.png"))).toBe(true);
  });
});
