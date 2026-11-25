/**
 * (T1041) ONE FILE PER LEDGER ROW AND PER OWNER-TODO ITEM: `tools/ledger.mjs`.
 *
 *   1. the row block: put under the Status paragraph, taken out again byte for byte, set in place; what is wrong with one;
 *   2. the views: id order, and a row's view follows its status keyword (T540's "DONE.md holds only DONE / DROPPED",
 *      now by construction);
 *   3. the owner to-do: split into items and built back, non-blank lines identical; what is wrong with an item file;
 *   4. the import and its round trip, on made-up files (each loss the round trip must catch, caught) and on the REAL
 *      files as they stood when the import was written (commit `MIGRATION_BASE`: 983 rows, 74 owner sections);
 *   5. the carry (`--resolve`) of an old-style side's edits, pure and through git in a made-up repository, both ways.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
// A plain .mjs tool (it runs under node), imported for its functions.
import {
  HEAD_FILE, LEDGER_POINTER, OLD_DONE, OWNER_DIR, OWNER_PAGE, README, ROW_HEADER, ROW_SEP, carry, gitMerge3, importOld,
  insertRow, itemFile, ledgerViews, ownerTodoText, readOwnerItems, roundTrip, setRow, splitOwnerTodo, stripRow, tableRows,
  ticketRow, treeAt, claimPath, claimProblems, staleViews, readWorkTree,
  CLOSED_TSV, archivePath, assetIds, archiveProblems, archiveProblems, citeTokens, gunzipTar, gzipTar, nextNumber, parseTsv, planArchive, releaseOf, tarBytes, tarFiles, tsvLine,
} from "./ledger.mjs";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const SCRIPT = fileURLToPath(new URL("./ledger.mjs", import.meta.url));
/** The main commit the import was written against: its README.md / DONE.md / OWNER-TODO.md are the real old files. */
const MIGRATION_BASE = "be4df3d6a";
/** A merge needs a committer even with --no-commit; a CI runner has none configured (the made-up repositories below). */
const WHO = ["-c", "user.email=t@t", "-c", "user.name=t", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"];
const T = "handoff/PLAN/app-integration/tickets";
const cleanEnv = () => Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("GIT_")));

const row = (id: string, status: string, commits = "—", title = `title of ${id}`) => `| ${id} | ${title} | ${status} | ${commits} | — |`;
const ticket = (id: string, status: string, more = "Source: x.\n\n## Work\n\nsome work\n") => `# ${id} — a ticket\n\nStatus: ${status}\n\n${more}`;
const oldLedger = (title: string, rows: string[]) => `# ${title}\n\nintro\n\n## Ledger\n\n${ROW_HEADER}\n${ROW_SEP}\n${rows.join("\n")}\n\n## Owner questions\n\nq1\n`;

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

const PAGE = [
  "# Owner to-do", "", "head line `handoff/PLAN/x.md`", "", "",
  "## 2026-11-24: newest (verbatim)", "", "- a", "  continued", "- b", "", "",
  "## 2026-11-23: no blank before the next heading", "text", "1. one", "",
  "### a sub-heading inside an item", "- c", "   ",
  "## Decisions (undated)", "", "| table | row |", "|---|---|", "| x | y |", "",
].join("\n");

describe("the owner to-do", () => {
  it("splits into a head and one item per ## section, and builds back with every non-blank line in place", () => {
    const { head, items } = splitOwnerTodo(PAGE);
    expect(head).toBe("# Owner to-do\n\nhead line `handoff/PLAN/x.md`");
    expect(items.map((i) => i.heading)).toEqual(["## 2026-11-24: newest (verbatim)", "## 2026-11-23: no blank before the next heading", "## Decisions (undated)"]);
    const files = [{ name: HEAD_FILE, text: `${head}\n` }, ...items.map((it, k) => ({ name: `i${k}.md`, text: itemFile((items.length - k) * 10, it.text) }))];
    const back = readOwnerItems(files);
    expect(back.problems).toEqual([]);
    const nonBlank = (t: string) => t.split("\n").filter((l) => l.trim() !== "");
    expect(nonBlank(ownerTodoText(back.head, back.items))).toEqual(nonBlank(PAGE));
  });
  it("orders items highest first, a tie by file name", () => {
    const files = [{ name: HEAD_FILE, text: "# h\n" }, { name: "b.md", text: itemFile(5, "## B") }, { name: "a.md", text: itemFile(5, "## A") },
      { name: "c.md", text: itemFile(7.5, "## C") }, { name: "d.md", text: itemFile(-10, "## D") }];
    const o = readOwnerItems(files);
    expect(ownerTodoText(o.head, o.items)).toBe("# h\n\n## C\n\n## A\n\n## B\n\n## D\n");
  });
  it("names what is wrong with an item file: the order line, the heading, two items, a heading used twice, no head", () => {
    const got = readOwnerItems([
      { name: "a.md", text: "## A\n" }, { name: "b.md", text: "order: 1\n\ntext first\n## B\n" },
      { name: "c.md", text: "order: 1\n\n## C\n\n## D\n" }, { name: "e.md", text: itemFile(1, "## E") }, { name: "f.md", text: itemFile(2, "## E") },
      { name: "README.md", text: "how it works\n" },
    ]).problems;
    expect(got).toEqual([
      `${OWNER_DIR}/_head.md is missing: the owner to-do's head (title and how the page works)`,
      `${OWNER_DIR}/a.md: the first line must be "order: <number>" and the second blank`,
      `${OWNER_DIR}/b.md: the item must start with its "## " heading after the order line`,
      `${OWNER_DIR}/c.md: holds more than one item (## D): one file per item`,
      `${OWNER_DIR}: "## E" is the heading of e.md and f.md: a heading names one item`,
    ]);
  });
});

describe("the number claims and conflict markers (T1041 review)", () => {
  const t = [{ name: "T5-a.md", text: "" }, { name: "T6-b.md", text: "" }];
  it("every ticket file has its claim naming it, every claim a ticket of its number", () => {
    expect(claimProblems(t, [{ name: "T5", text: "T5-a.md\n" }, { name: "T6", text: "T6-b.md\n" }])).toEqual([]);
    expect(claimProblems(t, [{ name: "T5", text: "T5-a.md\n" }])).toEqual([
      "T6: no number claim handoff/PLAN/app-integration/tickets/ids/T6 (node app/tools/ledger.mjs --claim writes it): two PRs that take one number must collide there"]);
    expect(claimProblems(t, [{ name: "T5", text: "T5-other.md\n" }, { name: "T6", text: "T6-b.md\n" }, { name: "T7", text: "T7-gone.md\n" }])).toEqual([
      'T5: handoff/PLAN/app-integration/tickets/ids/T5 claims "T5-other.md", but the ticket file is T5-a.md: one number, one ticket',
      'handoff/PLAN/app-integration/tickets/ids/T5: claims "T5-other.md", which is not a ticket file of that number',
      'handoff/PLAN/app-integration/tickets/ids/T7: claims "T7-gone.md", which is not a ticket file of that number']);
  });
  it("an owner item or the head holding a conflict marker line is refused", () => {
    const got = readOwnerItems([{ name: HEAD_FILE, text: "# h\n>>>>>>> old-style side\n" }, { name: "a.md", text: itemFile(1, "## A\n<<<<<<< new-style side") }]).problems;
    expect(got).toEqual([`${OWNER_DIR}/_head.md: holds a merge-conflict marker line (<<<<<<< / >>>>>>>): settle it`,
      `${OWNER_DIR}/a.md: holds a merge-conflict marker line (<<<<<<< / >>>>>>>): settle it`]);
  });
});

const OLD = {
  tickets: [ticket("T1", "OPEN."), ticket("T2", "DONE — abc1234"), ticket("T3", "DROPPED — not needed"), ticket("T10", "IN PROGRESS — lane")]
    .map((text, k) => ({ name: `${["T1", "T2", "T3", "T10"][k]}-x.md`, text })),
  readme: oldLedger("Tickets", [row("T10", "IN PROGRESS — lane"), row("T1", "OPEN"), "", row("T3", "DROPPED — not needed")]),
  done: oldLedger("Closed", [row("T2", "DONE", "abc1234")]),
  ownerTodo: PAGE,
};

describe("the import and its round trip (made-up files)", () => {
  const out = importOld(OLD);
  it("splits everything and the round trip holds, with its counts", () => {
    expect(out.problems).toEqual([]);
    const rt = roundTrip(OLD, out);
    expect(rt.problems).toEqual([]);
    expect(rt.counts).toMatchObject({ "rows in README.md": 3, "rows in DONE.md": 1, "ticket files": 4, "rows in the LEDGER view": 2,
      "rows in the DONE view": 2, "closed rows that sat in README.md (now in the DONE view)": 1, "owner sections in": 3, "owner item files out": 3 });
    expect(out.readme).toContain(LEDGER_POINTER.trim());
    expect(tableRows(out.readme)).toEqual([]);
    expect(out.readme).toMatch(/\n\n## Owner questions\n\nq1\n$/);
    expect(out.ownerFiles.map((f) => f.name)).toEqual([HEAD_FILE, "2026-11-24-newest-verbatim.md",
      "2026-11-23-no-blank-before-the-next-heading.md", "decisions-undated.md"]);
  });
  it("catches each loss (mutation): a row dropped, a row doubled, a ticket text changed, an owner line lost", () => {
    const t1 = out.tickets.find((t) => t.name === "T1-x.md")!;
    const without = { ...out, tickets: out.tickets.map((t) => (t === t1 ? { ...t, text: stripRow(t.text) } : t)) };
    expect(roundTrip(OLD, without).problems).toEqual([`row dropped: ${row("T1", "OPEN")}`]);
    const doubled = { ...OLD, done: oldLedger("Closed", [row("T2", "DONE", "abc1234"), row("T2", "DONE", "abc1234")]) };
    expect(roundTrip(doubled, out).problems).toEqual([`row dropped: ${row("T2", "DONE", "abc1234")}`]);
    const extra = { ...out, tickets: [...out.tickets, { name: "T4-x.md", text: insertRow(ticket("T4", "OPEN."), row("T4", "OPEN")) }] };
    expect(roundTrip(OLD, extra).problems).toContain(`row added or duplicated: ${row("T4", "OPEN")}`);
    const edited = { ...out, tickets: out.tickets.map((t) => (t === t1 ? { ...t, text: t.text.replace("some work", "other work") } : t)) };
    expect(roundTrip(OLD, edited).problems).toEqual(["T1-x.md: its text changed beyond the row block"]);
    const lost = { ...out, ownerFiles: out.ownerFiles.map((f) => ({ ...f, text: f.text.replace("  continued\n", "") })) };
    expect(roundTrip(OLD, lost).problems[0]).toMatch(/^owner to-do differs at non-blank line 5: "  continued" became "- b"/);
  });
  it("refuses an old ledger it cannot split cleanly", () => {
    expect(importOld({ ...OLD, done: oldLedger("Closed", [row("T2", "DONE", "abc1234"), row("T1", "OPEN")]) }).problems)
      .toEqual(["T1: two old ledger rows (README.md and DONE.md): settle them before importing"]);
    expect(importOld({ ...OLD, tickets: OLD.tickets.filter((t) => t.name !== "T3-x.md") }).problems).toEqual(["T3: old ledger row with no ticket file"]);
    expect(importOld({ ...OLD, readme: OLD.readme.replace(row("T1", "OPEN"), "") }).problems).toEqual(["T1: T1-x.md has no row in the old ledger"]);
    expect(importOld({ ...OLD, tickets: out.tickets }).problems).toContain("T1: T1-x.md already holds a row table (imported before?)");
  });
});

describe("the import and its round trip on the REAL files as they stood (commit MIGRATION_BASE)", () => {
  it("every row and every owner line comes through; nothing dropped, nothing doubled", () => {
    const tree = treeAt(ROOT, MIGRATION_BASE);                       // a missing commit or file throws: never "no rows"
    const show = (p: string) => { const t = tree.get(p); if (t === undefined) throw new Error(`${MIGRATION_BASE}:${p} not read`); return t; };
    const tickets = [...tree.keys()].filter((p) => p.startsWith(`${T}/T`)).sort().map((p) => ({ name: p.slice(T.length + 1), text: show(p) }));
    const old = { tickets, readme: show(README), done: show(OLD_DONE), ownerTodo: show(OWNER_PAGE) };
    const out = importOld(old);
    expect(out.problems).toEqual([]);
    const rt = roundTrip(old, out);
    expect(rt.problems).toEqual([]);
    expect(rt.counts).toEqual({
      "rows in README.md": 244, "rows in DONE.md": 739, "ticket files": 983, "rows in the LEDGER view": 162, "rows in the DONE view": 821,
      "closed rows that sat in README.md (now in the DONE view)": 82, "owner sections in": 74, "owner item files out": 74,
      "owner non-blank lines in": 3340, "owner non-blank lines out": 3340 });
  });
});

// ------------------------------------------------------------------------------------------------------------ carry

type Tree = Map<string, string>;
const oldTree = (o: typeof OLD): Tree => new Map([...o.tickets.map((t) => [`${T}/${t.name}`, t.text] as [string, string]),
  [README, o.readme], [OLD_DONE, o.done], [OWNER_PAGE, o.ownerTodo]]);
const newTree = (o: typeof OLD): Tree => {
  const out = importOld(o);
  return new Map([...out.tickets.map((t) => [`${T}/${t.name}`, t.text] as [string, string]), [README, out.readme], [OWNER_PAGE, out.ownerPage],
    ...out.ownerFiles.map((f) => [`${OWNER_DIR}/${f.name}`, f.text] as [string, string]),
    ...out.claims.map((c) => [claimPath(c.name), c.text] as [string, string])]);
};
/** OLD as an old-style branch edited it: T1 closed (Status + row), T4 added, an owner item added on top, one edited,
 *  one removed, README's owner questions edited */
const BRANCH = {
  tickets: [...OLD.tickets.map((t) => (t.name === "T1-x.md" ? { ...t, text: t.text.replace("Status: OPEN.", "Status: DONE — fff0001") } : t)),
    { name: "T4-x.md", text: ticket("T4", "OPEN.") }],
  readme: oldLedger("Tickets", [row("T10", "IN PROGRESS — lane"), row("T1", "DONE", "fff0001"), "", row("T3", "DROPPED — not needed"), row("T4", "OPEN")])
    .replace("q1", "q1\nq2 (asked on the branch)"),
  done: OLD.done,
  ownerTodo: PAGE.replace("## 2026-11-24: newest (verbatim)", "## 2026-11-25: from the branch\n\n- new\n\n## 2026-11-24: newest (verbatim)")
    .replace("- b\n", "- b\n- b2 (the branch)\n").replace(/## Decisions \(undated\)[\s\S]*$/, ""),
};

describe("the carry of an old-style side (pure)", () => {
  const base = oldTree(OLD), neu = newTree(OLD), old = oldTree(BRANCH);
  const res = carry({ base, old, neu }, gitMerge3);
  const at = (p: string) => res.write.get(p) ?? neu.get(p)!;
  it("rows, ticket text, new tickets, owner items and README text come across; nothing else is touched", () => {
    expect(res.conflicts).toEqual([]);
    expect(ticketRow("T1-x.md", at(`${T}/T1-x.md`)).row?.line).toBe(row("T1", "DONE", "fff0001"));
    expect(at(`${T}/T1-x.md`)).toMatch(/^Status: DONE — fff0001$/m);
    expect(ticketRow("T4-x.md", at(`${T}/T4-x.md`)).row?.line).toBe(row("T4", "OPEN"));
    expect([...res.write.keys()].filter((p) => p.startsWith(T)).sort()).toEqual([`${T}/README.md`, `${T}/T1-x.md`, `${T}/T4-x.md`, `${T}/ids/T4`]);
    expect(res.write.get(claimPath("T4"))).toBe("T4-x.md\n");                 // the added ticket claims its number
    expect(res.remove).toEqual(new Set([OLD_DONE, `${OWNER_DIR}/decisions-undated.md`]));
    expect(at(README)).toContain("q2 (asked on the branch)");
    expect(tableRows(at(README))).toEqual([]);
    const items = readOwnerItems([...new Map([...neu, ...res.write])].filter(([p]) => p.startsWith(OWNER_DIR) && !res.remove.has(p))
      .map(([p, text]) => ({ name: p.slice(OWNER_DIR.length + 1), text })));
    expect(items.problems).toEqual([]);
    expect(ownerTodoText(items.head, items.items)).toBe(BRANCH.ownerTodo.replace(/\n+$/, "\n").replace(/\n{3,}/g, "\n\n").replace("   \n", ""));
    expect(res.carried).toEqual({ "ticket files": 2, rows: 2, "owner items": 3, "README text": 1 });
  });
  it("a row both sides changed, differently, is a conflict with both rows in the file", () => {
    const neu2 = new Map(neu);
    neu2.set(`${T}/T1-x.md`, setRow(neu.get(`${T}/T1-x.md`)!, row("T1", "IN PROGRESS — main's lane")));
    const r = carry({ base, old, neu: neu2 }, gitMerge3);
    expect(r.conflicts.map((c: { path: string | null }) => c.path)).toEqual([`${T}/T1-x.md`]);
    expect(r.write.get(`${T}/T1-x.md`)).toContain(`<<<<<<< new-style side\n${row("T1", "IN PROGRESS — main's lane")}\n=======\n${row("T1", "DONE", "fff0001")}`);
  });
  it("a row only the new side changed stays the new side's; an old side that changed nothing writes nothing", () => {
    const neu2 = new Map(neu);
    neu2.set(`${T}/T10-x.md`, setRow(neu.get(`${T}/T10-x.md`)!, row("T10", "DONE", "abc9999")));
    expect(carry({ base, old, neu: neu2 }, gitMerge3).write.has(`${T}/T10-x.md`)).toBe(false);
    const none = carry({ base, old: base, neu }, gitMerge3);
    expect([none.write.size, [...none.remove], none.conflicts]).toEqual([0, [OLD_DONE], []]);
  });
});

describe("the carry: nothing vanishes (each case a conflict with a FILE)", () => {
  const base = oldTree(OLD), neu = newTree(OLD);
  const item = (tree: Tree, heading: string) => [...tree].find(([p, t]) => p.startsWith(OWNER_DIR) && t.includes(`\n${heading}`));
  const NEWEST = "## 2026-11-24: newest (verbatim)";
  it("an owner item edited on the old side and removed on the new side comes back with markers", () => {
    const neu2 = new Map(neu); neu2.delete(item(neu, NEWEST)![0]);
    const old = oldTree({ ...OLD, ownerTodo: OLD.ownerTodo.replace("- a\n", "- a (edited on the release)\n") });
    const r = carry({ base, old, neu: neu2 }, gitMerge3);
    expect(r.conflicts.map((c: { path: string }) => c.path)).toEqual([`${OWNER_DIR}/2026-11-24-newest-verbatim.md`]);
    expect(r.write.get(`${OWNER_DIR}/2026-11-24-newest-verbatim.md`)).toMatch(/<<<<<<< new-style side\n\(removed on the new-style side\)\n=======\n## 2026-11-24: newest \(verbatim\)\n\n- a \(edited on the release\)/);
  });
  it("an owner item removed on the old side and edited on the new side keeps its file, with markers", () => {
    const [p, t] = item(neu, NEWEST)!;
    const neu2 = new Map(neu); neu2.set(p, t.replace("- a\n", "- a (edited on main)\n"));
    const old = oldTree({ ...OLD, ownerTodo: OLD.ownerTodo.replace(/## 2026-11-24: newest[\s\S]*?(?=## 2026-11-23)/, "") });
    const r = carry({ base, old, neu: neu2 }, gitMerge3);
    expect(r.conflicts.map((c: { path: string }) => c.path)).toEqual([p]);
    expect(r.remove.has(p)).toBe(false);
    expect(r.write.get(p)).toContain("- a (edited on main)");
    expect(r.write.get(p)).toMatch(/\n=======\n\(removed on the old-style side\)\n>>>>>>> old-style side\n$/);
  });
  it("a row changed on the old side whose file is gone (renamed) on the new side is written back with markers", () => {
    const neu2 = new Map(neu); neu2.set(`${T}/T1-renamed.md`, neu.get(`${T}/T1-x.md`)!); neu2.delete(`${T}/T1-x.md`);
    const old = oldTree({ ...OLD, readme: OLD.readme.replace(row("T1", "OPEN"), row("T1", "IN PROGRESS — the release")) });
    const r = carry({ base, old, neu: neu2 }, gitMerge3);
    expect(r.conflicts.map((c: { path: string }) => c.path)).toEqual([`${T}/T1-x.md`]);
    expect(r.write.get(`${T}/T1-x.md`)).toContain(row("T1", "IN PROGRESS — the release"));
    expect(r.write.get(`${T}/T1-x.md`)).toMatch(/^<<<<<<< new-style side\n\(this file is gone on the new-style side/);
  });
  it("a ticket the old side removes goes, with its claim (no conflict)", () => {
    const old = oldTree({ ...OLD, tickets: OLD.tickets.filter((t) => t.name !== "T3-x.md"), readme: OLD.readme.replace(row("T3", "DROPPED — not needed"), "") });
    const r = carry({ base, old, neu }, gitMerge3);
    expect([r.conflicts, r.remove.has(`${T}/T3-x.md`), r.remove.has(claimPath("T3"))]).toEqual([[], true, true]);
  });
  it("an old-side section under a heading already used is carried as a second item, and is a conflict", () => {
    const old = oldTree({ ...OLD, ownerTodo: `${OLD.ownerTodo.trimEnd()}\n\n${NEWEST}\n\n- a second one, from the release\n` });
    const r = carry({ base, old, neu }, gitMerge3);
    const [p] = r.conflicts.map((c: { path: string }) => c.path);
    expect(r.write.get(p)).toContain("- a second one, from the release");
    expect(r.conflicts[0].why).toMatch(/adds a second item headed/);
  });
  it("a heading both sides renamed is a conflict, never two silent copies", () => {
    const [p, t] = item(neu, NEWEST)!;
    const neu2 = new Map(neu); neu2.delete(p); neu2.set(`${OWNER_DIR}/main-name.md`, t.replace(NEWEST, "## 2026-11-24: newest, as main calls it"));
    const old = oldTree({ ...OLD, ownerTodo: OLD.ownerTodo.replace(NEWEST, "## 2026-11-24: newest, as the release calls it") });
    const r = carry({ base, old, neu: neu2 }, gitMerge3);
    expect(r.conflicts.map((c: { why: string }) => c.why)).toEqual([expect.stringMatching(/renamed "## 2026-11-24: newest \(verbatim\)" to "## 2026-11-24: newest, as the release calls it"; the new-style side removed or renamed it too/)]);
  });
  it("an old-side item added on top goes ABOVE the current top, a number no item holds", () => {
    const neu2 = new Map(neu); neu2.set(`${OWNER_DIR}/main-top.md`, itemFile(40, "## 2026-11-26: newer on main"));
    const old = oldTree({ ...OLD, ownerTodo: OLD.ownerTodo.replace(NEWEST, `## 2026-11-25: from the release\n\n- r\n\n${NEWEST}`) });
    const r = carry({ base, old, neu: neu2 }, gitMerge3);
    const added = [...r.write].find(([, t]) => t.includes("from the release"))!;
    expect(added[1]).toMatch(/^order: 50\n/);
  });
  it("an old-side ticket whose number the new side already took is a conflict on the claim", () => {
    const neu2 = new Map(neu); neu2.set(`${T}/T4-main.md`, insertRow(ticket("T4", "OPEN."), row("T4", "OPEN"))); neu2.set(claimPath("T4"), "T4-main.md\n");
    const r = carry({ base, old: oldTree(BRANCH), neu: neu2 }, gitMerge3);
    expect(r.conflicts.map((c: { path: string }) => c.path)).toEqual([claimPath("T4")]);
    expect(r.write.get(claimPath("T4"))).toBe("<<<<<<< new-style side\nT4-main.md\n=======\nT4-x.md\n>>>>>>> old-style side");
  });
});

describe("--resolve through git, both ways (a made-up repository)", () => {
  const write = (dir: string, tree: Tree) => { for (const [p, text] of tree) { mkdirSync(dirname(join(dir, p)), { recursive: true }); writeFileSync(join(dir, p), text); } };
  const repo = () => {
    const dir = mkdtempSync(join(tmpdir(), "ledger-resolve-"));
    const git = (...a: string[]) => execFileSync("git", ["-c", "user.email=t@t", "-c", "user.name=t", "-c", "init.defaultBranch=main",
      "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", ...a], { cwd: dir, env: cleanEnv(), encoding: "utf8", stdio: "pipe" }).trim();
    git("init", "-q");
    write(dir, oldTree(OLD)); git("add", "-A"); git("commit", "-q", "-m", "base");
    git("checkout", "-q", "-b", "old");
    write(dir, oldTree(BRANCH)); git("add", "-A"); git("commit", "-q", "-m", "old-style edits");
    git("checkout", "-q", "main");
    git("rm", "-q", OLD_DONE); write(dir, newTree(OLD)); git("add", "-A"); git("commit", "-q", "-m", "import");
    return { dir, git };
  };
  const resolve = (dir: string) => spawnSync(process.execPath, [SCRIPT, "--resolve"], { cwd: dir, env: cleanEnv(), encoding: "utf8" });
  const settled = (dir: string, git: (...a: string[]) => string) => {
    expect(git("diff", "--name-only", "--diff-filter=U")).toBe("");
    expect(readdirSync(join(dir, T)).sort()).toEqual(["README.md", "T1-x.md", "T10-x.md", "T2-x.md", "T3-x.md", "T4-x.md", "ids"]);
    expect(readdirSync(join(dir, T, "ids")).sort()).toEqual(["T1", "T10", "T2", "T3", "T4"]);
    expect(staleViews(dir)).toEqual([]);
    expect(ticketRow("T1-x.md", readFileSync(join(dir, T, "T1-x.md"), "utf8")).row?.status).toBe("DONE");
    expect(ticketRow("T4-x.md", readFileSync(join(dir, T, "T4-x.md"), "utf8")).row?.status).toBe("OPEN");
    expect(readFileSync(join(dir, README), "utf8")).toContain("q2 (asked on the branch)");
    expect(readFileSync(join(dir, OWNER_PAGE), "utf8")).not.toMatch(/^## /m);
    expect(readFileSync(join(dir, "handoff/PLAN/views/OWNER-TODO.md"), "utf8")).toContain("## 2026-11-25: from the branch");
    expect(readFileSync(join(dir, "handoff/PLAN/views/DONE.md"), "utf8")).toContain(row("T1", "DONE", "fff0001"));
  };
  it("the new-style branch merges an old-style one (a newer main, before this lands)", () => {
    const { dir, git } = repo();
    spawnSync("git", [...WHO, "merge", "--no-ff", "--no-commit", "old"], { cwd: dir, env: cleanEnv() });
    const r = resolve(dir);
    expect([r.status, r.stderr]).toEqual([0, ""]);
    expect(r.stdout).toMatch(/carried from the old-style side \(the branch being merged in\): 2 ticket files, 2 rows, 3 owner items, 1 README text; 2 removed/);
    settled(dir, git);
    const t2 = join(dir, T, "T2-x.md");
    writeFileSync(t2, setRow(readFileSync(t2, "utf8"), row("T2", "DONE", "abc1234", "a title edited after the views were built")));
    expect(staleViews(dir)).toEqual(["handoff/PLAN/views/DONE.md"]);                    // self-declaring: --check / the guard's warning
  });
  it("an old-style branch merges the new-style main (release/0.9.0, any open PR)", () => {
    const { dir, git } = repo();
    git("checkout", "-q", "old");
    spawnSync("git", [...WHO, "merge", "--no-ff", "--no-commit", "main"], { cwd: dir, env: cleanEnv() });
    const r = resolve(dir);
    expect([r.status, r.stderr]).toEqual([0, ""]);
    expect(r.stdout).toMatch(/carried from the old-style side \(this branch\)/);
    settled(dir, git);
  });
  it("a conflict it cannot settle stays unmerged in git's index, so a commit refuses it", () => {
    const { dir, git } = repo();
    git("checkout", "-q", "-b", "main2", "main");
    const p = `${T}/T1-x.md`;
    writeFileSync(join(dir, p), setRow(readFileSync(join(dir, p), "utf8"), row("T1", "IN PROGRESS — main's lane")));
    git("commit", "-q", "-am", "main moves T1");
    spawnSync("git", [...WHO, "merge", "--no-ff", "--no-commit", "old"], { cwd: dir, env: cleanEnv() });
    const r = resolve(dir);
    expect(r.status).toBe(1);
    expect(r.stderr).toContain(`${p}: both sides changed it`);
    expect(git("diff", "--name-only", "--diff-filter=U")).toBe(p);
    expect(() => git("commit", "--no-edit")).toThrow(/unmerged|not possible/i);
  });
  it("an item the release edits and main removes stays UNMERGED: a plain commit refuses (no silent loss)", () => {
    const { dir, git } = repo();
    git("checkout", "-q", "-b", "main2", "main");
    git("rm", "-q", `${OWNER_DIR}/2026-11-23-no-blank-before-the-next-heading.md`); git("commit", "-q", "-m", "main drops an item");
    git("checkout", "-q", "old");
    writeFileSync(join(dir, OWNER_PAGE), readFileSync(join(dir, OWNER_PAGE), "utf8").replace("text\n", "text, edited on the release\n"));
    git("commit", "-q", "-am", "the release edits the item");
    spawnSync("git", [...WHO, "merge", "--no-ff", "--no-commit", "main2"], { cwd: dir, env: cleanEnv() });
    const r = resolve(dir);
    expect(r.status).toBe(1);
    expect(r.stderr).toMatch(/edited on the old-style side and removed on the new-style side/);
    expect(git("diff", "--name-only", "--diff-filter=U")).toBe(`${OWNER_DIR}/2026-11-23-no-blank-before-the-next-heading.md`);
    expect(() => git("commit", "--no-edit")).toThrow(/unmerged|not possible/i);
  });
  it("(claims) two new-style branches that take one ticket number COLLIDE in git; different numbers merge", () => {
    const { dir, git } = repo();
    const add = (branch: string, name: string) => {
      git("checkout", "-q", "-b", branch, "main");
      writeFileSync(join(dir, T, name), insertRow(ticket(idOf(name), "OPEN."), row(idOf(name), "OPEN")));
      writeFileSync(join(dir, claimPath(idOf(name))), `${name}\n`);
      git("add", "-A"); git("commit", "-q", "-m", `add ${name}`);
    };
    const idOf = (n: string) => n.split("-")[0]!;
    add("a", "T50-from-a.md"); add("b", "T50-from-b.md"); add("c", "T51-from-c.md");
    git("checkout", "-q", "main"); git("merge", "-q", "--no-ff", "a", "-m", "merge a");
    const b = spawnSync("git", [...WHO, "merge", "--no-ff", "b", "-m", "merge b"], { cwd: dir, env: cleanEnv(), encoding: "utf8" });
    expect([b.status, b.stdout]).toEqual([1, expect.stringMatching(/CONFLICT \(add\/add\): Merge conflict in handoff\/PLAN\/app-integration\/tickets\/ids\/T50/)]);
    git("merge", "--abort");
    git("merge", "-q", "--no-ff", "c", "-m", "merge c");                                   // another number: clean
    expect(claimProblems(readWorkTree(dir).tickets, readWorkTree(dir).claims)).toEqual([]);
  });
  it("outside a merge it refuses, saying how to start one", () => {
    const { dir } = repo();
    const r = resolve(dir);
    expect([r.status, r.stderr]).toEqual([1, expect.stringMatching(/no merge in progress \(MERGE_HEAD\)/)]);
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
      "handoff/PLAN/app-integration/tickets/ids/T9: T9 is in handoff/PLAN/app-integration/tickets/closed.tsv (archived): its number is kept there, delete the claim"]);
    expect(claimProblems([{ name: "T9-a.md", text: "" }], [{ name: "T9", text: "T9-a.md\n" }], new Set(["T9"]))).toHaveLength(2);
    expect(claimProblems([], [], new Set(["T9"]))).toEqual([]);
  });
  it("nextNumber is one past the highest number anywhere (closed ids, claims, files, other branches' claims)", () => {
    expect(nextNumber(["T3", "T10-x.md", "T1141", "handoff/PLAN/app-integration/tickets/ids/T1150", "T15b-y.md"])).toBe(1151);
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
    mkdirSync(join(dir, "handoff/PLAN/owner-todo"), { recursive: true });
    writeFileSync(join(dir, "handoff/PLAN/owner-todo", HEAD_FILE), "# head\n");
    for (const [id, n, text] of [["T3", names.T3, done("T3")], ["T4", names.T4, insertRow(ticket("T4", "OPEN."), row("T4", "OPEN"))], ["T10", names.T10, done("T10")]] as const) {
      writeFileSync(join(td, n), text); writeFileSync(join(td, "ids", id), `${n}\n`);
    }
    for (const d of ["T3-shots", "T4-shots", "T10-files", "T11-shots"]) { mkdirSync(join(dir, "handoff/PLAN/app-integration", d), { recursive: true }); writeFileSync(join(dir, "handoff/PLAN/app-integration", d, "a.png"), "x"); }
    const run = (...a: string[]) => spawnSync(process.execPath, [SCRIPT, ...a], { cwd: dir, env: cleanEnv(), encoding: "utf8" });
    const dry = run("--archive", "--dry");
    expect([dry.status, dry.stdout]).toEqual([0, expect.stringMatching(/2 ticket files, 2 claims, 2 picture\/data folders/)]);
    expect(existsSync(join(td, names.T3))).toBe(true);                                      // a dry run deletes nothing
    const r = run("--archive");
    expect([r.status, r.stderr]).toEqual([0, ""]);
    expect(readdirSync(td).sort()).toEqual(["T4-b.md", "archive", "closed.tsv", "ids"]);
    expect(readdirSync(join(td, "ids"))).toEqual(["T4"]);
    expect(readdirSync(join(dir, "handoff/PLAN/app-integration")).sort()).toEqual(["T11-shots", "T4-shots", "tickets"]);
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
  });
});
