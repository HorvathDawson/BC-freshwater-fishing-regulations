/**
 * (T998) THE COMMIT FORMAT CHECK: `tools/check-commits.mjs` with `tools/commitlint.config.mjs`.
 *
 *   1. the ticket's acceptance: `T994 fix x` is refused, `fix(T994): x` passes, `fix(T9999): x` is refused (no ledger
 *      row) — by the function, by the CLI, and by the real commit-msg hook on a message file;
 *   2. the shared contract (`tools/commits-contract.json`): commitlint's parser and ignore list read every message as
 *      the release command's bump does (pipeline/tests/test_release_version.py reads the same file);
 *   3. git's and GitHub's merge subjects pass; a PR title is checked as its merge commit (`<title> (#N)`);
 *   4. a range leaves out the cutover and the exempt history, by commit id.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import isIgnored from "@commitlint/is-ignored";
import load from "@commitlint/load";
import parse from "@commitlint/parse";
import { describe, expect, it } from "vitest";
// A plain .mjs gate (the hook and CI run it under node), imported for its functions.
import { asMergeSubject, cleanMessage, commitsIn, lintMessage } from "./check-commits.mjs";

const tools = fileURLToPath(new URL(".", import.meta.url));
const ROOT = fileURLToPath(new URL("../..", import.meta.url));
const SCRIPT = join(tools, "check-commits.mjs");
const contract = JSON.parse(readFileSync(join(tools, "commits-contract.json"), "utf8")) as {
  messages: { message: string; type: string | null; scope: string | null; breaking: boolean; ignored: boolean }[] };
const versioning = JSON.parse(readFileSync(join(tools, "versioning.json"), "utf8"));
/** git without the caller's repository variables (a hook's GIT_DIR must not point a temp repo at this one) */
const cleanEnv = () => Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("GIT_")));

describe("the acceptance messages (T998)", () => {
  it("refuses `T994 fix x`, passes `fix(T994): x`, refuses `fix(T9999): x` naming the ledger", async () => {
    expect((await lintMessage("T994 fix x")).valid).toBe(false);
    expect(await lintMessage("fix(T994): x")).toEqual({ valid: true, errors: [], warnings: [] });
    const bad = await lintMessage("fix(T9999): x");
    expect(bad.valid).toBe(false);
    expect(bad.errors).toEqual(["T9999 has no ledger row (the row table in its file, tickets/Tnnn-*.md, T1041): " +
      "add the ticket file and its row first, or fix the id [tickets-in-ledger]"]);
    // a ticket named in the subject counts too (the ledger guard reads subjects the same way)
    expect((await lintMessage("docs(handoff): T9999 notes")).errors.join()).toContain("T9999 has no ledger row");
    expect((await lintMessage("docs(handoff): Q-T9999 is an owner question, not a ticket")).valid).toBe(true);
  });

  it("allows only the ticket's types, and a header of at most headerMax", async () => {
    for (const t of Object.keys(versioning.bump)) expect((await lintMessage(`${t}: x`)).valid, t).toBe(true);
    expect((await lintMessage("revert: x")).errors.join()).toContain("[type-enum]");
    expect((await lintMessage("feature: x")).errors.join()).toContain("[type-enum]");
    const long = "fix(T994): " + "x".repeat(versioning.headerMax - 11);
    expect(long.length).toBe(versioning.headerMax);
    expect((await lintMessage(long)).valid).toBe(true);
    expect((await lintMessage(long + "x")).errors.join()).toContain("[header-max-length]");
    // subjects start with code names and acronyms; bodies wrap at the repo's 120 and hold URLs
    expect((await lintMessage("fix(T995): CI status finds runs by commit")).valid).toBe(true);
    expect((await lintMessage("fix: x\n\n" + "y".repeat(160))).valid).toBe(true);
  });

  it("passes git's and GitHub's merge subjects", async () => {
    for (const m of ["Merge pull request #12 from HorvathDawson/feat/T998-versioning",
                     "Merge branch 'main' into feat/T998-versioning", "Merge remote-tracking branch 'origin/main' into x"])
      expect((await lintMessage(m)).valid, m).toBe(true);
  });

  it("checks a PR title as the merge commit GitHub writes from it", async () => {
    expect(asMergeSubject(" fix(T994): x ", "12")).toBe("fix(T994): x (#12)");
    const title = "fix(T994): " + "x".repeat(versioning.headerMax - 11 - 6);
    expect((await lintMessage(asMergeSubject(title, "12"))).valid).toBe(true);
    expect((await lintMessage(asMergeSubject(title + "x", "12"))).valid).toBe(false);
  });
});

describe("the contract with the release command's bump", () => {
  it("commitlint reads every contract message as the contract says", async () => {
    const config = await load({}, { file: "commitlint.config.mjs", cwd: tools });
    expect(contract.messages.length).toBeGreaterThan(20);
    for (const c of contract.messages) {
      const p = await parse(c.message, undefined, config.parserPreset!.parserOpts as object);
      const breaking = p.notes.some((n) => /^BREAKING[ -]CHANGE$/i.test(n.title));
      expect({ type: p.type, scope: p.scope, breaking, ignored: isIgnored(c.message.trimEnd()) }, c.message)
        .toEqual({ type: c.type, scope: c.scope, breaking: c.breaking, ignored: c.ignored });
    }
  });
});

describe("the edited message and the range", () => {
  it("leaves out git's comment lines and everything under the scissors line", () => {
    const text = "fix(T994): x\n# Please enter the commit message\n\nbody\n# ------------------------ >8 ------------------------\ndiff --git a/x b/x\n";
    expect(cleanMessage(text)).toBe("fix(T994): x\n\nbody");
    expect(cleanMessage("; c\nfix: x\n", ";")).toBe("fix: x");
  });

  it("a range never lists the cutover, an exempt commit or their ancestors", () => {
    const dir = mkdtempSync(join(tmpdir(), "check-commits-"));
    const git = (...a: string[]) => execFileSync("git", ["-c", "user.email=t@t", "-c", "user.name=t", "-c",
      "init.defaultBranch=main", "-c", "commit.gpgsign=false", ...a], { cwd: dir, env: cleanEnv(), encoding: "utf8" }).trim();
    git("init", "-q");
    const commit = (m: string) => { git("commit", "-q", "--allow-empty", "--no-verify", "-m", m); return git("rev-parse", "HEAD"); };
    const root = commit("free-form history");
    const cut = commit("docs(T998): the cutover");
    git("checkout", "-q", "--orphan", "old");
    const old = commit("the old main, free-form");
    git("checkout", "-q", "main");
    git("merge", "-q", "--no-verify", "--allow-unrelated-histories", "-m", "Merge old into main", "old");
    const after = commit("fix(T994): after the cutover");
    const v = { cutover: cut, exempt: { [old]: "why" } };
    const run = (range: string) => {
      const prev = process.cwd();
      process.chdir(dir);
      try { return commitsIn(range, dir, v).map((c: { message: string }) => c.message); } finally { process.chdir(prev); }
    };
    expect(run(`${root}..HEAD`)).toEqual(["Merge old into main", "fix(T994): after the cutover"]);
    expect(run(`${cut}..${after}`)).toEqual(["Merge old into main", "fix(T994): after the cutover"]);
    // mutation: without the exemptions the free-form history is listed
    expect(commitsIn(`${root}..HEAD`, dir, { cutover: root, exempt: {} }).map((c: { message: string }) => c.message))
      .toContain("the old main, free-form");
    expect(() => commitsIn(`${root}...HEAD`, dir, v)).toThrow(/--range wants <a>..<b>/);
  });
});

describe("the CLI and the commit-msg hook", () => {
  const msgFile = (text: string) => {
    const f = join(mkdtempSync(join(tmpdir(), "commit-msg-")), "COMMIT_EDITMSG");
    writeFileSync(f, text);
    return f;
  };
  const node = (...a: string[]) => spawnSync(process.execPath, [SCRIPT, ...a], { cwd: ROOT, env: cleanEnv(), encoding: "utf8" });
  const hook = (f: string) => spawnSync("sh", [join(ROOT, ".githooks", "commit-msg"), f], { cwd: ROOT,
    env: { ...cleanEnv(), GIT_DIR: join(ROOT, ".git-not-here") }, encoding: "utf8" });

  it("the CLI refuses with exit 1 and names each problem, passes with 0, and cannot-run is 2", () => {
    const bad = node("--title", "T994 fix x");
    expect(bad.status).toBe(1);
    expect(bad.stderr).toContain("the PR title: T994 fix x");
    expect(bad.stderr).toContain("type may not be empty [type-empty]");
    expect(node("--title", "fix(T994): x", "--number", "8").status).toBe(0);
    // (T1144) a PR title at the limit of its merge commit: the contract pipeline/tests/title_check_stand_in.py keeps
    for (const [number, limit] of [["266", versioning.headerMax - 7], ["1000", versioning.headerMax - 8]] as const) {
      const at = "ci(T1144): " + "x".repeat(limit - 11);
      expect(node("--title", at, "--number", number).status).toBe(0);
      const over = node("--title", at + "x", "--number", number);
      expect(over.status).toBe(1);
      expect(over.stderr).toContain("header-max-length");
    }
    expect(node("--range", "nope").status).toBe(2);
    expect(node("--bogus").status).toBe(2);
  });

  it("the hook refuses `T994 fix x` and `fix(T9999): x`, passes `fix(T994): x` (git's repository variables dropped)", () => {
    const bad = hook(msgFile("T994 fix x\n# comment\n"));
    expect(bad.status, bad.stderr).toBe(1);
    expect(bad.stderr).toContain("this commit: T994 fix x");
    expect(hook(msgFile("fix(T9999): x\n")).stderr).toContain("T9999 has no ledger row");
    const ok = hook(msgFile("fix(T994): x\n\n# Please enter the commit message\n"));
    expect(ok.status, ok.stderr).toBe(0);
  });
});
