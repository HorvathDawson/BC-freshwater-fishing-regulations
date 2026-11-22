#!/usr/bin/env node
/**
 * (T998) EVERY COMMIT MESSAGE FOLLOWS CONVENTIONAL COMMITS: commitlint with the repo's config
 * (`tools/commitlint.config.mjs`: the types of `tools/versioning.json`, a ticket id in the header has a ledger row,
 * header at most `headerMax`). The standard three layers, one command:
 *
 *   node app/tools/check-commits.mjs --edit <file>         the commit-msg hook (.githooks/commit-msg): the message git
 *                                                          is about to commit (its `#` comment lines and everything
 *                                                          under the scissors line left out, as git does)
 *   node app/tools/check-commits.mjs --range <a>..<b> ...  before_push.py and docs-ci's `commits` job: every commit in
 *                                                          the range (repeatable)
 *   node app/tools/check-commits.mjs --title <t> --number <n>   docs-ci: the PR title AS ITS MERGE COMMIT (repo setting
 *                                                          merge_commit_title=PR_TITLE: `<title> (#<n>)`)
 *
 * HISTORY BEFORE THE CUTOVER IS EXEMPT, by commit id (`versioning.json` `cutover`, and each `exempt` commit, with their
 * ancestors): a range never lists them. Exit 0 all pass, 1 a message is refused (each one named with every problem),
 * 2 the check could not run (no app/node_modules, a git failure, bad arguments) — never read as a pass.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const here = fileURLToPath(new URL(".", import.meta.url));
const SCISSORS = /^# -+ >8 -+$/m;

/** The message git commits from an edited message file: comment lines out, nothing under the scissors line. */
export function cleanMessage(text, commentChar = "#") {
  const cut = text.search(SCISSORS);
  const kept = (cut >= 0 ? text.slice(0, cut) : text).split("\n").filter((l) => !l.startsWith(commentChar));
  return kept.join("\n").trim();
}

/** The PR title as GitHub writes its merge commit's subject (merge_commit_title=PR_TITLE). */
export function asMergeSubject(title, number) {
  return number ? `${title.trim()} (#${number})` : title.trim();
}

let loaded;
/** commitlint's lint function and the repo config, loaded once. A missing install is said with its fix (exit 2). */
async function commitlint() {
  if (loaded) return loaded;
  let lint, load;
  try {
    ({ default: lint } = await import("@commitlint/lint"));
    ({ default: load } = await import("@commitlint/load"));
  } catch (e) {
    if (e?.code !== "ERR_MODULE_NOT_FOUND") throw e;
    const err = new Error(`commitlint is not installed in this checkout (${e.message.split("\n")[0]}): ` +
      "pnpm -C app install --frozen-lockfile --prefer-offline");
    err.exitCode = 2;
    throw err;
  }
  const config = await load({}, { file: "commitlint.config.mjs", cwd: here });
  const opts = { parserOpts: config.parserPreset?.parserOpts ?? {}, plugins: config.plugins ?? {},
                 ignores: config.ignores ?? [], defaultIgnores: config.defaultIgnores ?? true, helpUrl: config.helpUrl };
  loaded = { lint: (message) => lint(message, config.rules, opts) };
  return loaded;
}

/** One message's verdict: { valid, errors: string[], warnings: string[] } (an ignored message — a merge — is valid). */
export async function lintMessage(message) {
  const { lint } = await commitlint();
  const r = await lint(message);
  const say = (x) => `${x.message} [${x.name}]`;
  return { valid: r.valid, errors: r.errors.map(say), warnings: r.warnings.map(say) };
}

/** git's comment character (`core.commentChar`; unset or `auto`: `#`). git failing otherwise is an error. */
function commentChar() {
  const got = spawnSync("git", ["config", "core.commentChar"], { encoding: "utf8" });
  if (got.status === 1) return "#";                               // git config: 1 = the key is not set
  if (got.status !== 0) throw Object.assign(new Error(`git config core.commentChar: status ${got.status} ${got.stderr ?? ""}`.trim()), { exitCode: 2 });
  const c = got.stdout.trim();
  return !c || c === "auto" ? "#" : c;
}

const git = (args, cwd) => execFileSync("git", args, { cwd, encoding: "utf8", maxBuffer: 256 << 20 });

/** (T998) The commits of `a..b` the check applies to, oldest first: [{ sha, message }]. The cutover, every exempt
 *  commit and their ancestors are left out (`versioning.json`). */
export function commitsIn(range, cwd, versioning) {
  if (!/^\S+\.\.\S+$/.test(range) || range.includes("...")) throw Object.assign(new Error(`--range wants <a>..<b>, not ${range}`), { exitCode: 2 });
  const not = [versioning.cutover, ...Object.keys(versioning.exempt ?? {})];
  const out = git(["log", "--reverse", "--format=%H%x00%B%x1e", range, "--not", ...not], cwd);
  return out.split("\x1e").map((x) => x.replace(/^\n/, "")).filter(Boolean).map((x) => {
    const [sha, message] = x.split("\0");
    return { sha, message: message.trim() };
  });
}

/** Each refused message as text (empty: all pass) and the warnings, for `items` = [{ label, message }]. */
export async function checkAll(items) {
  const refused = [], warned = [];
  for (const { label, message } of items) {
    const v = await lintMessage(message);
    const head = `${label}: ${message.split("\n")[0]}`;
    if (!v.valid) refused.push(`${head}\n      ` + v.errors.join("\n      "));
    else if (v.warnings.length) warned.push(`${head}\n      ` + v.warnings.join("\n      "));
  }
  return { refused, warned };
}

const FORMAT = "`<type>(<scope>)!: <subject>` — types: feat fix perf (bump the app) | docs chore ci test refactor build " +
  "style (no bump); the scope is the ticket id when there is one (`fix(T983): …`); `!` or a `BREAKING CHANGE:` footer " +
  "is breaking (AGENTS W48)";

async function main(argv) {
  const items = [];
  const versioning = JSON.parse(readFileSync(new URL("versioning.json", import.meta.url), "utf8"));
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i], v = argv[i + 1];
    if (a === "--edit" && v) {
      const message = cleanMessage(readFileSync(v, "utf8"), commentChar());
      if (!message) return 0;                                     // git itself refuses an empty message
      items.push({ label: "this commit", message });
      i++;
    } else if (a === "--range" && v) {
      for (const c of commitsIn(v, process.cwd(), versioning)) items.push({ label: c.sha.slice(0, 12), message: c.message });
      i++;
    } else if (a === "--title" && v !== undefined) {
      const n = argv[i + 2] === "--number" ? argv[i + 3] : "";
      if (!v.trim()) throw Object.assign(new Error("--title is empty"), { exitCode: 2 });
      items.push({ label: `the PR title${n ? ` (its merge commit, #${n})` : ""}`, message: asMergeSubject(v, n) });
      i += n ? 3 : 1;
    } else {
      throw Object.assign(new Error(`usage: check-commits.mjs --edit FILE | --range A..B ... | --title T [--number N] (got ${a})`), { exitCode: 2 });
    }
  }
  const { refused, warned } = await checkAll(items);
  for (const w of warned) console.warn(`commits: warning: ${w}`);
  if (refused.length) {
    console.error(`✗ ${refused.length} of ${items.length} message(s) do not follow Conventional Commits (T998):\n  ` +
      refused.join("\n  ") + `\n\n  The format: ${FORMAT}.`);
    return 1;
  }
  console.log(`✓ commits: ${items.length} message(s) follow Conventional Commits` +
    (items.length ? "" : " (none to check: the range is empty or before the cutover)"));
  return 0;
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isMain) {
  main(process.argv.slice(2)).then((rc) => process.exit(rc), (e) => {
    console.error(`commits: the check could not run: ${e.message}`);
    process.exit(e.exitCode ?? 2);
  });
}
