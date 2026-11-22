/**
 * (T998) THE COMMIT FORMAT: Conventional Commits 1.0.0, `<type>(<scope>)!: <subject>`, checked by commitlint
 * (`@commitlint/config-conventional`, pinned in app/package.json) with this repo's rules on top. One config for the
 * three places that check: the commit-msg hook (.githooks/commit-msg), before_push.py, and docs-ci's `commits` job, all
 * through `tools/check-commits.mjs`.
 *
 * - The types and what each bumps are `tools/versioning.json` `bump` (the release command reads the same file).
 * - The scope is optional; it is the ticket id when there is one (`fix(T983): …`). A ticket id in the HEADER (scope or
 *   subject, the ticket guard's own `T_IN_SUBJECT` rule) must have a ledger row: `fix(T9999): x` is refused at commit
 *   time, not later by the ledger guard (`tools/check-tickets.mjs`, whose loader this reuses).
 * - The header (and a PR title with its ` (#N)`, the merge commit GitHub writes from it) is at most `headerMax`.
 * - Off, each with why: subject-case (subjects start with a code name or an acronym: `CI status …`, `MapLibre …`);
 *   body / footer line length (the repo wraps prose at 120, and a URL or a command does not wrap).
 * - Ignored as commitlint does by default: git's and GitHub's merge subjects (`Merge pull request #N from …`, `Merge
 *   branch …`, `Merge remote-tracking branch …`), `Revert "…"`, `fixup!` / `squash!` / `amend!`.
 */
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import conventional from "@commitlint/config-conventional";
import { ledgerIds, ticketsIn } from "./check-tickets.mjs";

const here = new URL(".", import.meta.url);
/** (T998) tools/versioning.json: the types, the header length, the cutover. */
export const VERSIONING = JSON.parse(readFileSync(new URL("versioning.json", here), "utf8"));
const repoRoot = fileURLToPath(new URL("../..", import.meta.url));

let ledger;
/** The ledger's ids, read once per process (the ledger of THIS checkout: a lane's new ticket counts). */
const inLedger = (id) => (ledger ??= ledgerIds(repoRoot)).has(id);

/** (T998) A ticket id in the header (scope or subject) has a ledger row. */
function ticketsInLedger(parsed) {
  const named = [...new Set(ticketsIn(parsed.header ?? ""))];
  const missing = named.filter((id) => !inLedger(id));
  return [!missing.length, `${missing.join(", ")} ${missing.length > 1 ? "have" : "has"} no ledger row ` +
    "(handoff/PLAN/app-integration/tickets/README.md or DONE.md): add the ticket file and its row first, or fix the id"];
}

/** @public — `@commitlint/load` reads it by file path (tools/check-commits.mjs), which knip cannot see. */
export default {
  parserPreset: conventional.parserPreset,
  plugins: [{ rules: { "tickets-in-ledger": ticketsInLedger } }],
  rules: {
    ...conventional.rules,
    "type-enum": [2, "always", Object.keys(VERSIONING.bump)],
    "header-max-length": [2, "always", VERSIONING.headerMax],
    "subject-case": [0],
    "body-max-line-length": [0],
    "footer-max-line-length": [0],
    "tickets-in-ledger": [2, "always"],
  },
};
