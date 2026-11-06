# T334 audit — tests and docs (finder report, 2026-11-05)

Read-only. Method: AST scan of every pytest file (assert-free tests, self-comparing asserts, skip sites), regex scan of
every vitest file (skip/only, env-guarded skips, assert-free `it`), the allowlist in `test_no_silent_skips.py` re-counted
against the tree, `needs_*` markers checked against what each test reads, ticket file `Status:` vs ledger row, every
`python -m …` / script / backticked path in the docs checked for existence, `git log` for ticket mentions.
Scripts used are beside this file (`scan.py`, `dups.py`, `tickets.py`, `docscan.py`). Ranked most harmful first.

## Findings

1. **The app's golden-parity suites pass green on zero cases, and nothing ever makes them strict** · silent-skip / gap /
   conflict · `app/conformance/src/regs/golden.ts:284-294` (`cases()` yields nothing and only `console.warn`s when there
   is no paired store), `app/conformance/src/regs/standing.parity.test.ts:107` (`if (!values.length && !none) return;`),
   `app/conformance/src/fixtureRelease.ts:96`, `app/conformance/src/regs/blob.release.test.ts:27` ·
   Evidence: every `*.parity.test.ts` iterates `cases()`; with no store the `it` body runs zero iterations and passes.
   The one strictness switch is `REGS_STORE_REQUIRED=1`, and a repo-wide grep finds it only in app sources and
   `app/MAP.md:70` — not in `scripts/test-full.sh` (its "app check" step is a plain `pnpm -C app check`), not in
   `.github/workflows/app-ci.yml`, not in `scripts/heavy.sh`. `app/MAP.md:70` says "the strict run goes through
   `scripts/heavy.sh`" — there is no such run. So Tier 2 never proves the rules → bindings → bundle → app-answer chain
   on this machine unless a human types the variable; AGENTS 62 "NO SILENT SKIPS" is pipeline-only. ·
   Fix: `scripts/test-full.sh` exports `REGS_STORE_REQUIRED=1` for the app step when a release is on disk (it already
   computes `REL`); `cases()` throws on zero cases unless an explicit `REGS_STORE_OPTIONAL=1`; one app test (mirror of
   `test_no_silent_skips.py`) lists every env-guarded skip with its reason. · confidence: high.

2. **18 env-guarded skips in the app suite with no allowlist, no count, no reason check** · skip / conflict ·
   `app/tools/{search-golden,overzoom,one-part-one-page,release-perf,regs-fixture,handover,release-boot,
   release-placeable,one-formula,release-budget,fish-looks-shots,adipose-shots}.test.ts`,
   `app/workers/feeds/test/gauges.parity.test.ts:16` (`describe.skipIf(!existsSync(PY))`) · Evidence: grep for
   `skipIf(` — 18 sites; `pnpm check` reports them as skipped and exits 0. The pipeline refuses exactly this
   (`pipeline/tests/test_no_silent_skips.py` ALLOWLIST with exact counts). · Fix: `app/tools/no-silent-skips.test.ts`
   scanning for `skipIf|describe.skip|it.skip|xit|todo` with an exact-count allowlist and a planted-skip mutation test,
   as the pipeline's. · confidence: high.

3. **Two `describe.skip` suites waiting on a decision that was made (T350 DONE 11-03)** · skip-left-behind / dead code ·
   `app/workers/feeds/test/dfo.parity.test.ts:32`, `app/workers/feeds/test/dfo.corpus.test.ts:67` · Evidence: both
   comments say "Byte parity is suspended until T350 decides the worker's live path"; ledger row 394 has T350 DONE
   (4fb701fb7), T225 DROPPED → T412 "DONE in code"; owner 11-02: cascade resolved in Python, worker gets pre-resolved
   results. `const hasPython` is now unused in both files. `app/workers/feeds/parity/dfo/{harness.py,typed_dump.py,
   time.mjs}` and `test/dfo.scenario.ts` (`runPy`/`runTs`) exist only for these suites. · Fix: delete the two suites
   and `parity/dfo/` (or re-scope to "the worker serves the Python's pre-resolved output byte-for-byte" if T412's
   contract wants a parity). · confidence: high.

4. **Feeds worker: the cron → job dispatcher and the run-timing job have no test** · gap ·
   `app/workers/feeds/src/index.ts:570-579` (`scheduled()` picks the job by the cron string's minute field),
   `index.ts:443` (`runTimingCheck`), `wrangler.toml:66-73` · Evidence: grep for `runTimingCheck` in `test/` = 0;
   no test calls `.scheduled(`; 8 tests import `../src/index` but for `noDataStations`/`run`. A minute-field typo
   routes "dfo" to "gauges" and nothing fails. · Fix: a test feeding every `wrangler.toml` cron string through
   `scheduled()` with stubbed jobs, asserting which ran (read the toml, so a new cron needs a line); a
   `runTimingCheck` test with a stubbed fetch and clock. · confidence: high.

5. **Ticket file `Status:` disagrees with its ledger row (12 tickets)** · stale-ticket ·
   `handoff/PLAN/app-integration/tickets/{T200,T201,T218,T219,T223}-*.md` say OPEN, ledger says POSTPONED;
   T234 PARTIAL vs POSTPONED; T319 IN PROGRESS vs POSTPONED; **T322 PARTIAL vs ledger DONE**; **T390 PARTIAL vs ledger
   DONE**; T333/T334/T335 status lines read "IN PROGRESS 2026-11-05 … POSTPONED" (two keywords, ledger POSTPONED). ·
   Evidence: `tickets.py` output; `app/tools/check-tickets.mjs:15-16` says it deliberately does not compare the two.
   The ledger is "the sole source" (owner 10-22) and now says DONE for two tickets whose files list work left. ·
   Fix: one alignment pass; then make `check-tickets.mjs` compare the keywords (the lane closes file + row in the same
   commit — the README already says so at "Closing a ticket"). · confidence: high.

6. **"IN REVIEW" is a status the guard accepts and the README never defines** · conflict ·
   `app/tools/check-tickets.mjs:32` (`STATUSES` includes "IN REVIEW"), `tickets/README.md:24-31` (seven statuses, no IN
   REVIEW); used by T252, T295, T297, T305, T306, T315, T354. · Fix: define it in the README ("merged, proof pending —
   name the proof") or rename those seven to PARTIAL. · confidence: high.

7. **The smoke set covers 205 of 385 test files; the "one per test file" promise has no gate** · gap ·
   `pipeline/tests/smoke.txt:1-2` ("One test per test file"), `conftest.py:22-31` (root: refuses only a listed id that
   no longer exists). · Evidence: distinct files in smoke.txt = 205; `test_*.py` under the two testpaths = 385. A new
   test file is never asked for a smoke id. · Fix: the root conftest also refuses a collected test file with no smoke
   id (allowlist: files whose every test carries a `needs_*` marker), with a planted-file mutation test. ·
   confidence: high.

8. **AGENTS 18 and `app-integration/tests.md` describe a `check.mjs` that no longer exists** · dead-doc ·
   `AGENTS.md:139` ("check.mjs `ONLY=smoke`"), `handoff/PLAN/app-integration/tests.md:1-30` (built around
   `app/design/regs-v11/tools/check.mjs`, `tools/cdp.mjs`, `tools/shoot.mjs`, `tools/compare-regs.mjs`). · Evidence:
   no `check.mjs`, `cdp.mjs`, `shoot.mjs`, `compare-regs.mjs`, `snap.mjs` under `app/tools`; `app/design/` deleted in
   T87 (app/MAP.md:52); `scripts/test-smoke.sh` runs two steps, neither is check.mjs. · Fix: drop the clause from
   rule 18; delete tests.md (its live half is `golden.ts` + `exclusions.ts`, already documented in app/MAP.md). ·
   confidence: high.

9. **AGENTS 17 names a side build that does not exist and a workflow the build command replaced** · dead-doc ·
   `AGENTS.md:131-133` ("`data/generated/atlas/full` and `data/generated/atlas/full_new` already exist"; "~18 min and
   ~9 GB"). · Evidence: `data/generated/atlas/full_new` is absent; MAP.md and README say a side atlas is
   `pipeline build --atlas` into `<build>_next`, `--side DIR` for everything else (rule 62); README says peak memory
   12 GB, rule 39 says 10 GB. · Fix: rewrite 17 as "never rebuild to test: `pipeline build --atlas`/`--side`, then
   `build_parity.py`", one memory figure, in MAP.md only. · confidence: high.

10. **AGENTS 16 asks for a parity re-run it names no command for, and admits the number is stale** · stale rule ·
    `AGENTS.md:117-122` ("3,038 of 3,038 … has not been re-run since; re-run it before relying on it"). · Evidence:
    no test or tool named; the review app now builds through the T422 sandbox (`conftest.py:84-105`) and
    `entry_reaches` → `classify` is the only path. · Fix: either name the check (a `needs_reach_run` test that runs
    `entry_reaches` over the live run and compares bindings) and record its last run, or delete the rule as enforced by
    construction. · confidence: medium.

11. **The golden oracle is a deleted prototype's snapshot with 14 permanent carve-outs** · tautological-golden ·
    `app/fixtures/golden/golden.json` (frozen since T87, app/MAP.md:46), `app/conformance/src/regs/exclusions.ts`
    (19 `until:` entries, 14 read "never …"). · Evidence: the prototype that produced golden.json was deleted 10-28;
    every disagreement since is resolved by adding an exclusion, never by re-cutting, so the pin shrinks toward what
    already passes. `sync.mjs` re-cuts only hydrate/taps. · Fix: decide once — re-cut golden.json from the pipeline's
    answers (then the "never" exclusions go) or freeze it formally and refuse new exclusions without a ticket id. ·
    confidence: medium.

12. **Two determinism tests compare a function to itself in one process** · test-can't-fail (weak) ·
    `pipeline/tests/test_dfo_salmon.py:2428` (`build(scraped)` twice), `pipeline/tests/test_length_splits.py:121`
    (`junction_cuts` twice). · Evidence: same PYTHONHASHSEED both calls — set-order nondeterminism, the failure the
    docstrings name, is invisible; `test_prune.py::test_the_braid_prune_does_not_depend_on_the_hash_seed` shows the
    right shape (subprocess, different seeds). · Fix: second call in a subprocess with another seed, or compare to a
    pinned digest. · confidence: medium-high.

13. **Gate scanners with no planted-violation proof** · gap ·
    `pipeline/tests/test_one_fish_list_wording.py`, `test_one_rank_table.py`, `test_answers_28_gate_classes.py`,
    `test_no_slivers.py` (0 mutation mentions); `test_no_missing_file_fallbacks.py` (its one "mutation" at :72 is
    about an edited rule, not the fallback scanner); `test_one_acceptance_gate.py`, `test_one_name_caser.py` (a
    docstring mention only). · Evidence: grep counts; compare `test_no_silent_skips.py` (9 planted forms) and
    `test_dataflow_gates.py` (20). Memory "checks seeded from input launder" is exactly this class. · Fix: one
    planted violation per scanner in `tmp_path`. · confidence: medium (heads read, not every body).

14. **Data tests whose scope depends on the machine, without a marker** · marker / gap ·
    `pipeline/tests/test_dfo_quota.py:157-158` (globs `cache/dfo_salmon/**`, gitignored, no `needs_cache`: in CI the
    corpus is the fixtures only, locally it is hundreds of pages — same test, two meanings);
    `test_dfo_suggest.py:202,324`, `test_tidal_triage.py:16`, `test_licence_records.py:19` read
    `data/generated/review/**` with no marker — correct only because those files are committed exceptions, and only
    the gauge queue has a pin that they stay committed (`test_check_curated.py:140-149`). · Fix: split the cache half
    of `_corpus()` into a `needs_cache` test; extend the `git check-ignore` pin to every `data/generated` file a no-data
    test reads. · confidence: medium.

15. **Write-through: the "splits" proposal domain has no test and AGENTS 67 says split marks are refused** · gap /
    conflict · `pipeline/regs/review/apply.py:41` (`PROPOSAL_DOMAINS = ("dfo", "entries", "splits")`),
    `pipeline/regs/review/export.py:42` (`"splits": ("split",)`), `pipeline/tests/test_review_write_through.py`
    (covers sloughs, names, dfo, entries, tidal). · Evidence: AGENTS 67 "a split mark is refused (the old ones stay
    readable)". Either `splits` is a closed/read-only domain (then it should not be in PROPOSAL_DOMAINS) or its apply
    path is live and untested. · Fix: decide; if closed, move it out of PROPOSAL_DOMAINS and pin 423 like ponds
    (`test_review_write_through.py:367`). · confidence: medium.

16. **GOTCHAS names a module and constant that no longer exist** · dead-doc · `pipeline/docs/GOTCHAS.md:161`
    (`pipeline/atlas/registry/mine.py` (`DFO_UNMATCHED_LEADS`)). · Evidence: no `mine.py` under `pipeline/`;
    `DFO_UNMATCHED_LEADS` appears only in GOTCHAS. · Fix: name where the leads live now (a `data/generated/review`
    proposal file) or drop the pointer. · confidence: high.

17. **A locked 1.4 GB agent worktree inside the repo** · dead files · `.claude/worktrees/agent-a456fa0c12fcc21e3`
    (branch `worktree-agent-a456fa0c12fcc21e3` @ 13e5debbb, `git worktree list` says locked). · Evidence: gitignored,
    so invisible to `git status`, but every `rglob`/grep (mine included) hits the tree twice; it is a full stale copy
    of app, pipeline and docs. · Fix: `git worktree remove --force` + delete the branch (T335). · confidence: high.

18. **Six "start here" documents, each superseded by the next, all still in `handoff/PLAN/`** · duplicate / dead-doc ·
    `NEXT-SESSION.md` (11-03), `NEXT-SESSION-0.5.0.md` ("Superseded once 0.5.0 is on Play"), `NEXT-SESSION-BATCH2.md`,
    `NEXT-SESSION-UI.md`, `NEXT-SESSION-after-0.5.0.md` ("replaces NEXT-SESSION-0.5.0"), `NEXT-SESSION-after-0.6.0.md`
    (current); plus `IMPLEMENT-HANDOFF.md:3` ("This is the single entry point", 10-23) and `implement_progress.md:3-6`
    ("Restart here … CURRENT STATE 2026-10-28"), `RELEASE-PLAN-0.5.0.md` (0.5.0 shipped), `proposed-tickets-2026-10-28.md`
    ("approved ALL 16 → tickets"). · Evidence: none is linked from README, AGENTS, either MAP or the current
    NEXT-SESSION (grep); `tickets/README.md:10` still says implement_progress "records what happened" while STATUS.md
    is the live log. · Fix: delete all but one; rename the current one `START-HERE.md` so the name stops carrying a
    version. · confidence: high.

19. **AGENTS points at `archive/` as deep context and as a gate, two commits before T335 deletes it** · conflict ·
    `AGENTS.md:9-11` (four `pipeline/docs/archive/*.md` as "deep context"), `AGENTS.md:290-293` rule 37 ("the grep
    gate in `pipeline/docs/archive/HANDOFF-curated-layout.md` §3.3 is what keeps it from creeping back"),
    `AGENTS.md:146-150` rule 21 (`archive/webapp`, `archive/mobile`), `AGENTS.md:342` (`archive/2026-10-CURRENT-STATE.md`);
    `pipeline/docs/00-README.md:24` says "nothing reads them". · Fix: in T335, rule 37 points at the live test
    (`pipeline/tests/test_no_missing_file_fallbacks.py`), rule 21 keeps the lesson without the path, the header drops
    the archive list. · confidence: high.

20. **Three numbers for one rebuild, in three places** · conflict · `README.md:118` ("Peak memory is about 12 GB"),
    `AGENTS.md:131` ("~18 min and ~9 GB"), `AGENTS.md:303` ("~27 min and ~10 GB"); README:107 "reach → tiles →
    deliver" vs MAP.md:14 "clim → reach → tiles → deliver" vs `pipeline/README.md:22` "clim + reach + tiles side by
    side". · Fix: the build's own manifest has the numbers (`--status`); the docs say "see `pipeline build --status`"
    and the stage order lives in MAP.md only. · confidence: high (low harm).

21. **Both maps carry a "checked against the code 2026-10-24" stamp that nobody refreshes** · stale header ·
    `pipeline/docs/MAP.md:3`, `app/MAP.md:3` — both edited 2026-11-05 (git), stamp unchanged. · Fix: drop the date
    ("the code wins — fix the map" is the rule); or make the stamp the commit of the last check. · confidence: high.

22. **Root README's branch note cites a "P3 note" that is not in the docs it points to** · dead-doc ·
    `README.md:10-12` ("Nothing is merged to `main` until the feed crons are reworked (see `pipeline/docs/` and the P3
    note)"). · Evidence: no P3 in `pipeline/docs/00-README.md`'s table; feeds are the worker now (T412, "never a
    GitHub schedule"). · Fix: one sentence on what still blocks `main` (or that it is a decision, not a cron). ·
    confidence: medium.

23. **Owner-facing questions in two places** · duplicate · `tickets/README.md` "Owner questions" (32 `| Q` rows) and
    `handoff/PLAN/OWNER-TODO.md` (32.5 K). · Evidence: memory "keep OWNER-TODO current"; ledger README says "The owner
    answers in one place: the Owner questions table below". · Fix: OWNER-TODO holds only runs/reviews the owner does
    and links each question by Q-id; never restates a default. · confidence: medium.

24. **`test_parse_prompt_teaches.py` proves 21 of its 29 cases by "did not raise"** · weak ·
    `pipeline/tests/test_parse_prompt_teaches.py:66-361` (assert-free bodies per `scan.py`; :361
    `test_mutation_without_the_guard_the_artefact_rule_passes` asserts nothing). · Evidence: a validator that returns
    silently on bad input passes every one of them. Low harm (the sibling tests use `pytest.raises`), but the
    "mutation" name promises more than a no-raise. · Fix: each `_ok(...)` returns the parsed entry and the test pins
    one field of it. · confidence: medium.

25. **`feeds/test/gauges.parity.test.ts` and `gauges.py.test.ts` are fine; `dfo.scenario.ts` is not** · note ·
    the gauge parity runs in CI (app-ci installs `.venv` at :35) and asserts counts; `dfo.scenario.ts` is reached only
    by the two skipped suites (finding 3). No action beyond 3.

## One index of what is current (proposal)

```
README.md                       what it is, layout, the three commands (release / build / tests)
AGENTS.md                       the rules (numbered; a rule names the test or tool that enforces it, never an archive doc)
pipeline/docs/MAP.md            the pipeline as it is          app/MAP.md     the app as it is
pipeline/docs/RULINGS.md        how the book is read           pipeline/docs/GOTCHAS.md   data that looks wrong and is not
handoff/PLAN/data-flow.md       every stream end to end (owner map) + data-flow-decisions.md
handoff/PLAN/app-integration/tickets/README.md   THE queue (ledger + owner questions)
handoff/PLAN/STATUS.md          what happened (plain, newest first)     handoff/PLAN/OWNER-TODO.md   what only the owner can do
handoff/PLAN/START-HERE.md      (= NEXT-SESSION-after-0.6.0.md, renamed) the one restart page
handoff/PLAN/runbooks/          release · annual-synopsis · fresh-machine · secrets
curation-review/README.md + API.md · app/tools/testbed/README.md · pipeline/README.md (commands)   per-tool
```
Everything else under `handoff/PLAN/**` is a ticket's working papers (shots, reports, audits) and is reached from its
ticket, never from the index. `pipeline/docs/0N-*.md` design notes stay listed in `00-README.md` only.

## Docs safe to delete outright

- `handoff/PLAN/NEXT-SESSION.md`, `NEXT-SESSION-0.5.0.md`, `NEXT-SESSION-BATCH2.md`, `NEXT-SESSION-UI.md`,
  `NEXT-SESSION-after-0.5.0.md` (each says it is replaced; nothing current links them)
- `handoff/PLAN/RELEASE-PLAN-0.5.0.md`, `handoff/PLAN/proposed-tickets-2026-10-28.md` (all 16 are tickets)
- `handoff/PLAN/IMPLEMENT-HANDOFF.md`, `handoff/PLAN/implement_progress.md` (the queue is the ledger, the log is STATUS.md;
  fix `tickets/README.md:10` in the same commit)
- `handoff/PLAN/app-integration/tests.md` (describes a deleted prototype's check.mjs; the harness is in `golden.ts`)
- `pipeline/docs/archive/**` and `archive/**` (T335, after moving rule 37's pointer — finding 19)
- `.claude/worktrees/agent-a456fa0c12fcc21e3` (not a doc; 1.4 GB stale copy — finding 17)
- `app/workers/feeds/parity/dfo/**` + `test/dfo.scenario.ts` + the two skipped suites (finding 3)

## Checked and sound (so nobody re-audits them)

`test_no_silent_skips.py` ALLOWLIST: all six entries still hold at exactly one site each (conftest:145, dfo_suggest:248,
export_codec:446, no_slivers:229, mu_regions:20, stocking_match:20); CI's `-m` string deselects every `needs_*` kind and
`slow`; `pipeline/tests` has no identical test bodies across files; the review app's write routes are exactly
`POST /api/marks` + `POST /api/inseason/matches` (`/api/marks/splits`, `/api/build` are GET) as AGENTS 66 says; every
path AGENTS names under `pipeline/` and `scripts/` exists except those in findings 8, 9, 16; `vitest` cannot pick up
`dist/` copies (include patterns end in `.test.ts`, dist holds `.test.d.ts`).
