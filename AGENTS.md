# Rules for agents working in this repo

Decisions already made and paid for: each rule exists because something broke or was measured. Read this before
changing anything. The code comments, tests and tickets carry the history; this page carries only the rule.

- Maps of what exists now: `pipeline/docs/MAP.md` (the pipeline) and `app/MAP.md` (the app). Fix a map when the code
  moves under it. Deep context for the reach builder: `pipeline/docs/REACH-BUILDER.md`.
- **How the book is read, every owner ruling on interpretation, is `pipeline/docs/RULINGS.md`. It wins over any other
  note**, this page included; the "Reading the regulations" rules below are how those rulings are built and guarded.
- Rule ids are cited from code, tests and tickets: **never renumber or reuse one**. A new rule takes a new id. The
  docs guard (`node app/tools/check-tickets.mjs`) fails on an id used twice.

---

## ⛔ Absolute

A1. **Never run the LLM parser.** `pipeline/regs/parsing/run_parse.sh`, `python -m pipeline.regs.parsing.dispatch`, or
    anything that spawns `claude -p`: it spends the owner's credits. Hand over the command; the owner runs it. "Run
    it" from the owner means *he* will.
A2. **Back up `data/curated/regulations/synopsis/region-*.json` before writing to them.** They can hold uncommitted
    curation. Copy them to the scratchpad, make the change, then diff to prove only what you meant moved.
A3. **Prefix shell commands with `rtk`** (it passes through when it has no filter). A run that gates a decision (a
    test suite you will report as green) runs unfiltered: `rtk proxy <cmd>`, since rtk once reported a clean suite
    while two tests failed.
A4. **Run `graphify query "<question>"` before grepping the codebase.**
A5. **Kill only what you started** (its PID, its port); never `pkill` / `killall` by pattern (one took down the
    owner's tile server). **Heavy jobs go only through `scripts/heavy.sh`** (rule 18).
A6. **Commit only your own paths: `git commit -- <paths>`.** Several agents share a checkout; `git add -A` or a bare
    `git commit` takes another lane's half-done edits. Never commit store images.
A7. **`CONFIRM_PROD` is typed only by the owner, unless he says so in that chat, that time only, for one named step**
    (owner 2026-11-22, confirmed 2026-11-25). No agent sets it otherwise, in any form (`<r-id>` for
    `pipeline release --prod`, `canifishthis.ca` for the deploy scripts), for anything production: site, data,
    feeds worker, store builds, the Play production track.
A8. **Never touch the owner's review marks** (`data/curated/review/marks.jsonl`): only the review app appends to them.
    Never edit, reorder, regenerate or delete a line. Committing the file as it is, is allowed.

Also binding everywhere: `main` is protected and every change reaches it through a pull request (W47).

---

## Working style

42. **Measure before asserting.** Every number in the docs is reproducible; several "obvious" designs here were
    killed by one measurement (the hash id, auto-including straddlers, parallelism in the reach builder).
44. **Report honestly what you did not verify.** An unverified claim is worse than a known gap. Say "scaffolded, not
    run" when that is what happened.
W45. **Spend the owner's credits on the work, not on a long context.** Every turn re-reads the conversation. Start a
    new chat at a natural break (a release shipped, a lane merged, an unrelated next item), after updating your
    chat's restart page; compact instead when live context is worth keeping (a build or agent still running).
    Delegate wide reads and long loops to agents that report briefly; grep and tail rather than re-read big files.
W46. **The guards stay in. A change that trips one fixes the cause; it never loosens, skips or works around it.**
    - **No silent fallbacks, anywhere:** no `try/catch` that turns an error into an empty answer, no default path,
      no `|| []` over a failed read. A failure is shown AND reported (`reportFailure` in the app, a 503 naming the
      cause in the review backend, a raise naming the file and its command in the pipeline). Deploy scripts never
      `|| true` and always `set -o pipefail`.
    - **Review write-through refuses a shrink of more than 10 rows** (`pipeline/regs/review/apply.py`);
      `--accept-shrink` only for one the owner asked for.
    - **The review app's route table** `tools/curation-review/API.md` is regenerated (`tools/curation-review/api_routes.py
      --write`) in the same commit as a route change.
    - **Fixtures are release-backed.** The release re-cuts them on its candidate (rule 62); when LATEST moved any
      other way, `node app/tools/regs-recut/sync.mjs` re-cuts them all before an app build.
    - **Test anchors:** a test stands on data it builds, a frozen fixture, or the data under test through the one
      helper (app: `liveRelease()` / `namedRelease()` / `sectionNames`; pipeline: `pipeline/tests/under_test.py`).
      No raw section id, `data/…` path, `CURATED` / `GENERATED` or `LATEST` in a test: a section is a stream key +
      start metre. The scanners (`app/tools/check-test-anchors.mjs`, `pipeline/tests/anchors.py`) fail on every
      offender; never reword past one. Pin printed text as text, never a hash.
    - **Feeds worker:** its tests gate every deploy; `REPORT_SALT` is required (no salt is a 503).
    - **Staging upload** (`deploy/staging/upload.sh`) refuses commits not on GitHub. Production is the owner's (A7).
    - **Test bed:** a walk that matters starts from a FRESH install.

## Branches, commits, pull requests

W47. **`main` is the protected trunk; every change reaches it through a pull request. Never push to `main`.** The
    trunk's name is typed once: `scripts/ci_paths.py` `TRUNK` (TypeScript: `app/packages/core/src/repo.ts`).
    - Branch from a fresh `origin/main` (`feat/`, `fix/`, `perf/`, `ci/`, `docs/`, `chore/`, `test/` + ticket id),
      open the PR and enqueue it (`gh pr merge --merge --auto`); PRs land through the merge queue. Merge commits only: tickets cite lane commit hashes, which must survive.
    - Required checks are each workflow's last job (`scripts/ci_paths.py` `REQUIRED`); a skipped area is a pass.
    - Hooks live in `.githooks/` (`git config core.hooksPath .githooks` once per clone); the pre-push hook runs the
      tests the change reaches. `--no-verify` is an emergency skip only, said in the commit or PR. A long hook needs
      an ssh keepalive (`ServerAliveInterval`) or GitHub drops the push.
      Hooks queue on one machine lock; `BEFORE_PUSH_NO_LOCK="<reason>"` runs beside it, the reason printed.
    - **Once before opening a PR** (and before a push of substance to it) run `scripts/before_pr.sh --title "<the PR title>"`; open the PR
      only on its `before-pr: PASS` line. Read the PR's mutation report (report-only) before asking for review.
    - A release is cut (`python -m pipeline release --cut <version>`) to `release/<version>` and ships from there. A
      fix lands on `main` first, then is cherry-picked onto the release branch, which merges back into `main` by PR.
W48. **Commits follow Conventional Commits, and the app version comes from them.**
    - `<type>(<scope>)!: <subject>`, header at most 100 characters, the scope the ticket id. `feat` (a change
      anglers notice), `fix`, `perf` bump the app; `docs`, `chore`, `ci`, `test`, `refactor`, `build`, `style` do
      not (`app/tools/versioning.json`). `!` marks a change an installed app cannot take without updating. A PR
      title follows the same format and, with its merge-commit suffix ` (#N)`, fits 100: at most 93 characters while PR
      numbers have three digits, 92 from #1000. Checked by `before_pr.sh --title`, on a push to an open PR, and in CI.
    - `release --cut` computes the bump from the commits since the last `v*` tag and the owner confirms it. Never
      edit app.json's version by hand. A data-only release ships from `release/data-<date>` (`--cut --data-only`).
      Build numbers only go up. 1.0.0 is the owner's call.

## Tickets and docs

W49. **Tickets are the only work list, and `node app/tools/check-tickets.mjs` must pass** (in `pnpm check`,
    fast-fail and docs-ci; it is the ledger guard and the docs guard).
    - A ticket's file and ledger row exist before any commit or PR names its id (claim the number with
      `node app/tools/ledger.mjs --claim`); the ledger views are built, never typed. A DONE row names its commit
      hashes (commit the code first; remap them after a cherry-pick); an open row names what is left and whose step.
    - An owner step is written into its ticket the same turn it comes up or he answers; a decision goes in the
      decisions page by subject; a ruling on how the book is read goes to `pipeline/docs/RULINGS.md`.
    - Each live chat keeps one short restart page, holding no release id or build number (state is the top entry of
      `docs/agents/STATUS.md`). Every path a start, restart or owner page cites must exist.

## Builds and tests

17. **Never rebuild the live atlas to test a change.** Build beside it (`python -m pipeline build --atlas` writes
    `<build>_next`; `--side DIR` for a side build) and compare with `pipeline/tools/audit/build_parity.py`. Stage
    timings are `python -m pipeline build --status`, never a number written here.
18. **Test tiers, and heavy jobs.**
    - Tier 0 is the file you touched; Tier 1 `scripts/test-smoke.sh` (the smoke set is chosen by rule in
      `pipeline/tests/smoke.py`, no hand list); Tier 2 `scripts/test-full.sh` through `scripts/heavy.sh`, at
      milestones or before a merge. Test what changed while iterating; whole suites once at the end.
    - A plain `pytest` is CI's no-data tier; its NOT RUN line (hidden by rtk's filter: `rtk proxy` shows it) counts the
      data tests left out. With builds, Tier 0 adds `--with-data` (T1193). `slow` tests, where the determinism and
      full-build guards live, are left out either way: Tier 2 runs `-m slow` explicitly. Runner scripts source
      `scripts/test-env.sh`.
    - **Heavy jobs** (atlas build, reach run, `pipeline.deliver`, tile builds, full pytest, emulators and simulators)
      run as `scripts/heavy.sh <label> <min free %> -- <cmd>`: one at a time per lane (three lane locks), waiting
      for free memory and killed when it runs out. Ask `scripts/heavy.sh --status` before deciding a job died or
      starting it again; an identical job is refused while one holds or waits (`--again` to mean it).
19. **Determinism is a precondition.** Sorted iteration everywhere; no clocks in output. The reach builder's digest
    and the atlas build are identical across runs; a diff between two builds is real signal.
20. **Bump `pipeline.atlas.reach.cache.POLICY_VERSION`, with a changelog line, when `classify.py` changes an
    outcome.** A test fails if it goes stale.
20a. **Fast-fail first, and it grows.** Before any long job (build, release, store build, emulator, full suite), run
    `scripts/fast-fail.sh`, the cheap checks that stop a doomed job in a minute or two. The release preflight and
    the store builds run it themselves (their escape `--no-fast-fail REASON` prints its reason); `scripts/heavy.sh`
    does not, so for any other long job you run it. A step that reads this machine's data rather than the code is
    a `data: …` step, run only by the data release. **A late failure a cheap check could have caught gets that
    check added to `scripts/fast-fail.sh` in the same change as the fix**, with a comment naming what it caught.
39. **The mill and the guards are separate tiers.** The mill (fetch, build, match, bundle, tiles) is heavy and run by
    hand a few times a year. The guards (`pipeline-ci.yml`, `app-ci.yml`, `feeds-worker-ci.yml`, `docs-ci.yml`)
    take seconds on every push. Never add a CI step that needs `graph.pkl`: a hosted runner has 14 GB of disk and
    one build directory is 10.

## Data: three kinds, and how to touch them

35. **Three kinds of data; confusing them is the recurring bug in this repo.**

    | kind | example | a rebuild may | losing it costs |
    |---|---|---|---|
    | **authored** | `waters/cuts.json`, `name_variants.json`, `regulations/book_name_links.json` | only READ it | human hours |
    | **reviewed** | `gauges/matches.json`, `added_streams.json` | only READ it | human hours |
    | **generated** | anything under `data/generated/` | rewrite it freely | CPU |

    "Reviewed" is the one people get wrong: a machine produced it, a human checked it, and re-running the generator
    throws that review away.
36. **Read curated paths through `pipeline.common.curated.CURATED`** (`CURATED.waters.cuts`), never as a literal. The tree is
    validated at first access, so a bad key fails by name instead of returning an empty list builds later. All paths come from
    `pipeline.common.curated` (the roots, the FWA GeoPackage name and the atlas build come from `config.yaml`; every other file's
    name is declared on its schema field); CI refuses a literal `"data/..."` path in pipeline code (tests exempt).
36a. **Curated naming.** `*_calls.json` are the owner's per-item decisions, `*_waters.json` a list of waters,
    `*_stations.json` a list of gauges. Every dict-shaped curated JSON has an `_about` (what, who writes, who reads);
    every curated folder has a README. Machine output (proposals, the gauge review queue) lives in `data/generated/`.
    Check hand-edited waters files with `python -m pipeline.tools.audit.check_curated --waters`.
36b. **One writer for curated data.** A program that changes a curated file calls
    `pipeline.common.curated_write.write` (or `write_any`): it validates, keeps `_about`, backs up, writes
    atomically and logs who wrote what. `test_curated_writer.py` fails on a module that writes curated files any
    other way unless it is classified there. Hand edits follow A2.
37. **A loader for a curated file RAISES on a missing file, never returns `[]`.** Absent curated data is a bug,
    never an empty set. pipeline-ci's curated-path steps keep it from creeping back.
38. **Never regenerate a reviewed artifact to "check something".** `python -m pipeline.tools.audit.check_curated`
    says whether one is stale and prints the command without running it; `ok` means nothing to do. The gauge
    matcher writes `data/generated/gauges/match.json`; only `pipeline.gauges.generate.promote` writes
    `data/curated/gauges/matches.json`, one station at a time with `--only`. A full promote lists every demotion
    and refuses without `--accept-demotions`, the owner's call after reading the list.
40. **A value on both sides of the Python/TypeScript boundary is generated, never typed twice.** Python is the
    source; an emitter under `pipeline/tools/emit/` writes the TypeScript, and `--check` fails CI on drift
    (`scripts/check-format-pairs.py` refuses a feed format literal anywhere else). Where a rule must run in both
    languages (the feeds worker reading DFO rows), one shared committed fixture is the pin, never a copy per side;
    a Python-backed worker suite FAILS without `.venv`, never skips.
41. **Generated paths come from `pipeline.common.curated.GENERATED`; everything a program writes is under
    `data/generated/`.** A WRITER may create its directory; a READER may not: read a build through
    `GENERATED.require_build()` / `GENERATED.registry()`, which refuse a missing directory and print the command
    that makes it. Never `Path("data/generated/...")`.

## The data model

5. **`item_id` is durable across a rebuild (99.88%); `section_id` is not (94%).** Anything crossing a version
    boundary (live feeds, deep links, saved pins, bathymetry sheets) binds to `item_id`. **`section_id` and
    `dense_id` never leave the bundle**: not in a URL, a saved preference, a feed or an API contract. A PLACE (FWA
    stream key + distance along it) is durable and links may carry it. The exported section id stays
    `{blk}:{measure}`; a hash form bought one id in 49,542. Reopen only with a re-anchoring build showing more than
    2 points of improvement.
6. **A durable id is not a durable answer.** 15% of surviving items changed their section list in one rebuild.
    Never cache a resolved section list across bundle versions; re-resolve.
8. **`rule_id` is unique only WITHIN an entry** (49 collide corpus-wide). Every table keys on
    `(entry_id, rule_id)`, never on `rule_id` alone.
9. **`includes_tributaries` is three-valued.** On a rule, `None` inherits the entry's value. Reading only the
    rule's own field finds 132 tributary rules; the real number is 554.

## Resolution: where a rule applies

10. **The resolver never creates a section.** `pipeline/atlas/registry/build.py` filters existing graph nodes by
    route measure; splitting happens upstream in the sectionizer from `data/curated/waters/cuts.json`. If
    resolution could split, editing a regulation would silently change geometry.
11. **Reaches resolve by ROUTE MEASURE on the cut's own blue line, never by a flow walk.** A `between` across two
    blue lines is the intersection of two half-lines. (A flow walk returned 36 of the Chilliwack's 39 sections for
    both `upstream_of` and `downstream_of` the same cut.)
12. **The builder never decides where a regulation applies; it reports and the curator decides.** Silent widening
    is a defect, even in the "safe" direction of over-closing.
    - A DETACHED piece (reaching its water only through a lake, other water or the sea) is placed by its flow path
      (`extent.OFF_LINE_PIECES_FOLLOW_THEIR_FLOW`); what stays unplaced carries a typed reason.
    - A braid across a cut is cut at build (RULINGS E17, `splits.braid_sweep`); a cut severs every line it crosses,
      and a braid piece cut at both ends by a `between`'s own cuts is inside it (E18). Length cuts never take a
      junction a braid spans.
    - The build FAILS on a piece still `straddles_the_cut` (`build.straddle_gate`) and on any unplaced piece the
      owner has not accepted in `data/curated/waters/unplaced_piece_calls.json` (`build.unplaced_gate`; a `proposed`
      row is no acceptance). Promote refuses a curated cut the atlas cannot place.
    - A nameless line that leaves a lake and comes back into it is that lake's (`registry.lake_loops`), unless it
      lies outside the lake's cut areas. A 2300-only ditch with no name is in no river item.
13. **A rule's extents are its own, never inherited from its entry. Never widen a rule that names a place it could
    not bind.** Every rule says where it is (`extents`, or the place in words: `extent_text` /
    `unresolved_locators`, and then it stays UNBOUND); no reader hands a rule its entry's extents.
    - Ingest writes the whole water onto a rule that says nothing about location, so the file states it.
    - A part of the rule's own water that nothing draws is `undrawn_part` beside `[{op: whole}]`: a note on the
      water, never a colour, and it does not walk tributaries.
    - A complement ("all other parts") is op `rest` with its `siblings`; a sibling that does not bind leaves it
      `complement_unknown`, never the whole water. A DFO place's rest may name its own bounds and places left out,
      and answers after them.
    - An area rule with a carve-out no cut expresses stays unbound unless `outside_items` subtracts the excepted
      lakes. A `within(area)` holds every lake the polygon touches: check them, take a mostly-outside one out.
    - A licensing record with no extents takes its entry's at placement; what `tributary_excludes` removes goes to
      the excluded water's own designation, and anything unheld is reported, never re-widened.
14. **Every rule ends bound, or unresolved with a typed reason**: never absent, bound-and-empty, or unexplained.
    Enforced in `RuleBinding.__post_init__`.
15. **`tributaries_pending` means the tributary walk could not run, and a pending binding is never complete.**
    "Tributaries" walks streams only (p.80); "watershed" keeps lakes. EVERY INFLOW of a lake a rule binds is its
    tributary, the river on the lake's own line too (a row of its own or not), and the walk climbs through every
    lake above, walking each but never covering it (`tributaries.EVERY_INFLOW_OF_A_RULES_LAKE_IS_ITS_TRIBUTARY`,
    RULINGS T19); only a printed exception keeps water out (a carve-out, the rule's words; a lake row's carve-out
    also stops every walk through that lake, `build.A_LAKES_PRINTED_EXCEPTION_STOPS_EVERY_WALK_THROUGH_IT`). A rule
    bound to its WHOLE water climbs through the lake that water flows out of
    (`tributaries.A_WHOLE_WATERS_WALK_CLIMBS_ITS_HEAD_LAKE`, RULINGS T18); a stretch rule stops at the lake at its top
    (a stream is not a tributary of itself), as does a licensing record; a DFO place walks as the rows do (T1177).
16. **The review app and the builder share one implementation.** `entry_reaches` calls `pipeline.atlas.reach.classify`
    (pinned by `test_review_reaches.py`). Never give the review app its own resolver: it is where a human signs off,
    so divergence stays invisible until an angler hits it.
43. **Do not build parallelism into the reach builder.** The whole corpus resolves in about 0.1 s.

## The app (`app/`)

22. **`packages/core` has no React and no platform imports; `packages/ui` may import `react` but never `react-dom`
    or `react-native`.** Enforced by `app/tools/check-boundaries.mjs`: fix your code, never weaken the gate.
23. **Status is rendered by ONE function in `core/`, never re-implemented per surface.** Navigation may differ
    between phone and desktop; the answer may not. (Nine status surfaces in four vocabularies is how it drifted.)
24. **Hooks are shared; components are not.** A hook returns data, so it runs under both renderers.
25. **Logic never lives in a component**; it lives in `core/` or a hook. A layout that wants to duplicate logic has
    it in the wrong place.
26. **Phone web renders the same `packages/ui-native` components as the native app** (react-native-web), and
    desktop is a layout class, not a second app: the same components arrange themselves by `useLayoutClass()`.
    There is no `apps/web` and no separate DOM component set.
27. **One React version, workspace-wide**, pinned through `pnpm.overrides`. Two Reacts in one bundle is
    `Invalid hook call`, found only at bundle time.
28. **Map layers are added only by editing `app/packages/map/style/layers.source.json`**, then `pnpm style:build`.
    Colours reference tokens by name; literals are rejected. No app imports a map SDK directly.
29. **A categorical colour mode over an enum colours every member, and a continuous mode defines `missing`**, so
    `unknown` and `default_only` can never be drawn as something they are not.
29a. **The trout and char table is written whole by Python** (`pipeline/deliver/answers/trout/`): every row, word,
    number and date. `core/src/regs/trout.ts` only fills the day slot and the screen-reader label; the app draws
    what it is given. A frame the app cannot hold to its shape is reported and no table is drawn, never a fallback.
    A trout word or row change is a Python change, proved by `trout/check.py` and the shared vectors
    (`app/fixtures/trout/`).
30. **Every app dependency has a line in `app/deps.md`** saying why it is there.
31. **`pnpm check` must pass.** Its steps are `app/package.json` `scripts.check`, not a list here. They include
    knip: no unused file, export or dependency; an allowance needs its reason in `app/knip.jsonc`. Generated native
    projects (`apps/mobile/ios|android`) are not source.
32. **A screen never asserts a fact about the data it sits on; it asks.** Counts come from `source.counts()` and
    the feed's own index. A figure that is not available renders as NOTHING: "we have not asked" and "there are
    none" are different claims, and only one is safe to show.
33. **Assert the whole composed string, never the fragment you interpolated.** A regex over a substring you
    supplied cannot fail (it passed while "It a major branch of it." shipped on 97.7% of gauged sections).
    Sentences are composed in `core/`, where a test pins the finished string.
34. **`accessibilityState` is dead; use the ARIA props.** react-native-web ignores it silently. `aria-checked`
    (role=radio), `aria-selected` (role=tab) and `aria-disabled` work on both targets; `app/tools/check-platform.mjs`
    fails on the old spelling.
34a. **Web and native code are split by FILE, never by branch.** Code that differs by platform is a pair,
    `foo.web.ts(x)` + `foo.native.ts(x)`, typed by one `foo.d.ts`, doing the same job; a fix to one half is checked
    against the other in the same commit. Shared files have no `Platform.OS` and no `typeof window / document /
    navigator`; a one-value difference may use `Platform.select`. A fix only one platform needs goes in that
    platform's file; the map's `src/web/` and `src/native/` are imported only from their own platform. Enforced by
    `pnpm platform` (`app/tools/check-platform.mjs`): fix the code; a new `INLINE_ALLOWED` entry needs the owner's
    reason. Details: `app/README.md` "Web and native code".
34b. **R8 is on for every Android release build, and its mapping goes to Play with the AAB.**
    `app/apps/mobile/plugins/with-r8.js` turns it on with `plugins/r8-keep-rules.pro` (each keep rule with its
    reason); `app/tools/r8-report.mjs check` holds the AAB's scores over the floors. R8 off is refused for any build
    that can reach a phone. Never lower a floor or add a broad `{ *; }` keep rule to get a build through. A new
    native module, or a library in `with-r8.js` `IGNORED_RULES` added or updated, gets a keep-rule review and a
    walk of its path. Before the app SCHEDULES a notification for later, R8 must keep what expo-notifications'
    triggers' serialVersionUID is computed from (keep rule 8), or it will not load after an update.

## Reading the regulations, and the pipeline that builds the answer

45. **The book's species list (p.80) is closed, and a bull trout IS a Dolly Varden.** 22 fish in
    `catalogue.BOOK_FAMILIES`; any other code is refused. `BT` is refused ("bull trout" is `DV`). Chinook `CH` is
    the one salmon the book names: in SALMON, never a game fish.
46. **"Trout" is `TROUT_CHAR`; it excludes char only by (a) or (b).** (a) the line excludes char in words; (b) the
    same row or zone table gives char a RELATED rule of its own (same aspect, `catalogue.rule_aspects`; same kind
    of water and days, `catalogue.related_rules`). A related rule for ONE char takes only that char out
    (`species_except: [DV]`); all char leave only when the char group as a whole has one (RULINGS C2). A combined
    "trout/char" never excludes char. `TROUT` is refused; `trout_scope_problems` refuses any other set.
47. **A `;` ends a dating run.** "Trout/char catch and release; bait ban, June 15-Oct 31" dates only the bait ban;
    an "and"/comma chain shares the date. The model refuses a `when` that crosses a `;`, and a quote that prints
    dates its rule does not carry.
48. **A pointer is `see`, never a rule.** "See Lonzo Creek" goes in the entry's `see` list; the model refuses an
    `advisory` that is a pointer. A pointer never moves a water into the target's region.
49. **Half of a river is `side`, not a part and not the whole.** It is read beside the other rules and displaces
    none. A lake's half is `undrawn_part`. A DFO place for one bank is read the same way (RULINGS E28): placed, it
    rides its sections' chains in `u`, never `l`; not placed, it is an undrawn place, never widened.
50. **A water's own dates override a dated zone rule on the overlap only, and never a closure.** An undated water
    quota never silences a dated zone release unless it is the exact same statement.
51. **"(any size)" drops every size constraint of the region for that fish** (RULINGS Q5a): the bundle derives the
    lifts (`rules._any_size_lifts`, `exempts[].basis: any_size`).
52. **A zone release limited to a kind of water displaces its own region's table on that water** (same base
    dimension; `read.released_on_water`, `effective_rules` step 4b). Closures, water rows and other regions' rules
    are untouched; a count-free size clause is handled by step 5b (rule 57).
53. **Competition lives in one function:** `pipeline/deliver/bundle/read/ladder.py::effective_rules` is the
    reference reader. The export `guide` restates it and the app must match it; change a ruling in both together.
54. **Steelhead regulations come only from the book; the curated list is a presence indicator**
    (`pipeline/atlas/reach/steelhead.py`).
    - The provincial and zone steelhead rules bind STREAMS of Regions 1, 2, 3, 5 and 6. A steelhead row is one any of
      whose rules or licensing records prints steelhead, the stamp included (`steelhead.prints_steelhead`).
    - BOOK-KNOWN water is a steelhead row's own water plus every stream section its rules bind; a lake only as its
      own row's water (`LAKES_ONLY_BY_OWN_ROW`); no tributary walk. It carries the provincial set through the twins
      (extent op `steelhead_waters`). Never put an item or region extent on a base steelhead rule: it changes its
      competition key.
    - A PACIFIC STREAM is known too (RULINGS S14, `PACIFIC_STREAMS_ARE_STEELHEAD_WATER`): every stream of those regions
      whose FWA basin reaches the Pacific, less what `steelhead_barriers.json` holds out (stays "possible"); lakes out.
    - STEELHEAD RULES APPLY where every rule of the provincial set binds (`Presence.rules_apply`); elsewhere a known
      part carries `steelhead_rules: false` and its rainbow rules answer for every rainbow. An outright stamp waiver
      lifts the steelhead stamp; "unless fishing for steelhead" lifts only the classified-water stamp.
    - The curated list `data/curated/waters/steelhead_waters.json` binds no rule; it only marks presence. It is made
      by `python -m pipeline.regs.steelhead.known_waters`, outside the build, and a human copies it to the curated
      path; never run it to check something (rule 38). `test_steelhead_waters.py` pins both.
55. **A slough is a stream, and the water kind is decided ONCE, in the registry.** `item.kind` is `stream` for every
    flowing water, including a polygon whose name's head noun flows (`pipeline/common/water_kind.py::flows`), else
    `lake` / `wetland`; `pipeline/atlas/registry/flowing.py` folds such a polygon into its river or makes it a stream
    item. Every consumer reads that answer and nothing recomputes it (`test_one_water_kind.py`). The shape a section
    is drawn as is per section, never the water's kind. An absorbed item's id is mapped to its survivor at exactly
    three read points and refused everywhere else; curated files name survivors only. A lake cut into parts owns
    no section of its own.
56. **One data flow: every derived fact is computed once and READ everywhere else** (`test_one_data_flow.py`). The
    atlas and the reach run write sidecars; the bundle reads them and opens no curated waters file beyond the few
    that test lists. A PROMOTED ATLAS IS IMMUTABLE: build to `<build>_next`, promote with `pipeline.atlas.promote`.
    The reader runs once, into `verdicts.sqlite`, and the status index, export and answers look answers up there
    (`test_dataflow_gates.py` refuses a reader call anywhere else).
57. **The competition rulings, as built.** Each is a RULINGS row; the reader is `read.effective_rules`.
    - Classified Waters: one unit per section, the first classified water it flows into
      (`reach.licensing.first_classified_downstream`). A tributary whose own row prints no designation is not
      classified, nor is anything above it; it carries the unit below only as a "may need" (`licensing.CarriedUnit`).
      Only a designation that reaches its tributaries claims a walked section above it.
    - A joining water with its own row that signs pull into a cut goes with it only to its first lake
      (`reach.build._stem_to_first_lake`). A water the book puts outside an area its polygon touches is stated once,
      in `areas.json` `outside_items`, never on a rule or in a reader.
    - Strict lift: a zone closure and a water's row both apply (the union of their closed dates). A row beats a zone
      closure only by a printed exemption, or a dated release, opening or quota inside it: those dates, those fish.
      An exemption carries to a neighbouring region only where the water has no entry there (G3; never when
      `own_region_only`); a ✱ row's exemption walks to its tributaries (T13). "Open all year" lifts only full
      blanket closures (G4). A row's dated bait ban replaces its zone's all-year one (G5). What a `row_closure`
      note claims is re-asked of the reader (`closures_combine_problems`, `test_lift_decisions.py`).
    - Reader steps (`test_competition.py`): RU-3 a same-row dated release or closure silences the row's undated
      quota on its dates, its daily and its annual one (Q32); RU-4 a zone release with no water kind empties its table's keepers; RU-5 a water's take-0
      band displaces a zone size clause inside it; RU-6 "ST" with no steelhead rule is answered as "RB" (and S12: a
      size-free rainbow rule speaks for "ST"); RU-7 a full closure displaces the keepers it beats; RU-8 two regions'
      identical statements show once; 5b a zone size-only clause under a release of every origin it keeps is moot;
      4c a water's own full closure silences every keeping rule for its fish (`read.WATER_CLOSURE_DOMINANT`).
    - The steps remove rules only through `lose`, so `trace=True` lists every loser (`test_reader_answers.py`).
      `requirements_in_force` drops a requirement for the other kind of water, and a superior authority's (the
      national park permit) displaces the rest (`test_requirements_g5.py`). Gear rules compete only with the same
      subject (`test_answers_gear.py`). A range printed to Feb 28 runs through Feb 29 (`catalogue.range_days`).
    - The export REFUSES a dated water release under a blanket closure, and a water quota a zone closure silences,
      unless the owner listed it in `data/curated/regulations/under_closure_calls.json`.
58. **The UI export ships ENCODED; the model is `export.model.build()`.** `pipeline/deliver/export/codec.py`
    `expand` is the reference decoder, and the run refuses a pair that does not decode to the model. Every check
    and every word of the guide describes the DECODED model; tests and tools read the files through
    `export.files.load`. A new SHAPE must be taught to the codec, which refuses what it does not know. Neither
    file names a section.
59. **Boundaries are cut once, at the source; the build only asserts.**
    - The B.C. outline is the exact union of the `wmu` units, never simplified or buffered; every other area is
      clipped to it. How an area is cut is `areas.json` `cut`: `"clean"` (parks, reserves) or `"first_last"`.
    - A STRADDLER CARRIES BOTH REGIONS' RULES (RULINGS Z4), the stricter per fish. One function answers region
      membership, `registry.regions.region_sections` (`test_one_region_membership.py`); the home region is display
      only.
    - The sliver gate (`sliver_gate.check`) stops the build on any stream piece under 5 m a cut made. It repairs
      nothing: the fix goes where the cut came from.
60. **The ✱ and own-row rulings.** A row whose name carries the global ✱ includes tributaries for every rule; a row
    without it narrows to the clause printing one. A tributary with its own row gets both its own and its inherited
    rules, and on one competition key its own row speaks (`read.OWN_ROW_BEATS_INHERITED`). Of two ✱ rows on one
    tributary, the nearer water wins (`read.NEARER_ROW_BEATS_FARTHER`, RULINGS T14). Neither ever beats a closure.
    A fish exactly on a printed size bound is legal (`catalogue.EXACT_BOUND_IS_LEGAL`). A record's `verbatim`
    carries no extraction markup (`catalogue.clean_verbatim`). A `between` whose sentence prints signs ends at the
    curated sign split, never a gauge.
61. **A rule held on some weekdays or hours DECIDES at those moments.** The reader is asked at a `calendar.Moment`;
    a day is closed in the status index only when every moment of it is (a night closure closes its hours, never
    the day). Shipped angler text carries no developer instruction (`test_moments.py`).
62. **One rebuild command, one release command, no silent skips.**
    - `python -m pipeline build` runs every stage, each keyed by what it reads and skipped when up to date. A new
      curated read in the atlas build goes into `ATLAS_INPUTS` (`pipeline/atlas/inputs.py`).
    - A test reading generated or fetched data carries a `needs_*` marker and FAILS when the data is missing
      (`test_no_silent_skips.py`). The bundle refuses a reach run made from another corpus or registry.
      `marks.jsonl` is no build input (`build.NOT_BUILD_INPUTS`).
    - `python -m pipeline release --staging` is the one release path (runbook `docs/runbooks/release.md`);
      a binding diff stops it until `--accept-diff`. Anything that must follow a release is a row in
      `pipeline/release_follows.py`, never a hand step. It pushes only with `--push`, only to its release branch.
    - Promote and preflight refusals (a moved boundary, a stale atlas, a lost boundary) are passed only by their
      own `--accept-…` / reason flag, after reading the list.
63. **A tributary walk never enters the entry's own water**
    (`tributaries.THE_WALK_NEVER_ENTERS_THE_ENTRYS_OWN_WATER`): every non-area item the entry matches is governed by
    the rule's extents, even when FWA codes it as a tributary; a piece on the reach's own watershed code is a side
    channel, not own water. A lake-only reach walks PAST the own stream flowing into its lake, never collecting it (what
    joins it is the lake's: `build.WATER_JOINING_THE_ROWS_OWN_STREAM_ABOVE_ITS_LAKE_IS_THE_LAKES_TRIBUTARY`).
    **Before promoting any split or name change, diff the reach run's per-rule bindings
    against live**; atlas parity cannot see this. A dead end of a water the walk collected wholly goes with that
    water (never on the reach's own code, never past a carve-out) and flows where its water flows, never read as
    the sea (`A_DEAD_END_GOES_WITH_ITS_WATER`, `A_DEAD_END_FLOWS_WHERE_ITS_WATER_FLOWS`).

## Curation review

65. **A reviewer signs off only on ONE VINTAGE.** Whether an entry is built is
    `pipeline.regs.review.vintage.entry_state`, nowhere else: until its digest agrees across reach, bundle and
    release on the served atlas, it reads "awaiting build" and only T and R may be marked (`refuse_unbuilt`).
    "Bundle matches" is per rule, label and section count.
66. **The review app writes one thing: a mark.** There is no fix mode; a fix is a reviewed change to the curated
    files. `test_curation_review_safety` pins the app to `POST /api/marks` and `writes.py` as its only file writer.
    Never add a write route. The one exception is the in-season matches file
    (`data/curated/inseason/notice_waters.json`), written only through `writes.commit` by its two routes; the
    ranges it resolves to are generated, never committed. The verdict, tiers and coverage strip are one function
    each.
67. **The review queue has one order and one reader of the run.** The order is `tiers.queue_rank`. Entries only: a
    split is checked on the map of each entry whose rules name it, never as a review target. The run reaches the
    review side only through `regs/review/runfacts.py`, and a run without `report.json` is refused, never read as
    "no findings". "Commit marks" is a CLI the screen prints, never a route.
68. **A closed area inside a DFO place is a location of its own, never a precedence rule.** Each live excluded item
    becomes `<water_id>#closed:<key>` with its own rule and wins on its sections because it is narrower. It BINDS
    only when the owner confirmed it for the live words; anything else is CARRIED on its water, never widened,
    never dropped. The feeds worker must emit a piece only while today's row names it, as the pipeline does. A
    rest of a DFO place (rule 13) is a different thing: it leaves out other places of its water.

## Owner-only steps

64. **A newer HYDAT is the owner's run: `scripts/update-hydat.sh`; agents never run it.** It rebuilds through
    `scripts/heavy.sh`, uploads nothing and prints the release command. Every feed run writes what is due
    (`index.json` `maintenance`, with the exact command) and tells the owner once through `FEED_NOTIFY_URL`.

The other owner calls are stated where they apply: A1, A7, A8, W48 (the bump, 1.0.0), 12 (unplaced pieces), 38
(gauge demotions), 54 (the steelhead list).
