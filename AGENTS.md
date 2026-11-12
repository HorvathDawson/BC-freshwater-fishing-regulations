# Rules for agents working in this repo

Decisions already made and paid for. Each line exists because something broke or was
measured. **Read this before changing anything**; re-deriving these costs a day and
re-litigating them costs trust.

**Maps of what exists now:** `pipeline/docs/MAP.md` (the pipeline) and `app/MAP.md` (the app). Fix a map when the code
moves under it.

Deep context: `pipeline/docs/REACH-BUILDER.md`. The historical plans (`13-build-plan`, `10-plan` issues ①–㊸,
`RESOLVER-HANDOFF`) and the v1 code were removed with `archive/` (T335); read them with `git show d1b36ca44:<path>`.

**How the book is read — every user interpretation ruling — is `pipeline/docs/RULINGS.md`; it is the source of truth for interpretation and wins over any other note.**

---

## ⛔ Absolute

A1. **Never run the LLM parser.** `pipeline/regs/parsing/run_parse.sh`,
   `python -m pipeline.regs.parsing.dispatch`, anything that spawns `claude -p`. It spends the
   user's credits. Hand over the command; the human runs it. This holds even if the user
   says "run it" — that means *they* will.
A2. **Never write to `data/curated/regulations/synopsis/region-*.json` without backing
   them up first.** They hold in-progress curation that may not be committed. `cp` them to the scratchpad, make
   the change, then diff to prove only what you intended moved. A lock was lost in this
   repo once; do not be the second time.
A3. **Prefix shell commands with `rtk`.** Token-optimised proxy; passes through when it has
   no filter.
A4. **Run `graphify query "<question>"` before grepping the codebase.**
A5. **Kill only what you started** (its PID, its port). Never `pkill` / `killall` by pattern: one took down the owner's
   tile server. Heavy jobs go through `scripts/heavy.sh`, one at a time.
A6. **Commit only your own paths: `git commit -- <paths>`.** Several agents share this checkout; `git add -A` or a bare
   `git commit` takes another lane's half-done edits. Never commit `data/generated/review/pfma_basin_areas.json`,
   `data/generated/review/write-through.jsonl`, or store images.

---

## The data model

5. **`item_id` is durable (99.88% across a rebuild). `section_id` is not (94%).**
   Measured, `pipeline/tools/audit/build_parity.py`. Anything crossing a version boundary — live
   feeds, deep links, saved pins, bathymetry sheets — binds to `item_id`.
   **`section_id` and `dense_id` must never leave the bundle**: not in a URL, a saved
   preference, a feed, or an API contract.
   (Owner 2026-11-03, T369) A PLACE on a water — the FWA stream key + a distance along it — is a durable FWA
   location, not a section id: links may carry it, and the app resolves it to the section/part of the release it holds.
6. **A durable id is not a durable answer.** 15% of surviving items changed their section
   list in one rebuild. Never cache a resolved section list across bundle versions —
   re-resolve.
7. **The exported section id stays `{blk}:{measure}`.** The hash form was measured and
   buys **one id out of 49,542**. Open decision #1 is closed; do not reopen without a
   *re-anchoring* build showing >2 points of improvement.
8. **`rule_id` is unique only WITHIN an entry** — 49 collide corpus-wide. Every table keys
   on `(entry_id, rule_id)`. Never on `rule_id` alone.
9. **`includes_tributaries` is three-valued.** On a rule, `None` inherits the entry's
   `includes_tributaries`.
   Reading only the rule's own field gives 132; the real number is 554 across 264 entries.

## Resolution

10. **The resolver never creates a section.** `pipeline/atlas/registry/build.py` filters
    pre-existing graph nodes by route measure. Splitting happens upstream in the
    sectionizer from `data/curated/waters/cuts.json`. If resolution could split, editing a
    regulation would silently change section geometry.
11. **Reaches resolve by ROUTE MEASURE on the cut's own blue line, never by a flow walk.**
    A `between` spanning two blue lines is the intersection of two half-lines. A flow walk
    was tried and is wrong: `upstream_of` and `downstream_of` the same cut both returned 36
    of the Chilliwack's 39 sections.
12. **The builder never decides where a regulation applies.** It reports; the curator
    decides. An earlier version auto-included "straddling" pieces into closures, reasoning
    over-closing is safe. Measurement killed it: **all 45 unclassified pieces are DETACHED,
    none are straddlers**, and it attached a 380 m stub hanging off Cowichan Lake to
    "No fishing, Cowichan Lake outlet to Greendale Trestle". Silent widening is a defect
    here (㉗), even in the "safe" direction. A DETACHED piece (linked to its water only through a
    lake, pond, other water or the sea) is placed by its FLOW PATH to the first sections of its
    water (D5, 2026-10-12, `extent.OFF_LINE_PIECES_FOLLOW_THEIR_FLOW`, `test_reach_flow_placement.py`);
    what stays unplaced carries a typed reason (`straddles_the_cut`, `meets_the_line_at_the_cut`,
    `flows_to_the_sea_unjoined`, … in the `unclassified` diagnostic's `reasons`). A BRAID ACROSS A CUT
    IS CUT AT BUILD (RULINGS E17, owner 2026-10-24, T67; E15's majority side is gone): `splits.braid_sweep`
    cuts every channel of the host's own water that attaches to the mainstem on BOTH sides of a cut — for
    every split kind, along the perpendicular to the ±100 m chord — and never one touching a single side
    (South Atnarko); the reach reads the cut on every line it crosses
    (`extent.A_CUT_SEVERS_EVERY_LINE_IT_CROSSES`) and the run FAILS on a piece still `straddles_the_cut`
    (`build.straddle_gate`); EVERY unplaced piece, any reason, a rule's or a DFO place's, stops it too unless the owner
    accepted that piece in `waters/unplaced_piece_calls.json` `accepted` (`build.unplaced_gate`, T632; `proposed` rows are not
    acceptances); a braid piece cut at BOTH ends by a `between`'s own two cuts is inside it (RULINGS
    E18, T187, `extent.A_PIECE_BETWEEN_THE_WINDOWS_CUTS_IS_INSIDE`). Length cuts never take a junction a braid spans. A 2300-only ditch with no gnis
    of its own is in no river item (`registry.build.DITCHES_LEAVE_RIVER_ITEMS`).
13. **A rule's extents are its own — never inherited from its entry. Never widen a rule
    that names a place it could not bind.** No reader (reach builder, bundle,
    `pipeline/deliver/bundle/read/source.py` `source_of`) hands a rule its entry's `extents`; the entry's only clip.
    So every rule says where it is, or `CatalogueEntry` refuses it: `extents`, or its place in
    words — `extent_text` / `unresolved_locators` — and then it stays UNBOUND. Ingest writes
    the whole water (a zone entry: its area) onto a rule that says nothing about location
    (`validate_catalogue.default_extents`), so the file states it. Defaulting applies a 500 m
    closure to an entire lake arm. A place that is a PART of the rule's own water which nothing
    draws ("500 m upstream and downstream of Causeway Road", "on parts") is `undrawn_part` beside
    `[{op: whole}]`: placed on the water as a note, never colouring it (130 rules, 2026-09-24).
    A part that is a place ON the water does not walk the row's tributaries
    (`includes_tributaries: false`). A COMPLEMENT ("other parts", "all other parts") is not an undrawn
    part: it is `Extent` op `rest` with its `siblings` named — the rule's water, with its own tributary
    scope, minus every section those rules bind (`build._build_rest`); a sibling that does not bind
    leaves it `complement_unknown`, never the whole water (Bull, Elk, Findlay, 2026-09-28). What is left with no
    extents (14 rules) names a place that is not a part of the row's water, or a carve-out.
    An AREA rule (every extent `within`) with `unresolved_locators` is a carve-out no cut
    expresses ("No powered boats … except Gold, Upper Campbell and Buttle lakes") and stays
    unbound too (`classify.AREA_CARVE_OUTS_UNBIND`, 4 rules); `Extent.outside_items` can now subtract
    the excepted lakes. A `within(area)` also holds every LAKE the polygon merely touches (lakes are
    never cut): check the lakes an area rule lands on and take a mostly-outside one back out with
    `outside_items` (Kootenay Lake from the Creston Valley WMA, Bennett Lake from the Chilkoot Trail).
    **Licensing records differ:** one with no `extents` takes its entry's at placement
    (`reach.licensing.place_record`). What a designation's `tributary_excludes` removes goes to
    the excluded water's own designation when it has exactly one (`carve_outs_to_owner`);
    anything left unheld is reported by `carve_out_orphans`, never re-widened.
14. **Every rule ends bound, or unresolved with a typed reason. Never absent, never
    bound-and-empty, never unresolved-and-unexplained.** Enforced in
    `RuleBinding.__post_init__` (⑪ + ㊳).
15. **The tributary walk exists; `tributaries_pending` means it could not run.** The reach
    builder walks every rule that extends to tributaries (451 rules carry a `tributaries`
    diagnostic, reach run 2026-09-28). "Tributaries" walks STREAMS only (p.80); "watershed"
    keeps lakes. `report.json` `tributaries_pending` is **8** — exactly the unresolved
    `no_extents` rules whose rule or entry asks for tributaries (no seed to walk from). A
    binding flagged pending is still never complete; nothing downstream may treat it so.
16. **The review app and the builder share one implementation.** `entry_reaches` calls
    `pipeline.atlas.reach.classify`; covered items come from `pipeline.atlas.reach.covered`. That is enforced BY
    CONSTRUCTION (one module; `test_review_reaches.py` drives `entry_reaches` through `reach.extent`), not by a
    whole-corpus comparison: no current run proves every rule identical. Never give the review app its own copy of
    a resolver — the app is where a human signs off, so divergence is invisible until a user hits it.

## Builds and tests

17. **Never rebuild the live atlas to test a change.** Build beside it (`python -m pipeline build --atlas` writes
    `<build>_next`; `--side DIR` for a side build, `pipeline/README.md`) and compare with
    `pipeline/tools/audit/build_parity.py`. How long a stage takes is `python -m pipeline build --status`, not a
    number written here.
18. **`pytest.ini` deselects the `slow` tests by default** (㊷) — 197 of 2,263 on
    2026-09-29 (`pytest --collect-only -q -m slow`, run unfiltered, not through rtk). Determinism and full-build
    guards live there, so CI must run `-m slow` explicitly or they never run.
    **Test tiers (P00, 2026-10-20):** Tier 0 is the one file you touched; Tier 1 is `scripts/test-smoke.sh`, run per
    commit (≈ 75 s: `pnpm -C app check`, `pytest -m smoke` from `pipeline/tests/smoke.txt`); Tier 2 is
    `scripts/test-full.sh` via `scripts/heavy.sh`, once per phase or before a merge. **Heavy jobs** (atlas build, reach run,
    `pipeline.deliver`, tile builds, full pytest, the Android emulator) go through `scripts/heavy.sh <label> <min free %> --
    <cmd>`: one at a time per lane (two lanes: the default and `HEAVY_LOCK=~/.claude/heavy-job-2.lock`), waits for free
    memory, kills the job if free memory falls under 6 % or its physical footprint passes the machine's RAM; stopping the
    wrapper stops the job (T536). **`scripts/heavy.sh --status`** says what holds each lane (command, elapsed, free memory,
    last log lines): ask it before deciding a quiet job died or starting it again — an identical label + command is refused
    while one holds or waits (`--again` to mean it).
19. **Determinism is a precondition, not a nice-to-have.** Sorted iteration everywhere;
    no clocks in output. The reach builder's digest must be identical across runs.
20. **`pipeline.atlas.reach.cache.POLICY_VERSION` must be bumped, with a changelog line, when `classify.py` changes an
    outcome.** A test fails if it goes stale. It is the reach policy's changelog; the per-entry cache that module once
    held was never used and was deleted (T561).
20a. **Fast-fail first, and it grows (owner 2026-11-07, T483).** Before any long job (build, release, APK, emulator,
    full suite), run `scripts/fast-fail.sh`: the cheap checks that stop a doomed job in a minute or two (each item
    is listed there with a "caught:" comment: conflict markers, ticket ledger, smoke.txt covering every pipeline test
    files, changed Python compiles, colours, answers-stale, app typecheck, vitest related to the changed app files;
    2026-11-09: format pairs Python ↔ app/worker, goldens cut from LATEST, no-silent-skips on a changed test file).
    Run it while a build waits, too. `python -m pipeline release --staging` (preflight, with the branch-pushed check)
    and `deploy/staging/build-apk.sh` run it first; the escape (`--no-fast-fail REASON` / `--no-fast-fail=REASON`)
    prints its reason. `FAST_FAIL_ALL=1` goes on past a failed step and ends with the list of every one (T666: the
    release preflight runs it so). Checks that need a release or an out-dir sit where those are known (release preflight;
    `pipeline.deliver all --out-dir` refuses a scratch release root without the released tiles / basemap).
    **Whenever a failure shows up late — after a long job, at review, on the device — that a cheap check could
    have caught, add that check to `scripts/fast-fail.sh` in the same change as the fix**, with a one-line comment
    naming what it caught and when. A late failure that could have failed fast is a missing fast-fail item.

## The app (`app/`)

21. **Greenfield. Inherits nothing from v1's webapp or mobile (the removed `archive/`, T335).** Those are
    prior art: four shared-logic files there diverged completely (`waterbodyDataService.ts`
    is 1,157 lines on web, 116 on mobile) because two apps were built independently and
    neither was ever the shared source.
22. **`packages/core` has zero React and zero platform imports.** `packages/ui` may import
    `react` but never `react-dom` or `react-native`. Enforced by
    `app/tools/check-boundaries.mjs` — fix your code, never weaken the gate.
23. **Status is rendered by ONE function in `core/`, never re-implemented per surface.**
    A coherence review counted **nine status surfaces across four vocabularies** in five
    parallel design decks — map line, tap card, search row, rule line, gauge strip, and more,
    each inventing its own wording for the same five states. That is the §0.5 drift failure
    (1,203 diverged lines) reproducing itself in a new layer before a line of app code exists.
    Navigation may legitimately differ between mobile and desktop — `README.md` sanctions a
    sheet on one and a page on the other — but the *answer* may not.
24. **Hooks are shared; components are not.** A hook returns data, so it runs under both
    renderers. `<div>` and `<View>` do not.
25. **Logic never lives in a component.** That is what keeps a wider arrangement (tablet,
    desktop) a purely presentational choice. If a layout wants to duplicate logic, the logic
    is in the wrong place — move it to a hook.
26. **Mobile web renders the same `packages/ui-native` components as the native app**, via
    react-native-web, so the phone experience matches by construction. **Desktop is a layout
    class, not a second app:** the same `ui-native` components arrange themselves by
    `useLayoutClass()` (`@app/ui` `layout.ts`, T45: phone / tablet / desktop from the
    breakpoint tokens). There is no `apps/web` and no separate DOM component set.
27. **One React version, workspace-wide**, pinned via `pnpm.overrides`. Two Reacts in one
    bundle is `Invalid hook call`, and it surfaces only at bundle time.
28. **Map layers are only ever added by editing `app/packages/map/style/layers.source.json`**
    then `pnpm style:build`. Colours reference tokens by name; literals are rejected. No app
    may import a map SDK — that is how the two apps start rendering different maps.
29. **A categorical colour mode over an enum must colour every member**, and a continuous
    mode must define `missing`. This is how `unknown` and `default_only` cannot be rendered
    as something they are not (⑨/㉜ — the one failure with real consequences).
30. **Every dependency needs a line in `app/deps.md`.** v1 accumulated chart.js + pdf-lib +
    pdfjs + fuse + suncalc without anyone deciding to.
31. **`pnpm check` must pass.** Its steps are `app/package.json` `scripts.check` (and `app/MAP.md`), not a list
    retyped here: the gates, the generated-file checks, knip (no unused file, export or dependency; allowances with
    reasons in `app/knip.jsonc`), typecheck, vitest.

32. **A screen may not assert a fact about the data it is sitting on — it asks.**
    `App.tsx` passed `waters={255} reaches={6967} surveyed={35} stations={5}` and a fixed
    `fetchedAt`, all transcribed from a design mock-up whose fixture is one valley. The
    shipped bundle holds 19,699 waters and 2,324 stations, and the Layers sheet reported
    five of them under a heading reading "EVERY VALUE HAS AN AGE". Counts come from
    `source.counts()` and the feed's own index. A figure that is not available renders as
    NOTHING — `count()` returns undefined for null, because "we have not asked" and "we
    asked and there are none" are different claims and only one is safe to show.

33. **Assert the whole composed string, never the fragment you interpolated.**
    `GaugeBadge` built `` `It ${trustWord(t)}.` `` and the fragment only read after "It" for
    one of three bands — "It a major branch of it." rendered for 97.7% of gauged sections.
    A test aimed at that exact line passed the entire time, because it matched
    `/a major branch of it/`, which is equally true of the broken sentence. A regex over a
    substring you supplied cannot fail. Sentences are composed in `core/` where a test can
    pin the finished string.

34. **`accessibilityState` is dead — use the ARIA props.** react-native-web 0.21 dropped it
    silently: the prop is ignored and the element renders with no attribute at all. Ten
    components used it, so every radio group, tab and disabled button in the app was
    invisible to a screen reader while looking correct, because the state was also carried
    by a background colour. `aria-checked` (role=radio) / `aria-selected` (role=tab) /
    `aria-disabled` work on BOTH targets. `app/tools/check-platform.mjs` fails on the old
    spelling.
34a. **Web and native code are split by FILE, never by branch (T410, owner 2026-11-06: "so it doesn't diverge").**
    Code that differs by platform lives in a pair, `foo.web.ts(x)` + `foo.native.ts(x)`, typed by one `foo.d.ts`; the
    bundler picks one, so the other platform's code is not in the bundle. Both halves export the same surface and do the
    same job; a fix to one half is checked against the other in the same commit (say so in the message when the other
    needs nothing). In shared files: no `Platform.OS`, no `typeof window / document / navigator` — a one-value difference
    (a size, a flag, a word) may use `Platform.select`, or one `Platform.OS` in a file `INLINE_ALLOWED` lists with its
    reason. `packages/core` stays platform-free (rule 22); the map's `src/web/` and `src/native/` folders are imported
    only from their own platform. A behaviour fix (lag, gestures, a11y) that only one platform needs still goes in that
    platform's file, never as a branch in shared code. Enforced by `pnpm platform` (`app/tools/check-platform.mjs`, in
    `pnpm check` and CI) — fix the code, never widen `INLINE_ALLOWED` to pass; a new entry needs the owner's reason.
    Details: `app/README.md` "Web and native code".

## Curated data, and the artifacts you must not regenerate casually

35. **Three kinds of data, and confusing them is the recurring bug in this repo.**

    | kind | example | a rebuild may | losing it costs |
    |---|---|---|---|
    | **authored** | `waters/cuts.json`, `name_variants.json`, `regulations/book_name_links.json` | only READ it | human hours |
    | **reviewed** | `gauges/matches.json`, `added_streams.json` | only READ it | human hours |
    | **generated** | anything under `data/generated/` | rewrite it freely | CPU |

    "Reviewed" is the one people get wrong: a machine produced it, a human then checked it,
    and **re-running the generator throws that review away**. It is curated data that
    happens to have been typed by a program.

36. **Read curated paths through `pipeline.common.curated.CURATED`, never as a literal.**
    `from pipeline.common.curated import CURATED` then `CURATED.waters.cuts`. The tree is pydantic-
    validated at first access, so a bad path fails naming the key instead of returning an
    empty list four builds later. `ProjectConfig.get_path` used to return `Path()` for a missing
    key (it raises since T435) — silently the current directory — which is how a `--splits` flag with no default
    dropped all 376 curated cuts from three full builds with nothing looking wrong.

36a. **Curated names (T324, owner 2026-11-05):** `*_calls.json` = the owner's per-item decisions, `*_waters.json` = a
    list of waters, `*_stations.json` = a list of gauges; every dict-shaped curated JSON has an `_about` (what, who
    writes, who reads), every curated folder a README. Machine output (proposal files, the gauge review queue) lives in
    `data/generated/`, never curated. Hand-edited waters files: `python -m pipeline.tools.audit.check_curated --waters`
    (seconds; the build's first step). Build stage keys hash curated files by CONTENT, so a move or rename reruns nothing.

37. **A loader for a curated file must RAISE on a missing file, never return `[]`.**
    Absent curated data is a bug, never an empty set. This is the same failure as ㉟ one
    layer down, and the grep gate in `pipeline/docs/HANDOFF-curated-layout.md` §3.3 is what
    keeps it from creeping back.

38. **Never regenerate a reviewed artifact to "check something".** Regenerating
    `gauge_match.json` needs a completed build and rewrites 2,324 hand-checked decisions,
    including 23 `NO_MATCH` entries a human verified against the map. Run
    `python -m pipeline.tools.audit.check_curated` to see whether it is actually stale; it prints
    the command and never runs it. If it says `ok`, there is nothing to do.
    The matcher writes `data/generated/gauges/match.json` and refuses the curated file; only
    `pipeline.gauges.generate.promote` writes `data/curated/gauges/matches.json` (and the review queue
    `data/generated/gauges/candidates.json`, T324), one
    station at a time with `--only <station…>`. A full promote lists every demotion and refuses
    without `--accept-demotions`, which is the owner's call after reading the list (T94).

39. **The two tiers are not interchangeable.** The *mill* (fetch → build → match → bundle →
    tiles) is heavy (minutes and gigabytes: `python -m pipeline build --status`) and runs by hand a few times a year. The *guards*
    (`pipeline-ci.yml`, `app-ci.yml`) are seconds and run on every push. Never add a step
    to CI that needs `graph.pkl` — a hosted runner has 14 GB of disk and one build directory
    is 10, and a job that fails for unrelated reasons is one people learn to ignore.

40. **A value that exists on both sides of the Python/TypeScript boundary is generated,
    not typed twice.** `pipeline/gauges/consume/shed.py::TRUST_BANDS` is the source;
    `python -m pipeline.tools.emit.gauge_policy` writes the TypeScript and JSON, and
    `--check` fails CI when they drift. Three copies of the trust rule once existed — the
    pipeline's, one in `@app/core`, one in `build-fixture.mjs` with a band the pipeline has
    never written — and a test pinning the *constants* passed the whole time, because they
    agreed on the thresholds and disagreed on the rule.
    **T556 (2026-11-11):** a live feed's format string is typed once, in `pipeline/common/feed_formats.py` (TypeScript:
    `FEED_FORMATS`, `python -m pipeline.tools.emit.feed_formats`; `scripts/check-format-pairs.py` refuses the literal
    anywhere else). WHERE A RULE MUST EXIST IN BOTH LANGUAGES (the feeds worker types DFO rows live), ONE SHARED COMMITTED
    FIXTURE IS THE PIN: `python -m pipeline.tools.emit.dfo_vectors` writes what the Python reads on the saved DFO pages
    (fingerprints, typed rows, calendars, name keys), the worker's `dfo.vectors.test.ts` must reproduce it, and hand rows
    both sides test live in one file (`app/workers/feeds/test/fixtures/dfo/dfo-rows.json`) — never a copy per side. A
    Python-backed worker suite FAILS without `.venv` (`needPython`), it never skips.

41. **There is no `output/`. Generated paths come from `pipeline.common.curated.GENERATED`.**
    `from pipeline.common.curated import GENERATED` then `GENERATED.build()`,
    `GENERATED.tiles`, `GENERATED.regs.parse`. Everything a program writes lives under
    `data/generated/`, so the three kinds of data are one listing (`source/`, `generated/`,
    `curated/`) instead of three plus a fourth that meant the same as one of them.

    `output/` was retired because it drifted and nothing said so: six of its declared
    directories — `output/pipeline/{graph,atlas,matching,anglerinfo,hydro,deploy}` — had
    never existed on disk, and `default_registry_path()` handed seven parser tools a
    `registry.json` inside one of them. A missing output directory is created on demand, so
    the class of bug is silent by construction.

    **The rule that replaces it is asymmetric.** A WRITER may create its directory; a READER
    may not. Read a build through `GENERATED.require_build()` / `GENERATED.registry()`,
    which refuse a directory that is not there and print the command that makes it. Never
    `Path("data/generated/...")` as a literal — that is `output/v2/full` hard-coded twice
    with a new prefix.

## Working style

<!-- 42-44: these were numbered 32-34, which collided with the app section. The app rules
     run 21-34 and the curated ones 35-41, so this section continues from there. -->

42. **Measure before asserting.** Every number in the docs is reproducible; several
    "obvious" designs here were killed by one measurement (the hash id, the straddler
    policy, parallelism in the reach builder).
43. **Do not build parallelism in the reach builder.** Full corpus resolves in ~0.1 s.
44. **Report honestly what you did not verify.** An unverified claim is worse than a known
    gap. Say "scaffolded, not run" when that is what happened.
W45. **Spend the owner's credits on the work, not on a long context (owner 2026-11-06).** Every turn re-reads the
    whole conversation, so a long chat costs more per step and remembers worse.
    - **Start a new chat** at a natural break: after a release ships, after a lane merges, when the next item is
      unrelated to what is in context. Before it: STATUS.md top entry current, a short
      `handoff/PLAN/NEXT-SESSION-*.md` (what is in flight, what is next, in order, with ticket ids), commits pushed
      where allowed. Then tell the owner it is a good moment for a new chat.
    - **Compact** (rather than a new chat) when mid-task with live context worth keeping — a background build or
      agent still running, a half-merged lane. Write STATUS first so the summary loses nothing.
    - Say so early, not at the limit: when the context is large and the next step is a fresh topic, propose the new
      chat instead of continuing.
    - Delegate wide reads, long measurement loops and emulator work to agents (their file dumps stay out of the main
      context); keep the main chat for decisions, merges and the owner. Agents report briefly.
    - Avoid re-reading big files and logs: grep for the lines needed; tail logs; do not re-run full suites per
      iteration (once at the end).
W46. **The guards the 2026-11 audits put in (T429–T442) stay in. A change that trips one fixes the cause; it never
    loosens, skips, or works around the guard.** Each was added after real damage or a hidden failure:
    - **No silent fallbacks, anywhere** (owner 2026-11-03): no `try/catch` that turns an error into an empty answer, no
      `or ROOT` / default-path fallback, no `|| []` over a failed read. A failure is shown AND reported
      (`reportFailure` in the app, a 503 naming the cause in the review backend, a raise naming the file and the
      command that makes it in the pipeline). Deploy/staging scripts fail on any tool error (rclone, wrangler) —
      never `|| true`, never a pipe that hides the exit code (`set -o pipefail`).
    - **Review write-through refuses a big shrink** (`pipeline/regs/review/apply.py` SHRINK_LIMIT): a write that drops
      more than 10 rows from a curated file is refused. `--accept-shrink` is for a shrink the OWNER asked for, never to
      get a run through. Review loaders raise on a missing input.
    - **Paths come from `config.yaml`** (CURATED / GENERATED / SOURCE, `project_config.py` (repo root) `get_path`, which raises
      on an unknown key): no literal `"data/..."` path in pipeline code (CI greps for it; tests are exempt).
    - **Review app API**: the route table in `curation-review/API.md` is generated (`python curation-review/api_routes.py
      --write`); a route added or removed regenerates it in the same commit. No "splits" domain (lake splits are
      deferred).
    - **Ticket ledger** (T539): `node app/tools/check-tickets.mjs` must pass (in `pnpm check` and fast-fail). It checks that
      every ticket file has a Status line and a ledger row whose leading keyword agrees (T540: the ledger is `tickets/README.md`
      plus `tickets/DONE.md`, which holds only DONE / DROPPED rows; a row sits in exactly one); DONE names its commit hashes
      (commit the code first, then the ledger); IN REVIEW / PARTIAL name their step as `owner: OWNER-TODO "<text that
      exists in OWNER-TODO.md>"` or `left (Claude): …` (a ticket whose only step is Claude's is IN PROGRESS). The same
      script is the docs guard: STATUS.md starts with its title, every repo path cited in START-HERE / the restart page /
      OWNER-TODO exists, the restart page holds no release id or build number, and AGENTS rule ids are unique (the
      lettered ones are `A` Absolute, `W` Working style; the Reading section's 45/46 are the numbers other files cite).
      It does not compare the Status text with the row's text beyond the keyword. After a cherry-pick, remap the hashes
      the tickets cite.
    - **Smoke coverage gate** (`pipeline/tests/smoke_pending.txt`): the list only shrinks; a new module gets a smoke
      test, not a line in the file. Golden parity is strict.
    - **No frozen prototype oracles**: `golden.json` and its tests were retired (T439); the regs oracles are
      release-backed (`app/fixtures/golden/taps.json`, re-cut by `node app/tools/regs-recut/sync.mjs`). The release
      re-cuts EVERY release-backed fixture itself, on its candidate, before the promote (T666); when LATEST moved any
      other way, re-cut them before an app build: `node app/tools/regs-recut/sync.mjs` does
      them all in one step (T441: taps, hydrate, the fixture release, `dev/section-lines.json`, VM fixtures); no test
      types a raw section id — name it by stream key + start metre (or a lake by water + part) through `sectionNames`
      (`@app/data/release/node`), so a renumbering is no test edit and a moved section fails by name.
    - **Test anchors (T667, owner 2026-11-11 D4)**: a test stands on data it builds, a frozen fixture, or THE DATA UNDER
      TEST through the one helper — the app's `liveRelease()` / `namedRelease()` / `openRegsFixture*` / `sectionNames`
      (`.stream`, `.lake`, `.part` by its ends, `.partAt`), the pipeline's `pipeline/tests/under_test.py` (never
      `GENERATED` / `CURATED` / a `data/…` path / an override variable / `LATEST` in a test file; sections by blue line
      + metre through `under_test.sections`). `RELEASE_DIR` names the release under test in both suites (the release
      command's candidate), else LATEST. The scanners (`app/tools/check-test-anchors.mjs`, `pipeline/tests/anchors.py`;
      the rule is each one's header) FAIL on every offender since 2026-11-11 — no baseline, no list of excused files:
      `pnpm anchors` (in `pnpm check` / `check:fast`), `python -m pipeline.tests.anchors` (in `scripts/fast-fail.sh`) and
      `test_test_anchors.py::test_the_live_tree_has_no_offender` (the pytest suite); the failure says what to write
      instead. Never reword a hit past the scanner; a rule that is wrong for a case is changed in the scanner's header
      with a test for the accepted case and for the near-miss that must still fail. Printed text is pinned as text,
      never by a hash of it (a short hash reads as a build digest, and a failure should show the words).
    - **Feeds worker**: its tests gate every deploy (`deploy/staging/feeds.sh`); `REPORT_SALT` is a required secret
      (no salt = 503, never an unsalted report). Crash/report enums match the app (wire parity test).
    - **Staging**: `deploy/staging/upload.sh` refuses commits not on GitHub, clears old releases only after the
      pointer flips, and re-points the test channel (`test/`, T442). Production is human-only, in two
      steps: `CONFIRM_PROD=<r-id> python -m pipeline release --prod <r-id>` (the release id, typed again at the prompt),
      which then runs the deploy scripts with `CONFIRM_PROD=canifishthis.ca` (their own guard; production APK / iOS
      builds need that form). An agent never sets either.
    - **Test bed**: a walk that matters includes a FRESH install (`install` first): today's walks passed on an app
      that already held data and hid a broken first run. `--no-r2` runs check `test/` is the live release and stop
      if not.
    - **Dead code stays dead** (knip in `pnpm check`): no unused file/export/dependency; an allowance needs a reason
      in `app/knip.jsonc`. Generated native projects (`apps/mobile/ios|android`) are not source: tools walking the
      tree skip them.

## Reading the regulations

<!-- 45-53: rulings from 2026-09-25 to 2026-09-29 that were only in commit messages and memory.
     Appended, not interleaved, so the numbers above stay stable. The mechanics are in
     2026-10-CURRENT-STATE.md (history: `git show d1b36ca44:pipeline/docs/archive/…`); the reference reader is read/ladder.py `effective_rules`. -->

45. **The book's species list (p.80) is closed, and a bull trout IS a Dolly Varden.** 22 fish
    in TROUT/CHAR/WHITEFISH/BASS/OTHER (`catalogue.BOOK_FAMILIES`); any other code is refused.
    `BT` is refused — "bull trout" is `DV` (p.80: "Any bull trout that you catch and keep must be
    counted as part of your Dolly Varden quota"). Chinook `CH` is the one salmon the book names:
    in SALMON, never a game fish. (29ef0a20, aeb070cf)
46. **"Trout" is `TROUT_CHAR`; it excludes char only by (a) or (b).** Trout includes char unless
    (a) the line excludes char in so many words, or (b) the same row or zone table SPECIFIES CHAR
    SEPARATELY WITH A RELATED RULE of its own — one governing the same aspect (how many are kept /
    released / closed, sizes, gear: `catalogue.rule_aspects`), kind of water and days
    (`catalogue.related_rules`) — which is the same as excluding char (user ruling 2026-10-07,
    TROUT/CHAR CLARIFIED, superseding the 2026-09-28 "mentions char" reading). Region 1's "2 from
    streams (must be hatchery)" beside "All char" release: trout only; "no trout under 25 cm" beside
    "bull trout catch and release" (another aspect): covers bull trout. A combined "trout/char" never
    excludes char. Such a line carries `species_except: [CHAR]` — never one char alone; `TROUT` is
    refused; `trout_scope_problems` refuses it both ways.
47. **A `;` ends a dating run.** "Trout/char catch and release; bait ban, June 15-Oct 31" dates
    only the bait ban; an "and"/comma chain shares the date. The model refuses a `when` that
    crosses a `;` and a quote that prints dates its rule does not carry. (3b5c6e9c, 1c8dfac8)
48. **A pointer is `see`, never a rule.** "See Lonzo Creek" goes in the entry's `see` list
    (`entry_ids`, or `unresolved` with a reason); the model refuses an `advisory` that is a
    pointer. A pointer never moves a water into the target's region (Mara Lake). (3b5c6e9c)
49. **Half of a river is `side`, not a part and not the whole.** "No Fishing on the west half of
    river …" is `side: west` on the stretch; it is read "beside" the other rules and displaces
    none, so the other half follows the water's other regulations. A lake's half is
    `undrawn_part`. (38149b92)
50. **A water's own dates override a dated zone rule on the overlap only.** A water row printing
    its own dates for a fish replaces a dated zone release or quota for that fish on the days
    both hold (Cheslatta/Murray lake trout Nov 1-30: the lake's 3) — never a closure. An undated
    water quota never silences a dated zone release unless it is the exact same statement
    (Shuswap char 1 vs Region 3's lake trout release Oct 15-Jan 31). (aeb070cf, 38149b92)
51. **"(any size)" is a caution, not a reading.** A larger water quota overrides the zone's "1
    over 50 cm" either way; only where the row prints "(any size)" does it carry a structured
    interpretation caution (no size limit, or just no minimum?). Rows with their own sizes or none
    get no caution. (aeb070cf) SETTLED by the owner 2026-10-16 (RULINGS Q5a): "(any size)" drops EVERY
    size constraint of the region for that fish — the bundle derives the lifts (`rules._any_size_lifts`,
    `exempts[].basis: any_size`; Robb Lake's bull trout under Zone 7A's 1 over 50 cm).
52. **A zone release limited to a kind of water displaces its own table on that water.** A zone
    release with `water: stream` (extents `feature_types: [stream]`) in force on a stream
    displaces its OWN region's quotas and clauses that keep the fish in the same base dimension
    ("daily"), as a no-water release does (`read.released_on_water`, `effective_rules` step
    4b): Region 3's "Bull trout (Dolly Varden) from streams, Aug 1-Oct 31", Region 4's
    "Trout/char release: in streams from Nov 1-Mar 31". Closures, water rows and other regions'
    rules are untouched. A size clause with no count ("none under 60 cm", `daily/size`) is not
    reached here but by step 5b (rule 57, MOOT SIZE CLAUSE). (2026-09-29, working tree)
53. **Competition lives in one function.** `pipeline/deliver/bundle/read/ladder.py::effective_rules` is
    the reference reader: per fish, per day, lifts, naming before place, water-vs-zone quota
    rulings. The export `guide` restates it; the app must match it. Change a ruling there and in
    the guide together.
54. **Steelhead regulations come only from the book; the curated list is a presence indicator.**
    (user rulings 2026-10-01/02, `pipeline/atlas/reach/steelhead.py`.) The provincial and zone
    steelhead rules bind STREAMS of Regions 1, 2, 3, 5 and 6. A STEELHEAD ROW is a water row ANY of
    whose rules or licensing records PRINTS steelhead (`steelhead.prints_steelhead`): a rule naming
    `ST` or saying "steelhead", or the Steelhead Stamp in any wording — "Steelhead Stamp mandatory
    <dates>" (Kingcome, Babine …), "(Steelhead Stamp not required)" (Chilko …), "not required unless
    fishing for steelhead" (Seymour, Ecstall, Skeena) — or one flagged `anadromous_rainbow`
    (Chilliwack/Vedder, by ruling). A Classified Water designation that prints no steelhead (Region
    4's 25 "Class II water when open" rows) is not one. 57 rows (user ruling 2026-10-02, as
    corrected). A steelhead row's OWN
    water (its matched waters within its scope, held as its rules are) and every STREAM section ANY
    rule of it binds are BOOK-KNOWN (the Stellako in 7A; the Kingcome, whose row has no rule, only
    its Class II water and stamp). A LAKE (or wetland) is book-known ONLY as a steelhead row's own
    water — its own row prints steelhead (Khartoum, Lois: "hatchery steelhead") — never because
    another line of a steelhead row binds it, and never by the curated list (user ruling 2026-10-06,
    `steelhead.LAKES_ONLY_BY_OWN_ROW`): Tenas Lake, bound by the Atnarko's spring closure but whose
    own row prints only "No Fishing Apr 1-June 30", carries no steelhead rule, stamp or zone
    steelhead release (resident big rainbow are rainbow). **There is no tributary walk.** Book-known water carries the whole provincial set
    (annual hatchery 10, wild release, record duty, stamp) through the TWINS `zp:steelhead`
    r1b/r2b/r4b and `steelhead_targeting_known`, whose one extent is `{op: steelhead_waters,
    siblings: [<base>]}` (book-known minus the base's sections; only `build_reaches` resolves it —
    `build_reach` alone says `needs_corpus`); a book-known LAKE also gets its region's zone wild
    release through that line's twin `<release>b` (`z1` r5b, `z1:hg_quota` r6b, `z2` r7b, `z3` r5b,
    `z5` r6b, `z6` r9b), whose extent adds the zone's `area_id`/`outside_area`. Never put an item or
    region extent on a base steelhead rule: it changes its competition key and ranks it as a water
    rule (`read.source_of`). STEELHEAD RULES APPLY on a section bound by every rule of the
    provincial set, base or twin (`steelhead.Presence.rules_apply`; the export's
    `steelhead_rules_apply` is the same predicate). `anadromous_rainbow` (a rainbow over 50 cm is a
    steelhead) holds exactly on KNOWN ∧ STREAM (rule 55) ∧ STEELHEAD RULES APPLY — book-known or on
    the curated list alike (the Cowichan River) — and on an unnamed pond a steelhead row's own river takes as
    its T153 widening (`Presence.anadromous`, `A_STEELHEAD_RIVERS_WIDENING_IS_ANADROMOUS`, Q152: the pond is the
    river; the Kitimat's pond). "If no steelhead rules exist, rainbow rules still
    apply to a steelhead" (user ruling 2026-10-03): a known part no steelhead rule applies to
    carries `steelhead_rules: false` in the export, with the display line "Steelhead have been
    recorded here, but no steelhead rule applies: treat any rainbow, however big, as a rainbow
    trout."
    **The stamp waiver.** An OUTRIGHT waiver ("(Steelhead Stamp not required)") means NO steelhead
    stamp on that designation's sections while it is in force (user ruling 2026-10-02): the
    classified-water stamp, and every requirement carrying `waived_where: steelhead_stamp_waived`
    — the provincial `steelhead_targeting` and its twin `steelhead_targeting_known`. A dated lift
    the reader applies per section and day (`read.requirements_in_force`; the export's
    `stamp_waiver`). The Classified Waters Licence and every steelhead rule still apply there. A
    waiver "unless fishing for steelhead" (Seymour, Ecstall, Skeena River 2) lifts the
    classified-water stamp only.
    **THE CURATED LIST** is `data/curated/waters/steelhead_waters.json` (`CURATED.regulations.
    steelhead_waters`): `{"$comment": …, "generated": {source, generator, date, fingerprint},
    "waters": [{"item_id": "wbk:…", "note": "…"}, {"name": "Cowichan River", "region": "1"}]}` —
    exactly one of `item_id`/`name`; `region` ("1".."8") / `mu` ("1-4") choose among same-named
    waters. **It binds no rule**: it makes its waters' own sections `steelhead: known`
    (`steelhead_source` `"curated list"`) — no rule binding, twin or stamp; `anadromous_rainbow`
    follows on a listed stream only where steelhead rules already apply. In the steelhead regions it
    turns "possible" into "known" (and a big rainbow is a steelhead there); elsewhere (the Okanagan
    River, Inkaneep and Vaseux creeks in Region 8; the Fraser in Zone 7A) a listed stream is known,
    carries no steelhead rule (`steelhead_rules: false`), and its rainbow rules answer for every
    rainbow (the Okanagan's "Rainbow trout catch and release"). `test_steelhead_waters.py` pins by
    mutation that adding any water to the list moves only the presence code and `anadromous`. The reach builder refuses an unknown, ambiguous, contradicted or duplicate entry;
    the bundle refuses a reach run made with a different list. The export's
    `waters[].steelhead_source` is `"regulations"` and/or `"curated list"`; `steelhead_rows` names
    the rows. **THE LIST IS GENERATED BY A SEPARATE STEP**, `python -m
    pipeline.regs.steelhead.known_waters` — NOT part of the atlas build, the reach run or the
    bundle. It fetches once into `data/source/steelhead/` (cached; `fetched.json`), keeps its hand
    review in the module (`HAND`, `REVIEW`: REVIEWED data, rule 35 — edit by hand, never
    regenerate), writes `data/generated/steelhead/`, and a HUMAN copies `steelhead_waters.json` to
    the curated path. `test_steelhead_waters.py` fails while the curated copy's fingerprint and the
    generated file disagree. Do not run it to "check something" (rule 38).
55. **A slough is a stream, and the water kind is decided ONCE, in the registry.** (user rulings
    2026-10-03.) `item.kind` IS the water kind: `stream` for every flowing water — including a water
    FWA draws as a POLYGON whose name's head noun flows (slough, canal, channel, river, creek:
    `pipeline/common/water_kind.py::flows`, the last word before any "at …"/"near …", so "Bear Creek
    Reservoir", "Corn Creek Marsh", "River Lakes" stay lakes) — `lake`/`wetland` otherwise. The registry
    pass `pipeline/atlas/registry/flowing.py` folds such a polygon into its river's item (the Stellako's
    wide reach, the Rancheria's 42, Nicomen Slough's 4 unnamed polygons, the Vedder Canal into the Vedder
    River `gnis:3062` by FWA's gnis — user ruling) or makes it a stream item of its own (Six Mile Slough
    `gnis:6438`, Hansen, Bowman, Lewis, Taylor — a slough threaded by a creek of ANOTHER name does not
    join it, user ruling, `THREADED_JOINS`). EVERY consumer reads that one answer: the reach
    (`reach/water_kind.kind_of` = the owner's kind, read by every `feature_types` filter, the tributary
    walk and the confluence step; a rule's `water` is enforced through `feature_types: [water]`, so every
    "in streams" rule follows with no per-rule change), `regions.in_region`, the bundle (`item.kind`),
    the status index, the export (`waters[].kind`), the tiles (`water` on lake/wetland features; a
    stream's polygon route is in the `stream` layer) and the apps. NOTHING recomputes `flows` after the
    registry (`test_one_water_kind.py` is the gate; the only other reader is the steelhead list
    generator). The SHAPE a section is drawn as is per section (`section_span.shape`), never the water's
    kind: a river's polygon ON its stem is placed by its measure window (`graph/windows.polygon_window`,
    one function, no stored field) and runs with the line through it. ABSORBED IDS: an absorbed item's id
    is in the survivor's `aliases` (bundle `item_alias`); it is mapped to the survivor at exactly three
    read points — `parsing.io.read_entryfile(registry)` (the reach CLI, the bundle, the review app),
    the DFO `entries.load(registry)` and `steelhead.resolve_list` — and REFUSED everywhere else
    (`validate_catalogue`, `build_reaches`, `flowing.absorbed_refs`); the curated files name survivors
    only. A LAKE CUT INTO PARTS owns no section (Kootenay, Williston, Shannon: the parts tile it to
    99.98 %): the bundle refuses a parent with one, search finds the parts, the tiles do not draw the
    ghost polygon, the export lists it with `divided_into` and its parts' entries.
56. **One data flow: every derived fact is computed once and READ everywhere else** (data-flow
    review, 2026-10-03; `pipeline/tests/test_one_data_flow.py` is the gate). The atlas writes its
    sidecars (`region_home.json`, `registry.part_of`, `splits.resolved.json` with `source` and the
    authored offsets); the reach run writes `tidal.jsonl` / `outside_bc.jsonl` and `rules` (steelhead
    rules apply) in `steelhead_presence`; the bundle READS them (`section_home`, `tidal`, `outside_bc`,
    `steelhead_known.rules`, `item.part_of`, cut labels and offsets) and proves the stored form
    reproduces the run — it opens NO curated waters file (`CURATED.` under `pipeline/deliver/bundle`
    may name only `regulations.synopsis` (the corpus), `regulations.reference` (the glossary's book text) and
    `waters.continuations` (the owner-reviewed links); `test_one_data_flow.py`). "Closure" is `rules.closure_grade`, asked by the status index, the
    competition and the export. A PROMOTED ATLAS IS IMMUTABLE: `pipeline.atlas.build` refuses an
    `--out` whose handle digest a shipped bundle or tile set carries; the review app builds to
    `<build>_next` and promotes by `pipeline.atlas.promote`. `python -m pipeline.deliver` chains
    bundle → status index → export (each still runs alone); the index header (version 2) carries the
    bundle's `reach_digest` beside the handles, and the app refuses a mismatch. An atlas built before
    the sidecars existed gets them from `python -m pipeline.atlas.sidecars` (same inputs, never a
    rebuild). THE READER RUNS ONCE (DATAFLOW, 2026-10-08): the bundle decides the rule order
    (`rule_ix`), each section's rule key (`section_ruleset.key_ix`), every rule's `closure_grade`
    and THE PARTS of every named water (`part`/`part_section`); `python -m pipeline.deliver
    verdicts` writes `verdicts.sqlite` beside it (the traced reader per rule key × reading × fish ×
    origin, typed and CHECKed, never shipped); the status index, the export and the answers
    (answers/2, `pipeline/deliver/answers/model/`) LOOK ANSWERS UP there. `test_dataflow_gates.py` refuses a reader
    call, a `may_target` read, a part grouping or a calendar spelled anywhere else.
57. **The 2026-10-03 rulings, as built (Phase 3).** Classified Waters: ONE UNIT PER SECTION — a
    walked section takes the designation of the FIRST classified water it flows into (the nearest
    downstream water with a designation of its own: Gosnell → Morice, not Bulkley; the Nanika above
    Morice Lake → Morice, handed on from the Bulkley's walk), and a tributary with a ROW OF ITS OWN
    that prints no designation is NOT classified, it and everything above it (the Endako under the
    Stellako). ONE ENTRY PRINTING TWO UNITS on two stretches (the Zymoetz: A below Limonite Creek,
    B above): a walker's own water is no stop EXCEPT its sibling designations' reaches, so each
    walked section takes the one stretch it actually flows into (115 Limonite/Zymoetz sections
    read both). `reach.licensing.first_classified_downstream`, every move a diagnostic
    (`first_classified_downstream`, `own_row_not_classified`, `inherited_from_upstream_walk`);
    `test_licensing_first_classified.py`, `test_phase3_rulings.py`. A joining water WITH A ROW OF
    ITS OWN that signs pull into a cut is bounded like a `between` — its stem from its mouth to its
    FIRST LAKE, its tributaries below the lake with it (`reach.build._stem_to_first_lake`; the Nass
    takes the Meziadin to Meziadin Lake, never Hanna/Tintina/Strohn); one with no row still goes
    with the cut whole. A water the book's geography puts OUTSIDE an area its polygon touches is
    stated ONCE on the area definition (`areas.json` `outside_items`, Kennedy Lake / Pacific Rim)
    and applied to the registry's area item (`registry.outside_area_items`, build + sidecars;
    IDEMPOTENT — the sidecar step reapplies it to a registry that already holds it), so
    the closure, `outside_area_kind`, `province_except` and the designations all agree — never on a
    rule, never in a reader. STRICT LIFT RULING (user, 2026-10-05): a zone closure and a water's
    row BOTH apply — the water is closed on the UNION of their closed dates, and a row's own
    closed season NEVER shortens the zone's (the Thompson below Kamloops Lake: Oct 1-May 31 ∪
    Jan 1-Jun 30). A row beats a zone closure ONLY where (a) the book prints an exemption (the
    row's "exempt from spring closure", or the zone page lists the water: Region 5's "other
    streams listed in the tables", p.42), or (b) the row prints a dated catch and release,
    opening or quota INSIDE the closure — lifted on exactly those dates, for exactly those fish
    (`nicola_river.r3x`: trout only, Jan 1-Feb 28; the whitefish stays closed). The p.28 list
    naming the Stein and the Nahatlatch below its lake is a STEELHEAD closure list, not an
    exception list: `stein_river.r1x` and `nahatlatch_river.r2x` are gone, both closed to Jun 30.
    G3 — A PRINTED EXEMPTION CARRIES to the same KIND of closure in a neighbouring region the
    water (or, through the walk, its tributaries) reaches (`rules.equivalent_regions`: the
    water's regions ∪ the zone regions the run bound beside the lifter, `co_bound_regions`) ONLY
    WHERE THE WATER HAS NO ENTRY OF ITS OWN in that region (`own_entry_regions`; a row whose
    every rule is `tributaries_only` is not one): the Fraser (rows in 3, 5, 7) and the Canim
    never carry; West Road's mainstem pieces in Region 6 / Zone 7A keep its lift (only tributary
    rows there) and NO lift reaches a West Road tributary in any region. An exemption marked `own_region_only` (owner
    2026-11-11, RULINGS L22: the Nechako's Region 7 "Exempt from spring closure") never carries. A ✱ ROW'S PRINTED EXEMPTION
    WALKS TO ITS TRIBUTARIES (owner ruling D4, 2026-10-12, RULINGS T13, superseding Q3 of 2026-10-07):
    the row's ✱ carries its lifts like its closures, quotas and gear — no lift-only rule of a ✱ row
    carries `includes_tributaries: false` (Nitinat, Quinsam, Duncan, Lardeau, Dutch, Hevenor, Fulton,
    Babine's Rainbow Alley — that channel's tributaries only — and the Similkameen, whose lift again
    reaches its Region 3 tributaries); `test_rules_round.py::test_d4_*` refuses one that does not walk,
    and the export's `gotchas.star_carries_exemptions` lists them. A row without the ✱ still lifts
    nothing on a tributary (L7). G4 — "open all year" lifts only FULL blanket
    closures, never a species closure: Region 6's steelhead closure (May 15-Jun 15, p.49) holds
    on every Region 6 river and stream except the Skeena, Nass, Iskut, Stikine and Taku
    mainstems (its own sibling lift, which binds exactly those five items). G5 — a row's DATED
    bait ban REPLACES its zone's all-year one (Region 1's Quatse, Somass, Sproat, Stamp, by dated
    lift-only rules); no other region prints that shape. Duck Lake's creeks stay closed Apr 1-Jun
    14 (its bass release does not lift the stream closure). The export's `gotchas` carry
    `closures_combine` (every entry whose own closure overlaps a zone seasonal closure, or whose
    rule the zone closure silences: both hold) and `dated_bait_ban_replaces_zone`, both generated
    from the bundle (`export.closures.closures_combine`, `dated_bait_ban_replaces_zone`); pinned
    in `test_lift_decisions.py`. WHAT A `row_closure` NOTE CLAIMS IS THE READER'S
    (`effective_rules_bound`, once per rule set): "both hold" only on the days and fish the zone
    closure speaks outside the row's dates (`zone_holds`), every lift in part named
    (`zone_lifted`); the Fulton (its "Open June 16-Apr 30" lifts the winter closure) is no note;
    `closures_combine_problems` re-asks the reader for every claim (mutation: the Fulton). READER
    (`read.effective_rules`, each pinned in `test_competition.py`): a same-row DATED release OR
    CLOSURE silences the row's UNDATED counted quota on its dates (RU-3; size clauses are their
    own subject; as built a row's dated closure silences its own quota too — Quatse, the Region 7
    lakes' winter closures, Kitimat's hatchery 2); a zone release with no water kind and no
    `while`/`when_targeting` empties its table's keepers in the base dimension, THE SAME KEY
    INCLUDED (RU-4: the "from streams" clause, and a same-key tie inside one table — the 7B
    grayling release over that table's "2 per day" on its dates); a water's take-0 size band displaces a zone size clause wholly
    inside it for the origins it releases (RU-5); a full closure displaces the keepers it beats by
    the ladder whatever their key (RU-7); two regions' identical statements show once (RU-8);
    "ST" where no steelhead rule applies (`section_steelhead_rules`) is answered as "RB" over
    every length (RU-6; the status index asks the same). A RAINBOW RULE WITH NO SIZE BAND speaks for "ST" too, without naming it
    (owner 2026-11-11, RULINGS S12, `read.size_free_rainbow`: the Stellako's "Rainbow trout catch and release"). MOOT SIZE CLAUSE (user ruling 2026-10-05,
    step 5b): a zone-side size-only clause ("none under 60 cm") is NOT SHOWN under ANY outright
    release or closure in force for the fish — water, zone (with or without a water kind) or
    superior — that releases every origin it keeps over its lengths (Bonaparte Lake Nov 1 lake
    trout; Eleven Mile Creek gnis:8623 Aug 1 bull trout, May 1 lake trout); a hatchery-only
    clause under a wild-only release stays. The release/closure itself is untouched. 10,650
    clause removals in 10,466 of 104,304 sampled answers, nothing else; status index
    byte-identical. DENETIAH (user ruling 2026-10-06, step 4c, `read.WATER_CLOSURE_DOMINANT`): a
    water's OWN full closure in force (a row's rule bound at rank 0 — not by the walk, not an area
    row) is the most dominant rule: it silences EVERY keeping rule for the fish it covers, of any
    source and key (zone, area rows like the Liard watershed's, rows by the walk, possession /
    annual / size-only), save a superior authority's and the closure's own partial lifters.
    Denetiah Creek Jul 1-15 shows only its closure, not the Liard row's bull trout "1 in
    possession". 224 answers over every key changed (all keepers, under 27 such closures of 26
    waters);
    status index byte-identical; `_closure_scan` counts a quota the water's own closure silences
    as speaking (Mahood r2 left `QUOTA_UNDER_CLOSURE_KNOWN`). LOSERS (gap G1):
    `effective_rules(trace=True)` also returns every rule that took part and lost — `state`
    lifted / displaced / moot, `reason` (`read.LOSS_REASONS`, the step that removed it first),
    `by` (the winner's rid); the steps remove rules only through `lose`, so the speakers ARE the
    untraced answer (`test_reader_answers.py`, every key in the slow test). PER ORIGIN (gap G2):
    `effective_rules(origin="hatchery"|"wild")` — a lift limited to that origin lifts outright,
    one for the other origin not at all (the consumer page's reading; the 6 Kitimat lifts);
    `origin=None` is today's answer, "partly lifted". `requirements_in_force` folds a record
    that `restates` a holding one into it (`also_printed`) and holds a zone table's
    `on_designation` restatement on its own region only (RU-12; 7A/7B tables are Region 7 to the
    rows). G5 (adopted 2026-10-06, from the answers layer): it drops a requirement for the other
    KIND of water (`wrong_water`: `water: stream` on a lake or wetland; a section of no named water
    keeps it; a restating record binds as the record it restates, `read.as_bound`), and a
    SUPERIOR authority's requirement (the national park permit) moves every other holding
    requirement with a `satisfied_by` to `displaced` — "provincial licences are not valid here".
    `designations_in_force` is public. The licence answer reads these; it holds no copy
    (`test_requirements_g5.py`, mutation-pinned). GEAR/CONDUCT DIMENSIONS (2026-10-06): a method or
    tackle rule's dimension carries each clause's condition (`slot@when`) and the rule's means
    (`@while=`), a duty its acts (`conduct:a+b`) — never the water kind — so (type, dimension)
    competition sets only the same subject against itself (`test_answers_gear.py`). FEB 29: a range
    printed to Feb 28 runs through Feb 29 (`catalogue.range_days`, the one place dates become days;
    `test_feb29.py`). The export REFUSES a dated water release that speaks under a blanket closure unless
    listed as known (`export.closures.RELEASE_UNDER_CLOSURE_KNOWN` — how RU-2 would have been
    caught), AND a water row's keeping quota in force that a blanket zone closure silences
    (`QUOTA_UNDER_CLOSURE_KNOWN`, 20 as built: RU-7 hides such a quota from the page, so a missed
    exemption shaped like a quota would otherwise vanish). Both read every set, every start day of
    the rule and of the closures, every fish (`_closure_scan`).
58. **The UI export ships ENCODED; the model is `export.model.build()`.** (Phase 4, 2026-10-05.)
    One run writes `ui-rules-export.json` (data: rules/licensing as arrays beside `rule_ids` /
    `licensing_ids`, rule-set members as integer indexes with the zone/province `bases` interned,
    waters' parts and runs positional, defaults and derivable fields dropped, no indent) and
    `ui-rules-guide.json` (`guide`, `field_dictionary`, `species`), same bundle digests.
    `pipeline/deliver/export/codec.py` `expand` is the reference decoder and `main` refuses a pair
    that does not decode to the model or whose integer references dangle (`wire_problems`). Every
    check (`problems`) and every word of the guide describe the DECODED model; a test or tool reads
    the files through `export.files.load`, never `json.load` of the data file alone. A new
    field on a record ships as is; a new SHAPE (a part flag, a run field) must be taught to the
    codec, which refuses what it does not know. Only the lake edges something names ship. Neither
    file names a section: a closure note's example is a key (`example_sid` resolves it in the bundle).
59. **Boundaries are cut once, at the source, and the build only asserts.** (user rulings
    2026-10-06, BOUND round.) THE ATLAS BUILD IS DETERMINISTIC: the braid prune's greedy router
    (`nests.essential_routes`) took its demands and entries in set order, and unnamed waterbodies
    were minted from a set; both are sorted (`test_prune.py::test_the_braid_prune_does_not_depend_on_
    the_hash_seed`; `test_no_slivers.py::test_two_builds_of_identical_inputs_are_identical` with
    `ATLAS_BUILD_TWIN`). THE B.C. OUTLINE is the EXACT union of the `wmu` units the regions are
    dissolved from (`bc_boundary.load_outline`: a valid coverage, cached as WKB; never simplified,
    never buffered; an invalid coverage is refused); the border prefilter queries it in 64-vertex
    chunks (`border.candidates`, 593 s -> 4 s, identical cuts). EVERY AREA is clipped to it in
    `load_area_polys` except the `wmu` coverage itself (`areas.json` `_clip_policy`). HOW an area is
    cut is `areas.json` `cut` (`area_splits.cut_mode`; `true` is refused):
      * `"clean"` (national parks, ecological reserves, the Chilkoot — closures), with a MEASURED
        `rejoin_m` (1,000 m) and its note (`clean_cut.decide_rejoin`): A STRADDLE IS INSIDE — the
        line's inside stretches are joined across every continuous outside run of at most
        `rejoin_m`; an EXIT happens only after a longer run out, and re-entry is a new entry cut;
        cuts sit at the straddle's OUTER crossings; an isolated dip under 5 m (positional error) is
        ignored, as is an outside run that short at a stretch END (the line's mouth or source, a
        lake edge, a border cut — `clean_cut.stretches`); the area is cut only inside B.C.
        (`bc_inside`), and a cut within 5 m of a boundary already on the line IS that boundary
        (`clean_cut.onto_existing`). THE CUTTER ASSIGNS MEMBERSHIP: a stream piece is in the area
        exactly when it lies in the inside the cutter decided (`mark_inside_areas(cutter=)`, no
        overlap test; lakes keep the outline test).
      * `"first_last"` + `"membership": "both_sides"` (regions, MU groups, sign zones, restricted
        land), and the curated `area_boundary` splits: AS BEFORE — first entry and last exit
        (`anchors._area_transition_measures`), overlap membership, a straddle carries both regions.
        A STRADDLER CARRIES BOTH REGIONS' RULES (T183, owner 2026-10-24, reversing T115): a stream piece
        or lake in two region polygons (each holding more than `SLIVER_M` of it, `regions.holds`; a
        touch is not membership) takes both zone tables, both regions' per-region rows and both
        steelhead twins, the most strict per fish (RULINGS Z4) — every cap the stricter of the two, a narrower cap
        never displacing a wider one (`ladder.A_NARROWER_CAP_NEVER_DISPLACES_A_WIDER`), the card's group taking the
        peer table's caps (`rows.groups._peer_totals`) and naming both regions (`scope.both`). ONE function answers it,
        `registry.regions.region_sections` / `region_members` (`test_one_region_membership.py` scans for
        a second copy); the home region (`region_home.json`, `section_home`) is display only.
        Two differences only: a crossing on the outline follows the border's single cut, and a cut
        that would leave a piece under the gate is not made (`area_splits.resolve_area_splits`).
    THE BORDER is cut by the zone rule (`clean_cut.decide`, `border.BORDER_CROSSING_ZONE_M` = 10 m:
    crossings closer than that are one place, cut once at the representative middle crossing; a
    zone at a stretch end is no cut — FWA draws streams on past the province). A cut at the place another boundary holds joins it as an ALIAS and the
    border keeps its name (`sectionizer._coincident`, `SAME_PLACE_M` 1e-6 = float identity); an
    area whose inside ends at a border cut is offered there so the cut carries both names; an auto
    (gauge) split may defer to a lake edge at a line's end (`_pickup`). THE SLIVER GATE
    (`sliver_gate.check`) stops the build on any stream piece under 5 m a cut made, or a clean-cut
    piece straddling its cutter's inside, naming each (`sliver_gate.json`); it repairs nothing, and
    a failure is fixed where the cut came from. Pins: `test_clean_cut.py`, `test_bc_boundary.py`,
    `test_border.py`, `test_anchors.py`, and on a built atlas `test_no_slivers.py` (`-m slow`).
60. **The 2026-10-07 rulings (RULES round).** A ROW WHOSE NAME CARRIES THE GLOBAL ✱ includes tributaries
    for EVERY rule, even when a clause repeats a ✱ (San Juan, Inland, Sooke, Bull River, Kitsumkalum);
    only a row WITHOUT the global ✱ narrows to the clause printing one (p.4: Oyster r2, Anderson r4,
    Dinosaur r1 walk, their other rules do not). A ✱ ROW'S EXEMPTION WALKS WITH IT (rule 57, D4/T13). A TRIBUTARY WITH
    ITS OWN ROW gets both its own and its inherited rules; on one competition key its OWN row speaks,
    before naming (`read.OWN_ROW_BEATS_INHERITED`, Granby under "Kettle River's tributaries") — never a
    closure either way (an inherited closure is only lifted). TWO ✱ ROWS COMPETING ON ONE TRIBUTARY: the row whose water
    is NEARER the stream wins, before naming, never a closure (owner 2026-10-16, RULINGS T14,
    `read.NEARER_ROW_BEATS_FARTHER`): nearer = its own water drains into the other's, read from the bundle's
    `trib_nearer` (built from `item_down`, each water's outlets followed down the graph, `bundle/nearer.py`). A FISH EXACTLY ON A PRINTED SIZE BOUND IS
    LEGAL: a take-0 band does not hold its own bounds (`catalogue.EXACT_BOUND_IS_LEGAL`); the book's "X
    cm or more/or less" is the band's `closed: true`. TROUT/CHAR: rule 46 (a)/(b), relatedness per rule
    pair. A RECORD'S `verbatim` CARRIES NO EXTRACTION MARKUP (`**`,
    `[Includes Tributaries]`): ingest cleans every quote (`catalogue.clean_verbatim`), the model refuses
    one; `regs_verbatim` keeps the batch's text. A `between` whose sentence prints signs ends at the
    curated sign split, never a hydrometric gauge (`test_rules_round.py`).
61. **A rule held on some weekdays or some hours DECIDES at those moments** (user ruling 2026-10-08,
    review D2/G5, answers 2.1). The reader is asked at a MOMENT (`calendar.Moment`: a weekday class,
    inside or outside the key's one hours window; `calendar.moments` cuts each rule key by its own
    rules, a key with two different windows is refused); `read.in_force(when, on, at)` says yes/no
    for weekday and hours rules at a moment and "part" (beside) only without one. The verdicts store
    every moment (`verdicts/2`: `moment`, `segment.moment`, `reading.moment`; 108 keys have more than
    one). A day is CLOSED in the status index only when every moment of it is (a night closure closes
    its hours, never the day — byte-identical index). The answers repeat a segment's start once per
    moment group (`keys[k][10]` -> `segment_moments`, top-level `moments`); the display frame carries
    `closing` (the decided closing rules, gap G1, `verdicts.project.closing`). The export's tidal
    `guide` is angler words only; how to show it is in the field dictionary (B17,
    `test_moments.py::test_no_shipped_angler_text_carries_a_developer_instruction`).
62. **Guardrails (P2, 2026-10-09).** ONE REBUILD COMMAND: `python -m pipeline build` (`pipeline/build.py`; a bare
    `python -m pipeline` refuses) runs clim (the gauge envelope from HYDAT, N9) → reach → tiles → deliver, each stage keyed by what it reads (atlas handles + registry, the corpus,
    the curated tree, source stamps, its own code) and skipped when up to date; `--atlas` builds a side
    atlas and stops after parity, `--promote` adopts it; runs are recorded in
    `data/generated/build-manifest.json`; it ends with the STRICT vintage check
    (`vintage.report(strict=True)`). NO SILENT SKIPS: a test reading generated or fetched data carries a
    `needs_*` marker and FAILS when the data is missing (`pipeline/tests/conftest.py` `need`);
    `test_no_silent_skips.py` refuses any skip outside its exact allowlist; CI deselects by marker. NO
    MISSING-FILE FALLBACKS: a missing input raises, naming the file and the command that makes it (only a
    writer about to create a region file passes `missing_ok`). TWO MORE DIGESTS: the reach run stamps
    `entries_digest` (full corpus content, `io.corpus_digest`) and `registry_digest`
    (`section_handles.registry_digest_for`); the bundle refuses a run made from another corpus or registry
    (`rules.check_corpus`) and records both; the tile sidecar records the registry and the vintage check
    compares registries. The gauge review queue is curated (`curated.gauges.candidates`); only ACTIVE
    stations need a match decision (`check_curated`, user 2026-10-09).
    **T295/T297 (2026-10-31):** the stages are ONE table (`build.STAGE_GRAPH`, `--graph`); clim / reach / tiles are
    independent and run side by side (`--jobs`, default 2, max 4); the manifest keeps per-stage seconds, peak tree
    RSS and per-input digests (the plan prints which input moved; `--status`); `--side DIR` runs every stage into
    DIR without touching live outputs or LATEST. **T266/T408:** `marks.jsonl` is the review LOG; what it decides is
    computed in memory (`pipeline.regs.review.export.load`, provenance per record — no `decided/` copy, owner
    2026-11-05), and the writers (`review.apply`: `apply_proposals`, `tools.mined_names` / `slough_calls` /
    `tidal_limits`) write only their final curated file. No stage reads the marks (in no key —
    `build.NOT_BUILD_INPUTS`); a build runs with marks.jsonl absent.
    **T408 (2026-11-05): ONE RELEASE COMMAND**, `python -m pipeline release --staging` (`pipeline/release.py`): preflight
    (code + curated committed) → keep the live release's reach run → `pipeline build --hold-latest` through heavy.sh (the
    release waits in `data/release/CANDIDATE`) → checks (manifest, `release.app_gate` — the one upload.sh runs — and the
    binding diff vs the live release; a diff STOPS until `--accept-diff <its json>`) → follows → LATEST → staging upload; resumable,
    ledger `data/generated/release/ledger.json`. **T666 (2026-11-11): ONE RELEASE PATH.** What is cut from the live release
    is refreshed BY the release, on the candidate, before the promote (stage `follows`; the one table is
    `pipeline/release_follows.py`: in-season matches, the app's fixtures re-cut — a renumber-only change is committed with
    the release id, a changed golden page stops for `--accept-fixtures` — and the tests that ask for the release under test,
    found by rule, run with `RELEASE_DIR` = the candidate). A new thing that follows the release is a row there, never a
    hand step in a runbook. Preflight runs EVERY check and lists every failure (`release.PREFLIGHT`, which `--dry-run`, the
    rehearsal, reads too); fast-fail's pass is keyed on the tree's content; the command remembers its flags (never an
    escape); the release pushes only with `--push`, and only its own commits. `--prod <r-id>` re-uploads that same checked release and refuses an agent:
    agents never run it. Runbook `handoff/PLAN/runbooks/release.md`.
    **T547 (2026-11-10): THE ATLAS RECORDS ITS CURATED INPUTS** (`pipeline/atlas/inputs.py`): the build writes one digest
    per input it reads (cuts by anchor, tidal limits, name variants, found names, added lakes/streams/areas, area
    closures, slough calls, the gpkg stamp) into `graph.pkl.meta.json` `input_hashes`. Release preflight STOPS on an
    atlas behind any of them, naming the file (`--ship-stale-atlas REASON` goes on, printed); `pipeline build` prints the
    same list as a warning. `python -m pipeline.atlas.inputs` asks; `--backfill` proves an older atlas's inputs by git
    and records only what it proved. A new curated read in the atlas build goes into `ATLAS_INPUTS` (test).
    `pipeline.atlas.promote` refuses a candidate in which a boundary id a curated rule names (an extent's `splits`) now
    has another `ref` (a lake clash suffix `_2` swapped); `--accept-moved-boundaries` after reading the list. It also
    refuses a candidate without the build's last file `COMPLETE.json` (removed when a build starts; `--no-marker
    REASON`) and a net boundary loss in the parity report (`--accept-boundary-loss`), and undoes the first rename if the
    second fails. `pipeline build --atlas` clears an old `<build>_next` before building into it.
63. **A tributary walk never enters the entry's own water** (`tributaries.THE_WALK_NEVER_ENTERS_THE_ENTRYS_OWN_WATER`,
    2026-10-11). Every non-area item the entry matches is governed by the rule's extents, never collected by the walk —
    even when FWA codes it as a tributary (the Chilliwack is a "tributary" of the Vedder: after the Vedder's cut moved
    onto Vedder Crossing, the Vedder's downstream-of-the-crossing rules walked up the whole Chilliwack, 1,805 sections).
    A piece on the reach's own watershed code is a side channel, not own water (Old Kootenay River Channel). Before
    promoting any split or name change, DIFF THE REACH RUN'S PER-RULE BINDINGS against live — atlas parity cannot see this.

## Runbook: upstream updates the owner runs

64. **A newer HYDAT: `scripts/update-hydat.sh`** (fetch HYDAT → rebuild `clim.json` into the feed dir → `pipeline build`
    through `scripts/heavy.sh`; T557: no feed republish — the feeds worker is the one gauge publisher and re-ranks once
    clim.json is on R2; `pipeline/gauges/feed/publish.py` is its parity spec and the local dev feed only). Every feed run checks HYDAT's listing and ECCC's
    real-time list and writes what is due into `index.json` `maintenance` (`{kind, since, current, latest, action}`;
    kinds `hydat`, `roster`; `action` is the exact command); a notice clears itself on the next run once current ==
    latest. The owner is told ONCE per kind+latest through `FEED_NOTIFY_URL` (ntfy plain POST; Discord/Slack JSON),
    off when unset (`pipeline/gauges/feed/notify.py`, state in `<feed dir>.notified.json`). Agents never run the script.

## Curation review

65. **A reviewer signs off only on ONE VINTAGE** (T133, curation-review PLAN §4 phase 2). Per-entry digests
    (`io.entry_digest`, the per-entry half of `corpus_digest`) are written by the reach run (`report.json`
    `entry_digests`) and the bundle (`meta` `entry_digests`); the release manifest states its bundle's
    `entries_digest` + `reach_digest` (`manifest.bundle_block`); the atlas records each split's anchor
    (`split_anchors.json`, `splits.anchor_digest` — the same digest the review fingerprint holds). Whether an entry
    is built is `pipeline.regs.review.vintage.entry_state`, nowhere else: until its digest agrees across reach, bundle
    and release on the served atlas, with every split it reads built as authored, it reads "awaiting build" and only
    T and R may be marked (`refuse_unbuilt`); no bundle is awaiting build; a reach run that rebound the bundle's own
    corpus (`vintage.bundle_rebound`: same `entries_digest`, other `digest` — a reach-code change) holds EVERY entry
    until deliver, since no entry digest moves. "Bundle matches" is per rule — label AND
    section count, the bundle's (`fingerprint.rule_section_counts`) against the builder's now
    (`vintage.bundle_matches`). The review app re-reads the digests by file stamp: a rebuild clears it with no restart.
    `check_corpus` and the build stage keys are unchanged.
66. **The review app writes one thing: a mark** (T134, T185). There is no fix mode: the editors and their routes
    were removed (2026-10-24), and a fix is a reviewed change to the curated files, never the app's.
    `test_curation_review_safety` pins the app to exactly `POST /api/marks` and that no backend module but
    `writes.py` writes a file (its one caller: the mark); the screen test's route sweep proves nothing under
    `data/curated` changes. Never add a write route. The one exception is the in-season matches file,
    `data/curated/inseason/notice_waters.json` (Claude/owner tooling, not a mark), written only via `writes.commit` by
    two routes: `POST /api/inseason/matches` (T230, owner 2026-10-28: an edit, a history note per edit) and
    `POST /api/inseason/resolve` (T520, 2026-11-09: re-derives the saved matches' ranges on the shipped release, never
    changes a match; the screen test pins it to that one file). The verdict is `marks.verdict_from_answers`, the
    tiers `regs/review/tiers.py`, the coverage strip `regs/review/coverage.py` (its fold IS `squash`) — one function each.
67. **The review queue has one order and one reader of the run** (T135). The order is `tiers.queue_rank` (tier, the
    most urgent risk factor `tiers.RISKS`, the severity-A pre-checks). ENTRIES ONLY (T184): no split is a review
    target — a split is checked by proxy on the map of each entry whose rules name it (`reuse.entry_split_points`,
    every one labelled), a wrong one is that entry's "Place: wrong", and a split mark is refused (the old ones stay
    readable);
    the run's ambiguous cuts / unresolved rules reach the review side only through `regs/review/runfacts.py` (a run
    without `report.json` is still being written and is refused, never read as "no findings"); the pre-checks are
    `precheck.prechecks`, a ticket `tickets.ticket_text` / `parse_ticket` (a round trip). "Commit marks" is
    `commit_marks` — `git commit --only` of the marks store — a CLI the screen prints, never a review-mode route.
    The splits still to verify are `tiers.splits_to_verify` (the `split_to_verify` pre-check); a cut decided by hand is `runfacts.is_curated_pick` (a hint, never the
    `ambiguous cut` risk); a CLI that judges marks reads the vintage through `fingerprint.Context.on_disk` and
    the corpus through `io.read_entryfile` with the registry — as the review app does.
