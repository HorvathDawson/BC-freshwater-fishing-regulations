# Rules for agents working in this repo

Decisions already made and paid for. Each line exists because something broke or was
measured. **Read this before changing anything**; re-deriving these costs a day and
re-litigating them costs trust.

Deep context: `pipeline/docs/13-build-plan.md` (delivery), `pipeline/docs/10-plan.md`
(issues ①–㊸), `pipeline/docs/REACH-BUILDER.md`, `pipeline/docs/RESOLVER-HANDOFF.md`.

---

## ⛔ Absolute

1. **Never run the LLM parser.** `pipeline/regs/parsing/run_parse.sh`,
   `python -m pipeline.regs.parsing.dispatch`, anything that spawns `claude -p`. It spends the
   user's credits. Hand over the command; the human runs it. This holds even if the user
   says "run it" — that means *they* will.
2. **Never write to `pipeline/regs/parsing/entries/*.json` without backing them up first.**
   They hold in-progress curation that is not committed. `cp` them to the scratchpad, make
   the change, then diff to prove only what you intended moved. A lock was lost in this
   repo once; do not be the second time.
3. **Prefix shell commands with `rtk`.** Token-optimised proxy; passes through when it has
   no filter.
4. **Run `graphify query "<question>"` before grepping the codebase.**

---

## The data model

5. **`item_id` is durable (99.88% across a rebuild). `section_id` is not (94%).**
   Measured, `pipeline/tools/build_parity.py`. Anything crossing a version boundary — live
   feeds, deep links, saved pins, bathymetry sheets — binds to `item_id`.
   **`section_id` and `dense_id` must never leave the bundle**: not in a URL, a saved
   preference, a feed, or an API contract.
6. **A durable id is not a durable answer.** 15% of surviving items changed their section
   list in one rebuild. Never cache a resolved section list across bundle versions —
   re-resolve.
7. **The exported section id stays `{blk}:{measure}`.** The hash form was measured and
   buys **one id out of 49,542**. Open decision #1 is closed; do not reopen without a
   *re-anchoring* build showing >2 points of improvement.
8. **`rule_id` is unique only WITHIN an entry** — 49 collide corpus-wide. Every table keys
   on `(entry_id, rule_id)`. Never on `rule_id` alone.
9. **`includes_tributaries` is three-valued.** `None` inherits `entry.tributaries.included`.
   Reading only the rule's own field gives 132; the real number is 554 across 264 entries.

## Resolution

10. **The resolver never creates a section.** `pipeline/atlas/registry/build.py` filters
    pre-existing graph nodes by route measure. Splitting happens upstream in the
    sectionizer from `data/curated/waters/splits.json`. If resolution could split, editing a
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
    here (㉗), even in the "safe" direction.
13. **Never default a rule with no extent to `whole`.** All 12 such matched rules carry
    `needs_review`; 11 carry `unresolved_locators`. They are real, specific locations
    ("500 m upstream and downstream of Causeway Road") with no boundary to bind to.
    Defaulting applies a 500 m closure to an entire lake arm. They need curated splits.
14. **Every rule ends bound, or unresolved with a typed reason. Never absent, never
    bound-and-empty, never unresolved-and-unexplained.** Enforced in
    `RuleBinding.__post_init__` (⑪ + ㊳).
15. **A rule extending to tributaries is `tributaries_pending`** — the reach-scoped walk
    does not exist. 547 rules. Nothing downstream may treat such a binding as complete.
16. **The review app and the builder share one implementation.** `entry_reaches` calls
    `pipeline.atlas.reach.classify`; covered items come from `pipeline.atlas.reach.covered`. Verified:
    **3,038 of 3,038 rules identical**. If you change one, re-run that parity check — the
    app is where a human signs off, so divergence is invisible until a user hits it.

## Builds and tests

17. **Full builds take ~18 min and ~9 GB.** Never rebuild to test a change. Build to a new
    `--out` and compare with `pipeline/tools/build_parity.py`. `data/generated/atlas/full` and
    `data/generated/atlas/full_new` already exist.
18. **`pytest.ini` deselects 153 `slow` tests by default** (㊷). Determinism and full-build
    guards live there, so CI must run `-m slow` explicitly or they never run.
19. **Determinism is a precondition, not a nice-to-have.** Sorted iteration everywhere;
    no clocks in output. The reach builder's digest must be identical across runs.
20. **`pipeline.atlas.reach.cache.POLICY_VERSION` must be bumped when `classify.py` changes an
    outcome.** A test fails if it goes stale — a cache serving confidently wrong answers is
    worse than no cache.

## The app (`app/`)

21. **Greenfield. Inherits nothing from `archive/webapp` or `archive/mobile`.** Those are
    prior art: four shared-logic files there diverged completely (`waterbodyDataService.ts`
    is 1,157 lines on web, 116 on mobile) because two apps were built independently and
    neither was ever the shared source.
22. **`packages/core` has zero React and zero platform imports.** `packages/ui` may import
    `react` but never `react-dom` or `react-native`. Enforced by
    `tools/check-boundaries.mjs` — fix your code, never weaken the gate.
23. **Status is rendered by ONE function in `core/`, never re-implemented per surface.**
    A coherence review counted **nine status surfaces across four vocabularies** in five
    parallel design decks — map line, tap card, search row, rule line, gauge strip, and more,
    each inventing its own wording for the same five states. That is the §0.5 drift failure
    (1,203 diverged lines) reproducing itself in a new layer before a line of app code exists.
    Navigation may legitimately differ between mobile and desktop — `README.md` sanctions a
    sheet on one and a page on the other — but the *answer* may not.
24. **Hooks are shared; components are not.** A hook returns data, so it runs under both
    renderers. `<div>` and `<View>` do not.
25. **Logic never lives in a component.** That is what makes desktop's separate component
    set free. If you want to share a component with desktop *to avoid duplicating logic*,
    the logic is in the wrong place — move it to a hook.
26. **Mobile web renders the same `packages/ui-native` components as the native app**, via
    react-native-web, so the phone experience matches by construction. Desktop gets its own
    DOM components in `apps/web/src/desktop/`.
27. **One React version, workspace-wide**, pinned via `pnpm.overrides`. Two Reacts in one
    bundle is `Invalid hook call`, and it surfaces only at bundle time.
28. **Map layers are only ever added by editing `packages/map/style/layers.source.json`**
    then `pnpm style:build`. Colours reference tokens by name; literals are rejected. No app
    may import a map SDK — that is how the two apps start rendering different maps.
29. **A categorical colour mode over an enum must colour every member**, and a continuous
    mode must define `missing`. This is how `unknown` and `default_only` cannot be rendered
    as something they are not (⑨/㉜ — the one failure with real consequences).
30. **Every dependency needs a line in `app/deps.md`.** v1 accumulated chart.js + pdf-lib +
    pdfjs + fuse + suncalc without anyone deciding to.
31. **`pnpm check` must pass**: boundaries → platform → style → deps → typecheck → test.

## Curated data, and the artifacts you must not regenerate casually

35. **Three kinds of data, and confusing them is the recurring bug in this repo.**

    | kind | example | a rebuild may | losing it costs |
    |---|---|---|---|
    | **authored** | `splits.json`, `name_variants.json`, `overrides.json` | only READ it | human hours |
    | **reviewed** | `gauge_match.json`, `bc_station_waterbody_type.json` | only READ it | human hours |
    | **generated** | anything under `data/generated/` | rewrite it freely | CPU |

    "Reviewed" is the one people get wrong: a machine produced it, a human then checked it,
    and **re-running the generator throws that review away**. It is curated data that
    happens to have been typed by a program.

36. **Read curated paths through `pipeline.common.curated.CURATED`, never as a literal.**
    `from pipeline.common.curated import CURATED` then `CURATED.splits`. The tree is pydantic-
    validated at first access, so a bad path fails naming the key instead of returning an
    empty list four builds later. `ProjectConfig.get_path` returns `Path()` for a missing
    key — silently the current directory — which is how a `--splits` flag with no default
    dropped all 376 curated cuts from three full builds with nothing looking wrong.

37. **A loader for a curated file must RAISE on a missing file, never return `[]`.**
    Absent curated data is a bug, never an empty set. This is the same failure as ㉟ one
    layer down, and the grep gate in `pipeline/docs/HANDOFF-curated-layout.md` §3.3 is what
    keeps it from creeping back.

38. **Never regenerate a reviewed artifact to "check something".** Regenerating
    `gauge_match.json` needs a completed build and rewrites 2,324 hand-checked decisions,
    including 23 `NO_MATCH` entries a human verified against the map. Run
    `python -m pipeline.tools.check_curated` to see whether it is actually stale; it prints
    the command and never runs it. If it says `ok`, there is nothing to do.

39. **The two tiers are not interchangeable.** The *mill* (fetch → build → match → bundle →
    tiles) is ~27 min and ~10 GB and runs by hand a few times a year. The *guards*
    (`pipeline-ci.yml`, `app-ci.yml`) are seconds and run on every push. Never add a step
    to CI that needs `graph.pkl` — a hosted runner has 14 GB of disk and one build directory
    is 10, and a job that fails for unrelated reasons is one people learn to ignore.

40. **A value that exists on both sides of the Python/TypeScript boundary is generated,
    not typed twice.** `pipeline/gauges/consume/shed.py::TRUST_BANDS` is the source;
    `python -m pipeline.tools.emit_gauge_policy` writes the TypeScript and JSON, and
    `--check` fails CI when they drift. Three copies of the trust rule once existed — the
    pipeline's, one in `@app/core`, one in `build-fixture.mjs` with a band the pipeline has
    never written — and a test pinning the *constants* passed the whole time, because they
    agreed on the thresholds and disagreed on the rule.

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

32. **Measure before asserting.** Every number in the docs is reproducible; several
    "obvious" designs here were killed by one measurement (the hash id, the straddler
    policy, parallelism in the reach builder).
33. **Do not build parallelism in the reach builder.** Full corpus resolves in ~0.1 s.
34. **Report honestly what you did not verify.** An unverified claim is worse than a known
    gap. Say "scaffolded, not run" when that is what happened.
