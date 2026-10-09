# Rules for agents working in this repo

Decisions already made and paid for. Each line exists because something broke or was
measured. **Read this before changing anything**; re-deriving these costs a day and
re-litigating them costs trust.

Deep context (historical, archived): `pipeline/docs/archive/13-build-plan.md` (delivery), `pipeline/docs/archive/10-plan.md`
(issues ①–㊸), `pipeline/docs/archive/REACH-BUILDER.md`, `pipeline/docs/archive/RESOLVER-HANDOFF.md`.

**How the book is read — every user interpretation ruling — is `pipeline/docs/RULINGS.md`; it is the source of truth for interpretation and wins over any other note.**

---

## ⛔ Absolute

1. **Never run the LLM parser.** `pipeline/regs/parsing/run_parse.sh`,
   `python -m pipeline.regs.parsing.dispatch`, anything that spawns `claude -p`. It spends the
   user's credits. Hand over the command; the human runs it. This holds even if the user
   says "run it" — that means *they* will.
2. **Never write to `data/curated/regulations/entries/catalogue/region-*.json` without backing
   them up first.** They hold in-progress curation that may not be committed. `cp` them to the scratchpad, make
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
9. **`includes_tributaries` is three-valued.** On a rule, `None` inherits the entry's
   `includes_tributaries`.
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
13. **A rule's extents are its own — never inherited from its entry. Never widen a rule
    that names a place it could not bind.** No reader (reach builder, bundle,
    `pipeline/deliver/bundle/read.py` `source_of`) hands a rule its entry's `extents`; the entry's only clip.
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
    `pipeline.atlas.reach.classify`; covered items come from `pipeline.atlas.reach.covered`. Verified
    once: **3,038 of 3,038 rules identical** — a number that predates the current corpus (3,317
    rules, `rest`, `Extent.watershed`) and has **not been re-run since**; re-run it before relying
    on it. If you change one, re-run that parity check — the app is where a human signs off, so
    divergence is invisible until a user hits it.

## Builds and tests

17. **Full builds take ~18 min and ~9 GB.** Never rebuild to test a change. Build to a new
    `--out` and compare with `pipeline/tools/build_parity.py`. `data/generated/atlas/full` and
    `data/generated/atlas/full_new` already exist.
18. **`pytest.ini` deselects the `slow` tests by default** (㊷) — 197 of 2,263 on
    2026-09-29 (`pytest --collect-only -q -m slow`, run unfiltered, not through rtk). Determinism and full-build
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

32. **A screen may not assert a fact about the data it is sitting on — it asks.**
    `App.tsx` passed `waters={255} reaches={6967} surveyed={35} stations={5}` and a fixed
    `fetchedAt`, all transcribed from `design/riffle.html`, whose fixture is one valley. The
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
    `aria-disabled` work on BOTH targets. `tools/check-platform.mjs` fails on the old
    spelling.

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
    layer down, and the grep gate in `pipeline/docs/archive/HANDOFF-curated-layout.md` §3.3 is what
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

<!-- 42-44: these were numbered 32-34, which collided with the app section. The app rules
     run 21-34 and the curated ones 35-41, so this section continues from there. -->

42. **Measure before asserting.** Every number in the docs is reproducible; several
    "obvious" designs here were killed by one measurement (the hash id, the straddler
    policy, parallelism in the reach builder).
43. **Do not build parallelism in the reach builder.** Full corpus resolves in ~0.1 s.
44. **Report honestly what you did not verify.** An unverified claim is worse than a known
    gap. Say "scaffolded, not run" when that is what happened.

## Reading the regulations

<!-- 45-53: rulings from 2026-09-25 to 2026-09-29 that were only in commit messages and memory.
     Appended, not interleaved, so the numbers above stay stable. The mechanics are in
     pipeline/docs/CURRENT-STATE.md; the reference reader is read.py `effective_rules`. -->

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
    get no caution. (aeb070cf)
52. **A zone release limited to a kind of water displaces its own table on that water.** A zone
    release with `water: stream` (extents `feature_types: [stream]`) in force on a stream
    displaces its OWN region's quotas and clauses that keep the fish in the same base dimension
    ("daily"), as a no-water release does (`read.released_on_water`, `effective_rules` step
    4b): Region 3's "Bull trout (Dolly Varden) from streams, Aug 1-Oct 31", Region 4's
    "Trout/char release: in streams from Nov 1-Mar 31". Closures, water rows and other regions'
    rules are untouched. A size clause with no count ("none under 60 cm", `daily/size`) is not
    reached here but by step 5b (rule 57, MOOT SIZE CLAUSE). (2026-09-29, working tree)
53. **Competition lives in one function.** `pipeline/deliver/bundle/read.py::effective_rules` is
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
    the curated list alike (the Cowichan River). "If no steelhead rules exist, rainbow rules still
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
    **THE CURATED LIST** is `data/curated/regulations/steelhead_waters.json` (`CURATED.regulations.
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
    may name only the corpus). "Closure" is `rules.closure_grade`, asked by the status index, the
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
    (answers/2, `answers/model.py`) LOOK ANSWERS UP there. `test_dataflow_gates.py` refuses a reader
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
    rows there) and NO lift reaches a West Road tributary in any region. NO EXEMPTION PRINTED ON A ✱
    ROW REACHES ITS TRIBUTARIES (user ruling Q3, 2026-10-07): the row's ✱ carries its closures,
    quotas and gear, never its lifts — every lift-only rule of a ✱ row is `includes_tributaries:
    false` (Nitinat, Quinsam, Duncan, Lardeau, Dutch, Hevenor, Fulton, Babine's Rainbow Alley, and
    the Similkameen, whose lift no longer reaches its Region 3 tributaries); `test_rules_round.py`
    refuses one that walks, and the export's `gotchas.exemption_stays_on_its_water` lists them. G4 — "open all year" lifts only FULL blanket
    closures, never a species closure: Region 6's steelhead closure (May 15-Jun 15, p.49) holds
    on every Region 6 river and stream except the Skeena, Nass, Iskut, Stikine and Taku
    mainstems (its own sibling lift, which binds exactly those five items). G5 — a row's DATED
    bait ban REPLACES its zone's all-year one (Region 1's Quatse, Somass, Sproat, Stamp, by dated
    lift-only rules); no other region prints that shape. Duck Lake's creeks stay closed Apr 1-Jun
    14 (its bass release does not lift the stream closure). The export's `gotchas` carry
    `closures_combine` (every entry whose own closure overlaps a zone seasonal closure, or whose
    rule the zone closure silences: both hold) and `dated_bait_ban_replaces_zone`, both generated
    from the bundle (`export_ui_rules.closures_combine`, `dated_bait_ban_replaces_zone`); pinned
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
    every length (RU-6; the status index asks the same). MOOT SIZE CLAUSE (user ruling 2026-10-05,
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
    listed as known (`export_ui_rules.RELEASE_UNDER_CLOSURE_KNOWN` — how RU-2 would have been
    caught), AND a water row's keeping quota in force that a blanket zone closure silences
    (`QUOTA_UNDER_CLOSURE_KNOWN`, 20 as built: RU-7 hides such a quota from the page, so a missed
    exemption shaped like a quota would otherwise vanish). Both read every set, every start day of
    the rule and of the closures, every fish (`_closure_scan`).
58. **The UI export ships ENCODED; the model is `export_ui_rules.build()`.** (Phase 4, 2026-10-05.)
    One run writes `ui-rules-export.json` (data: rules/licensing as arrays beside `rule_ids` /
    `licensing_ids`, rule-set members as integer indexes with the zone/province `bases` interned,
    waters' parts and runs positional, defaults and derivable fields dropped, no indent) and
    `ui-rules-guide.json` (`guide`, `field_dictionary`, `species`), same bundle digests.
    `pipeline/tools/export_codec.py` `expand` is the reference decoder and `main` refuses a pair
    that does not decode to the model or whose integer references dangle (`wire_problems`). Every
    check (`problems`) and every word of the guide describe the DECODED model; a test or tool reads
    the files through `export_ui_rules.load`, never `json.load` of the data file alone. A new
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
    Dinosaur r1 walk, their other rules do not). NO EXEMPTION ON A ✱ ROW REACHES ITS TRIBUTARIES (rule 57 G3). A TRIBUTARY WITH
    ITS OWN ROW gets both its own and its inherited rules; on one competition key its OWN row speaks,
    before naming (`read.OWN_ROW_BEATS_INHERITED`, Granby under "Kettle River's tributaries") — never a
    closure either way (an inherited closure is only lifted). A FISH EXACTLY ON A PRINTED SIZE BOUND IS
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
