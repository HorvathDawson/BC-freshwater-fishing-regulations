# Duplicated logic, and fallbacks that hide errors

A running list. Two implementations of one rule is the failure this project keeps producing,
and it is the hardest kind to see: each copy is correct on its own, and only the pair is
wrong. The same goes for a fallback — `?? "note"` looks like robustness and is a silent
answer to a question nobody asked.

**Rule:** if a thing is decided twice, one of the two is deleted. If it must exist in two
languages, a test executes both and compares. If neither is possible, it is listed here.

## Fixed

| what | the two copies | why it mattered |
|---|---|---|
| **`within` label filter** | `runtime-style.ts` built `["all", ["!", ["within", mask]], rawFilter]`; `Map.web.tsx` built the same through `toExpression` | Only one was fixed when the legacy-filter bug bit. The mask is module-cached, so a *first* load took the fixed path and a *remount* took the broken one — `filter[1][0]: "!" found`, on the second visit only. Declarative copy deleted. |
| **date windows** | bundler and `build-fixture.mjs` both wrote the curated strings verbatim; the client reads `{from:{month,day}}` | Crashed every regulation screen with a season on it. Survived a full suite because the FIXTURE had the same bug — the app tests run against it, so two agreeing wrongs looked green. Both now call `pipeline/regs/parsing/dates.py`. |
| **month names** | four tables: two upper-case, one title-case, one of full names inside `seasonPhrase` | One `MONTHS` + `monthAbbr` in core. |
| **catchment format** | `DonorPanel` kept a decimal under 10 km²; `GaugeBadge` always rounded | Same creek read "3 km²" under the map and "3.4 km²" in the panel. |
| **`OUTCOMES`** | `Shell.tsx` and `outcome-colour.test.ts` each declared the list | The test proving every outcome is coloured was proving it about its OWN list. |
| **`rgb()` + `SCALE`** | `hatch.ts` and `pill.ts` | Fallback differs by design, so it is a parameter now, not a second function. |
| **`ordinal`** | `@app/core` exports a real one; `@app/ui-native` re-exported `percentileLabel` under the same name | `"3rd"` vs `"p3rd"`, picked by autocomplete. Alias deleted. |
| **marker colour** | `#5F26E0` typed into `Map.web.tsx` | The light theme's accent, worn in every theme. |
| **legend swatches** | three hexes typed into `LayersSheet` | The colour-blind legend described a map painted differently. |

## Guarded across languages

These cannot be deleted — they must exist on both sides — so a test executes both.

| contract | guard |
|---|---|
| donor weighting (`panel.py` ↔ `trust.ts`) | `tools/one-formula.test.ts` runs real Python against real TS over 224 cases |
| restriction types (`RestrictionType` ↔ `RuleKind` ↔ `KINDS`) | `tools/rule-kinds.test.ts` — three copies, all read from source, must match exactly |
| trust bands | `emit_gauge_policy --check`, now armed in `pnpm check` |
| basin handover zoom | `tools/handover.test.ts` — pipeline, style and Shell held equal |
| outcome colours | `outcome-colour.test.ts` — chrome vs map, hex for hex |

## Fallbacks that hide errors

| where | the fallback | why it is dangerous |
|---|---|---|
| `toRule` | unknown `kind` → `"note"` | A note closes nothing. A seventh restriction type would reach a reader as open water with a remark. Now guarded by `rule-kinds.test.ts`, but the fallback itself remains. |

## Fixed in the sweep (7 Sep 2026)

Four subagents read the TypeScript app, the Python pipeline, the cross-language contracts,
and every error-hiding fallback. Verified each before acting.

| what | the problem | why it mattered |
|---|---|---|
| **`meta` keys** | the bundler wrote `schema/build/generated_by/trust_bands/reach_digest/reach_run`; the client reads `version` and `valid_until` — **neither existed** | `validUntil` is the STALENESS GATE. It was permanently `null`, so the app could never know its regulations had expired. Survived because the fixture writes a *third* key set containing `version` — the tests were green against a file the pipeline does not produce. Now written, and `bundle-meta.test.ts` reads the keys out of both writers and the reader. |
| **empty rule table shipped as success** | a missing reach run called `cov.skip`, which prints "not wired" and exits 0 | `statusFor` is written so "no rule row" means *open under the general rules* — correct against a populated table. Against an empty one **every water in BC renders OPEN** and the build reports success. Now `SystemExit` with the command to run. |
| **`BASIN_RECORD_FULL_YEARS = 30`** | claimed in a comment to mirror the panel's ceiling, which is **20** | A 25-year station was fully trusted colouring a reach and 83% trusted colouring the catchment around it — and the comment told the next reader the reconciliation was done. Second constant deleted; there is one now. |
| **`"1th percentile"`** | `ConditionsPanel` and `Chrome` each hand-wrote `${Math.round(p*100)}th` | Printed "1th / 21th / 31th percentile", and collapsed everything under 1% to "0th" — saying a river has never been lower. The exact three-spellings bug the codebase already fixed once. Both call `percentileOrdinal` now. |

## Open — found, verified, not yet fixed

Ranked by consequence. Each was confirmed by reading both sites.

### Wrong today, user-visible

1. **The stocking legend promises colours the map does not wear.** `theme.ts` has five
   `stock` hexes; the style maps five categories onto **four** tokens (`year` and `recent`
   both → `color.stock.recent`) and paints `never` as plain lake fill. Three of five
   swatches name colours nothing on the map wears, in all three themes — and the fifth is
   labelled "over 10 years" while the map category is *never stocked*. This is precisely the
   failure `theme.ts`'s own header warns about.
2. **The donor table can disagree with the number above it.** `data/panel.ts` computes
   weights for display; `core/trust.ts` `estimate()` recomputes them for the answer, and
   skips any donor at percentile exactly 0 or 1. A record-low donor is shown carrying 62% of
   an estimate it contributes nothing to. `panel.ts` claims it "restates the arithmetic
   without being able to disagree with it".
3. **Gauge dots and the sheet disagree about which quantity a station reports.**
   `gaugePoints.ts` uses the *declared* `parameter`; `ConditionsPanel` twice *infers* it from
   which value is non-null. A station publishing both, declaring `level`, shows stage on the
   dot and discharge in the sheet one tap away. Stage precision differs too — `3.42 m` on the
   dot, `3.4179 m` in the sheet.
4. **Route thumbnails draw the boundaries the Conditions map hides.** `views[].hide` is
   documented as a property of the view but is honoured by one caller; `MiniMap` has no
   `hide` prop.
5. **Gauge dots are labelled with today while the map is coloured by the forecast.**
   `Shell` passes `horizon` to the reaches and catchments, not to `useGaugeGeoJSON`. At +3
   the rivers show Friday and the dots show today, with nothing saying so.

### Two implementations of one question

6. **Four upstream walks**, with guards that already differ: `graph.ancestors`,
   `reach.tributaries_of_reach`, `export_gpkg.export_tributaries`, `shed._walk`. Only the
   reach walk has the Strahler guard that stopped McLennan Creek absorbing 20.6% of BC — and
   `export_gpkg` is **what a curator opens in QGIS to check a sweep**, so a leak can be
   signed off as correct.
7. **`atlas/graph/tributaries.py` has no production caller.** Verified: referenced only by
   tests and comments. It is a fully-tested, superseded second answer to the pipeline's most
   expensive question, missing the Strahler guard and the confluence-mouth seeding. Its
   passing suite is positive evidence for a walk nothing ships — and it is the module the two
   red cross-check tests were comparing production against.
8. **Five "is this watershed code a descendant" implementations**, three conventions; two
   apply the sibling dash-guard (`100-025956` vs `100-0259560`) and two do not.
9. **Three ΔE implementations** guarding the same palettes: `build-style.mjs` and
   `theme.test.ts` are line-for-line ΔE76; `tools/cvd.ts` is CIEDE2000. The same threshold of
   12 means two different things.
10. **Two camera fitters**, and the older one is the bug the newer one documents fixing —
    `routeCamera`'s two-point branch is still degrees-with-one-assumed-viewport.
11. **`_slug`, `_canon`, `normalise`/`_norm`** — name-keying duplicated; the last pair
    **already differs** (`"Wahleach (Jones) Lake"` → `wahleach lake` vs `wahleach jones lake`),
    so a gauge and a regulation can resolve to different items for one name.
12. **`_SAME_NAME_TOL` differs** — 250 m in `muni_graph.py`, 400 m in `build_dataset.py`. In
    that band one stage splits a creek and the next bridges it.

### Unguarded across the language line

13. `MIN_TOTAL_WEIGHT = 0.05` — the "say nothing" floor, in `panel.py` and `trust.ts`, and
    the only threshold in the pair that decides whether an answer exists at all.
14. `HORIZONS = (1,3,5)` — the feed-contract test asserts against the checked-in fixture, not
    against the Python constant, so a change makes the fixture stale and the test still pass.
15. `PENTADS = 73` and `PCTILES = (10,25,50,75,90)` — the `Band` tuple is **positional**.
    Change the cut points and every column keeps its `p10…p90` name while every river in the
    province is misclassified.
16. `stats.json` is nested by parameter in Python and read flat in TypeScript — latent, since
    nothing calls `feed.record()` yet, and the test that should catch it invents its own flat
    fixture.
17. Temperature bands, `ScopeKind`, `RuleVia` — vocabularies on both sides with no guard.
    `via` is the worst shape: an unknown value falls through to `"reach"`, so a tributary
    binding (98.6% of the corpus) would be told to the reader as "no fishing **here**".
18. `{blk}:{measure}` — minted in three Python places with **two rounding rules** (`int()`
    truncates, `int(round())` rounds), and parsed by SQL in `queries.ts`. Nothing checks the
    parser against the minter, and `runsOfSameRules` assumes list adjacency is adjacency on
    the water.

### Fallbacks that hide errors

19. Unparseable date windows are dropped **one at a time**. Total loss is safe (all-year);
    *partial* loss is not — a closure with two seasons keeps one and renders OPEN through the
    other. `date_parse_errors` is enforced at Rule construction, not at bundle time.
20. A `matched` item that vanished from the registry is dropped silently if *some* survive.
    All-gone correctly raises `unknown`; a partial survivor binds to the subset, so a closure
    curated for three lakes binds to one and the other two render OPEN.
21. `AMBIGUOUS_CUT_IS_FATAL = False` — 63 rules, boundary silently taken at the lower of two
    measures, and the diagnostic never becomes `uncertain`.
22. **The root cause of 19–21:** the pipeline has *two* channels for doubt and only one
    reaches the app. `rule_unresolved` → `uncertain=1` → `unknown` works. `Diagnostic` and
    `Coverage.skip` are printed to a terminal and discarded. Everything routed to the second
    channel ships as a confidently bound rule, and renders as OPEN.
23. `EvaluateInput.feedUnreachable` has **no production caller** — the `"feed-unreachable"`
    branch is dead code, so an unreachable in-season override feed cannot raise `unknown`.

### Also

- Three lakes and two stream sections where `tributaries_of_reach` and the older primitives
  disagree at production settings — `KNOWN_LAKE_WALK_DISAGREEMENTS`. Given finding 7, this is
  a disagreement with a module nothing ships.
- `bundle-schema.test.ts` checks table names, never **column** names; `queries.ts` hard-codes
  ~50 against `schema.sql`.
- `cutting.py` carries a comment to keep in sync with `graph_builder.py`, **a file that does
  not exist**.
- `promote.py` tests `verdict in ("bound", "bind")` and `"bound"` is not a legal `Verdict` —
  pydantic rejects it, so half that condition is dead.
