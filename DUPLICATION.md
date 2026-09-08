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

## Open

- Three lakes and two stream sections where `tributaries_of_reach` and the older primitives
  disagree at production settings — `KNOWN_LAKE_WALK_DISAGREEMENTS`. Not a duplication so
  much as two implementations of one question that have never been reconciled.
