# T337 — DFO water bindings refresh inside the build (no manual `match apply` after an atlas rebuild)

Status: OPEN (found 2026-10-31 answering the owner's "when is matching applied?").

`WaterBinding.item_ids` in the DFO entries is a CACHE re-derived by `python -m pipeline.regs.dfo_salmon.match apply`
"after a registry rebuild" (DESIGN.md §3), but no build stage runs it. `deliver/salmon.py` reads the cache directly, so
an atlas rebuild that renumbers or merges items (e.g. T322's slough fold) can leave DFO places pointing at stale items
until someone remembers to run it.

Fix: either run the exact-only, lock-respecting match step as part of the build after the atlas (and fail the build on
any binding that no longer resolves), or have deliver resolve the curated ids (`gnis_ids`/overrides) through the
registry itself and drop the cache. Suggestions stay suggestions; locked waters untouched (tests in test_dfo_salmon).
Check the T322 slough release for any stale binding first.
Size: S–M.

## T322 check (2026-11-03): no stale binding
`match apply --registry <t322b full_next registry>` into a scratch copy of the DFO entries: 161/161 bound, `diff -r`
empty. The slough fold left every DFO place on a live item. The fix (the match inside the build) is still open.

## T408 re-scope (owner walkthrough 2026-11-05, `handoff/PLAN/data-flow-decisions.md`)
Owner: the 162 DFO matches are done and verified = owner decisions. Keep the ids; every build checks each one
resolves in that build's registry and **fails naming the place** if not. No re-match by name, no manual `match apply`.
New DFO names are already handled by the worker (`unplaced.json` + notice email).
