# T693 — An in-season notice matched to a water this release does not hold goes to the bell

Status: DONE — df0136892 (left (Claude): a look on the next staging check).

Source: owner 2026-11-12, by dialog: "Fix gap 1 only". In-season notices are BC Fish and Wildlife's, not DFO's. RULINGS O6.

## The gap (proved by a test that failed before)
The links name waters by item id. A phone on an older release than the links (T673) can be handed a notice matched to a
water it does not have: `joinInSeason` counted it matched, so it was not in the bell, and no page of that water exists on
the phone, so it showed nowhere, with no report.

## Built
- `joinInSeason(..., absent)` (core): a notice whose every water is absent is not matched (`waters` null): it joins the
  unmatched list, so the bell says "not on the map yet". A notice with a held water stays matched.
- `useInSeason(..., holds)` (ui): asks the open release once per water (`itemOrd`), `Shell` passes the release's own shared
  store. The absent ones are reported once per notice (`INSEASON_FAILURES.absentWaters`). A failed ask is reported by
  `useAsync` (`noHolds`) and the notices stay matched, never dropped. No release open: nothing to ask.
- Tests: `app/packages/ui/src/inseason.render.test.tsx` (the three cases, failing before), `app/packages/core/src/inseason.test.ts`.

## Not built
- Gap 2 (a "new" mark on an amended matched notice): T694, after closed testing.
- Per-release matches files: T673, parked by the owner until after closed testing.

Depends: T673 (parked). Size: S.
