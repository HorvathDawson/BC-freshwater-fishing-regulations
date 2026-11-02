# T356 — Salmon card: move what core salmon.ts still resolves into the Python file

Status: OPEN (found by T350 lean pass, 2026-11-02). Owner rule: all cascade / default / quota logic is resolved in
Python and shipped pre-resolved.

`app/packages/core/src/salmon.ts` still decides these answers itself:
- **Region closure** (`regionClosedChain`, :235-238): reads the tier from the location id string
  (`l.split(":")[1] === "region"`) and replaces the chain's answer with the header's `closed` note. Python already does
  this in `quota.header_closure` (and for EVERY place of the page, not only region rows). A T349 page default
  (`2:default:…`) fails the id test. Ship a per-chain closure flag/note id instead.
- **Water bucket tier built in TS** (:685): when `bucket.shared` is missing, the card builds a TreeTier from the header
  note. Root cause in Python: `quota.bucket()` returns None before `shared_buckets()` when the page has no region
  `daily` note (quota.py:308 vs :339), so `shared` is never shipped for such a region. Emit `shared` independently of
  the daily bucket, then delete the TS branch.
- **Header sizes per species** (`sizesOf`, :288): picks min / adult / jack lengths and lets a water's adult note beat
  the region's. Resolve per chain + species in Python.
- **Partial coverage per species/day** (`agrees`, :614): compares undrawn chains' verdicts in TS. Could ship as the
  chain's own runs.
- Dead fallbacks: `P.default ?? 0` (:186, :590, :597, :620; `default` is always written), `P.verdicts[ix] ??
  P.verdicts[0]` (:536, hides a bad index as "closed"; validate the release on decode like `parseSalmonFeed`).

Format change → salmon/4 + golden. Size: M.
