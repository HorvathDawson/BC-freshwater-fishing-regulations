# T356 — Salmon card: move what core salmon.ts still resolves into the Python file

Status: DONE 2026-11-03 except partial coverage per species/day (left, see Done) — salmon/4 + dfo-feed/3, needs the batch-3 rebuild (found by T350 lean pass, 2026-11-02). Owner rule: all cascade / default / quota logic is resolved in
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

Unblocked 2026-11-03: its dependency (T332 merged 0c276aa10 / T350) is DONE.

## Done 2026-11-03 (salmon/4, one change set with T358)
- **Region closure:** `quota.header_closure`'s rule carries `header_note`; its verdict carries `c` (the note id); a chain
  answered by it every day (and whose undrawn places are closed by it too) carries `c` (note ids) and its map says
  closed, never partial (`deliver.salmon.closed_by_header`, `chain_calendar`, `live.compose`; the app's `composeChain`
  mirrors compose, pinned by the golden's new `closedFeed`). The card reads `chain.c`; `regionClosedChain` /
  `regionClosedNote` and the location-id test are gone. A closed page's water rows now also show the closure card
  (Python already closed every place of the page).
- **Shared:** `regions[r].shared` (`quota.shared_buckets`) is the region's own, with or without a bucket
  (`deliver.salmon.header_answers`, also `live._set_bucket`). The TS tier builder is deleted; an aggregate note with no
  shared entry throws.
- **Sizes:** `regions[r].sizes` (`quota.sizes`): LengthBands per species, ordered, water entries before the region's,
  first that holds wins. `sizesOf` only words them (same text).
- **Dead fallbacks:** `P.default ?? 0`, `P.verdicts[ix] ?? P.verdicts[0]` gone; `checkPayload` validates the head on
  decode (indices, chains, default, unmapped, region fields, closure ids); `parseSalmonFeed` refuses a region with notes
  and no `shared`/`sizes`.
- **Left:** partial coverage per species/day (`agrees`, and the bar's `at1`) stays in TS — it would need per-chain runs
  composed in both `live.compose` and `composeChain`; the ticket marked it "could".
- Format: salmon/3 → salmon/4 (`SALMON_NEEDS` 4 → 0.5.0), dfo-feed/2 → dfo-feed/3, dfo-header/1 → /2. Tests:
  test_salmon_file, test_dfo_live (golden), test_dfo_header (+ worker fixture t356_header.json), test_dfo_quota,
  test_t349_page_defaults; core salmon/salmonFeed, ui-native SalmonCard, worker dfo.quota.
