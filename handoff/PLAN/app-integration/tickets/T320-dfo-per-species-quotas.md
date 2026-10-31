# T320 — DFO salmon quotas per species, per water/extent, as structured data (bucket + sub-limits)

Status: DONE (cf5301f59) — rides the next release + a feeds-worker deploy (owner 2026-10-31: "We should have the per species values from the table in the DFO pages for each
water/section (Skeena has sections too) like each extent has a list of quotas etc. we need to make them interpretable …
The 4 salmon per day combined is the overall limit for the salmon bucket with sub amounts for each species within.").

## Work
1. **Parse (deterministic, no LLM):** DFO freshwater salmon pages list, per water / extent (incl. Skeena sections), a table of
   species × (daily limit, possession, size min/max, retention: keep / release / hatchery-marked only, dates). Extend the
   existing DFO scraper/parser (pipeline/regs/dfo_salmon) to keep those rows structured per extent, keyed like the extents
   (geography curated once, rules re-scraped each run — dfo-salmon-churn memory).
2. **Model the bucket:** the region-wide "N salmon per day combined" (and possession = 2× etc.) is the salmon bucket's
   aggregate; each species has its sub-limit within it — same shape as the game-fish quota tree (aggregate + children),
   reuse that model. Unknown/unparsed rows keep their text and are flagged, never guessed.
3. **Bundle + app:** the salmon table (T318) shows per-species daily / possession / size / keep-or-release for the extent
   the user is on, with the bucket total line ("Up to 4 salmon a day in total"); per-place rows use the same.
4. Tests: a parser fixture per page shape (Fraser, Skeena sections, Vancouver Island), round-trip of every scraped row,
   and a check that each extent's species rows add up under its bucket.

Depends: T164, T318. Size: M–L. Owner: spot-check a few extents.

## Owner decisions (2026-10-31)
1. Possession: the bucket's only ("8 in possession"); no per-species possession numbers.
2. Every wording in current + archived pages reads fully or is flagged with a named reason; "N of which can be unmarked" is structure.
3. Squamish: a water-level bucket on the Squamish and its tributaries, shown only while coho is open there.
4. Regions 5A / 7: the header closure wins. Investigated: the open rows are the 2025 in-season notices (FN0846, FN0851,
   Aug 22 - Sep 30, 2025, read on notices.dfo-mpo.gc.ca); the 2025-08-21 pages dropped the closure header for them and the
   2026-04-01 pages restored it without removing the rows. Stale.
5. Change detection: a change in a page's header / general text (not parsed into rules) is an URGENT owner notice with old
   vs new text and what to do; a new place with no curated extent was already urgent (unplaced). Rule changes on known
   places stay unflagged (the feed publishes them).

## Done
- `pipeline/regs/dfo_salmon/quota.py`: `read_limits` / `residue` / `unread_reason` (named reasons), `species_quota` (the
  verdict's `q`: d, h, w wild allowance, o size sub-limits, mx, mo monthly, a, u), `bucket` (region total tier: n, says,
  possession, min_cm, per), `header_closure`, `over_bucket`.
- `typed.py`: new shapes — "N of which can be unmarked" (origin wild, within), "Monthly quota = N over X cm" (period monthly),
  "Daily limit of N of which M may be over X cm", "only two over", "less than 15mm" (no "point to shank"), "Closed to
  fishing", "No angling", "Natural bait not allowed", species lists ("Chinook, Sockeye, Pink, Chum"); unread numbers set
  `review_reason`. Today's typed feed files are byte-identical.
- Corpus (75 pages: tracked fixtures + cache raw + 53 archived, 5,492 rows): 5,376 read fully, 116 flagged — TBD 65, bare
  "0 per day" 17, empty species cell 8, another fish 7, natural bait allowed 7, recording rule (Tseax quota) 6, place
  closure inside the cell (Fort Babine) 4, "1 65cm or less and 1 over" 2. 0 errors, 0 unnamed.
- `deliver/salmon.py` + `live.py` + the Worker port (`app/workers/feeds/src/dfo/{typed,quota,salmon,live}.ts`,
  `email.ts`): `q` on open verdicts, `regions[slug].bucket`, header closure, `texts` + the `dfo-text` notice (replaces
  `dfo-header`; was/now lines in the email). Parity with the Python holds (all scenarios).
- App: core `salmonTable` bucket line ("Up to 4 salmon a day in total · 8 in possession"), per-species quota line
  ("4 a day · only 2 over 50 cm", hatchery-marked / wild / monthly words), "Limit: see DFO's words below" for an
  unread row, Squamish water bucket; `SalmonCard` draws them.
- Tests: `pipeline/tests/test_dfo_quota.py` (shapes per page, corpus round trip, bucket fit, closure), core
  `salmon.test.ts`, `SalmonCard.test.tsx`, worker `email.test.ts`, `test_dfo_header.py`; golden regenerated.

## To ship
`python -m pipeline.regs.dfo_salmon.feed` (typed feed), then the bundle + release build (via scripts/heavy.sh), and the
feeds worker deploy (owner command if refused) so the live feed carries `q`, `bucket` and `texts`.

## Owner spot-check (2026-10-30)
Owner reviewed the 5 extents (Chilliwack/Vedder, Stave, Kispiox, Stamp, Quesnel 5A): DFO words read correctly, interpretations confirmed.
