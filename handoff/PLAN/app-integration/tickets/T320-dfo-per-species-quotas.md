# T320 — DFO salmon quotas per species, per water/extent, as structured data (bucket + sub-limits)

Status: OPEN (owner 2026-10-31: "We should have the per species values from the table in the DFO pages for each
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
