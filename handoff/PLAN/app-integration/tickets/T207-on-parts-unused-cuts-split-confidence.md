# T207 — "on parts" is a note, unused-cut false positives, split confidence end to end, owner cuts

Status: DONE (04315ec5 0e605022 48239620 c127eff6) — built in r-d32f814feac0

Source: owner 2026-10-26 (decisions.md "T207"). Audit: handoff/PLAN/curation-review/split-name-audit.md "Unused, re-audited".

## What was done
1. **"On parts" is not an error.** If a rule's `undrawn_part` names no place (`catalogue.part_identifies_place`: "on parts",
   "on part", "various locations (as buoyed and signed)", and now "parts of the lake (not specified in the book)"), it
   is a pre-check of the new severity `I` (`unidentified_part`, a note). It is not the severity-A `undrawn_part`, not
   the `undrawn part` risk, and not a `flag` (`tiers.unidentified_part`). The review screen shows it as "only on …
   the book does not say which part" without the warning style. This covers 39 rules; Hatzic r2/r3 are among them.
   As a result, 3 closures move from P1 to P2 and 13 entries move from P3 to P4. Pinned by `test_t207_on_parts_is_a_note.py`.
2. **Unused-cut false positives.** `pipeline.regs.review.split_use.used_split_ids` is now the single answer for both
   the review app and the audit. A cut counts as used if it is named by id, by the area an extent names, through a merged place (an alias or
   another name at that place), or by a DFO entry, anywhere in the corpus. Pitt River's Garibaldi Park cut and
   Wood River's Hamber Park cut are now used. The true unused count is **28** (it was 30); each one has a proposal in the audit doc.
   Pinned by `test_t207_split_use.py`.
3. **Split confidence.** `splits.json` now accepts an optional `confidence` on each split: `high` (the default when absent), `medium` or `low`. It is display only
   (`SPLIT_NOT_ANCHOR`, so it does not change the anchor digest). It is carried through SplitDef → SplitPoint →
   splits.resolved.json (sidecars too) → `RegistryBoundary.confidence` (`registry.add_boundary_confidence`, registry.json)
   → bundle `split.confidence` and `part_mark.confidence` → the export's splits → the part labels (`end_name`: "…, approximate
   location (low confidence)") → the release `water.part_mark.confidence` (additive, last, still water/1;
   pack.generated.ts regenerated) → the app's `NavMark.confidence`, `markLabel` / `markA11y` (`@app/core`
   `confidenceWords`) → the review app's boundary labels and map labels. The wording is
   `models.splits.confidence_words` in Python and `confidenceWords` in TypeScript, and both are pinned. Covered by `test_t207_split_confidence.py`
   and `waterNav.test.ts`.
4. **Owner cuts.**
   - White River pool: two `low` cuts, 100 m below and 100 m above the salmon viewing platform point. The point is 8.5 m
     off the FWA White River, at about 841 m, below the Sayward Road bridge (about 1,139 m). `white_river.r1` now binds `between` the two cuts.
   - Fulton River: two cuts, opposite the spawning channels (9.9 m and 6.1 m off the Fulton; 3.44 to 5.58 km). `fulton_river.r3`
     binds `between` them.
   - Chuckwalla River: both Ten Mile Pool cuts are now `low`.
   - Cranberry River: there is no canyon cut in git history or in the data (see the audit doc). The rule stays undrawn, and
     its finding now reads "needs a split (owner to locate)", which the review screen shows as a badge.

## Left
- ~~The owner runs the atlas build, the reach run and deliver~~ — done: later releases (r-8b43fb267199 on) carry the cuts.
- ~~The owner decides on the 28 unused cuts~~ — owner 2026-10-27: "unused curated cuts: ignore for now" (decisions.md
  "needs a split" list); Burton + the Kootenay dolphin decided 2026-10-26 (445643c2).
- ~~The Cranberry canyon signs need a point from the owner~~ — owner 2026-10-26: falls point (medium) + DEM bottom (low),
  r1 bound between them (48239620). Left for the owner: a look at the low bottom cut (OWNER-TODO Reviews).
- Pre-existing failures that are not from this ticket: `test_review_screen` Z 122≠132, `test_basin_areas` z6:trout_char_quota,
  and `test_region_homes` z7b:daily_quotas (all three from the zone daily_quotas merge, 6335f73d).
