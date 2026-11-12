# T606 — Atlas inputs: what T547 left

Status: OPEN

Source: T547's report 2026-11-10 (commits 9c701a58b … 4acd5d0f1). T547's ticket file numbers the sections.

## Work
1. Proof owed (owner: "consistent with the builds now"): one side atlas build with the new code, compared with the
   live atlas (the build is deterministic; no curated file is newer than the live atlas, so every difference must be
   the new `input_hashes` and `COMPLETE.json` or be explained). Command:
   `HEAVY_LOCK=heavy-job-2.lock scripts/heavy.sh t547-side 50 -- python -m pipeline build --side <DIR> --atlas`.
2. T547 §3 second half: lake-boundary clash suffixes numbered by the boundary itself; §5 lake names. Both change the
   atlas, so they ride the next planned atlas rebuild, with a rule-binding diff before any promote.
3. Atlas-only files leave the deliver stage's cache key.
4. A cut's label edit is not caught by the new input check (cuts are fingerprinted by position only): decide whether
   a label edit should count, and say why in `pipeline/atlas/inputs.py`.

## Acceptance
- Item 1's diff is in the ticket; items 2–4 done or each closed with a reason.

Depends: T547. Size: M (one heavy build). Owner: no.

## Item 1 result, 2026-11-10 (side build with the new code, all stages, 85 min, rc 0)
Side release r-7445e205856e against live r-a178299f6005 (built 2026-11-09 17:56), 56 manifest entries:
- **34 identical by hash**: every geo, regs, lines, core, access, climate and tiles entry. Tiles and bundle vintage
  agree with live (section handles d5242c0c70509877, reach digest 1a11ea11a5b3fb3e).
- **11 salmon files differ**: expected, not T547. The DFO salmon inputs moved after the live release was cut (nine
  commits on 2026-11-10 by the review-app chat: T573 closed areas as a second entry, T607, T562, plus the owner's
  marks). Not traced row by row.
- **11 water files differ in one metadata row only** (`store_digest`, `export_subset_sha256`); every table (item,
  part, part_section, part_line, part_mark, water_exit, xpart, picker, str) is identical (checked on water-8; the
  other ten have the same byte size as live). The digest covers the export, which the salmon change moves.
- Note: the reach step logged "NOT keyed — pipeline/atlas/reach/build.py changed while it ran" (T546 was editing it);
  the reach digest still equals live.
Conclusion: T547's change does not alter what is built. Item 1 done; items 2–4 stay open.

## Note 2026-11-11 (the rebuild lane)
Item 2 did NOT ride the T664 + T665 side build (`app-integration/next-rebuild-results.md`): that build carries no
change to lake clash suffixes or lake names. It still waits for an atlas rebuild of its own, or for the next one.
The promote guard it leans on is unchanged; promote also lists lost boundary ids by family now.
