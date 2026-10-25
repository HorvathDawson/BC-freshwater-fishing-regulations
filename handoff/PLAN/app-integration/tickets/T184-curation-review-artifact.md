# T184 — Keep the local curation review app; clean it up (the artifact version is retired)

Status: OPEN. Owner 2026-10-24: the review is better as "our old separate app — then we just clean it up". Phase 1's
artifact (the 30-target pilot page) was published, then deleted at the owner's request; its code goes too.

## What the review is for (owner)
Verify every ENTRY: (1) the regulation was parsed correctly (T text, R rules) and (2) its rules land on the right
water and extent (P), with A (the angler's answer) as the final sanity check — then the entry is saved as
HUMAN-REVIEWED. Splits are reviewed BY PROXY through each entry's extent: on the map the split shows its proper name
and place; a wrong split is a "Place: wrong" mark on an entry that uses it. No split is a review target of its own.

## Work
1. The queue and the pilot: entries only — drop the S0 split-point tier from `gate` (queue tiers, pilot quotas,
   reports); regenerate the 30-target pilot as entries across the tiers (zone entries rule by rule).
2. The map in review shows every split the entry's rules name, labelled with its name, so the proxy check is visible.
3. "Human-reviewed": a target marked verified on all its checks is shown as reviewed (by whom, when) on the screen and
   in the queue; it returns for a re-check when its fingerprint (text, rules or extent) changes (T97 marks).
4. Remove the artifact path: curation-review/artifact/, pipeline/regs/review/export_artifact.py,
   import_artifact_marks.py and their tests (keep the tributary-geojson fix in curation-review/backend/reuse.py).
5. Review mode only, as T185 says (fix-mode writers retired there); a tidy pass over the screen for the current build.
Acceptance: the local app reviews only entries; splits visible by name on each entry's map; the reviewed state shown;
artifact code gone; tests green.
