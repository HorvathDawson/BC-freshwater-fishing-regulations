# T606 — Atlas inputs: what T547 left

Status: PARTIAL — items 1 (18db8478b), 2 (0b23c1403, now in the promoted atlas: the five lakes no longer carry West Road / Blackwater names, checked 2026-11-20) and 4 done; left (Claude): item 3 (held, reason below).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T606 | Atlas inputs: what T547 left (side-build proof, clash suffix numbering + lake names at the next rebuild, cache key, cut label edits) | PARTIAL — left (Claude): item 3 (held: needs deliver's read set) | 18db8478b, 0b23c1403 | left (Claude): item 3 (held: needs deliver's read set); item 2 live in the promoted atlas |

Source: T547's report 2026-11-10 (commits 9c701a58b … 4acd5d0f1). T547's ticket file numbers the sections.

## Done
- Item 1, proof owed (owner: "consistent with the builds now"), 2026-11-10: side release r-7445e205856e against live
  r-a178299f6005: 34 of 56 manifest entries identical by hash (tiles and bundle vintage agree with live: section handles
  d5242c0c70509877, reach digest 1a11ea11a5b3fb3e); 11 salmon files differ (the DFO salmon inputs moved after the live
  release, not T547); 11 water files differ in one metadata row only (`store_digest`, `export_subset_sha256`). T547's
  change does not alter what is built.
- Item 2 (0b23c1403, now in the promoted atlas): `registry.build.item_boundaries` numbers a lake edge's clash suffix by
  the boundary's lowest (blue line, measure), curated splits first (`CLASH_SUFFIX_BY_PLACE`; 140 of 141 clash groups
  keep their numbers); a lake drops a flowing name ANY stream item owns (`LAKE_CHECKS_EVERY_STREAM_NAME`): Kluskoil,
  Tsacha, Tundra and both Euchiniko Lakes lose 'West Road River' and 'BLACKWATER RIVER'; Teslin River's two "Hutsigola
  Lake" edge ids swap. Tests: `pipeline/tests/test_next_atlas_items.py`.
- Item 4: already decided in T547 (9c701a58b): `pipeline/atlas/inputs.py` keys the cuts by their ANCHORS — a label,
  note or confidence edit moves no cut and reaches the bundle through `splits.resolved.json` / the sidecars (the module
  docstring says so). Closed.

## Left
3. Atlas-only files leave the deliver stage's cache key. HELD (2026-11-12): it only saves reruns, and a file wrongly
   called atlas-only would leave deliver stale with nothing said. It needs deliver's real read set first (the bundle's is
   pinned by `test_one_data_flow.py`; answers / export / salmon / status index are not). Not worth that risk before
   closed testing.

## Acceptance
- Item 1's diff is in the ticket; items 2–4 done or each closed with a reason.

Depends: T547. Size: M (one heavy build). Owner: no.
