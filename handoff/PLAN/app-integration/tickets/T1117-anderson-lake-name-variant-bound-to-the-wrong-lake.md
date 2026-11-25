# T1117 — The book's "ANDERSON" LAKE row (MU 1-3, Walbran) is bound to Anderson Lake on Dove Creek (MU 1-6): wrong rules on production

Status: DONE (19a8dc392, lane L21, in T1053's PR #166; ships with the 0.9.1 data release, which rebuilds the atlas).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1117 | The book's "ANDERSON" LAKE row (MU 1-3) is bound to Anderson Lake on Dove Creek (MU 1-6): wrong rules on production | DONE — fixed in PR #166 (T1053); binding pinned by a test; ships with the 0.10.0 data release | 19a8dc392 | 0.9.1 chat 2026-11-25 |

Source: 0.9.1 chat 2026-11-25 ~00:50 MST, found while checking the owner's Dove Creek question in T1053 (draft PR
#166). HIGH, on production today.

## Facts
- The Synopsis row "ANDERSON" LAKE (Region 1, p.14, MU 1-3: an unnamed lake in the Walbran Creek watershed) is
  bound through a name variant (`data/curated/waters/name_variants.json`, naming pass 73c04dbc2) to FWA's Anderson
  Lake on Dove Creek near Courtenay (`wbk:329095566`, MU 1-6).
- Production therefore shows, on that lake and on Dove Creek, rules that do not apply there: fly only, bait ban,
  single barbless, trout / kokanee catch and release.
- The right target is `wbk:329504289` (WBID 00105SANJ), named in the variant's own note.
- Owner 2026-11-25: "Yes, fix it in 0.9.1 (Recommended)". Fixed inside PR #166, so Dove Creek does not reopen by
  the head-lake walk either. This ticket is the record; it closes with #166 and the 0.9.1 data release.

## Acceptance
On the 0.9.1 release: the Walbran lake carries the row; Anderson Lake on Dove Creek and Dove Creek show the
regional rules; the answers diff lists exactly these waters; a test pins the binding.

Follow-up: T1118 (every name variant whose note records an MU mismatch). Severity: high (wrong, stricter rules
shown on a real lake and creek). Release: 0.9.1 (owner). Model: Opus. Size: S. Owner: no (he decided).

## Done (lane L21, 2026-11-25)

- `data/curated/regulations/book_name_links.json`: the override for `"ANDERSON" LAKE` (MU 1-3) now names
  `waterbody_keys: ["329504289"]` (was `gnis_ids: ["1657"]`); its note keeps the old text and adds the evidence.
- `data/curated/waters/name_variants.json`: the regulation name "ANDERSON LAKE" moves from `wbk:329095566` (which keeps
  its stocking name) to a new entry on `wbk:329504289` — as PROVOST DAM is done — so the atlas mints and names the
  Walbran lake (side atlas: exactly one registry item added, `wbk:329504289` "Anderson Lake", MU 1-3; nothing else
  changed; the atlas is current with its 14 inputs).
- `matched` restamped deterministically: `backfill_matched` (`matched_ids` + `backfill`, restamp
  `r1:anderson_lake@1-3` only) against the side registry; one line in region-1.json moved; the six pre-existing
  disagreements were left alone. No marks file edited; the parser never ran.
- Pinned: `pipeline/tests/test_t1053_anderson_binding.py` (reverting either curated file fails it).

Diff (side reach run on the fixed atlas vs the same code on the live atlas): **4 rules move, all `anderson_lake`**:
r1–r3 (fly only, bait ban, single barbless) from `lake:329095566` to `lake:329504289`; r4 (✱ trout and kokanee release)
loses the Courtenay lake, Dove Creek's top piece and 19 unnamed streams (21.6 km) and gains the Walbran lake and 25
unnamed streams feeding it and around it (36.0 km). 0 unresolved, 0 licensing, 0 DFO, 0 diagnostics changed. Answers:
the 20 Dove Creek / unnamed pieces that the T1053 own-row rule had reopened close again (Tsolum River ✱ "No Fishing");
live → branch, no section anywhere loses a closed day. The Walbran streams take a release, not a closure (no closed
days change there).
