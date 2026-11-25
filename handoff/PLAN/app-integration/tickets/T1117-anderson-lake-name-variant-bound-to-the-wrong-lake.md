# T1117 — The book's "ANDERSON" LAKE row (MU 1-3, Walbran) is bound to Anderson Lake on Dove Creek (MU 1-6): wrong rules on production

Status: OPEN.

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
