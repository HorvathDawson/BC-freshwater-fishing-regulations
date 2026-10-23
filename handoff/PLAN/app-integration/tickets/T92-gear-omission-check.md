# T92 — Gear check: every gear phrase in a rule's printed text lands in a stored field

Status: DONE — de0581e1 + review fix 750ba2ad. `catalogue_parts/gear_omission.py` (12-phrase vocabulary),
wired into `validate_catalogue.post_model_checks`; catalogue has 0 unexplained misses and 5 kept gaps (below).

Source: memory `gear-rules-need-rework` — "It prevents inversion, not omission — the 8 dropped conjuncts need a
validator". `validate_catalogue.py` and `catalogue_parts/checks.py` check field → text only (`count_in_words`). A quick
check was noisy (161 "electric motor" hits likely stored under another name), so it needs a phrase → field vocabulary.

## Work
A check over each entry's rules together, mapping printed gear phrases (single, barbless, bait ban, fly only, artificial
fly, treble, set line, ice fishing, motor/hp) to the field that should hold each; mutation tests (delete a field → fail);
run over the catalogue and record each miss.

## Acceptance
0 unexplained misses; kept exceptions allow-listed with a reason; part of `validate_catalogue`.

Depends: none (could sit under T66). Size: M. Owner: no.

## Result (2026-10-22)

Run over the catalogue (1,525 entries, 3,405 rules): **0 unexplained misses**. The eight "lost single"
lakes are already fixed in the data (254 rules print "single barbless hook": 249 store `points_per_hook: 1`
themselves, 4 are held by a sibling quoting the same span, 1 is Pend d'Oreille's lift).
The "161 electric motor" noise was `vessel_rule` `level: electric_only` — the vocabulary maps it.
Kept gaps (`KEPT_GEAR_GAPS`, each with its reason; a test fails if one stops firing):

1. `harrison_river.r3` powered_boats — "Speed restriction OR no powered boats … as signed": signage decides.
2. `zp:bait.r4` bait_ban — "unless a bait ban applies" defers to the bans held elsewhere.
3. `one_island_lake.r1` ice_hut — "See ice hut warning, page 63", a pointer.
4. `z7b:ice_fishing_huts.r1` ice_fishing + ice_hut — "huts SHOULD have the owner's contact information
   displayed": advice; no conduct token says it.

Also accepted as holding a phrase: a sibling quoting the same row span; a lift (`exempts`) when "exempt"
precedes the phrase in its clause AND the lift is of that phrase (`DEFAULT_LIFTS` per zone default; a
same-entry `target` that stores it; another entry's target on trust); a `note` (which costs a
review_reason) that prints the phrase.

Review (2026-10-22) closed two launders, test-first: (1) any `exempts` covered any phrase after
"exempt" in a comma-run clause — Duncan River prints "Exempt from spring closure, exempt from … catch
and release, bait ban", where the ban is imposed; (2) `hooks_per_line: 1` held "single barbless hook"
on an angling rule (a treble stays legal) — it now holds only on a set-line rule. Corpus unchanged:
0 misses, same 5 kept gaps, the 8 lift-held phrases all lift the right default/target.

Tests: `pipeline/tests/test_gear_omission.py` 29 (14 mutation pins, one per phrase or more, + 4 from
review); with test_validate_catalogue, test_parse_prompt_teaches, test_ingest_catalogue: 141 passed. Rebuild: none
(validator only; no data changed).

## Left / owner questions

- Q (owner): should the R7B ice-hut advice ("huts should have the owner's contact information
  displayed") get a conduct token (`display_contact_on_ice_hut`) so it shows as advice on the Gear tab?
  **Default: no** — it is "should", not law; kept as a gap.
- Not done: a ROW-level twin (a gear phrase in `regs_verbatim` that no rule, `see` or licensing quote
  carries). Surveyed: 0 misses once `see` pointers are excluded (the 22 raw hits are all `r5:*@6-1`
  pointer rows). Not wired because the validator's unit fixtures use partial rows; worth a follow-up
  ticket if a whole dropped rule is a concern.
