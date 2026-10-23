# T92 — Gear check: every gear phrase in a rule's printed text lands in a stored field

Status: OPEN.

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
