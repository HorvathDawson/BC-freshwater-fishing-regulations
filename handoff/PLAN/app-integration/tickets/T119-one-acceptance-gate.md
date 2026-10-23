# T119 — One acceptance gate: ingest and the review app each build a partial gate

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding matching 2
(CONFIRMED in code).

## Decision being unified
"Is this catalogue entry acceptable?"

## Where it is decided today
- Ingest calls `check_entry` without a registry, so `absorbed_ids_named` never runs there.
- The review app (`curation-review/backend/model_api.py:363`, `reuse.py:1879`) never calls `resolve_exempt_ids` or
  `canonicalise_splits`, despite its "ONE GATE" docstring.
- `pipeline/regs/parsing/validate_catalogue.py` runs the full set.

How they differ: an entry the review app or ingest accepts can still be refused by the full validator, or carry an
absorbed id unnoticed.

## Work
- `validate_catalogue.gate(entry, *, menu, registry, source_text=None)` runs every check; ingest and both app call sites
  call it. Uses T112's walker.
- Pin: a grep test that `check_entry` / `resolve_exempt_ids` / `canonicalise_splits` are called only inside `gate`.

## Acceptance
Run `gate` over the corpus: the refusal list equals the full validator's. Any entry newly refused is listed and fixed.
No answers change.

Rebuild: none. Depends: T112. Size: S. Owner: no.
