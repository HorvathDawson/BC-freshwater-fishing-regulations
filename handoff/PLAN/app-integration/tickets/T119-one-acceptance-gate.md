# T119 — One acceptance gate: ingest and the review app each build a partial gate

Status: DONE — e23ca33d (lane-T112, one code commit with T112).

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

## As built — 2026-10-23

- `validate_catalogue.gate(entry, *, menu, registry, source_text=None) -> Verdict`. `menu` and `registry` are keyword-only
  and REQUIRED (None stated, never forgotten). It deep-copies its input and runs, in order: `absorbed_ids_named`
  (registry), `resolve_exempt_ids`, `exemptions_stay_inside_what_they_lift`, `split_parents_named`,
  `canonicalise_splits` (menu), `CatalogueEntry`, layer 3 (source_text), `post_model_checks`, the long-row advisory.
  `Verdict`: `entry` (carries the alias rewrites), `refusals` `(path, msg)`, `model_error` (kept whole so the app renders
  it with `errors_of`), `advisories`, `ok`, `messages()` (ingest's lines). `check_entry` is deleted.
- Ingest: `ingest(candidates, batch, *, registry)`; `ingest_registry()` loads `GENERATED.registry()` (0.5 s). `run`
  (the parse agent's self-check) and `dispatch.find_invalid` pass it — dispatch loads it OUTSIDE its per-batch
  `except`, so a missing build fails the run instead of marking every batch invalid (re-buying them all).
- `default_extents` stays in ingest, NOT in the gate: it is ingest's normalisation of a parser's bare rule; in the gate it
  would give a curator's emptied rule the whole water (AGENTS 13). Pinned.
- Review app: `model_api.check(entry, *, menu, registry)` calls `gate`; `reuse._gate` builds the menu with
  `cut_point_menu(entry, reuse._cut_points)` (every matched item and every scoped item, `bindable` = built boundaries +
  pending splits.json splits) and the served registry. `reuse._checked` runs the gate ONCE for a check and a save, and
  `save_entry` writes the gate's entry — so an alias split id or an exemption's book name is stored canonically, as
  ingest stores it. App-only checks that stay (they are about the edit, not the entry): entry exists, region, the
  pass-through fields.
- Behaviour the app gains: exemption aliases resolved; split ids canonicalised; a scoped extent must bind a cut-point on
  its own item; every matched item's cut-points allowed (it used to allow only the primary item's + referenced items').
- Behaviour ingest gains: a batch item whose menu STATES no cut-points (`bindable_ids: []`) now refuses every split the
  parser wrote — the parser was shown none, so any is invented and would select no section. Before, an empty menu
  checked nothing (only a batch with no menu keys at all, written before boundaries were exported, still checks
  nothing). The app always refused there; one rule now. Pinned
  (`test_an_empty_menu_refuses_every_split_and_an_absent_one_checks_nothing`, mutation to the old `if not allowed`
  fails it).
- App error paths are finer: a split-parent refusal is addressed to the id (`rules.0.extents.0.item_id`, was `rules.0`);
  the editor marks every field the path starts with, so the rule is still marked.
  `test_curation_review_api::test_put_invalid_is_refused_at_the_field[a split parent]` updated to the new path.

## Measured (corpus 1,525 entries, live atlas, read-only, heavy.sh)

- Acceptance: `gate` with the registry and `cut_point_menu` over every entry refuses 0; the full validator (old
  `check_entry` with the same menu + registry) refuses 0 — equal lists, 0 entries differ.
- Review app `check_entry` over every entry, old code vs new: 0 refused both, 0 entries whose errors differ.
- What a SAVE now writes differently: 15 extents in 8 entries bind an ALIAS spelling the gate stores canonically
  (ingest always did; the app never did). The next app save of each rewrites them — same cut-point, the bundle resolves
  both, but the placement fingerprint changes:
  r1:john_hart_lake_s_tributaries@1-10 (`campbell_river__john_hart_lake` → `campbell_river__john_hart_dam_power_station`),
  r1:lower_campbell_lake_s_tributaries@1-6 (`campbell_river__campbell_lake` → `campbell_river__ladore_dam`),
  r3:fraser_river@3-14 ×6 (`fraser_river__thompson_river_into_fraser_river` → `thompson_river__thompson_river_into_fraser_river`),
  r3:guichon_creek@3-18 (`gauge__08LG041` → `guichon_creek__mamit_lake`),
  r4:columbia_river@4-15+4-26+4-34+4-38+4-8 ×2 (`gauge__08NB005` → `columbia_river__donald`),
  r4:elk_river_downstream_of_elko_dam@4-2 and r4:elk_river_upstream_of_elko_dam@4-2+4-23 ×2 (`gauge__08NK031` →
  `elk_river__elko_dam`), r8:okanagan_river@8-1 (`gauge__08NM247` → `okanagan_river__mcintyre_dam`).
- Answers: unchanged — no catalogue file is written by this change and no deliver module is touched.

## Pins (`pipeline/tests/test_one_acceptance_gate.py`)

One entry per check that only that check refuses, refused at its path through `gate` and by nothing once that check is
monkeypatched out; the app's errors equal the gate's refusals for each; ingest refuses an absorbed id given its registry;
AST pins: the six checks are called only inside `gate`, `gate` only from `ingest_catalogue.ingest` and `model_api.check`,
and `check_entry` is gone; a `needs_atlas` corpus test (1,525 accepted with the build). Mutations run (each restored):
gate skips canonicalise → 3 fail; skips absorbed → 2; skips exemption aliases → 2; runs `default_extents` →
`test_the_gate_never_widens_a_bare_rule`; ingest drops its registry → `test_ingest_refuses_an_absorbed_id`; app drops the
menu → 2; `reuse._gate` passes `menu=None` → `test_curation_review_api::test_put_invalid_is_refused_at_the_field[unknown split]`
(re-run 2026-10-23 on resume); a second caller of a check → `test_only_gate_calls_a_check`.

Rebuild: none (code only; the review app picks it up on restart).

## Owner questions

- The 15 alias spellings above: leave them for the next save to canonicalise (built default), or canonicalise all 8
  entries now in one reviewed change so their review fingerprints move once, together?

## Left / follow-ups

- `exemptions_stay_inside_what_they_lift` and `absorbed_ids_named` still return bare strings, so the gate addresses them
  to the entry / `matched` (as the app did before). Addressing them to the rule / the extent's id is a small follow-up.

## Checks (2026-10-23, resumed lane)

- Targeted: walker + gate + validator + ingest + parse-context + end-to-end + catalogue + dispatch + curation-review
  API/writes/safety + review reaches + T117 walks: all pass (the corpus gate test ran against the live build).
- Full suite (heavy.sh, unfiltered): 5,158 passed, 15 failed, 28 errors. The 15 fail identically with this change
  stashed (area_boundary curated zones, dataflow gate 4, dfo_salmon archives, entry_id inline parse in review/,
  export_codec on-disk pair, fixture_release, tidal_gauges, update_hydat_script) — pre-existing, not this lane. The 28
  errors are source-data tests (GeoPackage absent in the worktree).
