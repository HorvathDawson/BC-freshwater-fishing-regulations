# T97 — Curation review, phases 1–5: marks that last, one vintage, a review screen, queue, gate

Status: DONE for phase 1 — 303f6d9d (lane-T97). Umbrella open: phases 2–5 are T133–T136.
Owner approved PLAN.md 2026-10-23 ("The review app changes seem good"), all §6 defaults (Q5, Q34–Q41).
This ticket is now PHASE 1 (marks that last) plus the umbrella; phases 2–5 are T133–T136; a later salmon pass is T132.

Source: PLAN.md §4 phases 1–5, §5 (how review fits the pipeline), §6 Q2–Q9 (a3f66b7c, T66).

## Work (phase 1 here; 2 → T133, 3 → T134, 4 → T135, 5 → T136)
1. Marks store `data/curated/regulations/review/marks.jsonl` + one fingerprint module (text / meaning / binding /
   answer), typed stale reasons, `carry_marks`; retire `verification.py`.
2. One vintage on screen: per-entry `entry_digests` in reach report, bundle meta and release manifest; "awaiting build".
3. Review screen separate from fixing (review mode default, `--fix` owner-only), keys, evidence crops, split cards.
4. `precheck.py`, one risk-ordered queue, ticket export, "commit marks" touching only the marks file, `cut_pick` editor.
5. Gate: AGENTS 16 parity over all 3,406 rules via heavy.sh; a 30-entry pilot by another person.
Ingest: retire `--replace-edited` and the ledger; new ids only plus `--replace-entry <id>`.

## Owner 2026-10-23 — splits mid-review
"Make sure we could add splits and rebuild then continue review with the new splits etc." The binding fingerprint and
`carry_marks` (phase 1) must make this work: a new or moved split stales exactly the marks whose binding moved (reason
"binding moved"), keeps every other mark, and the new split gets its own `split`-scope mark to confirm. Phase 1's
acceptance adds: "add a split on a fixture water → only that water's entries go stale, with that reason". The rest of
the loop (pending-build panel, reload without restart, S0 queue, end-to-end check) is in T133–T136.

## Acceptance
Each phase's "Accept when" in PLAN.md (0 false-stale marks on a no-op normalisation; ≤ 1.5 min per simple entry; parity
100 % or every difference explained).

Depends: T96 (done). Size: M (phase 1: 3–4 days; all phases 3–4 weeks). Owner: no (plan approved).

## As built (phase 1, 303f6d9d)
- `pipeline/regs/review/` — `fingerprint.py` (THE one fingerprint module), `marks.py` (the `Mark` model, the store, the
  one staleness decision), `carry.py` (`carry_marks`; `review.carry_marks` is the name a migration calls).
- Store: `data/curated/regulations/review/marks.jsonl` (`CURATED.regulations.review.marks`, config.yaml), checked in
  EMPTY so a missing file raises (AGENTS 37). Append-only: the last mark on a (scope, target, rule_id) is in force.
- `Mark` (pydantic, `extra=forbid`): scope entry | rule | split, target, rule_id (rule scope), verdict, answers T/R/P/A
  (a split answers P only), problem, note, reviewer (required, not blank), date, `pdf_sha256` (64 hex, stamped by the
  server from the repo PDF), `pages` (printed; default the entry's `source_pages`), `prints`, `carried_by`. The
  verdict must follow from the answers (verified = all y; wrong = an n + problem + note; unsure = a ? and no n;
  deferred = a note).
- Fingerprints, per record ("entry" = the entry's own fields, "rule:<id>", "lic:<id>", "split:<id>"):
  `text` (raw name, regs_verbatim, source_pages, symbols); `meaning` (the record in the model's stored spelling, per
  record — measured equal to `io.dump_entry` over the whole corpus — less label, review_reason, id and placement
  fields); `binding` (placement fields with extents' defaults dropped, the canonical anchor of every split it names
  and of EVERY split on its matched waters, the matcher override deciding its row; ids folded through
  `flowing.canonical_ids`) and its bundle half `reach` (per named water the bound main-stem km to 0.1, the count of
  bound sections in no named water); `answer` (bundle rule `label` + `parts`; for the entry record the part facts —
  steelhead, tidal, province_except, home regions — of its matched waters). Never a section id.
- Stale reasons (typed, `StaleReason`): book text changed / rule changed / binding moved / answer changed / awaiting
  build (a record whose mark had a bundle half and the bundle has no copy now). Rule identity: same key + same
  meaning, else same kind + same meaning (renumbered); never by id alone.
- `carry_marks(before, after, *, marks, unchanged, by, ctx_before, ctx_after, renamed_splits)` re-stamps only marks
  that held before, on targets the migration declares unchanged, appended as copies with `carried_by`. The app's
  split rename uses it (declares every entry and the split unchanged).
- Gate: `POST /api/marks` runs the save gate (`reuse.check_entry`) and `marks.refuse_on_gate` refuses on any error.
- API: `POST /api/marks`, `GET /api/marks?target=&scope=`, `GET /api/marks/splits` (every split's status; a new split
  reads `unmarked`, queued for its own split mark); `GET /api/entries/{id}` carries `marks`; queue rows carry
  `verify` (unmarked | verified | wrong | unsure | deferred | stale) and `mark_reasons`. Retired:
  `backend/verification.py`, `GET /api/verification`, `PUT /api/entries/{id}/verify`, VerifyBar's buttons (the bar
  now shows the marks in force, read only — marking UI is T134).
- One decision, one function: `pipeline.atlas.splits.splits.applies_keys` / `splits_on` (the app's picker and the
  fingerprint), `pipeline.regs.matching.matcher.override_for` (`match_row` and the fingerprint).
- Tests: `pipeline/tests/test_review_marks.py` (34; the split acceptance is in `smoke.txt`, replacing the retired
  `test_curation_review_fingerprint.py`): whole-corpus no-op normalisation (pipeline writer, shuffled keys, spelled
  defaults) → 0 stale of 1,525, and one real edit in the same run → exactly 1; split move, ADDED split (owner),
  override edit, steelhead edit, label wording change, verbatim edit, rule edit, renumbering — each stales exactly the
  affected marks with the right reason; `carry_marks` caught when it carries everything, nothing, or a stale mark;
  no-second-copy pin. The steelhead / label / rebuilt-reach cases run against a fixture bundle written with the real
  `schema.sql` and read by the same `BundleFacts.read`, not a real rebuild.

## Owner questions (built with the default; say if not)
1. **The reach half counts km per named water, not total sections** (PLAN said "section count and length"). A split
   cuts one section into two without moving anything; counting sections would stale every zone rule and tributary
   walk passing over a new split, against "only that water's entries go stale". *Built:* km to 0.1 per named water +
   count of sections in no named water.
2. **A split added or moved on a water stales EVERY entry matched to that water** ("binding moved"), not only rules
   whose reach changed — the owner's acceptance wording. Rules on other entries that merely walk through stay put.
3. **`unsure` is a verdict** (PLAN §2's "?" outcome; §4's field list named only verified / wrong / deferred).
4. **No bundle → no bundle half.** A mark made with no bundle on disk holds reach/answer None and can never read
   "awaiting build"; phase 2's one-vintage gate (T133) is what refuses such a verified mark.

## Follow-ups
- Ingest: `--replace-entry` should print the marks a replace would stale (fingerprints, typed reasons) — with the
  ledger retirement (T135).
- Measure the false-stale rate across a real atlas rebuild (AGENTS: ~50 blocks differ between identical builds; the
  km / unnamed-count half may flutter) — in the T136 pilot; tighten `reach_summary` if it does.
- Licensing records carry meaning + binding + bundle label, no reach half.
- Not run: Tier 2 (`scripts/test-full.sh`, needs the owner's go-ahead) and the app/check.mjs smoke steps (no
  node_modules in the lane worktree). The frontend typechecks (`tsc -p tsconfig.json --noEmit`).

## Adversarial review (2026-10-23)
Each break was tried against the code; the defects are fixed with a test, and every new test is mutation-checked (the
fix reverted → that test fails).

**Fixed**
1. **Twin rules: a mark went to the other twin.** 38 rules in 16 entries say the same thing as a sibling ("No fishing"
   on two reaches). Rule identity paired on meaning alone, so the two twins numbered the other way round read "binding
   moved", and a twin whose text changed read "binding moved" instead of "rule changed". The fix is in
   `marks.match_records`, which now pairs in four passes: (1) key, meaning and binding; (2) kind, meaning and binding;
   (3) key and meaning; (4) kind and meaning, only once the marked key has gone. Pinned by
   `test_twin_rules_a_mark_follows_its_own_twin_never_the_other`; the real-corpus renumber test also fails without it.
2. **Rules that name rules broke on a renumber.** `within` (130 zone rules), `condition_of` (6), `derived_from` (3)
   and an extent's `siblings` (13) hold another rule's id, and that id went into the fingerprint. Renumbering the rules
   therefore staled every rule that referred to another, with "rule changed". The fix is `fingerprint._deref`: each
   reference now stands for the named rule's meaning (with that rule's own references blanked, so no id survives and
   no cycle recurses). A rule `within` a quota therefore still reads "rule changed" when the quota itself changes.
   `test_renumbering_every_rule_of_the_real_corpus_stales_no_mark` renumbers every entry backwards and rewrites the
   references: 0 stale out of 1,525 entry marks and every rule mark. Mutation-pinned by
   `test_the_renumber_check_is_pinned_by_mutation`.
3. **`carry_marks` laundered a rebuild.** It re-stamped every fingerprint from `ctx_after`, including the bundle half.
   A migration run against a rebuilt bundle therefore wiped every answer change that build made, even on targets the
   migration had declared unchanged. A migration speaks only for its curated edit. When the two contexts' bundle facts
   differ, the copy now keeps the mark's own `reach`/`answer` (`carry._keep_bundle_half`). Split marks are excluded,
   since they hold no bundle half. Carried copies are now validated (`Mark.model_validate`, not `model_copy`). Pinned
   by `test_carry_marks_never_launders_a_rebuild` (mutation included). The app's split rename uses one bundle and is
   unaffected.
4. **Concurrent appends lost lines across processes.** A store write reads the file, appends and replaces it, and
   `writes.LOCK` is a thread lock. The review app plus a migration's `marks.append` could drop one side's marks, and a
   lock on the file itself would not have helped, because the replace gives the next writer a new inode. The fix is
   `marks.store_lock`, an exclusive `flock` on the store's directory, held by `append`, the app's `post_mark` and
   `rename_split`. Pinned by `test_concurrent_appends_from_several_processes_lose_no_line` (4 processes × 25 → 100) and
   `test_the_store_lock_holds_off_another_process`; both fail with the lock removed.
5. **A split's `_offset_m` annotation counted as a cut.** 82 splits carry it and no builder reads it. Every
   `_`-prefixed key on a split is now a note, as it already was inside the anchor. Pinned in
   `test_a_split_label_or_note_edit_moves_nothing`.
6. **The queue lost renumbered rule marks.** `_entry_mark_state` compared raw rule ids, so a zone entry whose rule
   marks all still held read "unmarked" after a renumber. It now uses `marks.held_rule`, the rule a mark is on now as
   `match_records` decides it.
- The no-second-copy pin now also covers `def match_records(` and `flock(` (one copy each, in `marks.py`).

**Tried, held**
- No-op migration (key order, spelled defaults, the pipeline writer): 0 stale. Folded item ids (registry alias map):
  same prints.
- Override pick: the fingerprint's `override_for` call agrees with `match_row` on the synopsis row for all 1,379
  entries that have exactly one row (14 have no single row by name+MU).
- The 102 confluence anchors (no coordinates): canonical by `tributary_wsc` (+ offsets); `_name` is ignored. 409
  splits give 405 distinct anchors; the 4 pairs that coincide are listed under their own ids.
- Section ids: none in any fingerprint (`reach_summary` is item id → km; the real-bundle test asserts it).
- Override and steelhead edits come through the binding/answer fingerprints. A mark on a gate-failing entry gets 422.
  A missing or blank reviewer is refused by the model.
- `verification.json`: nothing reads it, no copy exists under `data/curated/`, and the frontend names no
  `/api/verification` or `/verify`.

**Open (not fixed; owner/T136)**
- **Unnamed water is counted, not measured.** Bound sections in no named water (1.89 M in the current bundle) have no
  `section_span`, so `reach_summary` counts them. If the non-deterministic atlas cuts an unnamed block differently,
  every tributary walk over it reads "binding moved". Measure this in the T136 pilot.
- **A registry REBUILD that renames an item stales the entries on it** ("binding moved"), because the canonical id
  changed. A catalogue migration that only rewrites ids to the registry's own fold is a no-op (tested).
- **Renaming a split leaves the old split's mark in force under the old id.** `/api/marks` lists it as stale with "rule
  changed", because the target is gone. The split itself reads correctly under its new id.
- **A rule mark carried over a migration keeps its own `rule_id`.** If its rule was renumbered and then the migration
  ran, the mark is not carried: it goes stale, which is conservative.
- The branch base already fails `test_one_data_flow::test_the_bundle_reads_no_curated_waters_file` (`CURATED.gauges.tidal`)
  and `test_no_silent_skips`, with or without this lane. Neither is in T97's files.
