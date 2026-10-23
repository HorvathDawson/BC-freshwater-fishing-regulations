# T133 — Curation review phase 2: one vintage on screen (entry digests, awaiting build)

Status: DONE — 94e06e52, review 5052824f — built as PLAN §4 phase 2 + the owner's splits-mid-review requirement; acceptance tests
green. Takes effect on the owner's next `pipeline build` (the artifacts on disk predate the digests — until then every
entry reads "awaiting build", see owner Q1).

Source: handoff/PLAN/curation-review/PLAN.md §4 "Phase 2: one vintage on screen" (and §5); split out of T97.

## Work
See PLAN.md §4 "Phase 2: one vintage on screen" — built as written there.

**Owner 2026-10-23 — splits mid-review:** "Make sure we could add splits and rebuild then continue review with the new
splits." In fix mode the owner adds or moves a split → the tool prints the rebuild commands (atlas → binding diff →
promote → reach → deliver; it never runs them) → after they run, the tool picks up the new vintage without losing
anything: the new split appears in tier S0 to confirm, entries whose binding moved go stale with reason "binding moved",
every other mark stands, and review continues where it was.

## Acceptance
PLAN.md §4 "Phase 2: one vintage on screen" "Accept when", run unfiltered (not through rtk).
- After a split is added in fix mode and the rebuild has run, the affected entries clear "awaiting build" with no tool
  restart (reload the reach report / bundle / release by digest).

Depends: T97. Size: M (2–3 days). Owner: no (plan approved).

## As built
- **Digests.** `pipeline.regs.parsing.io.entry_digest` / `entry_digests` — the per-entry half of `corpus_digest`, by
  the same serialisation (`_content_digest`; `corpus_digest`'s value is unchanged, pinned). The reach run writes
  `report.json` `entry_digests` (`reach/io.write_run`); the bundle writes `meta` `entry_digests` beside
  `entries_digest` (`bundle/rules.write`, right after `check_corpus`); the release manifest gains
  `bundle: {entries_digest, reach_digest}` (`release/manifest.bundle_block`, from the bundle's `meta`; part of what the
  release id covers; the app ignores it). The atlas build writes `split_anchors.json` — `{split id: anchor digest}` of
  the splits.json it read (`splits.write_anchor_record`; `splits.split_anchor` / `anchor_digest` moved here from the
  review fingerprint, which now calls them, so the atlas and the fingerprint hold one digest of a cut).
  `check_corpus` and the build stage keys are untouched (the reach/deliver code digests change, so the next
  `pipeline build` reruns both by itself).
- **The one decision.** `pipeline/regs/review/vintage.py`: `entry_state(entry_id, digest_now, anchors_now, built)` →
  `{state: current | "awaiting build", reasons, splits, splits_checked, commands}`. Current only when the reach run's
  and the bundle's `entry_digests[entry]` equal the entry's digest now, the newest release (`manifest.latest`, now the
  one "which release is current" answer — `publish.py` and `regs_fixture_release` call it) states that bundle's
  digests, the reach run and the bundle were made on the served atlas (handles + registry), and every split the entry
  reads (`fingerprint.entry_split_ids`: on its waters + named anywhere) is in the atlas as authored. Typed reasons
  (`Waiting`): no reach run / predates / other atlas / entry changed, the same for the bundle, no release / predates /
  cut from another bundle, split not built / moved. `split_state` for a split; `rebuild_commands` prints PLAN §5's
  commands (entry fix: `scripts/heavy.sh build 30 -- python -m pipeline build --no-tiles`; a split or another atlas:
  atlas build, `binding_diff`, `--promote`). `refuse_unbuilt`: while awaiting, a mark may answer only T and R — so no
  verified mark, and no split mark. **No bundle is awaiting build** (T97 Q4).
- **Bundle matches.** `vintage.bundle_matches` per rule: in both, same label, AND the same section count — the bundle's
  (`fingerprint.rule_section_counts`, the one query; the bundle panel reads it too) against the reach builder run live on
  the served atlas (`reuse.entry_reaches`, the builder's own function); a rule one entry cannot count alone
  (`needs_corpus`) is `unchecked`, never a match by default. Measured on the live bundle: 206 entries (200 random water
  rows + every Tacheeda / Twin / Main Lake entry), **0 rules differ** — no false mismatch. The Tacheeda shape (labels
  equal, one lake of two bound) reads `sections: 1 in the bundle, 2 now` (fixture bundle, real schema).
- **The tool** (`curation-review/backend`): `reuse.built()` reads the digests keyed on the files' stamps (reach
  report, bundle, every release manifest, the atlas's registry / handles / resolved splits / anchor record) — a
  rebuild in a terminal is read on the next request, **no restart**. `GET /api/entries/{id}` → `vintage`; queue rows →
  `vintage`, `vintage_reasons`; `/api/marks/splits` → each split's `vintage`; `GET /api/build` (new) → the pending-build
  panel (every entry and split awaiting, why, the commands). `POST /api/marks` refuses P/A answers while awaiting (422,
  naming the command). `/bundle` shows the bundle's copy only when current (else `{vintage, awaiting: true}`);
  `/answer/waters` → 409 while awaiting. Frontend: the VerifyBar says "awaiting build — only T and R can be marked" with
  reasons and commands; a rule's live binding is labelled `preview` while awaiting; a queue chip; a pending-build banner
  that polls `/api/build` once a minute while anything waits and refreshes the queue and the open entry when it shrinks.
  `CURATION_REACH_DIR` / `CURATION_RELEASE_ROOT` for tests. API.md updated; AGENTS rule 65.
- **Tests.** `pipeline/tests/test_review_vintage.py` (26, no data; one in `smoke.txt`): fresh build current; **an entry
  edit awaits reach AND deliver, then clears, every other entry current throughout**; **a split added mid-review**
  awaits atlas → reach → deliver then clears (the promoted atlas holds every entry until reach and the bundle are
  made on it); a moved split; an atlas without the anchor record sees an added split, not a moved one; no bundle;
  a release from another bundle; artifacts predating the digests; T/R-only marking; **Tacheeda reported as a
  mismatch**; the writers (`write_run`, `rules.write`, `ship`, `write_anchor_record`); no-second-copy pin. Mutation pins:
  vintage that asks only the reach run clears an edit one step early; no split check reads an unbuilt split built;
  `release_is_of` always true hides another bundle's release; labels-only `bundle_matches` calls Tacheeda a match.
  `pipeline/tests/test_curation_review_vintage.py` (3, `needs_atlas`): THROUGH THE APP with one TestClient — the edit
  flow (awaiting, pending panel, 422 on verified, T/R mark taken, reach alone not enough, deliver clears, verified
  taken) and the split flow (entries on the water await, another water untouched, the old mark reads "binding moved",
  the split is queued and refused a mark, the atlas holds it → clears, split mark taken) — with no restart; plus the
  app gate mutation-pinned. `test_curation_review_api.py` / `_writes.py`: the mark and bundle tests read the vintage as
  built (`as_built` fixture — they test marks, not the gate); `test_bundle_copy_of_an_entry` asserts the count compare.
- **End to end on real data (scratch, read-only).** A reach run of a 13-entry subset of region 7 (incl. Tacheeda) on
  the promoted atlas into the scratchpad wrote `entry_digests`; all 13 equal the tool's own digest of the same entries
  (the read through the registry). The tool's vintage over it + the live bundle/release reads exactly "the bundle
  predates per-entry digests, the release predates its bundle digests"; over the live reach run, all three predate.
  The scratch BUNDLE could not be built on a subset (`bundle` refuses a section with no rule set: the zone entries
  must bind every section), and a full-corpus scratch reach run writes ~20 GB with 27 GB free on the disk, so it was
  not run: the bundle meta and the release manifest are proved through their real writers in the tests
  (`rules.write` via the licensing fixture, `release.build.ship`), not on a scratch deliver.

Checks run (unfiltered): the review / curation-review / bundle / release / reach test files (347) and the splits /
sectionizer / release / vintage / catalogue neighbours (235); `pytest -m smoke` (199 passed); frontend `tsc --noEmit`. Pre-existing failures on the branch
base, untouched by this lane: `test_one_data_flow` ×2 (`CURATED.gauges.tidal`; the tiles contract),
`test_fixture_release` (the `access` file), `test_no_silent_skips`, `test_dataflow_gates::gate_4`. Not run: Tier 2
(`scripts/test-full.sh`), `pnpm -C app check`, check.mjs (no app changes; no node_modules in the lane).

## Owner questions (built with the default; say if not)
1. **Every entry reads "awaiting build" until the next `pipeline build`.** The reach run, bundle and release on disk
   predate the per-entry digests, so nothing can be marked verified until reach + deliver rerun (the code change
   reruns both by stage key). *Default:* run `scripts/heavy.sh build 30 -- python -m pipeline build --no-tiles` once
   this merges; no review starts before phase 3 anyway (Q41).
2. **A moved split is only seen once the atlas records its anchors.** The promoted atlas has no `split_anchors.json`;
   until the next atlas build the tool checks split IDS only (an added split → awaiting build; a moved one is not
   seen; the panel says `splits_checked: ids`). The sidecars migration does NOT write the record (it would declare
   today's splits.json built). *Default:* leave it to the next `--atlas` build.
3. **A promoted atlas holds every entry in "awaiting build"** until reach and deliver run on it (its sid space is new;
   every binding is the old atlas's). *Default:* as built — that is what the owner's rebuild sequence clears.
4. **The binding shown while awaiting is labelled a preview, not hidden.** PLAN: "the tool shows the binding … only
   when current". The live builder reach is what the curator checks a fix against, so it stays on screen marked
   `preview` (and P cannot be answered); the bundle's copy and the answer pane are withheld. *Default:* as built.
5. **"Bundle matches" counts sections, not the phase-1 km measure.** Both sides are the same atlas, so counts are exact
   and the km measure (built for stability ACROSS atlases) would hide a one-section difference. The bundle count is one
   function in `fingerprint.py` (`rule_section_counts`). *Default:* counts.
6. **Per-entry, not whole-corpus.** An entry Y untouched by an edit to X stays current after X's reach rerun even though
   the bundle's `reach_digest` is older (PLAN: "that entry's digest"). Y's ANSWER can still depend on X (competition);
   that shows as "answer changed" on Y's mark after the rebuild (phase 1). *Default:* as built. (Review: the one
   exception is a reach run of the SAME corpus with other bindings — a reach-code change — which no entry digest can
   see; that holds every entry until deliver, `vintage.bundle_rebound`.)

## Follow-ups
- Phase 3 (T134) puts the marking UI on this: the T/R checklist stays live while awaiting; P/A and the verdict bar
  wait for "current".
- The pending-build panel polls once a minute; a server-sent event on a stamp change would be immediate (T134 if the
  owner wants it).
- `bundle_matches` compares rules only; licensing records still show their bundle copy without a "now" count.
- When the owner next runs `--atlas`, check `split_anchors.json` lands in the promoted build (then `splits_checked`
  reads `anchors`).

## Adversarial review (2026-10-23, lane-T133)
Tried to break it on the points the brief named; three real defects fixed, the rest held.

**Fixed**
- **A reach rerun on the same corpus read every entry as current.** `entry_state` compared per-entry digests only,
  so a reach-code (or steelhead-list) change — rerun reach, bundle not yet rebuilt — changed no entry digest and every
  entry stayed "current" while the builder's binding on screen and the bundle's were two different runs: the
  2026-10-22 "reach 17:08 / bundle 15:49" pair the phase exists for. New `vintage.bundle_rebound(reach, bundle)`
  (same `entries_digest`, other `digest` than the bundle's `reach_digest`) → `Waiting.bundle_reach` on every entry
  until deliver (entry-fix command). Owner Q6 stands for a run of ANOTHER corpus (per entry). Tests:
  `test_a_reach_rerun_of_the_same_corpus_holds_every_entry_until_deliver` + mutation pin
  (`bundle_rebound` → False reads the old bundle as this run's).
- **A second answer to "which release is current" in the shell.** `scripts/test-full.sh` (`ls -dt`) and
  `scripts/update-hydat.sh` (`ls -1td`, the R2 upload) picked the release by folder mtime; both now ask
  `release.manifest.latest`. The no-second-copy test scans `scripts/*.sh` for an `ls -t … release/r-*` picker
  (fails on the old scripts).
- **The split clause of `refuse_unbuilt` was unpinned** (mutating `held or scope == "split"` → `held` passed all 26):
  a split mark with no answers (deferred, a note) is now pinned as refused while awaiting, an entry's as allowed.
- (small) `pending_build` read the vintage twice; the summary it returns could describe a rebuild that landed
  between the rows and the header. It now returns the one `built()` the rows were decided on.

**Held (checked, no change)**
- *Determinism.* Entry digests hash the entry only (no section ids); the reach `digest` (which holds section ids) is
  only ever compared run→bundle→release, all cut from one run; split anchors exclude coordinates resolved by a build.
  Nothing compares across two atlas builds.
- *Digest matches, binding moved.* Registry / atlas: the reach run and bundle must be on the served atlas (handles +
  registry digest). Split added/moved: `entry_split_ids` × the anchor record. Matcher overrides act at ingest (they
  move `matched`, so the entry digest). Absorbed ids: the tool digests through the served registry, as reach does.
  Residual (not fixed): a split added on a water a rule names only by `extents[].item_id` (not `matched`, not by
  split id) is not in `entry_split_ids`; it only re-sections a water bound whole, and the served atlas check still
  holds the binding on screen to one atlas.
- *Release from an older bundle.* `release_is_of` (+ mutation pin) holds; `latest` replaces a re-cut id by rename
  (fresh mtime). Residual: a bundle rebuilt by changed deliver code with identical corpus + reach digests is
  indistinguishable to the release check (no bundle content digest exists in `meta`).
- *Stale reach report / no restart.* `built()` is keyed on (mtime_ns, size) of report.json, the bundle, every release
  manifest and the atlas files; the atlas half on its own four stamps; `ensure_fresh` drops the registry on a promote.
  `test_curation_review_vintage` (3) passes through one TestClient.
- *Verified mark while awaiting.* `post_mark` calls `refuse_unbuilt` for entry, rule and split scope before
  fingerprints are stored; `Mark` requires all four checks `y` for verified, so T/R-only can never be verified.
  App gate mutation pin holds.
- *Second copy of the digest.* `entry_digest` / `anchor_digest` / `rule_section_counts` / `latest` each defined once
  (test); the review fingerprint's old `digest(split_anchor(..))` serialisation is byte-identical to
  `splits.anchor_digest` (same JSON options), so no phase-1 mark moved.
- *Tacheeda / Twin / Main Lake.* The fixture shape reads `sections: 1 in the bundle, 2 now`. Live bundle (rebuilt
  10-23 01:47), 31 entries (every Tacheeda / Twin / Main Lake entry + 25 random water rows, seed 7): 0 mismatched,
  0 unchecked — no false mismatch.
- Mutation sweep over `vintage.py` (13 mutants: moved split, reach/bundle atlas, release predates, no release, no
  bundle, count compare, split ids, reach entry, reach/bundle predates, atlas commands, split clause): all killed
  after the fixes, and so is the new `bundle_rebound` check.

Checks (unfiltered, via heavy.sh): review / curation-review / marks / reaches / publish / release / splits test files —
281 passed; 1 failed `test_fixture_release::test_the_bundle_half_only_paired_to_one_derived_handle_table` — the
`access` file, pre-existing (fails identically with the branch base's release code).

