# Curation review: plan to make the tool fit for a full human review

Written 2026-10-22 from three read-only audits (tool, integration, scope) and a critique that checked their claims against the code and data. No code or data was changed. Readers: the owner, deciding whether to pay for weeks of review; then the agent who builds the changes.

Tool: `curation-review/` (FastAPI backend plus Vite frontend; `run.sh`).

---

## 1. Verdict: not ready for a human today

**No.** The tool is technically sound: it uses the current model, has 106 passing API tests, its save gate accepts 1,520 of 1,525 entries, and every write is backed up and atomic. But a reviewer's sign-off would not mean what it says, and could be lost.

- **A verified mark would not last.** It hashes the entry's raw JSON. 1,480 of 1,525 entries changed since 2026-09-22 and 568 since 2026-10-01, mostly migrations that kept the meaning the same. After the next sweep, almost every mark would read "changed since verified".
- **A verified mark misses the changes that matter.** It ignores `splits.json`, overrides, name variants, steelhead waters, the atlas and the bundle. A rule can move to a different stretch of river and stay "verified". 165 of 409 split points are still auto-proposals or "Verify:" items (120 graded L), they carry 69 closures, and the tool hides those notes (`_note` is never sent).
- **A re-parse would not protect the reviewer's work.** The ingest ledger dates from 2026-09-23 and matches 9 of 1,525 entries. Unedited verified entries are not protected at all, and `--replace-edited` would overwrite everything.
- **The reviewer is shown the wrong evidence.** The row crop is shown at 24% scale and can't be read. The heading drops the name's parenthetical (213 entries). "What an angler is told" is a debug table, not the app page, and it opens on the wrong water (`AnswerPanel.tsx:73`). Today the reach run (17:08), the bundle (15:49) and the release (15:53) come from different builds.
- **Dangerous buttons sit next to the review buttons.** "Rebuild graph" and "Promote" start an 18-minute, 9 GB atlas build outside `heavy.sh`. Split save, delete and rename silently move other entries' rules. Vite listens on every interface, with CORS `*` and no authentication.

Nothing has been reviewed yet (`verification.json` does not exist), so changing the mark format now costs nothing.

---

## 2. What a reviewer checks for each entry

Four checks, in this order. They become keys T, R, P and A in the tool.

1. **T: text.** The book says what we stored. Compare the source row (or, for zone entries, the chapter passage) with the raw `name` and `regs_verbatim`. Chapter pages are printed page numbers: provincial pp.4–10; quota boxes R1 13, R2 21, R3 28, R4 34, R5 42, R6 49, R7A 58, R7B 64, R8 68; definitions p.80.
2. **R: rules.** Every clause is a rule and nothing is invented. For each rule check species, quota, size, dates and gear, and that tributary scope is inherited correctly (AGENTS 9). The coverage strip shows any text no rule quotes.
3. **P: place.** The rules land on the right water. Check:
   - the right water(s) in `matched`
   - each split point is in the right place
   - the ambiguous-cut pick is right
   - the tributary walk does not over-reach
   - an undrawn part or unresolved locator is acceptable as it stands
4. **A: answer.** The app page for one sample section, date and fish says the same thing, including open or closed.

Each check is answered y / n / ?. The verdict follows from them: all y gives **verified**; any n gives **wrong**, with a problem category and a note; any ? sends the entry to the **unsure** side queue for the owner. **deferred** is the K5 state (for example, lake splits that are deliberately not curated).

---

## 3. Size of the job and the order

**Corpus (live):**
- 1,525 entries: 1,393 water rows and 132 zone entries. 3,406 rules and 135 licensing records.
- Rules per entry: median 1, 90th percentile 4, max 20.
- 647 closures across 516 entries.
- 324 entries have a cut, and 56 cover more than one water.
- 232 rules have a `review_reason` and 32 have unresolved locators.
- 409 split points; 165 still need verifying.
- 14 water rows have rules but no matched water, so they bind nothing.
- 33 rules are unresolved in the reach run. 93 ambiguous cuts on 86 rules.
- Coverage gaps (rough heuristic): 348 entries have a quote that is not in the verbatim, and 417 have more than 15 characters of verbatim that no rule quotes.

**Effort:**
- About **127 hours** (about 16 working days) with today's tool.
- About **90 hours** after the changes in section 4 (keyboard flow, checklist, legible evidence).
- The backend's first map load takes about 32 minutes. It must be warmed before the reviewer sits down.

**Order:** one queue across all regions, worked tier by tier.

| Tier | What | Count | Why first |
|---|---|---|---|
| S0 | Split points still to verify, L grade first | 165 points (120 L) | A wrong cut is wrong in every entry that uses it |
| P1 | Closures with a risk factor: cut, several waters, flag, ambiguous cut, no match. Start with the 14 rows that bind nothing, the 2 unresolved closures, 52 closures with undrawn parts, and 32 closures with ambiguous cuts | 227 entries | A wrong closure is the worst error for an angler |
| Z | Zone entries, signed off per rule | 132 entries, 361 rules | Each rule binds a whole region |
| P2 | Plain closures | 261 entries | |
| P3 | Non-closure entries with risk: cuts, several waters, flags | 116 entries | |
| P4 | Plain entries | 730 entries | |
| Sweep | Pointer and empty rows | 59 entries | About 1 minute each |

Ambiguous cuts are decided by a fixed rule (ruling E16), so they get spot checks inside P1, not a separate list. Review everything; sampling is off by default because the owner asked for a full review.

---

## 4. Tool changes

Sizes: S is under a day, M is 2–4 days, L is about a week. Each phase ends with its acceptance checks passing, run unfiltered (not through `rtk`; see the "rtk pytest false pass" memory).

### Phase 0: make it safe (S, about 1 day)
- Delete `backend/rebuild.py`, `/api/rebuild*` and `RebuildButton`.
- Vite: `host: "127.0.0.1"` (not `false`, which brings back the IPv6 localhost problem). CORS limited to the Vite origin.
- Send each split's `_note` and auto-proposal grade to the frontend. Move the Hwy 19 bridge note off `adam_river__eve_river_into_adam_river`.
- Fix or remove `AnswerPanel` (it is removed from review mode in Phase 3).
- Correct the counts and paths: 117 → 132 (`QueueList.tsx:28`, `reuse.py:852,869`, README), the splits path in `API.md`, and `graph.gpkg` → `.pkl` in the README.
- Show errors that have no field path (path `''`) at the top of the entry.
- Add the curation API, writes, fingerprint and ingest tests to `pipeline/tests/smoke.txt`.
- **Needs an owner ruling first:** the 5 entries that fail the gate, and so also fail `validate_catalogue` (see section 6).
- **Accept when:** no endpoint starts a subprocess; the server is not reachable from another machine; split notes show in `SplitEditor`; all 5 entries pass the gate; the smoke tier is green.

### Phase 1: marks that last and mean something (M, 3–4 days)
- **New store:** `data/curated/regulations/review/marks.jsonl`, append-only, validated by a pydantic `Mark` (`extra=forbid`).
  - Fields: scope (`entry` / `rule` / `split`), target id, verdict (`verified` / `wrong` / `deferred`), the T/R/P/A answers, problem category, note, reviewer (required), date, `pdf_sha256`, printed pages, and the fingerprints below.
  - Marks are refused on an entry that fails the gate.
- **One fingerprint module** in `pipeline/regs/review/`, used by the tool, ingest and tests:
  - `text`: raw `name`, `regs_verbatim`, `source_pages` and `symbols`.
  - `meaning`, per rule: a canonical dump of the structured fields, without `label`, `review_reason` or served fields. Item ids are folded through the registry mapping `reuse` uses.
  - `binding`, per rule: matched item ids (taken from the entry), op, split ids, each split's canonical anchor JSON (not coordinates, because the 102 confluence anchors have none), plus the bundle's section count and length to 0.1 km. **Never section ids**, because the atlas is not deterministic.
  - `answer`, per rule: the rule's `parts` and label from the bundle. This is the backstop for changes coming from overrides, steelhead waters or the registry.
- **Typed stale reasons** instead of one "changed": book text changed, rule changed, binding moved, answer changed, awaiting build.
- **Rule identity:** a mark keys on `rule_id` plus the meaning fingerprint. If a re-parse renumbers the rules, a mark follows the matching meaning; if nothing matches, it goes stale. It never lands silently on another rule.
- **`review.carry_marks(before, after)`** for migrations and normalisation passes. It re-stamps a mark only where the migration declares the meaning unchanged. Pin it with mutation tests.
- Retire `verification.py` and the `verification.json` path. Nothing to migrate: the file was never written.
- **Accept when:**
  - a no-op normalisation over the whole corpus causes 0 stale marks;
  - each of these makes exactly the affected marks stale, with the right reason: a split move, an override edit, a steelhead edit, a label wording change (answer), and a verbatim edit;
  - the mutation tests fail when `carry_marks` is made to lie.

### Phase 2: one vintage on screen (M, 2–3 days)
- Write per-entry `entry_digests` into the reach `report.json` and the bundle `meta`. Carry `entries_digest` and `reach_digest` into the release manifest.
- The tool shows the binding and the answer, and accepts a verified mark, only when that entry's digest is the same in the reach run, the bundle and the release. Otherwise the entry reads **awaiting build**, and T and R can still be marked.
- Fix "bundle matches" to compare per-rule section counts, not just labels. In a 200-entry sample, 7 rules differed while the tool said they matched.
- Do not change `check_corpus` or the build stage keys.
- **Accept when:** after an entry edit, that entry shows "awaiting build" until reach and deliver have run, and then it clears; the Tacheeda / Twin / Main Lake mismatch is reported as a mismatch.

### Phase 3: a review screen, separate from fixing (L, about a week)
- **Two modes.**
  - Review mode (the default) exposes only `POST /api/marks` and reads.
  - Fix mode needs a `--fix` flag and is for the owner only. It holds the editors, entry save, and split save, delete and rename.
- **Layout:** evidence first, then the T/R/P/A checklist with the verdict bar always visible. Editors are hidden in review mode.
- **Keys:**
  - `t` `r` `p` `a` to move between checks, and `y` `n` `?` to answer
  - `j` / `k` for next / previous, `u` for the next unsure entry, `o` to open the app page, `m` to open the map
  - The URL hash holds the current entry, so a reload resumes in place.
  - The current row stays in the list after marking (fixes `pos` becoming "–").
- **Evidence:**
  - A `row_boxes.json` sidecar from a one-off extractor helper (no credits) gives a legible crop and a highlight on the whole page.
  - Zone passages are found by text search on the chapter page.
  - The heading uses the raw `name`.
  - A coverage strip marks the verbatim text that no rule quotes.
- **Splits (tier S0):** a split card with the note, the grade, the map, and a "confirm split point" mark (scope `split`).
- **Zone entries:** per-rule marks, plus a one-line impact summary ("binds N sections in region R"). Drop the "empty means it binds nothing" hint.
- **The answer:** remove `AnswerPanel` from review mode. Show the real app page via the start link (`?regs=<item>&part=&date=`).
  - Once the owner approves: an iframe served by the reviewer's own `serve-tiles` (`TILE_PORT` and `RELEASE_ROOT`) on a free port, never 39217 or 39218.
  - Until then: a deep link that opens in a new tab.
- Progress and an ETA, per tier and overall.
- **Accept when:** a timed run of 20 simple entries averages 1.5 minutes or less each; no review-mode request writes anything except a mark; Adam River shows "ADAM RIVER (except Eve River)" and a readable crop.

### Phase 4: queue, pre-checks and output (M, about 3 days)
- `precheck.py` plus a CLI. Severity-A codes are those already measured: A1, A2, A3, A4, A5, A7, A8, A10, A11, A12. A6, A9 and A13 stay at severity B until they are measured.
- One queue across all regions, ordered by the tiers in section 3, built from `rule_diagnostic.jsonl`, the bundle and the split grades.
- Rename the queue status "flagged" to "parser-uncertain", and stop showing the parser status "unreviewed" as if it were review progress.
- Show second-pass agent findings from `data/generated/regs/parse/reviews/` when any exist (none do yet).
- Export "wrong" and "unsure" marks as tickets.
- A "commit marks" step that runs `git commit -- data/curated/regulations/review/marks.jsonl` and nothing else.
- Add a `cut_pick` editor (ruling E16) in fix mode.
- **Accept when:** the tier counts match section 3 within the day's drift; the ticket export round-trips; the commit touches only the marks file.

### Phase 5: gate before full review (S, plus build time)
- Re-run the AGENTS 16 parity check (review-tool binding vs the reach builder) over all 3,406 rules, via `heavy.sh`. Explain every difference before going on.
- A 30-entry pilot across the tiers by a person other than the builder. Log the time per entry and the defects found.
- Startup: warm the caches before the reviewer arrives, or document the 32-minute cold start with a ready signal.
- **Accept when:** parity is 100% or every difference is explained; the pilot shows no tool blocker; the false-stale rate is 0.

**Total:** about 3–4 weeks of agent work before the human starts.

### What to delete
- `backend/rebuild.py`, `/api/rebuild`, `/api/rebuild/promote`, `RebuildButton.tsx`
- `backend/verification.py`, `/api/verification`, `VerifyBar`'s single verify button (replaced by marks)
- `AnswerPanel` in review mode
- `--replace-edited` and the `ingested.json` ledger, along with its `seed-ledger` path (see section 5)
- The stale "117" comments and the wrong paths in `API.md` and the README

---

## 5. How review fits the pipeline

**What an edit reruns.** The tool prints the command; it never runs it.

| Change | Rerun | Command for the human |
|---|---|---|
| A mark | Nothing | (commit the marks file) |
| Entry fix: rules, extents, matched, licensing | reach, then deliver; tiles are unchanged | `scripts/heavy.sh build 30 -- python -m pipeline build --no-tiles` (check that the vintage check passes with tiles unchanged) |
| Split save, delete or rename (fix mode only) | atlas, binding diff, promote, reach, deliver, tiles | `scripts/heavy.sh atlas 30 -- python -m pipeline build --atlas`, then a per-rule binding diff (AGENTS 63), then `--promote` |

A **pending build** panel lists the entries edited since the last bundle, using `entry_digests`. Those entries stay "awaiting build" until the panel is empty.

**How marks survive a re-parse.**
- Do **not** re-seed the ledger. Seeding declares every entry on disk replaceable, so a re-parse would wipe about 1,500 entries of hand curation. Normalisation also runs only once, after ingest.
- The checked-in catalogue is the truth. Ingest writes only **new** entry ids by default. It replaces an existing id only with `--replace-entry <id>` (repeatable), after printing a per-entry diff.
- Retire `--replace-edited` and the ledger.
- Marks live outside the catalogue (`marks.jsonl`), so ingest never touches them. After a replace, the fingerprints decide what goes stale and why.
- Migrations call `carry_marks`, so a meaning-preserving migration does not reset review.

**Attribution.**
- Each mark records the reviewer (required), the date, `pdf_sha256` and the printed pages.
- The repo PDF is not byte-identical to the gov.bc.ca copy, so recording the hash matters.
- Marks are append-only, so the file is the history, and git holds it after each "commit marks".
- Backups under `entries_backup/` are not history (newest 500 only).

---

## 6. Open questions for the owner

1. **The 5 entries that fail the gate** (Shuswap Lake `min_cm` 50/60 "not in own verbatim"; `zp:set_lining`, `zp:spear_fishing`, `zp:bait`, `zp:white_sturgeon_licence` "exempts share no ground"). These also fail `validate_catalogue`, so ingest for region 3 and prov is broken today. *Default:* check Shuswap against the book and fix the data; for the four `zp` exemptions, loosen the gate if the book really does exempt across ground they don't share.
2. **May the app's start link (labelled a web-only test affordance) back the review pane?** *Default:* yes, read-only, served on the reviewer's own port. Use the deep link until you approve.
3. **Who may fix as well as mark?** *Default:* reviewers mark only. Fix mode is owner-only, and wrong or unsure marks become tickets.
4. **Do reviewers sign off on `overrides.json` (498), `steelhead_waters.json`, `cross_references.json`, the DFO salmon entries and name variants?** *Default:* not in this pass. The `answer` fingerprint makes their effects show up as stale marks; review them later in their own pass.
5. **Per-rule or per-entry marks?** *Default:* per entry for water rows, per rule for the 132 zone entries, per point for splits.
6. **Should sampling be offered for P4?** *Default:* off. Full review, as asked.
7. **Retire the ingest ledger or move it into curated data?** *Default:* retire it and use the "new ids only plus `--replace-entry`" rule.
8. **Which PDF counts as the book?** *Default:* the repo copy, with its sha256 recorded on every mark, plus a one-time check of the chapter pages against the gov.bc.ca copy.
9. **Start before Phase 3?** *Default:* no. Phases 0–2 alone would let a reviewer start on tier S0 (splits) using the map, but entry review should wait for the checklist and the evidence, or the 127-hour cost is paid at the worse rate.
