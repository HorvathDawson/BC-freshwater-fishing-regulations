Saved by the main chat from the checker's hand-back (the checker could not write files).

# General audit, lane "code": what landed 2026-11-14 to 11-16 (FULL REPORT)

Correction first: my earlier hand-back pointed at a `report.md`. That file was never written (the write was refused), so this message is the report. The folder holds only my check scripts (`rest1.py`, `rest2.py`, `rest3.py`, `pieces.py`, `delta.py`) and `src_commits.txt` (the 97 commits).

Head `d0d584159`, branch `redesign/stream-sections`. Read only: I changed nothing in the repo. `handoff/PLAN/OWNER-TODO.md` shows as modified in the working tree; that is another lane's edit.

Scope: 97 non-merge commits since 2026-11-14 that touch source under `pipeline/`, `curation-review/`, `scripts/`, `deploy/` or `app/`. I read the pipeline, gate, upload and review-backend commits myself. Two Opus sub-reviewers read the app commits and the infra / test-tier commits; their findings are marked "(sub-reviewer)", and I spot-checked three against the files (the fast-fail trigger pattern, `lane.ts` `promote`, the `site.sh` dirty stamp).

**No defect found changes what an angler is told in release r-46e4355a7277 today.** Every finding is latent (needs a future DFO page change or a data shape not present now), in the review app, in a gate's diagnostics, or in app navigation and speed.

---

## Findings, most severe first

### 1. A dormant place that DFO lists again can be answered AFTER the rest-of place (T761)
- **Where:** `pipeline/deliver/salmon.py:367-405` (`rests_behind`, `rest_after`); `pipeline/regs/dfo_salmon/entries.py:1022-1071` (`rest_listing`, `A_DORMANT_LISTED_PLACE_TAKES_NOTHING`). Commit `d65d4e15d`.
- **What:** before T761 a dormant place with an extent still took its sections out of the rest, so the rest was not on them. Now a dormant place "takes nothing" and the rest binds those sections as its own. `rest_after` moves a rest behind its LIVE listed places only (`after[rest]` holds only `listed`). Between a rest and a dormant place the plain size order decides, and a rest with few own sections sorts first.
- **In the release:** the Stamp River. Chain 55 is `stamp-river:except-in-those-areas… [active, rest, 2 sections]` > `upstream-of-signs-located-at-girl… [dormant, 7]` > `whole [dormant, 10]` > region. Chains 20 and 56 have the same shape.
- **Failure scenario:** DFO prints the old wording "upstream of signs located at Girl Guide…" again. The live feed gives that place rules by fingerprint (dormant places are kept in their chains for exactly this). The app composes the release's chain in its stored order (`salmonFeed.ts:279`). The rest's rule (chinook and coho open 129 days on these sections) answers before the re-listed stretch, including any closure it prints. It lasts until the next release, when the place turns active and becomes a listed place.
- **Reproduced:** partly. The chain order is read from `data/release/r-46e4355a7277` (`rest2.py`). The feed step is by reading (`live.py` docstring, `salmonFeed.ts:279`).
- **Today's answers:** not affected. With and without each rest in its chain (`rest3.py`), the only status changes are the intended ones against the region default. Where a rest stands before another water-tier place (Skeena chains 188, 290) both say closed.
- **Fix:** in `rests_behind`, also put the rest behind its DORMANT left-out places on the sections they cover. They have no rules today, so nothing changes now, and they answer first when listed again. Pin with a test: a dormant place larger than the rest's own sections.

### 2. "Unless DFO's words name it" cannot fire for a DFO place (T743, and T679 D5 before it)
- **Where:** `pipeline/atlas/reach/build.py:1180-1194` (`_through_river_with_the_lake`) and `:874` (`confluence_excludes`); shaped by `pipeline/regs/dfo_salmon/entries.py:939-948` (`to_reach_input`). Commit `eafa7b9f5`.
- **What:** both readers take the rule's words from `rule["verbatim"]` / `rule["extent_text"]`. `to_reach_input` hands the builder neither; its keys are pinned by `test_dfo_salmon.py:864` (rule_id, extents, includes_tributaries, tributaries_only, tributary_excludes, `_joining_water_stays_out`). So `words` is always empty, `_rule_includes` is always false, and a DFO lake place's through-river, or a joining water at a confluence cut, ALWAYS stays out.
- **The test does not test it:** `test_lake_through_river.py::test_a_dfo_lake_place_whose_words_name_the_river_takes_it` passes a hand-built rule with `verbatim=`. The real path never builds that rule.
- **Failure scenario:** DFO writes "Kitsumkalum Lake, including tributaries, including the Kitsumkalum River". The river above the lake stays out of the place with only a `stays_out` diagnostic, and reads "not opened by DFO: closed".
- **Reproduced:** by reading, and checked on data. The run has 2 through-river and 7 confluence diagnostics; no active place's `specific_area` names the water that was kept out. Nothing is affected today.
- **Fix:** `to_reach_input` passes the place's `specific_area` as `extent_text` (location words only, no rule fields), and the test drives `to_reach_input`.

### 3. Review app: a confirmed closed area's new cuts do not reach the proposals list until a later save or a restart
- **Where:** `curation-review/backend/reuse.py:1203-1264` (`marks_delta`, `proposals_stamp`), commit `3920760a4` (13:44), against `pipeline/regs/review/marks.py` `cut_proposals_of`, commit `fe11e0af7` (13:13). Two lanes, 30 minutes apart.
- **What:** `marks_delta` says a save proposes a cut only when the line has top-level `new_splits`. A verified `closed_area` mark carries its new ends in `piece.new_splits`, which `proposal_groups` now reads. So `proposals_stamp()` keeps its old value, and the caches keyed on it are not rebuilt: `dfo._PROPOSALS_MEMO`, the withdrawn list, the real ids, the checklist context (`checks.py:177`, `dfo.py:79, 93, 1840`).
- **Failure scenario:** the owner confirms a closed area with a new end. Its cut proposal is not listed, and the area stays "carried" (not bound) with nothing on screen saying a proposal waits.
- **Reproduced:** yes, on a temp copy of the marks store (`delta.py`). Appending line 2742 (the Shuswap closed area, one new end) gives `new_splits: False` and an unchanged proposals stamp, while `proposal_groups` goes 1215 -> 1216. All five closed-area marks in the store have this shape. The four pieces bound today are past this.
- **Fix:** in `marks_delta`, also read `(m.get("piece") or {}).get("new_splits")`; better, compute it with `cut_proposals_of` so there is one reader.

### 4. The pieces gate hides why the worker "cannot be asked", and says nothing when it passes
- **Where:** `pipeline/deliver/release/pieces_gate.py:44-51` and `:69-70`; `deploy/staging/upload.sh:21`. Commits `be6484e1b`, `5c5078b2a`.
- **Tonight's fix is right:** the request names its client, and a test pins it.
- **The `except Exception: return None` does NOT let a release through:** None refuses. What it hides is the cause. A 403, a DNS failure, a timeout and bad JSON all read "cannot be asked (no readable /status.json)", which is why the 403 took a while to see. `site_gate` carries the exception text; this one does not.
- **A pass prints nothing,** so the log does not show which worker was asked.
- **Failure scenario for the override:** `deploy/staging/upload.sh:21` takes `FEEDS_URL` from the caller's environment. A shell that still has `FEEDS_URL` set for another bucket (the prod script's rehearsal form documents setting it) makes the staging gate ask that worker, silently. The bucket is hard-coded; the worker it is checked against is not.
- **Small:** `isinstance(True, int)` is true, so `dfo.pieces: true` passes as shape 1.
- **Reproduced:** no, by reading.
- **Fix:** return the reason with the None; print "asked <url>: dfo.pieces = n" on a pass; hard-code the staging worker in the staging script (or print the override); refuse a bool.

### 5. T778 `lake_cuts`: correct for the built data, two shapes it would place wrongly
- **Where:** `pipeline/atlas/reach/extent.py:508-514` and `:766-769`. Commit `a2ba7396b`.
- **Sound for the normal cut pick:** a window above a lake ends at the lake's inlet, and a window below starts at its outlet. I worked through all four combinations and the rule is right in each. The commit's full-run diff reports only the two Middle Shuswap places changing.
- **Shape (a), goes inside wrongly:** an entry with two waters where the second drains into the cut lake on its own (not a delta channel of the cut's river). Its detached pieces now read "just above the lower cut" and go INSIDE "X River upstream of L Lake". Before, they walked through the lake and read outside or straddling.
- **Shape (b), goes outside wrongly:** a window that INCLUDES the lake and ends at its inlet (a curated `cut_pick`, or a confluence cut aliased onto the lake's inlet edge). A piece draining into the lake mid-lake now reads "just above the upper cut", outside.
- **Reproduced:** no, by reading. No instance in the current run.
- **Fix if wanted:** take `lake_cuts` only for a piece whose watershed code is the cut line's own (a side channel of that river), and only when the cut is the lake end nearest the window.

### 6. `channels.py` caches by `id(registry)` without holding the registry
- **Where:** `pipeline/atlas/reach/channels.py:31-47` (`_owners`) and `:58-63` (`side_channels` memo). Commit `9d8d2f381`.
- **What:** `_OWNERS` and `_MEMO` are keyed on `id()`. `extent._HELD` keeps the object beside the id for exactly this reason; here the old registry can be freed and a new one can get the same id.
- **Failure scenario:** the review app reloads the registry after an atlas promote without a restart. A reused id serves the old build's side-channel table, so the preview and the reach run could differ (AGENTS rule 16). The reach run has one registry and is safe.
- **Reproduced:** no, by reading.
- **Fix:** store `(registry, table)` and compare by identity, as `_HELD` does.

### 7. App: a promoted file does not make the lane give way (T737) (sub-reviewer)
- **Where:** `app/packages/data/src/release/lane.ts:151-154` (`promote`, the "started" branch); `drivers/source.web.ts`, the `get` path for `askedAhead`. Commit `bf23b3819`.
- **Failure scenario:** the lane is fetching `core` ahead with `regs-sid` waiting behind it, and a tap asks for `regs-sid`. `promote` starts it as the reader's but never calls `yieldSoon()`. `core` is not aborted and the two share the line until `core` finishes. That breaks the module's own "reader first, aborted after joinMs" contract.
- **Reproduced:** yes, by the sub-reviewer. `lane.ts` run with `joinMs` = 10: after 50 ms `core` was not aborted, state `{running:'core', readers:1}`.
- **Effect:** speed only; no wrong data.
- **Fix:** call `yieldSoon()` after `start(it)` in `promote`, and add a lane test for promote-while-running.

### 8. App: after a salmon merge, Upstream may not reach the continuing water (T750) (sub-reviewer)
- **Where:** `app/packages/core/src/waterNav.ts:348-349` (`crossUp`). Commit `e920d4d0d`.
- **Failure scenario:** parts 1, 3 and 4 merge and side channel 2 does not. `mergeNav` orders merged entries by their lowest index, so the entries become [0], [1+3+4], [2]. `crossUp` needs the entry to be the last one. Upstream from the merged part goes to the side channel; at the side channel it reads "highest" with no cross. The step into the next water is unreachable. The end check in `waterGroup` runs before the merge, so it does not catch this.
- **Reproduced:** no, by reading. Navigation, not a regulation answer.
- **Fix:** in `mergeNav`, place the entry holding `prev.at` last (and `next.at` first), or check the ends after the merge.

### 9. App: the Flow / Level pick carries to the next reach (T728) (sub-reviewer)
- **Where:** `app/packages/ui-native/src/WaterViews.tsx:73` (`const [own, setOwn] = useState`); `WaterModeHost.tsx:44`. Commit `6dd66973d`.
- **Failure scenario:** `WaterConditions` is not keyed by item or `tapSid`. Pick Level in the card on reach A, then open reach B in the Conditions view: B opens on Level even when the map shows Flow. The same reach opens differently depending on the way in.
- **Reproduced:** no, by reading.
- **Fix:** reset `own` when `item` / `tapSid` changes, or key the component on them.

### 10. Tonight's loosened assertions
- **`pipeline/tests/test_review_closed_areas.py`** (`6e1993a9f`): honest about why (the Deadman cut is built now, so the point is no longer far from every cut). But the "far" branch is no longer exercised by any fixed input: `near is (metres <= 200)` passes for a mutation that always answers "near" on the live tree. Fix: keep the live case and add a coordinate known to be far, or a fixture.
- **`app/.../WaterScreen.unnamed.test.tsx:78`** (`39918037e`, sub-reviewer): `tribClosedUnnamed` picks a reach BY the properties the test then asserts ("Closed all year", a non-empty chain). The old check that the closure is the receiving water's own rule (`r1:harris_creek`) is gone. What is left real: the pick throws when no such reach exists. Fix: assert the chain rule belongs to the water the store says the reach drains to. The 500 -> 450 floor is honest (562 - 86 merged + 6 new, about 483).
- **`app/tools/regs-recut/taps.py`** (`fab8b8ed0`, `53d8e74c5`): both edits are right. I read the store: Fraser part 7 is "From the signs 2800 m downstream of Hell's Gate up to Hell's Gate", closed, 1 section; part 23 is the Region 7 part (113 sections). Labelled states are held by label and section count. The weak spot the second commit names is still there: a closed state is held only to "the store's part at the declared index is closed" (`taps.py:433-437`), so the next renumbering can land it on another closed part again. Fix: hold a closed state to its section count and its part's ends as well.

### 11. Infra (sub-reviewer; the first one spot-checked)
- **`scripts/fast-fail.sh:188`** (`c9f050557`, `ebc3ca7d9`): the mini-release check runs only for `pipeline/(atlas|deliver|gauges|tools/mini_release)`. Failure: change only `pipeline/regs/**`; fast-fail passes; CI fails after the push. That is what happened on 11-15 (fixed by `ebc3ca7d9`). By reading plus the commit list. Fix: trigger on any non-test pipeline Python file (about 10 s).
- **`deploy/staging/site.sh:61-62`** (`dae26147e`): run on its own it stamps `dirty:true` but does not refuse; the refusal and `SITE_DIRTY_OK` are only in `site-all.sh`. Failure: export from a dirty tree, run `site.sh`, it deploys. By reading. Fix: the same refusal block in `site.sh` when `SITE_DIRTY` is unset.
- **`deploy/prod/site.sh:17-25`** (`dae26147e`): stamps the commit and dirty state at deploy time, not the export's. Failure: build with uncommitted edits, commit or revert, deploy; `/site-release.json` says "clean" at a commit the export was not built from. Prod also has no refusal when `snapshot.json`'s release is not LATEST or is null (line 45). By reading. Fix: stamp at build time and compare at deploy.
- **`pipeline/deliver/release/__main__.py:41-50`** (`931f14e75`): `--no-basemap` / `--no-synopsis` are accepted for the live release root. Failure: a mistyped flag cuts a real release with `basemap: null`. By reading. Fix: refuse both unless `--out-root` is outside `data/`.
- **`pipeline/tools/mini_release.py`** `_sqlite_content` (~629) and `_gpkg_content` (`4a7c92f10`, `c9f050557`): rows are sorted before hashing, so "two runs identical" and `--check` cannot see row-order differences. Stray files inside `r-*/` that the manifest does not name are not compared either. By reading.
- **`deploy/staging/build-ios-sim.sh:74`** (`495c9d490`): `otool -l | grep -qE` under pipefail, the pattern `e7d642bcf` fixed in site.sh. Confirmed latent: safe today (30 KB output, 5 of 5 runs rc 0); a false fail, never a false pass, once output passes about 64 KB. Fix: `grep -c … >/dev/null`.

### 12. Small silent fallbacks (by reading)
- `curation-review/backend/reuse.py` `_region_rows` (`7cff9cc72`): a missing `splits.resolved.json` returns `{}`, so no region ends are offered and nothing is said.
- `curation-review/backend/reuse.py` `cut_label` (`3920760a4`): `except FileNotFoundError` falls back to the old border label.
- `app/packages/ui/src/prefetch.ts:220` (`673b1d8e3`, sub-reviewer): a rejected `waterRegionsOf` is swallowed and not reported, and the previous water's regions stay first in the background order. Order only. Fix: `reportFailure` and `want([])`.

### Notes, not defects
- **`rest_after` and unlisted places:** it puts a rest right after the LAST of its listed places, and so ahead of any larger unlisted place. On the Skeena that puts the rest (bounds 54 sections) before three active places of 72 to 94 sections, which agrees with narrowest-first by bounds. A listed place smaller than an unlisted place that is itself narrower than the rest's bounds would be ordered wrongly; no such chain exists today.
- **`entries.py:997`** (2026-11-10, before the window): a listed place rides in the rest's entry with `includes_tributaries: sib.binding.tributaries`, so None inherits the REST's flag, against `rule_of`'s own comment. Today every `behind` section is one the listed place binds itself, so there is no effect.
- **T573 pieces are not clipped to their parent place.** All four bound pieces lie inside their parents today (`pieces.py`).
- **`pipeline/deliver/salmon.py:939-941`** says the app answers "not known" where the feed lacks a bound piece. No code on this branch reads `piece` / `bound` (`salmonFeed.ts:279` skips a location the feed lacks); the T757 app half is on a branch and tracked. I did not work out whether a reworded closed area can open its stretch in between.
- **T731 `useKeptWhilePending`** (`b40336850`, sub-reviewer): keeps the last bar on any frame change, so for up to 1.2 s a water opened from its exit link shows the previous water's bar and map (inert, aria-busy). By design.

---

## Read and found sound (one line each)

**Pipeline and gates (mine)**
- `d65d4e15d` T761, apart from finding 1: bounds, own list, skipped / dormant listed places, `behind` rows; nine rests in the data, no skipped place, no gap between `behind` and the listed places' own rows.
- `a2ba7396b` T778, apart from finding 5.
- `7958b3c60`, `e3d6d5f26` T763: the band comes from the one size reader; any shape other than "over a size" stops the build.
- `93a481b71` dead ends: terminates, deterministic, never past a carve-out or a refusal, never the reach's own code.
- `a458c1fd4` T753 lake loops: 7 lines, 703 m moved, idempotent on reload.
- `3bbb20082` T753 criterion 5 and the confluence pick: Kotcho held; the anchors change is a move into `_place` with the inner `continue`s kept; the live atlas lists 3 unplaced cuts, all gauges.
- `b58aa325a` T573 build step: 17 pieces, 4 bound, 13 carried.
- `fe11e0af7` T573 new cuts become proposals: sound apart from finding 3's interaction.
- `be6484e1b` T573 piece marker and gate: sound apart from finding 4.
- `554558a12` T573 oracle as a committed fixture.
- `5ea02dfa9` reach cli: the loop variable no longer rebinds the report.
- `eafa7b9f5` T743 through-river stays out: the stay-out is right; only its exception is dead (finding 2).
- `9d8d2f381` T755: `channels.py` logic, `ExtentFix` fields, `extent_of`, `excludes_of` (apart from finding 6).
- `3920760a4` T755 follow-up: border ends, `Sources.parsed` copies (apart from findings 3 and 12).
- `0a6c82dae` T755 CI fixes.
- `7cff9cc72` T742 C region ends, reader side (apart from finding 12).
- `5c5078b2a` upload gate names its client (apart from finding 4).
- `962dddfa2` salmon-6 size gate: only `salmon-6` gets 420 kB.
- `deploy/staging/upload.sh`, `deploy/prod/upload.sh`: the only edit in the window is the pieces gate; staging's bucket is hard-coded; prod keeps `CONFIRM_PROD`, the byte-identical manifest check and the prod worker hard-coded.
- `pipeline/deliver/release/site_gate.py` (not changed in the window): refuses on an unreadable site and says the cause.

**App (sub-reviewer)**
- `a43e750b9` T737: `aheadDone` / `wholeHere`; the idle core is prefetched, then opened.
- `e974cabf7` T737: background order and the reader yield.
- `673b1d8e3` T737: `openDone` guarded by `dbs.get(name) === p` (apart from finding 12).
- `bf23b3819` T737, apart from finding 7: abort and requeue keep the same promise; the gate joins by key, so promoting cannot deadlock.
- `3d21000ad`: inventories only.
- `b35f91e0b` T737: the native ahead read returns empty bytes and its only caller discards them.
- `514f2cc14` T734: the `ready()` guard runs before each call; reject instead of crash.
- `7b60c75f1` T747: a missing frame now fails and is reported.
- `d3c7376a0` T752: split layout.
- `6c4547b13` T717: kept browse map and look.
- `f7c024932`, `5e07172a5`, `c099e9977` T729: credits Modal, back layer, close on look change.
- `b74512aee` T730: search memory cleared on close.
- `4e793d61f` T762: the keep-day sentence holds for a year with no run, a run over the New Year, several runs, the last keep day, and "next year"; it does not touch `pipeline/deliver`.
- `e62331249` T764: the sweep removes only `run-<digits>` folders under `<tmp>/app-tests` whose process is gone.
- `9af44b246`, `ffd44b943`, `f9d47f0d9` T724: a missing Team ID writes no file; an invalid one throws.
- `67c9b32f1` feeds worker: the per-address key cannot collide with the total key.
- `b40336850` T731: sound (see the note above).

**Infra (sub-reviewer)**
- `1e924f612` T667 slice 7: the hidden-builds mode switches on only when every `needs_*` / `slow` marker is deselected, and `need()` fails rather than skips.
- `26535857f`: smoke rule; smoke + data is refused.
- `424a4be66`, `9db8065fe`: live counts only list; `PIPELINE_PY` has no fallback to PATH.
- `acd561469`: with the flags absent the full atlas build reads exactly what it read before.
- `464c5d45c`, `1cb204a3f`: gauge tables read `data_dir` (default unchanged); a named missing input is refused.
- `4b62638da`, `30400c415`: sound.
- `4a7c92f10`, `931f14e75`, `c9f050557`, `ebc3ca7d9`: the mini-release `--check` is real and caught the drift (limits in finding 11).
- `c507c60a8`, `6a12b63cc`: an empty file list fails under `set -e`.
- `2d5f64048`: semver sort fix.
- `bbee2220f` heavy.sh third lane: lock handling and the memory kill unchanged; a lane cannot be shared.
- `e7d642bcf`, `f3028f9c3` site.sh: tested; a present tag on a 4 MB stream gives rc 0, an absent tag, corrupt gz, missing file and empty input all give rc 1.
- `dae26147e`: no path from staging to prod (separate wrangler names, buckets, routes); limits in finding 11.
- `9fe5e2d6c`, `2dc0c9a2c`, `164c928c2` R8: refused for production in both `app.config.js` and `build-apk.sh`; no stale `mapping.txt`.
- `b2dba0297`, `3c2d0ab9f`: version bumps.
- `216495454`, `f51870652`, `ab27dd95e`, and skimmed `1f9fc1263`, `49b7226b6`: every simctl call targets its own simulator; the only delete is the runner's own temp file.

---

## Not read
- `pipeline/tools/emit/dfo_vectors.py`: no commit in the window touches it (only `closure_oracle.py` did).
- T759: not on this branch (built on a branch).
- Review app frontend (`.tsx`, `dfoExtent.ts`) in every T742 / T755 / T761 commit.
- The backends of `2a14a32fd`, `52a89a87a`, `bd1489030`, `3d7a81bbd`, `97d0a6e68` beyond a skim. The anchors picker was read on the backend side only (`reuse.py` region and border ends).
- `pipeline/regs/dfo_salmon/suggest.py` (`319a447a0`, `8a28913c1`, and the T755 suggester): proposals only, skimmed.
- `ec75d1523`, `da9b1f4b0` (the marks audit tool, 1,080 lines): a read-only tool, not read.
- The fixture and data parts of `8566f792d`, `ebc3ca7d9`, `c9f050557`, `acd561469`, `1cb204a3f`, `931f14e75`; the accepted golden pages were not compared.
- Test-file diffs except where a finding needed them, including the rewrites in `424a4be66`.
- iOS and test-bed scenario bodies (`6572ee881`, `59aaa4de8`, `efd9ada22`, `1f3376368`, `f56b80ce5`, `942ec984b`, `bcbbe65e2`, `b266e8fd7`), `SearchSheet.tsx` / `SheetRow.tsx` layout, and the R8 keep rules.
- Remaining small commits not individually reviewed: privacy and data-safety page wording (`9048fbc20`, `fa0d0a45c`, `494ccf0f4`), and `9fe5e2d6c`'s keep-rule file.
- No run needed `graph.pkl`; nothing was run through `heavy.sh`. Findings 2, 4, 5, 6, 8, 9 and 12 are by reading only.
