# General audit, checker "code": the code that landed 2026-11-14 to 11-16 (the checker's hand-back; its fuller report, if sent, replaces this)

## Code audit, 2026-11-14 to 11-16: no defect changes what an angler is told in r-46e4355a7277 today

Every finding is latent, in the review app, in a gate's diagnostics, or in app navigation and speed. Nothing in the repo was changed by this lane (`handoff/PLAN/OWNER-TODO.md` shows modified in the working tree; that is another lane's edit).

Full report: `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/audit-general/code/report.md`

**Commits reviewed:** 97 non-merge source commits. I read the pipeline, gate, upload and review-backend ones. Two Opus sub-reviewers read the app commits and the infra / test-tier commits; I spot-checked three of their findings against the files.

### Confirmed defects

1. **T761: a dormant place DFO lists again can be answered after the rest-of place** (`deliver/salmon.py` `rests_behind` / `rest_after`, d65d4e15d).
   - A dormant place now "takes nothing", so the rest binds its sections. `rest_after` moves a rest behind its live listed places only, so against a dormant place plain size order decides.
   - In the release this is the Stamp River, chains 20, 55 and 56: the rest (2 sections) sorts before the dormant "upstream of signs at Girl Guide" (7 sections).
   - If DFO prints that wording again, the live feed gives it rules and the rest's opening (chinook and coho, 129 days) answers first, until the next release.
   - The chain order is read from the release; the feed step is by reading.
   - Fix: put the rest behind its dormant left-out places too.

2. **T743 (and T679 D5): "unless DFO's words name it" cannot fire** (`reach/build.py:1180` and `:874`, eafa7b9f5).
   - The builder reads `verbatim` / `extent_text`, and `to_reach_input` passes neither, so a DFO lake place's through-river or a joining water always stays out.
   - The test for the exception hand-builds a rule with `verbatim=`, which production never builds.
   - Today no place's words name a kept-out water (2 through-river and 7 confluence diagnostics checked).
   - Fix: pass the place's `specific_area` as `extent_text`, and drive the test through `to_reach_input`.

3. **Review app: a confirmed closed area's new cuts do not reach the proposals list** until a later save or a restart (`reuse.py` `marks_delta` / `proposals_stamp`, 3920760a4 against fe11e0af7, two lanes 30 minutes apart).
   - `marks_delta` looks only at top-level `new_splits`; a closed-area mark carries them in `piece.new_splits`.
   - Reproduced on a temp copy of the marks store: proposal groups go 1215 to 1216 and the stamp does not move. All five closed-area marks have this shape.

4. **App, T737: a file promoted out of the fetch-ahead lane does not make the lane give way** (`lane.ts:151`, bf23b3819). Reproduced by the sub-reviewer. Speed only.

5. **Tonight's loosened tests:**
   - `test_review_closed_areas.py`: honest, but the "far from every cut" branch is no longer exercised by any fixed input.
   - `WaterScreen.unnamed.test.tsx` (39918037e): the reach is now picked by the same properties the test asserts, and the check that the closure is the receiving water's own rule is gone. The 500 to 450 floor is honest.
   - `taps.py`: both edits are right. I checked the store: Fraser part 7 is the closed Hell's Gate part and part 23 is the Region 7 part. A closed state is still held only to "the part at the declared index is closed", so the next renumbering can mis-land it again.

### Plausible, not confirmed

- **Pieces gate** (`pieces_gate.py`): tonight's fix is right, and `except Exception: return None` refuses rather than passes. But it hides the cause (403, DNS, timeout and bad JSON all read "cannot be asked") and prints nothing on a pass. `deploy/staging/upload.sh:21` takes `FEEDS_URL` from the caller's shell, so a stray value makes the staging gate ask another worker silently.
- **T778 `lake_cuts`**: right for the normal cut pick and for the built data. Two shapes would be placed wrongly: a second water of the entry draining into the cut lake on its own (goes inside), and a window that includes the lake and ends at its inlet (goes outside). No instance today.
- **`reach/channels.py`** caches by `id(registry)` without holding the object; the review app could serve an old build's side-channel table after a promote without a restart.
- **T750** (sub-reviewer): after a salmon merge, Upstream may not reach the continuing water (`waterNav.ts` `crossUp`).
- **T728** (sub-reviewer): the Flow / Level pick carries to the next reach.
- **Infra** (sub-reviewer):
  - fast-fail's mini-release check does not trigger on `pipeline/regs/**`, which is what turned CI red on 11-15 (I confirmed the trigger pattern);
  - `deploy/staging/site.sh` run on its own does not refuse a dirty `app/`;
  - `deploy/prod/site.sh` stamps the commit at deploy time, not the export's;
  - `--no-basemap` / `--no-synopsis` are accepted for the live release root.
- **Review backend**: `_region_rows` returns `{}` when `splits.resolved.json` is missing; `cut_label` swallows `FileNotFoundError`.

### Found sound

- T761 otherwise: nine rests, no skipped listed place, no gap between `behind` and the listed places' own rows.
- T763 `_duty_band`, the dead-ends change, T753 lake loops (7 lines, 703 m), the confluence pick refactor.
- T573: 4 pieces bound, 13 carried, each bound piece first in its chain and inside its parent.
- Both upload scripts: the only edit in the window is the pieces gate. Prod keeps `CONFIRM_PROD`, the byte-identical manifest check and its own worker; staging's bucket is hard-coded.
- `site_gate.py` and the `site.sh` `grep -c` fixes.

### Not covered

- `pipeline/tools/emit/dfo_vectors.py`: no commit in the window touches it.
- T759: not on this branch.
- The review app frontend, and the backends of the T742 commits beyond a skim. The anchors picker was read on the backend side only.
- The DFO suggester (proposals only), the 1,080-line marks audit tool, fixture and golden-page contents, and iOS / test-bed scenario bodies.
- `salmon.py:939` says the app answers "not known" where the feed lacks a bound piece. No code on this branch reads `piece` / `bound`; the T757 app half is on a branch. I did not work out whether a reworded closed area could open its stretch in between.
- Nothing needed `graph.pkl` or `heavy.sh`. The pieces gate, T778, `channels.py` and review-backend fallback findings are by reading only.