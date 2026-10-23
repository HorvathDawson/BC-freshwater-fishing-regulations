# T80 — Small open items (audit N22, D29, HANDOFF-ui)

Status: PARTIAL — 3e563859, 0b9e9b30 (1); c3f91b83 (2); e306215d (3, needs deliver); 78f9f4bc, a75c942b (7); 2cb3a9da, e160fa1d (T56-review lines 1-2, need deliver). Left: 3's two open shapes + the klmb exclusion after the re-cut; 4's fix (reproduced, cause not pinned); 6 (needs a push); owner calls Q-T80a/b/c; after the re-cut the klmb_1010 conformance exclusion MUST be rewritten (it will fail). Reviewed c7a82035 (whole-set pin). See "Lane 2026-10-22" and "Review 2026-10-22".
Deliver fix 164a69cb: item 3's one-kind group stopped `pipeline.deliver all` (model.py refused it); the answers model now accepts exactly a TROUT_CHAR one-kind group on its own row's pool, and test_answers_one_kind_group.py runs the build's `validate_section` on the T80 waters (+2 mutation pins).

Each item closes with a fix + test, or a recorded "no" with the reason:
1. S1 photo minors (implement_progress): draft photos named after the first draft id; the web picker hangs where the
   input has no "cancel" event (Safari < 16.4); unit words "m" and "m3/s" in ui/leadUp.ts instead of words.ts; a second
   web photo-store instance does not revoke an object URL on delete.
2. The page-bug mutation "count comparison drops a 2-fish release" lost its Kitimat witness
   (test_answers_rows_fixes.py: "needs another water"). Find another witness water.
3. D19: klmb_1010's pooled row is drawn on no card path (`core/regs/keep.ts`, excluded from Keep parity). Fix
   data-side: a pipeline `group` for the row, then remove the exclusion (needs a deliver).
4. "Map → Conditions flash on tab switch" (HANDOFF-ui; one frame of the wrong colouring). Reproduce; fix or close.
5. Whole "Kootenay Lake" not a search result (log 2026-10-21). Probably fixed (search.test.ts); verify and close.
6. A real CI run on a scratch branch has never been done. Do one.
7. Flakes recorded in implement_progress and never fixed: the apps/mobile `back.test` browser-back step under load;
   `tools/no-localhost` racing check-boundaries' temp `_bnd.ts`. Reproduce under load; fix or pin with a reason.

Acceptance: each item is a line here marked fixed (commit + test) or declined (reason).
Depends: none (3 needs a deliver). Size: M. Owner: delegated. Default for D19: (a) a pipeline-side group.

## Added 2026-10-22 (T56 review)
- Own-row names keep the book's quote marks in "applies" lines: '"Diana" Creek', '"Lower Beaverpond" Lake'
  (place_names.row_name; see_name already strips them). Decide: strip, or keep as the book prints.
- Region 5 and Region 7 both print "Blackwater River: see West Road River", so search shows the pointer twice.
- The 5 "See X" pointers whose waters have no other row have no page of their own.

## Lane 2026-10-22 (worktree wf_4b4f5207-c7a-3) — item by item

1. **FIXED** 3e563859, 0b9e9b30. A re-captured draft keeps its first id (`ui/capture.ts`; capture.render.test +2); the
   web picker settles on page focus + 1 s grace where no `cancel` fires, and a file already on the input still wins
   (`pickPhoto.web.test.ts`, 5 tests; the grace is unexported so check-platform's native/web surfaces stay equal);
   `leadUpUnit` (from `CONDITIONS_WORDS.unit`) replaces the literals in `ui/leadUp.ts`; one object-URL map per IndexedDB
   factory, so a delete through the spots store's instance revokes the URL the app's instance drew (photos.test +1).
   Not verified on a device (Safari < 16.4).
2. **FIXED** c3f91b83. Witness: Humphrys Creek (gnis:2046), June 10 — z6 r7 sends CT+GB back against an EB+LT pool row
   (Region 6's steelhead closure holds ST apart). Found by scanning every shipped row (64 go-back lines name as many
   fish as their row, not the same ones). `test_humphrys_june_count_collision` asserts the whole back list; the
   count-comparison mutant drops exactly the CT+GB line.
3. **PARTIAL** e306215d (needs `deliver`). Bigger than klmb: the live release ships trout/char pooled rows with no group
   headline and no parent — which core `keep.ts` draws NOWHERE — on 78 row frames / 1,022 waters. Fixed: a one-kind
   trout/char total that is its row's OWN pool and not limited to a kind of water is a group
   (`rows.A_TROUT_CHAR_TOTAL_OF_ONE_KIND_IS_A_GROUP`, mutation-pinned; `test_answers_one_kind_group.py`, 5 tests):
   Kootenay Main Body's RB 10, Tranquille RB, the Region 7 brook trout lakes, ~15 other own brook trout quotas. Card gate
   over all 1,022 affected waters: 0 problems; answers suite 855 pass. Other fish rows untouched.
   **Still drawn nowhere (pinned whole in the test) → new ticket:** (a) Region 8's "20 brook trout from streams"
   (z8 trout_char_quota.r5, ~1,000 Region 8 streams — the bulk): as a group the card gate's zone-total proof counts lake
   fish (`card_check.PLACES` includes lakes) and refuses "20" (88 problems), and its worked "Kept 2 elsewhere" line says
   18 where the rules allow 19 (24) — the gate needs a water-limited total's places, then the worked line re-checked;
   (b) a one-kind row inside ANOTHER rule's total (Duncan River DV 2 inside Region 4's 5, on Crazy Creek): a group of
   the 5 over one kind is refused by the gate.
   **After the deliver + regs fixture re-cut:** klmb_1010's row is a GROUP, so conformance `KEEP_TAB_EXCLUDE` (D19) must
   be removed or rewritten — the golden draws that row by page v36's legacy path, so the app's group lines will not
   equal the golden's; expect a reasoned exclusion ("D19 (a) as built: the group path"), not parity. The one-kind
   group's tree sentence reads "Of the 10 a day: up to 10 may be rainbow trout, any size" (card_of, untuned).
4. **REPRODUCED, NOT FIXED.** Live web app (:39218, read only; canvas sampled every frame with preserveDrawingBuffer):
   Map → Conditions on a revisit draws ~8 frames (~70 ms) of the Conditions ground (wash on) under the Map tab's line
   colouring, then the flow colours. Lead: the wash and the stream mode are one `applyLook`, so the lines' colours come
   late from the standing CHANNEL — `useConditionsColour` gates the class values on `onConditions` (`useAsync(…,
   onConditions)`), so they are recomputed on every return. Next: log `Colouring.set` and feature-state per frame;
   candidate fixes — keep the last values across a tab switch, or hold the wash until the channel lands.
5. **NOT FIXED — by design today.** The live release's `core.item` has Kootenay Lake (wbk:328974235) with `pieces = 0`;
   `SEARCH` requires `pieces > 0`, so "Kootenay Lake" finds its three parts only (AGENTS 55). search.test.ts and
   source.test.ts pin its BOX, not a search hit. Owner question Q-T80a.
6. **NOT DONE.** A real CI run needs a push to GitHub, which this lane does not do. For the merger/owner:
   `git push origin redesign/stream-sections:ci-scratch/t80`, `gh run list --branch ci-scratch/t80`, then
   `git push origin :ci-scratch/t80`.
7. **FIXED** 78f9f4bc, a75c942b. no-localhost forgives a file deleted mid-walk (ENOENT only; any other error throws —
   unit test over a fake fs); back.test's browser-back steps await their popstate (armed before the action, 2 s ceiling)
   instead of a 30 ms sleep. Neither flake reproduced (8 parallel tool runs; 4 concurrent back.test runs); both fixed by
   the race's mechanism.

T56-review lines:
- Quote marks: **stripped** (Claude call, owner call 11) 2cb3a9da — `place_names.row_name` drops the book's quotes
  ('"DIANA" CREEK' → Diana Creek). Exactly 9 `entry.name` values move against the live bundle (review: corpus vs
  bundle `entry`): Link River (r1), Jones Lake, Paq Lake (r2), Alces Lake (r4), Blackwater River, Brown Lake (r5),
  Diana Creek (r6), Chinaman Lake, Lower Beaverpond Lake (r7). Undo: `quotes=True` in row_name.
- Blackwater twice: **FIXED** e160fa1d — `rules.ONE_WAY_IN_ACROSS_ROWS` (mutation-pinned): see_pointer 51 → 50 rows.
- The 5 pointer-only waters with no page: **not built** — owner question Q-T80b.

**Rebuild this lane needs: deliver only** (`python -m pipeline.deliver`: bundle → status index → export → answers),
then the release build + regs fixture re-cut. No reach, no atlas, no tiles. Diff after: `see_pointer` 51 → 50 (the r7
Blackwater row gone); `entry.name` of the ~11 quoted rows; ui-rules-answers.json rows — groups added on trout/char
own-pool one-kind rows only (none on Other fish rows, none on z8 r5); card-check.json 0 problems; conformance Keep
parity on klmb_1010 (see 3).

Owner questions (each with the default built):
- **Q-T80a** Whole "Kootenay Lake" as a search result? Default: no — search lists its three parts (AGENTS 55); a parent
  result would need a page for a water that owns no section.
- **Q-T80b** A "See X" row whose water has no other row: a page of its own? Default: no — the pointer opens X's page; the
  pointer has no rules of its own to show.

## Review 2026-10-22 (adversarial, same worktree)
- Reproduced on live data: 1,022 waters with a headless trout/char row in the shipped answers; rebuilt with e306215d,
  996 stay headless (982 z8 trout_char_quota.r5, 14 Duncan River DV) and 19 own pools gain a group; card gate over the
  rebuild 638 groups / 247 distinct cards / 0 problems. The builder's test pinned this on 5 waters only — now pinned
  over EVERY shipped water (`test_every_shipped_headless_water_is_drawn_but_the_two_open_shapes` + its mutation, which
  bites on a file shipped without the ruling and skips after the deliver).
- Expected besides the added groups (not a defect): on 4 Region 6 waters (gnis:2404, 29716, 30327, 37312, an EB own
  pool) the steelhead `presence` line moves off the row into the new group's `not_kept` steelhead line —
  `place_presence`'s designed home inside a group.
- see_pointer 51 → 50 and the 9 renamed rows reproduce against the live bundle / release core. Kootenay Lake
  `pieces = 0` in release core reproduces (item 5).
- **Merger, after the deliver + regs fixture re-cut:** conformance WILL fail on klmb_1010 until
  `app/conformance/src/regs/exclusions.ts` (D19_POOLED_IDS / the `plainRows.0` keep exclusion) and
  `keep.parity.test.ts`'s D19 handling are rewritten: the app then draws klmb's rainbow row as a group the golden does
  not have (golden: plainRows.0). Also `core/regs/keep.ts:125`'s comment goes stale. Default: keep the exclusion,
  reason "D19 (a) as built: drawn as a group, the golden's legacy path differs", and exclude the extra app group by key.
- Risk, not changed (no device to verify): the web picker's focus fallback also runs where `cancel` IS supported; a
  `change` arriving more than 1 s after focus (large file / slow disk on desktop Chrome, iOS transcoding) would resolve
  null and lose the pick (the person picks again). **Q-T80c** arm the fallback only where cancel is unsupported?
  Default: as built (a lost pick is re-picked; a hung picker was a dead button); verify on Safari < 16.4 and iOS.
