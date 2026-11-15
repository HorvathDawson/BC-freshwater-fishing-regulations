# Tatshenshini: the BC border point, and the place that will not edit (T742, 2026-11-15)

Owner, verbatim: "need to add bc boundary here i cant get it to add. also i cant save or edit the extent of the other
section there". DFO Region 6, water `6:tatshenshini-river`, places `…-al~2` (Kwatini, Stanley, Goat) and `…-al`
(Blanchard).

How this was checked: my own copy of the review app on :8794/:5180 from worktree `~/bcfish-tatshenshini`. It reads
the main checkout's built data (symlinks) and a copy of his `marks.jsonl` + `cuts.json` taken at 19:00Z. Marks went to
the worktree copy, with write-through OFF, inputs sync OFF and backups in the scratchpad. The main `marks.jsonl` md5 was
the same before and after both test saves. Labels below: **[ran]** = verified by running, **[read]** = read in
code/data, **[inferred]**.

## Geography in our data

- **Tatshenshini** `gnis:25410` is one item on one blue line, `blk 360886456`, 179.9 km, in three parts **[ran]**:
  | part | measure (m) | sections | |
  |---|---|---|---|
  | lower BC | 0 – 102,478 | `:0`, `:22759`, `:46772`, `:68300`, `:89932` | |
  | Yukon | 102,478 – 145,670 | `360886456:102477` (`out_of_bc`) | 43.2 km |
  | upper BC (along the Haines Hwy) | 145,670 – 179,865 | `360886456:145669`, `360886456:167367` | |

  The border cuts already exist as atlas cuts and are boundaries of the item **[ran]**:
  - `border:360886456:0` at m 102,478, about (-137.2138, 60.0008).
  - `border:360886456:1` at m 145,670, about (-136.8848, 59.9995).

  The curated note in `region-6.json` says 102,493 and 145,645, which is a little off from the build.
- **Kwatini** `gnis:14159`, **Stanley** `gnis:14551` and **Goat** `gnis:37919` have no `out_of_bc` section **[ran]**.
  Their mouths meet the Tatshenshini on `:167367`, `:145669` and `:167367` respectively, all above `:1`. So they lie
  wholly in BC, upstream of the border.
- **Blanchard** `gnis:13637` has its mouth in the Yukon, which is not in our data **[ran]**:
  - The line starts at m 6,453 of `blk 360860293`.
  - It weaves on 60°N: 5 crossings on its main line plus 1 on a side channel.
  - 4 of its sections are `out_of_bc`, 660 m in all.
  - 15 sections bind.

## A: "can't add the BC boundary"

**Cause [ran + read].** The point picker cannot offer a `border:*` cut:
- The "a split on this map" list is `dfo._splits_fc` (dfo.py:595).
  - The Tatshenshini's two border cuts reach `_split_feature` (dfo.py:564). It takes a point only from
    `reuse._splits_meta()`, which is `splits.resolved.json`.
  - The atlas writes **no** `border:` rows there: 0 of 7,053 rows. So both cuts land in `splits.unplaced` with "the
    served atlas has no point for it", and are not in the dropdown.
  - The dropdown showed only the 4 length cuts and the 4 lake cuts **[ran]**.
- "any curated split…" (`/api/splits/search`) searches `cuts.json` only. "BC boundary" returns `[]` **[ran]**.
- T742's `region_ends` covers only `area:<region>` cuts (reuse.py:2057 `REGION_CUT`), not `border:`.
- The `~3` mark could name `border:360886456:1` only because the editor opened on the curated extent, which already
  held it.
- The same gap hits every border cut in every picker, and the regs side's `entry_split_points` too (reuse.py:2444,
  same `_split_point`) **[read]**.

**For this place no border point is needed [ran + inferred].**
- "upstream of the BC/Yukon border along the Haines Hwy; Kwatini, Stanley and Goat Creek" = those three creeks
  **whole**. This is the extent it has now: preview 19 sections, the same as the current reach.
- A point would be wrong here. Reaches resolve on the cut's own blue line (AGENTS 11), so `upstream_of border:…:1`
  on the creeks does not limit them. On the Tatshenshini it adds the whole upper mainstem (preview 38 sections), and
  that belongs to `~3`.
- What the wording means is the owner's call.
- Whether the creeks' own tributaries walk is also his call. Now: "with tributaries", inherited from the water.

**Can it be said today without a new cut?** Yes, for both places:
- Kwatini/Stanley/Goat = `whole` on the 3 items.
- The upper mainstem = `upstream_of border:360886456:1` (already saved as `~3`).

No new kind of point ("where the water leaves BC") is needed. The cut exists; the picker only fails to show it.

## B: "can't save or edit the other place" (Blanchard)

**Cause [ran + read].**
- The place's curated extent is
  `{"op":"whole","item_id":"gnis:13637","within_area":"area:region:6"}` (region-6.json:5298-5304).
- `within_area` is not in `proposals.EXPRESSIBLE` (proposals.py:493), so `unexpressed` (proposals.py:496) fills
  `extent_now.cannot_change`.
- `CheckReview.tsx:382` then passes no `onEdit`, so the panel has **no "Edit this extent" button**
  (CheckReview.tsx:649).
- The reason ("It cannot be changed here …") is shown only in the needs-an-extent block (CheckReview.tsx:710), so a
  place that has an extent shows nothing.
- No request is made and no 4xx comes back: nothing for it in his `refused-saves.jsonl`.

Ruled out **[ran]**:
- **T742's multi-target 422** (app.py:532). It is about a mark's `targets`, not an extent's items. `~2` (3 items)
  saved 200.
- **"has ✗".** It is only the checklist's `bad` rows: "the words name a part; ours is the whole water" and
  "no row checks these words". Ticking them enables Confirm.
- **Stale mark.** Neither place has a mark.

**The `within_area` is redundant [ran].**
- `reach/outside.py` drops `out_of_bc` sections from every binding.
- Preview of plain `whole gnis:13637` = the same 15 section ids as the reach now.

**What he can do right now:**
1. Tick ✗ on rows 2 and 4 of each place.
2. Press "Confirm: the extent matches DFO's words (saves)".

Both saved 200 in my copy as `verified` marks. He cannot change the Blanchard extent in the app until the
`within_area` is gone, but it does not need changing.

## The three done places (how they were saved)

All are on `marks.jsonl` (main), reviewer Dawson, 2026-11-15:
- `…downstream-of-the-bc-yukon-border` and `~2` were saved from the checklist Confirm:
  - `verified`, `answers {"P":"y"}`, no `extent_fix`, all rows ok.
  - `extent_before` = `downstream_of border:360886456:0`.
- `…-al~3` was saved from the inline editor's confirm:
  - `answers T/R/P y`, with the note "T and R not asked: implied yes…".
  - `extent_fix` = `upstream_of` `{kind: split, split_id: border:360886456:1}`, items `gnis:25410`, tributaries true.

The two marks below follow the first form: confirm, no extent change.

## Proposals (nothing lands until he finishes marking)

1. **A, the picker** (dfo.py:564-592 `_split_feature`, and reuse.py:2024 `_split_point`): when a cut id is
   `border:<blk>:<i>` with no `splits.resolved.json` row, take its point from the graph section whose
   `lower_bound.boundary_id == "split:"+sid` (its `down_m` on `blk`). Label it like `region_ends`:
   "BC boundary · km 146", so `:0` and `:1` read apart. The longer-term fix is for the atlas to write border rows into
   `splits.resolved.json` (sidecars.py:82 already knows `source: border`), but that needs a rebuild.
2. **B, the UI** (CheckReview.tsx:645-651): when `extent_now.cannot_change` is non-empty, show it beside "the extent
   now" instead of hiding the edit button silently.
3. **B, the data** (owner's call, a curated hand edit under A2): drop `"within_area": "area:region:6"` from Blanchard's
   binding (region-6.json:5302). It is the same 15 sections, and the place becomes editable.

### Two marks ready for `marks.append` (only with his permission)

These are as my copy's save path wrote them, with reviewer set to Dawson and a note. The `prints`/`checklist` digests
are today's.

```json
{"scope": "dfo", "target": "6:tatshenshini-river:upstream-of-the-bc-yukon-border-al", "rule_id": null, "verdict": "verified", "answers": {"P": "y"}, "problem": null, "note": "saved for the owner from the T742 Tatshenshini investigation (his permission); rows 2 and 4 ticked: the place is these waters whole", "reviewer": "Dawson", "date": "2026-11-15", "pdf_sha256": "08f58537261ffe51c513193013ba165754285719dec8d0332beaef1cbab8ecfa", "pages": [], "prints": {"text": "Tatshenshini River\nupstream of the BC/ Yukon border – along the Haines Hwy.; Blanchard River.", "records": [{"key": "entry", "meaning": "ebdcf971f86eeb86", "binding": "eae00f234500cb64", "reach": null, "answer": null}]}, "carried_by": null, "seconds": 25.7, "extent_before": {"extents": [{"op": "whole", "item_id": "gnis:13637", "within_area": "area:region:6"}], "tributaries": null}, "checklist": {"digest": "d363c819c58cdd32", "rows": [{"n": 1, "kind": "water", "state": "ok", "ticked": false, "wrong": false}, {"n": 2, "kind": "where", "state": "bad", "ticked": true, "wrong": false}, {"n": 3, "kind": "tributaries", "state": "ok", "ticked": false, "wrong": false}, {"n": 4, "kind": "unchecked", "state": "bad", "ticked": true, "wrong": false}]}}
{"scope": "dfo", "target": "6:tatshenshini-river:upstream-of-the-bc-yukon-border-al~2", "rule_id": null, "verdict": "verified", "answers": {"P": "y"}, "problem": null, "note": "saved for the owner from the T742 Tatshenshini investigation (his permission); rows 2 and 4 ticked: the place is these waters whole", "reviewer": "Dawson", "date": "2026-11-15", "pdf_sha256": "08f58537261ffe51c513193013ba165754285719dec8d0332beaef1cbab8ecfa", "pages": [], "prints": {"text": "Tatshenshini River\nupstream of the BC/ Yukon border – along the Haines Hwy.; Kwatini Creek, Stanley Creek and Goat Creek.", "records": [{"key": "entry", "meaning": "6b8d7a47886c99bb", "binding": "b3da13508a184c21", "reach": null, "answer": null}]}, "carried_by": null, "seconds": 23.9, "extent_before": {"extents": [{"op": "whole", "item_ids": ["gnis:14159", "gnis:14551", "gnis:37919"]}], "tributaries": null}, "checklist": {"digest": "51cab3585b8a6cb5", "rows": [{"n": 1, "kind": "water", "state": "ok", "ticked": false, "wrong": false}, {"n": 2, "kind": "where", "state": "bad", "ticked": true, "wrong": false}, {"n": 3, "kind": "tributaries", "state": "ok", "ticked": false, "wrong": false}, {"n": 4, "kind": "unchecked", "state": "bad", "ticked": true, "wrong": false}]}}
```

## Other DFO places [ran: a scan of `dfo_salmon/places/region-*.json` against his marks at 19:00Z]

- **Wordings naming the border:** only the Tatshenshini's 5 (+1 dormant). The 3 marked ones bind `border:` cuts. Swift
  ("Yukon or BC licence"), Stikine, Taku and Nass are `whole` on their item and verified: the border is applied by
  `outside.py`, so no point is needed. Thompson's "easterly border of the Skihist reserve" is a curated cut, not the
  province (unsure). There are no DFO places on the Alsek, Liard, Flathead, Kootenay, Columbia, Okanagan or Skagit.
- **Several waters in one extent** (11): Cayeghle, Cowichan (dormant), Chilliwack-Vedder ×5 (4 dormant; 4 unsure),
  Stave (dormant), Bulkley/Morice ×2, and Tatshenshini `~2`. All are editable, and most are already marked, so a
  multi-item extent is not a blocker.
- **Uneditable in the app (`cannot_change`), 15, all unmarked:**
  - Region defaults with `outside_areas` / `within_area`: 1 / 2 ×2 / 5a / 6:e / 6:f, and areas 3-6, 5 and 6.
  - Skeena B(i)/B(ii) (`watershed`).
  - Skeena–Kispiox sign zone ×3 (`within_area`, needed there).
  - Blanchard.

  Of these, only Blanchard's extra field is redundant.
