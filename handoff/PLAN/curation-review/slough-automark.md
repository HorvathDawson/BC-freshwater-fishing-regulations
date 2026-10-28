# Slough / canal automark (T55 / T149): what your marks say you were looking for

2026-10-28. Read-only: nothing was written to the review page
(https://claude.ai/artifact/Es3Mokt7Fs2yNvm7JgQua8), its database or the atlas. You decide after reading this.

Files: `slough-automark.json` (one row per candidate: `decision` = owner / auto / owner_list / default, `verdict`,
`by` = owner or machine, `why`, `owner_rank`, `spot_check`, `lake_probe`), the code in
`handoff/PLAN/sloughs/v2/automark/` (`automark.py` learns and scores, `imagery.py` samples satellite pixels,
`report.json` has every number below).

## Your marks

151 marks on 2,424 candidates: 135 stream, 12 not stream, 3 unsure, 1 note only. The 3 unsure ones are the
dam pools (Quinsam: "a dammed area"; South Nanaimo: "probably a lake"; Bonaparte: "probably a lake"), so here they
count as not stream. A complex marked with one click counts as one decision, which leaves 109 independent
decisions, and only **10 of them are "not stream"**. That is very little to learn from. Read the accuracy below with
that in mind.

147 of the 150 verdicts are on candidates the rules already called STREAM_BY_BOOK, LIKELY_STREAM or UNSURE. You marked
2 of the 702 LAKE-class candidates and none of the 275 sewage/tailing ponds. The model therefore says nothing about
those two classes.

## What you were looking for, in plain words

**Width is the clearest signal. You call a piece a stream unless it is wide.**

- **Up to about 44 m wide, you called it a stream** in 117 of 122 cases (96%). **Up to 20 m wide: 57 of 57.**
- **Wider than about 44 m, it was close to a coin flip**: 18 stream, 10 not. Your not-stream marks are mostly wide
  open water: the dam pools (Quinsam, South Nanaimo, Bonaparte), the Sproat piece ("could be labelled as Sproat
  Lake"), the four Attichika Creek widenings, Kootenay and Big Bar.
- **Wide marsh or swamp on a river: you marked stream** (Nazko, Nakina and Endako complexes, 3 of 3). That is too few
  to automate, so those pieces sit last on your list.
- **Satellite.** Your not-stream pieces show more open water on ESRI World Imagery (median 30% of pixels dark open
  water vs 7%) and less green (41% vs 83%). That fits "I used satellite a lot". But in held-out testing, adding
  imagery made every tree *worse*: it repeats what width already says, and with 10 lake decisions it overfits. Each
  row shows the imagery numbers (`img_water`, `img_veg`), but they do not decide anything.
- **No effect on your marks:** shape (elongation, sinuosity), how much of the river line runs through the piece,
  whether a rule binds it, whether it is named, and FWA lake-definite vs indefinite. Your stream and not-stream
  marks have the same medians on these.

## Accuracy (grouped 5-fold cross-validation, 30 repeats; a complex is never split between train and test)

The rule is one split on mean width, learned again in each fold:

| | predicted not stream | predicted stream |
|---|---|---|
| your not stream (15) | 10.3 | 4.7 |
| your stream (135) | 39.1 | 95.9 |

- Accuracy 0.71; not-stream recall 0.68, stream recall 0.71, balanced 0.70.
- What matters for auto-marking: pieces at 44 m or narrower get "stream". Held out, those were right **94.8%** of
  the time and covered 72% of your stream marks. 35% of your not-stream marks would have been auto-marked stream
  (the narrow Lichen Creek, Coquitlam, Elk and Quinsam pieces).
- For comparison, the existing v2 rule classes on the same marks: accuracy 0.87, but they catch only 2 of 15
  not-stream marks. Width catches 10 of 15.
- Alternatives tried (`report.json` → `cv_grid`): all features, no imagery, width + imagery, depth 1 to 3. A
  3-level width tree scored a little higher (balanced 0.78, auto precision 0.968). It got there only through a
  "lake" band between 20.5 and 22 m, which matches four pieces by coincidence, so it was not used.
- Not included: Strahler order and distance to the main channel. The candidate file does not carry them, and adding
  them would mean loading the 585 MB graph. Nothing in your marks suggests they would help.

## Proposal

| decision | count | what |
|---|---|---|
| owner (kept) | 150 | your verdicts, never overwritten |
| **auto → stream** | **883** | STREAM_BY_BOOK / LIKELY_STREAM / UNSURE candidates ≤ 44 m wide (`confidence`: very_high ≤ 20 m, high 20–44 m); `by: machine` |
| **owner list** | **377** | wider than 44 m and either rule-bound or named (145) or larger than 2 ha (232) |
| **default** | **1,014** | no owner review: 701 LAKE-class stay lake, 275 sewage/tailing ponds stay out, 38 small unnamed rule-free wide pieces keep their v2 rule verdict (22 stream, 16 lake) |

- **Owner list order:** lake-drawn pieces before marsh/swamp; within each, rule-bound or named before big; then
  largest first. 242 lake-drawn (110 rule-bound or named, 132 big), then 135 wide marsh/swamp. 41 complexes cover
  part of it, so the list is **362 clicks**. If you agree that wide marshes are stream, 135 pieces leave the list.
- **Expected error in the auto-marks:** about 5% (46 of 883) at the held-out rate.
  **Spot check these 25 random auto-marks first** (`spot_check: true`):
  328993328 329106133 329115788 329115812 329115920 329128316 329162401 329186002 329191548 329206088 329206553
  329206606 329207129 329446471 329460967 329461983 329487314 329492883 329494286 329511333 329522663 329615939
  329637264 329637879 329663403.
- **Optional LAKE-class probe:** 315 LAKE-class pieces are rule-bound and 44 m or narrower, so the width rule would
  call them stream, but you have never marked that class. 10 random ones are flagged `lake_probe: true`:
  329057737 329057982 329373226 329492223 329492320 329493277 329518145 329603751 329640168 329640878.
  If most come back stream, that class needs its own pass.

## Intermittent spot check (separate tab, not used for training)

- **Group (a), on a river's course:** 32 stream, 12 not, 4 unsure (67% stream). That is not "nearly all", so T149's
  bulk fold of group (a) should **not** go ahead as planned.
- **Group (b), isolated:** 9 unsure, 1 not stream. Your notes say "it is empty in satellite, probably a pond that
  drains", and ask whether these could be shown as intermittent. That idea is worth its own ticket.

## Next, only after you say so

Write the 883 auto-marks (or the share that survives the spot check) to the page's `verdicts` collection as
`by: machine`, never touching your marks. Show the 377 as an owner-list view, then fold confirmed streams through the
usual dry run (registry → atlas → binding diff → reach + deliver).
