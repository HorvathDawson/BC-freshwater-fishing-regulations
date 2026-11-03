# T361 — Six region-1 DFO owner marks read "rule changed" after the 11-03 re-queue

Status: OPEN (found 2026-11-03 by the main session; caused by Claude's queue step)

`test_marks_never_orphaned` fails since `python -m pipeline.regs.dfo_salmon.live queue --regions 1` (11-03 ~10:01,
run to put the two held Nanaimo "tidal portions" places back in DFO mode). The step re-emits region 1 from the cached
page (`cache/dfo_salmon/raw/region1-eng.html`, 10-27), refreshes `data/generated/regs/dfo_salmon/typed/region-1.json`,
then seeds and groups the entry file (its only committed change: two Nanaimo locations dormant → active, 07cc55b).

Marks now stale as "rule changed" (entry record meaning moved; they carry no rule records):
campbell-river:downstream-of-the-confluence-with (unsure 10-26), quinsam-river:whole (10-28),
nanaimo-river:from-the-upstream-side-of-the-ceda, puntledge-river:downstream-from-the-base-of-stotan,
qualicum-river:those-waters-upstream-of-the-big-q, stamp-river:except-in-those-areas-and-times-li (all 11-02).

Checked: regenerating region 2's typed rules with today's code gives a byte-identical file, so it is not a general
code drift. Suspect: the region-1 rules/typed files the 11-02 marks were made against came from a newer page or an
older emit than the 10-27 cache gives now; or the entry meaning digest includes the status / grouping fields.

## Work
1. Find what moved in the entry meaning for one of them (`fingerprint.py`, `marks.status`): diff the mark's stamped
   digest inputs against now.
2. If it is a by-product (no content the owner judged changed): restore the inputs (re-fetch region 1 live with
   `live run --regions 1`, or regenerate as on 11-02) so the marks read current again. Never edit marks.jsonl.
3. If DFO's content really changed, the six go back to the owner as re-review.
