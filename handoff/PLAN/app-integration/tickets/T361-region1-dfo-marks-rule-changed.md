# T361 — Six region-1 DFO owner marks read "rule changed" after the 11-03 re-queue

Status: OWNER — cause found 2026-11-03 (chat 2): DFO really changed; the six go back to the owner for re-review

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

## Finding (2026-11-03, chat 2)
- A DFO mark's meaning is `_h(rows)`: the scraped table rows of the location's fingerprints (`review/dfo.prints`).
  The six locations' fingerprints did NOT change; the rows behind them did.
- An 11-02 15:36 copy of the typed feed (a scratchpad `typed.before/`) reproduces all six stamped meanings exactly.
  Today's typed/region-1.json (re-emitted 11-03 10:01 from `cache/dfo_salmon/raw/region1-eng.html`, page
  dateModified 2026-10-05) differs in 22 of 259 row sets: in-season fishery notices FN0937/0938/0939/0952/1013/1069.
  E.g. Quinsam/Campbell coho "Oct 1 to Dec 31 | 4 hatchery marked only, only 2 > 40 cm" → "Sept 1 to Dec 31 | 4
  hatchery-marked per day — FN0939"; Stamp coho one row → three date tiers (FN0504/1013/1069).
- The OLD rows are on neither the 10-05 page (raw cache) nor the 10-28 Wayback copy (`history/region1_20261028…`),
  so the typed feed the 11-02 marks were made against was emitted from an older page and never re-emitted after the
  10-27 page refresh. The new rows are DFO's current page.
- So restoring would put outdated limits back. Not done. Marks untouched.

## Left
1. **Owner:** re-review the six in the DFO tab (they show "rule changed"); `test_marks_never_orphaned` stays red until then.
2. Follow-up (small): the review app / queue should refuse or warn when typed/ is older than the raw page it is
   built from (stamp the raw page's sha in the typed file and compare). File as its own ticket if wanted.

## Owner question 2026-11-03: new places or historic?
Historic. All six place wordings are unchanged and long known (on DFO's pages since 2023, Puntledge since 2025-03;
the wording archive dates every one through 2026-10-28). The OLD rows match last season's page (the Jan 2026 copy,
dateModified 2025-11-07, and earlier). The current 10-05 page carries the 2026 in-season notices. So only the
rules (R) moved; the place and its extent (P) did not, and a re-mark is a check of the new rows only.
