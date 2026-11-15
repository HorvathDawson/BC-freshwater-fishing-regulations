# Final DFO marks audit: the runbook (T743)

Owner, 2026-11-15: "we should wait until fully done marks though" / "we just want to prepared for how we will audit them".
This is the day he says he is done marking. One read-only command gives one report. Everything in it is a candidate
for him, not a verdict.

## 1. Run it (Claude)

From the main checkout, once `review/marks-audit` is merged:

```
scripts/heavy.sh marks-audit 40 -- env PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.review.audit \
    --out <scratch>/marks-audit-1.md            # writes marks-audit-1.json beside it
```

- It only reads. It loads the atlas registry and graph (for F) and runs `apply --check` (for I). Both need memory, so it
  goes through heavy.sh. With `--no-graph --no-landed` it runs light, in seconds, without F and I.
- Exit 0 whenever it ran; 2 only when it could not run (a bad marks line, a missing file).
- Put the output in scratch, never in `data/`. Keep the `.json`: the next run diffs against it.

## 2. What each section means

| § | in plain words | whose call |
|---|---|---|
| A | A place saved more than once: the file is not what the last save says, or an older save left something behind (a "same place as" link, a tidal call, a tributary flag), or a cut made from a mark that nothing uses now. | Links and flags: **owner** (keep or undo). Orphan cuts: mechanical (delete after a look). Not-last-save rows go to I. |
| B | His last word is "wrong" or "unsure", but the place still has an extent, and that extent ships. Marks are not a build input. | **Owner**: re-mark the place with a fix. |
| C | Tributaries vs the words. C1: the words take tributaries in, the place does not. C2: the water's heading says "(includes tributaries)" or IS the tributaries, a place under it does not. C3: the words say mainstem, the place takes tributaries in or leaves it blank. C4: the words mean tributaries only (Bella Coola, Bulkley), the place is not that form. C5: blank everywhere. | C1/C2/C4: **owner** re-marks (C4 needs the tributaries-only form from the review-app lane). C3/C5: **owner**, one tick each ("off"). |
| D | "Rest of the water" / "all open portions" places and the places they are worked out from: a dormant sibling still counting, a sibling that cannot be drawn (the whole rest goes blank), two rests on one water. | Dormant-sibling policy: **owner** (one decision). Undrawable: mechanical (fix the sibling). |
| E | An end at a stream's mouth that is not the tidal boundary every stream gets, a cut from a mark close to one, a new mouth cut not built yet. | **Owner** where the words name another point (Pallant). Duplicate cuts: mechanical (do not accept). |
| F | A place on a river leaves out that river's own side channel (Fraser: Herrling, Seabird N, Jeperson; Squamish Powerhouse). | **Owner** for sloughs and anything excepted in the words. Channels: mechanical once he says "channels go with the mainstem". |
| G | A group-written mark (one click, several wordings) not saved again on its own. A wording that differs from the group's may have the wrong stretch (Nanaimo 2442). | **Owner** re-marks the differing one. |
| H | A one-bank wording ("west bank", "centre line") saved as the whole width. | Waits on T754 (a bank field). Nothing to re-mark before that. |
| I | Marks not landed (`apply --check`), grouped by what they wait on: a side rebuild, a new cut, a refusal. | Mechanical (the rebuild); refusals: the review-app fix first. |
| J | Active places still unmarked, by region. Places that ride a marked place are counted apart. | **Owner** marks them, or says they ride. |
| K | The words name a distance the cut lacks ("500 m below the power line"), or a cut has an offset the words do not name (Morice 2591/2592). | **Owner**: choose the cut. A new offset cut means an atlas rebuild. |

New asks become one row in `WORDING` in `pipeline/regs/review/audit.py`. If an ask needs a new comparison, that is one
small check function.

## 3. The order after the report

1. **Land the review-app fixes first.** The tributaries-only form and the excepted tributaries (branch
   `review/notes-1115`), the other_places save refusal, and the channel inclusion. Until they land, C4 and parts of F
   cannot be re-marked.
2. **Send the owner one list**: the rows marked "owner" above, each with its place, wording and a picture. Record each
   answer in the ticket and OWNER-TODO the same turn.
3. **He re-marks** the listed places in the review app (the write-through lands them).
4. **Re-run with `--since`**:
   `… -m pipeline.regs.review.audit --out <scratch>/marks-audit-2.md --since <scratch>/marks-audit-1.json`.
   The report then lists only new and resolved findings. Repeat 3–4 until only accepted rows are left.
5. **The rebuild.** The side rebuild for the waiting cuts (fraser-channels-2026-11-15.md §4 "Ready-to-run sequence",
   with the proposal ids from the final marks), then `apply dfo --side-checked …`, then `apply --check` (only the
   allowed waits left), then the release build through heavy.sh.

## 4. What it cannot see (from the 2026-11-15 hand audits)

- **Whether an extent matches the words on the map** (Little Qualicum falls vs the park line, Stamp's Deer Creek, the
  Atnarko sign position, which Lillooet edge). That needs a person and a map. The report only lists the places.
- **Whether a place's words are read right at all** (Chilliwack "upstream" as DFO's slip, Region 3 Fraser's "wrong"
  being a label). These show up as B or G rows. The reading stays with the owner.
- **New cuts not built yet** cannot be measured against the tidal boundary (E lists them by their words only), and F
  checks a not-built cut's window against the whole river.
- **Species words inside the place text** (Morice 2596). There is no pattern for them yet; one could be added to
  `WORDING` if the owner wants it.
- **A sibling that binds zero sections** (D) is not detected without a reach run. Only a missing cut or water is
  detected.
