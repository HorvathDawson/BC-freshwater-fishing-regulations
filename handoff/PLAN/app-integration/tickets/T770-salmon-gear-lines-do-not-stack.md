# T770 — Salmon gear lines of a wider place vanish when a narrower place has a gear line that day

Status: OPEN (pipeline, `pipeline/deliver/salmon.py`; needs the owner's yes on the rule before it is built).

Source: second-look fix lane A, 2026-11-15 night (`handoff/PLAN/curation-review/second-look-fixes-2026-11-15.md`,
"New findings" 1).

## What is wrong
`gear_on` (salmon.py, line ~311) returns the gear lines of the NARROWEST place in the chain that has any gear line
that day, and stops. A whole-river gear rule that holds all year is hidden on the days a narrower stretch has a gear
line of its own. Example: Little Qualicum, Sep to Nov: the stretch's "fly fishing only" shows and the river's
all-year hook rule does not. Both hold on the water.

## What to build (after the owner's yes)
- Gear lines of every place in the chain that hold that day are shown, narrowest first, equal lines once.
- A line a narrower place replaces (same kind of gear rule, different value) is not shown twice: decide and write down
  what "replaces" means before coding (bait ban vs bait allowed; hook count) or show both and let the words stand.
- Diff every answer key's gear lines before and after; the diff is the review list.

## Owner question (on the morning list)
Should a river-wide gear rule keep showing on a stretch that has its own gear rule that day? Recommended: yes.

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Question 13: "Show both (Recommended)". Build it; the diff of every card's gear lines is his review list. The eulachon line (Region 6-wide reminder) must never be displaced by a bait rule.
