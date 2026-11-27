# T890 — word density: measure every page, then cut the over-dense ones

Status: OPEN (app chat leads: the screens; main chat: the words the pipeline writes). After production.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T890 | Word density: measure every page, then cut the over-dense ones (pictures first) (owner) | OPEN | app chat + main chat | owner 2026-11-18 ~21:43 |

Owner 2026-11-18 ~21:43 (main chat), verbatim: "Another thing we should add a ticket for is checking every page for word
density then try to reduce the over dense ones".

## Work
1. Measure: every screen and sheet (map cards, water page tabs, fish sheets, source sheets, salmon/DFO folds, Conditions,
   Help, walkthrough, More pages) on a phone-width test bed: words on screen, words per line, the longest block, how
   many words before the answer. One table, worst first, with a shot of each.
2. Propose cuts for the over-dense ones, as pictures to the owner (before/after); no change lands unseen.
3. Words that come from the data (rule lines, source lines, salmon notes) are the main chat's: the cuts there land in
   the pipeline so every reader gets them.
4. A water's Regulations tab (tester, 2026-11-26, feedback log F51; owner the same day: "Add to T890"): "almost too much
   info all at once and at first glance looks overwhelming. I wonder if theres a way to seperate these things with very
   distinguished headings or a slight color change again? Maybe not too bad actually after I looked at it a few more
   times". Today the Salmon block, the Game fish block and "Species and limits" are parted by rules only (a 1.5 px ink
   rule, no background). Show the owner two pictures beside the current one: tinted block heads, and banded sections.
   This line goes in the 0.10.0 re-cut (owner 2026-11-26: "we will put all changes from this on it"); the rest of the
   ticket stays after production.
Pairs with T883 (the per-fish "how this was decided" sheet).
