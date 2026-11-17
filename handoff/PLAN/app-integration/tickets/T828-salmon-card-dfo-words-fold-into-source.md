# T828 — Salmon card: DFO's rows as printed fold into "Source (DFO)"

Status: IN PROGRESS (built on branch `app-fix/t828-t829-salmon-card`, not merged; Android bench owed)

Source: owner 2026-11-16, circling the block under the salmon species table: "why do we have this section in salmon";
picked "Fold into Source (Recommended)".

## What
The block T321 added (2026-10-31) under the species table, DFO's rows as printed (per-species row text such as "No
fishing for chinook (Apr 1–Mar 31)", in-season notice numbers like "Sockeye 4 per day (Jul 29–Aug 16) · FN0787",
"DFO's words for this water", the region reminder), moves under the existing "Source (DFO)" expander, closed by
default. Wording the app could NOT read ("DFO changed the wording here — read their text") stays visible above the
fold. No data change.

## Built (2026-11-17, branch `app-fix/t828-t829-salmon-card`)
- `SalmonCard.tsx`: the source fold (`SourceFold`, still closed by default, its summary "Source (DFO)" /
  "Sources (rule book and DFO)") now holds, under the rule book's lines and above DFO's source lines: the species' row
  words (`salmonTable(card).says`: "No fishing for chinook (Apr 1–Mar 31)", "4 per day (Jul 29–Aug 16) · FN0787"),
  "DFO's words for this water" (`dfoWords.notes`) and the region rules (`regionRules`, "Reminder: There is no fishing for
  eulachon in Region 6."). The "DFO changed the wording here — read their text" block (`dfoWords.changed`) stays above
  the table, outside the fold. No data change: the same VM strings, moved.
- One judgement call: where EVERY species is closed (`card.allClosed`: a region's closure, the rule book's closure, "not
  opened by DFO") those words are the card's one statement of why, so they stay visible under the table, said once.
- Tests: `SalmonCard.t828.test.tsx` (fold closed by default with the rows inside; the warning outside it and not repeated);
  `SalmonCard.test.tsx` opens the fold where it reads those rows. Mutation-checked: Says drawn outside the fold, the fold
  open by default, the warning dropped, the region rules dropped — each fails a test.
- Left: the owner's Android bench (Kitsumkalum Jul 15, Skeena below Terrace Aug 15: the fold closed, then opened).
