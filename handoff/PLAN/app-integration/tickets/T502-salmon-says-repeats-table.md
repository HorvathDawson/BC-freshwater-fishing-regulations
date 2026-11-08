# T502 — The words under the salmon table repeat the table

Status: IN REVIEW — a540631bb; owner sees it on the next build. Owner 2026-11-08 (phone, 0.6.9; T482 did not touch this block).

Under the species rows the card prints DFO's words once per group of species (`SalmonCard` `Says`, core
`tableSays`: each row group's `notes`, multi-species sizes, `speciesNotes`), e.g.:

    Salmon fishing in daylight only: one hour before sunrise to one hour after sunset   (T482: now the hours line)
    Chinook · sockeye · pink · chum
    Not opened by DFO: closed to salmon fishing
    Coho
    4 hatchery marked per day, only 2 over 35 cm (Sep 1–Dec 31)

Owner: "it just repeats the real table".

## Do
- Drop a group's line when its species' rows already say it: the "Not opened by DFO" closed words where every row of
  the group shows Closed; a quota note whose number the row's quota line reads (not `quotaUnread`).
- Nothing of DFO's rule may be lost: a fact the row does not show (hatchery marked only, the date window, a note the
  presenter did not read) moves onto the row (its quota / limit line) or stays. `salmonTableTexts` test: every fact
  still somewhere. Before/after shots for the owner (Vedder, Fraser R2, a coho-open water).

## Done (a540631bb)
- Core `salmonBlock` marks each row's `repeats`: DFO's "not opened" words (`d`) on a closed row; an open row's verdict
  words whose numbers its quota line reads (no notice, nothing unread). `tableSays` skips them; the dates those words end
  in ("Sep 1–Dec 31") show under the species' quota line (`window`). Everything else (real closure reasons, notices,
  unread rows, notes) stays. The in-part caveat's detail drops the same repeats (its "what" line says them).
- The T318 "every DFO word" tests now prove a repeat is said by its rows (closed rows all Closed / quota line + window).
  core + ui-native suites pass, tsc clean.
