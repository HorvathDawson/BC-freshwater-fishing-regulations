# T840 — a salmon verdict's short text carries its own condition

Status: OPEN (main chat; pipeline). After 0.8.0.

Source: the app chat's T767 item 4, 2026-11-17. Two salmon sections can share a verdict's short text `t` while their
quota trees differ ("hatchery-marked only" lives only in `tree.says`). The app now joins the tree's words beside `t` on
the card and puts them in the stretch-join key (app-fix/t766-t767 27186df29), so nothing is lost on screen; but the
pipeline's `t` is what every reader is meant to rely on.

Fix: compose `t` in the DFO salmon answers so that two verdicts with different meaning never share one `t` (the tree's
condition said in `t`), pinned with a test over the live feed that no two verdicts with different trees share a `t`.
Land it WITH the app chat dropping its T767 join (else the condition is said twice); the dfo_vectors move.

## Measured (lane L10, 2026-11-22, the live release r-741815c6090d's salmon payload; nothing changed)
239 verdicts; 7 short texts `t` are shared by verdicts that differ elsewhere. Six differ only in the species a tree
names (`"1 per day (Apr 1–Mar 31)"` for coho and for chum, …) or the notice number (`"4 hatchery-marked per day (Sep
1–Dec 31)"` under FN0937 / FN0938 / FN0939) — one meaning. ONE differs in meaning: `"4 per day, only 1 over 50 cm (Apr
1–Mar 31)"` (and the `only 2 over 50 cm` twin) is both a plain "4 a day" and Region 5's aggregate "Up to 4 chinook a
day in total, all waters of Management Units 5-6 to 5-11 combined", which only `tree.says` carries. The ticket's own
example ("hatchery-marked only") is in `t` already. Not built here: `t` is composed twice (`deliver.salmon._text` and
the feeds worker's `salmon.ts` `verdictOn`, pinned by `dfo_vectors`), so the fix moves Python, the worker and the
shared vectors together, and the app must drop its T767 join in the same landing.
