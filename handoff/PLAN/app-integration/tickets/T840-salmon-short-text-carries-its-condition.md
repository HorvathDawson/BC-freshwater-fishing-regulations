# T840 — a salmon verdict's short text carries its own condition

Status: OPEN (main chat; pipeline). After 0.8.0.

Source: the app chat's T767 item 4, 2026-11-17. Two salmon sections can share a verdict's short text `t` while their
quota trees differ ("hatchery-marked only" lives only in `tree.says`). The app now joins the tree's words beside `t` on
the card and puts them in the stretch-join key (app-fix/t766-t767 27186df29), so nothing is lost on screen; but the
pipeline's `t` is what every reader is meant to rely on.

Fix: compose `t` in the DFO salmon answers so that two verdicts with different meaning never share one `t` (the tree's
condition said in `t`), pinned with a test over the live feed that no two verdicts with different trees share a `t`.
Land it WITH the app chat dropping its T767 join (else the condition is said twice); the dfo_vectors move.
