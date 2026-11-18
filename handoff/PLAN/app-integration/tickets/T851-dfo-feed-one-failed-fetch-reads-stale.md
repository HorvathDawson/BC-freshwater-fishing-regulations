# T851 — The DFO salmon feed reads "stale" for every region after one failed daily fetch

Status: OPEN (owner 2026-11-18 13:43: "The feed check too" — a big issue).

Seen 2026-11-18: at the 14:15Z run every DFO region page answered 5xx (`http_5xx`, "fetch failed 1× in a row; the last
answers stand"); the staging feed's index.json then marked every region stale, though the last good check was ~18 h
old and the pages answered 200 again by 20:16Z. The next scheduled try was a day later. A redeploy's job run cleared
it (feeds.sh, 2026-11-18 ~13:20 PT).

## To decide and fix (feeds worker, app chat)

1. A transient 5xx should be retried inside the run (bounded backoff) and again later the same day, not left for 24 h.
2. "Stale" should mean the data is old enough to doubt (by age of the last good answer, against the feed's own cadence),
   not "the last fetch failed once". Whatever the app shows for a stale DFO region must stay honest.
3. Python twin (pipeline/regs/dfo_salmon live) and the parity tests stay in step; the owner email says what happened.
