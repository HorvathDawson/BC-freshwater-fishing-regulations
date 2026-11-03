# T346 — Go through every crash and problem report the feeds worker has collected

Status: DONE 2026-11-02 (6c08121b3; was OPEN) (owner 2026-10-31: "look if the digest collected any issues or crashes etc. with info. It should look for
more than just ours with this bug").

The app sends crash reports and "Report a problem" reports to the staging feeds worker (`/crash`, the report store;
the daily digest summarises them). Test-tagged reports from tools are marked and ignored (T291).

1. Pull every stored report (crashes, JS errors, native exits, problem reports), not only the digest summary, from
   the staging feeds worker / its R2 store. Read-only.
2. Group them by cause (stack/screen/water), with counts, app version, platform, first and last seen. Separate the
   owner's own reports from other testers', and drop test-tagged ones.
3. For each group, find the cause in the code or data, and either add it to an existing ticket (e.g. T345's
   flows-into crash) or open a new one with the evidence.
4. Note anything the reports are missing that would have helped (e.g. the water id or screen on a crash), and ticket
   that too.
5. Write a short summary into STATUS.md.

No user data leaves the machine; report contents are kept to what the reports already hold. Size: S–M.

## Result (2026-11-02)
Read from the staging bucket (`private/crashes/`, `private/reports/`), read-only. 26 reports: 25 crashes in 7 stored kinds plus 1 problem report; test-tagged ones (`crashes-test/`) dropped.
- Unknown module on a water pane, web, 19 reports, 0.1.2–0.3.0: the owner's Coquitlam crash, evidence added to T345.
- Native foreground crash, Android 37, 0.4.0: 2 reports (owner's phone): T351.
- Background kills (SIGNALED 9, LOW_MEMORY): 3 reports, not crashes: noted in T351.
- Dev-build ReferenceError `useRegsPrefetch` (env local, 1 report, already fixed in regs.ts): dropped.
- Problem report, colours not repainting on zoom/pan: owner's, evidence added to T348.
- Missing context on all of the above: T352.
5 groups (+ the dropped dev one); tickets created T351, T352; updated T345, T348.
