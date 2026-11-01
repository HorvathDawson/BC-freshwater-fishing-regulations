# T346 — Go through every crash and problem report the feeds worker has collected

Status: OPEN (owner 2026-10-31: "look if the digest collected any issues or crashes etc. with info. It should look for
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
