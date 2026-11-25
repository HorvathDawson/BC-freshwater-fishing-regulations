# T1109 — The first download respects mobile data: ask before downloading on data, and ask again if Wi-Fi drops

Status: OPEN.

Source: owner 2026-11-25 ~00:50 (planning chat), feedback from one tester, relayed: "we need to make sure the initial
app download respects mobile data and if the user loses wifi or is on data it should ask the user if it is okay to
download etc" and "this was feedback from one user". Related: T756 (a "Wait for Wi-Fi" queue exists for the big
files), T760 (a paused system download waits without a word).

## Work
1. Evidence first: what the first install does today on each connection (Wi-Fi, mobile data, metered Wi-Fi, a
   hotspot), on Android, iPhone and web: which files start at once, how many MB, where "Wait for Wi-Fi" is offered
   and what its default is. Say the size of the first download in the words the reader sees.
2. On mobile data (or a connection the system marks metered): before any large file starts, one question with the
   size: "Download 000 MB now on mobile data, or wait for Wi-Fi?" The small files the app cannot open without may
   go first only if the question says so. Never a silent start on data.
3. Wi-Fi lost mid-download: the download pauses, says so, and asks the same question; nothing carries on over data
   on its own. Back on Wi-Fi it resumes by itself. The answer is remembered and can be changed in Settings.
4. The same rule for later data releases and offline map packs (one rule, every download path;
   `one-load-path-per-screen`).
5. Tests per platform for: start on data, Wi-Fi → data mid-way, data → Wi-Fi, the remembered answer; emulator check
   with the network switched during a first install. Pictures of the question, light and dark, to the owner.

## Acceptance
A fresh install on mobile data downloads nothing large before the reader says yes; switching Wi-Fi off mid-download
pauses and asks; the tests fail without the change.

Severity: medium–high (a first install can spend a tester's mobile data without asking). Release: 0.9.2 (the
planning chat's proposal; the owner did not name a release). Model: Opus (download queue, three platforms). Size: M.
Owner: yes (the words of the question).
