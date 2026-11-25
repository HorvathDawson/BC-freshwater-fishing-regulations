# T1110 — Notifications are off after install: ask the reader on first load to turn on new-data notices

Status: OPEN.

Source: owner 2026-11-25 ~00:55 (planning chat), verbatim: "also notifications default off on install. need to make
sure it asks user to turn it on for new data notifications on first loade tc". Related: T234 (new-version notice),
T982 #6 (the location ask that was never offered again), T1098 item g (the push token and Don't Allow).

Owner 2026-11-25 ~01:10, added: "also we shouldnt allow notifications without new data notifications". The planning
chat's reading (confirm with him in the words batch): the new-data notice is the base one; no other notice of the
app (a water's alerts, in-season changes, the app-update notice) can be on while new-data notices are off. Turning
any notice on turns new-data on with it and says so; new-data cannot be switched off alone while another is on (the
switch explains why, or switching it off switches all off after a confirmation).

## Work
1. Evidence first: on a fresh install of 0.9.0, Android and iPhone, what is asked and when, what the app's own
   notification settings default to, and what a reader who never opens Settings receives.
2. On first load, after the first download is under way (not before the reader has seen what the app is): one
   plain card: "Get a notice when the regulations change?" with what will be sent (new data releases, in-season
   changes; the list the app really sends) and Yes / Not now. Yes leads to the system permission prompt; the
   system prompt is never shown cold.
3. Not now is respected: asked once more later at a moment that makes sense (after the first data update the
   reader would have been told about), then never; always available in Settings. A dismissed system prompt gets
   the same second offer (the T982 #6 lesson). On Android 13+ and iPhone the system allows one prompt only: after
   a refusal the card points to the system settings instead.
4. The privacy page and the store privacy answers stay true (T1098).
5. Tests for the three paths (yes, not now, system refusal); emulator and simulator check on a fresh install.
   Pictures of the card to the owner.

## Acceptance
A fresh install is asked once in plain words; saying yes turns on the new-data notice and one arrives on the next
release (staging); the tests fail without the change.

Severity: medium (testers do not learn that the rules changed). Release: 0.9.2 (the planning chat's proposal).
Model: Sonnet, Opus for the push registration part. Size: S–M. Owner: yes (the words and when it is asked).
