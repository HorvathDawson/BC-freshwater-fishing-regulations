# T1123 — iPhone may still show the "New app version" notification toggle text

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1123 | iPhone may still show the "New app version" notification toggle text | OPEN — 0.9.2, with T1073 | | not checked yet |

Source: the 0.9.1 chat's T1098 lane, 2026-11-25: "ui-native may still show 'New app version' toggle text on iPhone
(not checked)". Related: T1073 (the app-update notice only when the phone can install it), T234, T1110.

Check on the simulator whether the notification settings on iPhone show a "New app version" toggle, and what it
does there (an iPhone gets its updates from the App Store or TestFlight; the app's own update check is Android's).
If it shows and does nothing, remove it on iPhone or make it true; the privacy page's sentence follows.

Acceptance: a picture of the iPhone settings; the toggle either works or is gone, with a test. Severity: low.
Release: 0.9.2, with T1073. Model: Sonnet. Size: S. Owner: no.
