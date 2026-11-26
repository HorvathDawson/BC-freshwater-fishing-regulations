# T1163 — iPhone: a 12-step check for each TestFlight build, the built plist checked by the script, and the old audit's four leftovers

Status: OPEN — the 12 steps before the App Store submit of 0.10.0; the code leftovers in the first patch

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1163 | iPhone: a 12-step check for each TestFlight build, the built plist checked by the script, and the old audit's four leftovers | OPEN — the 12 steps before the App Store submit of 0.10.0; the code leftovers in the first patch | — | planning chat: the 12-step list as a runbook page and to the owner before the submit; a build lane: the plist check, appConfig test, the parking-pin Settings button, the non-affiliation sentence; S |

Source: the owner 2026-11-25 ("iPhone parity should be minor tests for iOS builds") and the read-only pass
that re-read the old iPhone parity audit (0.7.9, 2026-11-14, deleted from the docs by ed05f01e6 in the lean-out;
notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `ios-build-checks.md`, with a copy of the audit). Most of it is fixed in code (T721, T722, T723:
the background download, the iCloud backup exclusion, the low-disk check, the keyboard bar, back swipe, Apple
Maps directions, the Wi-Fi / Low Data wording, the alert wording, the "inactive" update bug).

## Work
1. The 12-step device list (about 12 minutes on one iPhone; in the notes) becomes a runbook page and is run on the
   TestFlight build before each App Store submit: lock and swipe-away during the first download, hotspot and Low
   Data Mode, the real size of the first download (never measured: the biggest review risk at about 500 MB), the
   backup exclusion seen in Settings.
2. The TestFlight build script reads the BUILT plist and checks the background modes and usage strings (they are
   used: `expo-background-task`, silent push; the built file was never read); an `appConfig.test.ts`.
3. Leftovers: the parking pin has no Settings button and no rough-fix warning (G5 / G6); the first-run notice has
   no "not affiliated with the Province" sentence (G15), with a test. F4 (a tap about 300 ms late) and F5 (a grey
   flash after a tab switch) are cosmetic and stay known unless he asks.

## Acceptance
The list is run and recorded for the 0.10.0 App Store build; the script fails on a plist missing a declared mode
(mutation). Severity: low–medium. Model: Sonnet. Size: S. Owner: the 12 steps on his iPhone.

## Owner

- [ ] 2026-11-25 — Before the App Store submit of 0.10.0: run the 12-step iPhone list (about 12 minutes; I hand it to you with the build). Picks (defaults first): the non-affiliation sentence in the first patch / skip; the parking-pin Settings button in the first patch / before the submit; F4 and F5 stay known cosmetic / a ticket each.
