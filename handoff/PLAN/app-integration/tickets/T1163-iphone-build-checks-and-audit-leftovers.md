# T1163 — iPhone: a 12-step check for each TestFlight build, the built plist checked by the script, and the old audit's four leftovers

Status: OPEN — code leftovers built (0280af2de, 3f183446e, a742462cb, 68de871ad, 8a7366475, 61f151b60, 228bdfaee); left: the 12 steps on the iPhone, and the check on a real build

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1163 | iPhone: a 12-step check for each TestFlight build, the built plist checked by the script, and the old audit's four leftovers | OPEN — code leftovers built (0280af2de, 3f183446e, a742462cb, 68de871ad, 8a7366475, 61f151b60, 228bdfaee); left: the 12 steps on the iPhone, and the check on a real build | 0280af2de, 3f183446e, a742462cb, 68de871ad, 8a7366475, 61f151b60, 228bdfaee | planning chat: the 12-step runbook page; owner: the 12 steps on his iPhone; the first real TestFlight build shows whether fetch is in the built plist |

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

## Built (2026-11-26, branch fix/T1163-iphone-audit-leftovers)
- G15 (0280af2de, 61f151b60): `NOT_AFFILIATED` in `core/src/menu/words.ts` (the Play description's words, held to it by
  `tools/not-affiliated.test.ts`), on the first-run notice's first point AND on About (`about.notOfficial`; T727 item 5 said About had it, it did
  not). Whole-string tests `legal.test.ts`, `menu.test.ts`. `FIRST_RUN_VERSION` NOT bumped (owner's pick: a bump re-asks everyone once).
- G5 / G6 (3f183446e, 8a7366475): `ui/src/parkingHere.ts` (good fix sets the pin; fix over `ROUGH_FIX_M` = 100 m sets it and says
  `roughFixLine`; far fix nothing; refusal says `MY_LOCATION.denied`; else `parkingNoFix`); `spot/Parking.tsx` draws `OpenSettings` (phone: button,
  web: nothing). A refused or timed-out location is the environment's on both platforms (`locate.web.ts`, `locate.native.ts`): shown, never reported.
- Plist check (a742462cb, 228bdfaee): `app/tools/check-built-plist.mjs <Info.plist>`; expectations come from `expo config --type introspect`
  (modes, task ids, every usage string), never typed, and a config with nothing to check is refused. Tests `check-built-plist.test.ts`
  (incl. one spawn of the command: exit 1 naming a dropped mode) and `appConfig.test.ts`. Called by `deploy/staging/build-ios-testflight.sh`
  after the archive guard (one line, pinned by a test); never run on a real build yet.
- Not verified on a device: the Settings button, the rough-fix line and a real built plist.

## Acceptance
The list is run and recorded for the 0.10.0 App Store build; the script fails on a plist missing a declared mode
(mutation). Severity: low–medium. Model: Sonnet. Size: S. Owner: the 12 steps on his iPhone.

## Owner

- [ ] 2026-11-25 — Before the App Store submit of 0.10.0: run the 12-step iPhone list (about 12 minutes; I hand it to you with the build). Picks (defaults first): the non-affiliation sentence in the first patch / skip; the parking-pin Settings button in the first patch / before the submit; F4 and F5 stay known cosmetic / a ticket each.
