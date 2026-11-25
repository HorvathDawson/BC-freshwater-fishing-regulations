# T1100 — More > Sources and licences: open-source notices are missing for the libraries and fonts the app ships

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1100 | More > Sources and licences: open-source notices missing for the shipped libraries and fonts | OPEN — 0.9.2 | | iOS chat 2026-11-24 |

Source: iOS release chat 2026-11-24 ~23:40, its read of build 0.9.0 (45) at 65f84020c for T1071 (App Privacy and review risk). File: `app/packages/core/src/menu/licences.ts` lines 90–168.

Missing: MapLibre (BSD-2), the fonts Archivo, Bricolage Grotesque and JetBrains Mono (OFL), react-native-pdf, Expo,
React Native, fflate, pmtiles, the Protomaps basemaps code.

## Work
1. Generate the list from the lockfile at build time (production dependencies that ship in the app or the web
   bundle, their licence and copyright line), plus a hand list for fonts and data; no hand-kept copy that can drift.
2. A build check: a shipped package with no notice, or an unknown licence, fails.
3. The same list on the website.

## Acceptance
The screen lists every shipped library and font with its licence text; the check fails when one is removed.

Severity: low (licence hygiene; BSD and OFL ask for the notice to travel with the app). Release: 0.9.2. Model:
Sonnet. Size: S–M. Owner: no.
