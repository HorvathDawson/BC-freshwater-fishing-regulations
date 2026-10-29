# Privacy labels / Data safety answers — DRAFT for owner sign-off (T244; nothing submitted)

Source of truth: the privacy page (`app/tools/seo/html.ts` `privacyBody`, https://canifishthis.ca/privacy) and what the code sends
(checked 2026-10-29 against `apps/mobile/src/config.ts`, `@app/data` feeds/report/crash/weather, `@app/map` imagery). If the app or the page
changes, change these. Where the honest answer is a judgement call it is marked **OWNER**.

## What leaves the device (the facts the answers rest on)
| Request | Goes to | Carries | Optional |
|---|---|---|---|
| Release, map, tiles, glyphs/sprites, gauge feed, salmon/in-season notices, HYDAT day flow | our servers (Cloudflare R2/worker) | app version header, IP (as any request); nothing of the user's | no (app function) |
| Problem report | our server (Cloudflare) | kind, text typed, switched-on context (water viewed, screen, version, release, platform, recent errors), email only if typed | yes, user presses Send |
| Crash report | our server (Cloudflare) | app version, env, OS name/version, data release, kind of screen, scrubbed error + code trace, fatal flag, time. No location/spots/searches/ids | on by default, switch in More → Data |
| Weather for a saved spot | Open-Meteo (third party) | the spot's coordinates + date (+ the device IP to them) | only when the user opens a spot's weather |
| Satellite imagery | Esri (third party) | tile requests for the viewed area + IP | opt-in layer, online only |
| Location | never sent to us; used on-device (see weather row for a pinned spot) | — | permission, while-in-use only |
| Photos / camera | on-device only | — | permission |
Not present: accounts, advertising IDs, analytics SDKs, third-party crash SDKs, cookies (site), background location, purchases.

## Google Play — Data safety form
- Does the app collect or share user data? **Yes.**
- Is all user data encrypted in transit? **Yes** (HTTPS everywhere).
- Can users request deletion? **Yes** — reports/emails: write privacy@canifishthis.ca (reports also auto-delete at 180 days; crash records at 90 days). Provide the deletion-request URL: https://canifishthis.ca/privacy
- Data types:
  | Type | Collected | Shared | Purpose | Optional | Note |
  |---|---|---|---|---|---|
  | Email address | Yes | No | App functionality (reply to a report) | Yes | only if typed and "may contact me" ticked |
  | Other user-generated content (report text) | Yes | No | App functionality | Yes | |
  | App info and performance: Crash logs | Yes | No | Analytics / App functionality (stability) | Yes (user can switch off) | our own server, no third-party SDK |
  | App info and performance: Diagnostics (report's recent app errors, version, platform) | Yes | No | App functionality | Yes | part of a report |
  | Location: Approximate / Precise | **OWNER** | **OWNER** | App functionality | Yes | The spot coordinates go to Open-Meteo for a spot's weather, and a pinned spot can be where the user stood; Esri sees viewed-area tile requests. Google counts data sent to a third party as collected; the careful answer is Location = collected + shared with Open-Meteo (service provider, app functionality, optional). Recommended: declare it. |
  | Photos, files, contacts, calendar, health, financial, web history, audio | No | — | — | — | stay on device |
  | Device or other IDs | No | — | — | — | none read or sent |
- Security practices: encrypted in transit yes; data deletion request yes; committed to Play Families policy: no (18+).

## Apple — App Privacy ("nutrition label")
- Data used to track you: **None** (no tracking, no ATT prompt).
- Data linked to you: **None** (no account, no identifier).
- Data not linked to you:
  - Diagnostics: Crash Data, Performance Data/Other Diagnostic Data — App Functionality (switchable off).
  - Contact Info: Email Address — App Functionality — only when typed into a report. (Report text: Other User Content.)
  - Location: Coarse/Precise Location — **OWNER** (as above; Open-Meteo gets a spot's coordinates) — App Functionality, not linked, not for tracking.
- Permission strings (already in app.json): location when-in-use, camera, photos (each says it stays on the device). No microphone.
- Export compliance: HTTPS only, standard encryption → `ITSAppUsesNonExemptEncryption = false` (owner confirms at first upload).

## Consistency checks (run before submitting)
1. Privacy page text == these answers (T268 rule: change the app, change the page).
2. `EXPO_PUBLIC_CONTACT` == the contact on the listing.
3. Crash reporting is default-on; if T242 flips that, the label stays (collected, optional).
