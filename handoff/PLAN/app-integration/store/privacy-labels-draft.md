# Privacy labels / Data safety answers — DRAFT for owner sign-off (T244; nothing submitted)

Source of truth: the privacy page (`app/tools/seo/html.ts` `privacyBody`, https://canifishthis.ca/privacy) and what the code sends
(checked 2026-10-29, rows 11–12 + Apple re-checked 2026-11-05 against `apps/mobile/src/config.ts`, `@app/data` feeds/report/crash/weather, `@app/map` imagery). If the app or the page
changes, change these. Where the honest answer is a judgement call it is marked **OWNER**.

## What leaves the device (the facts the answers rest on)
| Request | Goes to | Carries | Optional |
|---|---|---|---|
| Release, map, tiles, glyphs/sprites, gauge feed, salmon/in-season notices, HYDAT day flow | our servers (Cloudflare R2/worker) | app version header, IP (as any request); nothing of the user's | no (app function) |
| Problem report | our server (Cloudflare) | reason, text typed, switched-on context rows (water viewed, screen, version, release, platform, map layer/forecast day/zoom/**centre to 3 decimals ≈ 100 m**, the install id, recent errors), email only if typed | yes, user presses Send; each row can be switched off |
| Crash report | our server (Cloudflare) | app version, env, OS name/version, data release, kind of screen, scrubbed error + code trace, fatal flag, time, the open water's id, the random **install id** (T352). No location/spots/searches/device or ad ids | on by default, switch in More → Data |
| Weather for a saved spot | Open-Meteo (third party) | the spot's coordinates + date (+ the device IP to them) | only when the user opens a spot's weather |
| Satellite imagery | Esri (third party) | tile requests for the viewed area + IP | opt-in layer, online only |
| Location | never sent to us; used on-device (see weather row for a pinned spot) | — | permission, while-in-use only |
| Photos / camera | on-device only | — | permission |
Not present: accounts, advertising IDs, analytics SDKs, third-party crash SDKs, cookies (site), background location, purchases.

## Google Play: Data safety form
Moved to `handoff/PLAN/play/data-safety.md` (2026-11-04, T371). It was redrafted from the code with file:line evidence.
It answers Device or other IDs = YES (the T352 install id) and Approximate location includes a problem report's map
centre (T352 + T384); the table above now carries both.

## Apple — App Privacy ("nutrition label")
(Updated 2026-11-05 for the T352 install id; final answers at the first App Store upload, T365.)
- Data used to track you: **None** (no tracking, no ATT prompt).
- Data linked to you: **None** (no account; the install id is random and tied to no person).
- Data not linked to you:
  - Identifiers: Device ID — the random install id (crash + problem reports) — App Functionality. **OWNER** (proposal: declare it; Apple counts any per-install id).
  - Location: Coarse Location — a problem report's map centre (~100 m), only when sent with its row on — App Functionality.
  - Diagnostics: Crash Data, Performance Data/Other Diagnostic Data — App Functionality (switchable off).
  - Contact Info: Email Address — App Functionality — only when typed into a report. (Report text: Other User Content.)
  - Location: Precise Location (and Coarse for imagery) — collected via third parties Open-Meteo / Esri — App Functionality, not linked to you, not for tracking (owner 2026-10-29).
- Permission strings (already in app.json): location when-in-use, camera, photos (each says it stays on the device). No microphone.
- Export compliance: HTTPS only, standard encryption → `ITSAppUsesNonExemptEncryption = false` (owner confirms at first upload).

## Consistency checks (run before submitting)
1. Privacy page text == these answers (T268 rule: change the app, change the page).
2. `EXPO_PUBLIC_CONTACT` == the contact on the listing == support@canifishthis.ca.
3. Crash reporting is default-on; if T242 flips that, the label stays (collected, optional).
