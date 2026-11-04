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

## Google Play: Data safety form
Moved to `handoff/PLAN/play/data-safety.md` (2026-11-04, T371). It was redrafted from the code with file:line evidence.
Two things changed since this draft: Device or other IDs is now YES (the T352 install id), and Approximate location now
includes a problem report's map centre (T352 + T384). The table above predates both.

## Apple — App Privacy ("nutrition label")
(Not yet updated for the T352 install id: Apple may count it as an Identifier → Device ID, "not linked to you". Settle this in T365.)
- Data used to track you: **None** (no tracking, no ATT prompt).
- Data linked to you: **None** (no account, no identifier).
- Data not linked to you:
  - Diagnostics: Crash Data, Performance Data/Other Diagnostic Data — App Functionality (switchable off).
  - Contact Info: Email Address — App Functionality — only when typed into a report. (Report text: Other User Content.)
  - Location: Precise Location (and Coarse for imagery) — collected via third parties Open-Meteo / Esri — App Functionality, not linked to you, not for tracking (owner 2026-10-29).
- Permission strings (already in app.json): location when-in-use, camera, photos (each says it stays on the device). No microphone.
- Export compliance: HTTPS only, standard encryption → `ITSAppUsesNonExemptEncryption = false` (owner confirms at first upload).

## Consistency checks (run before submitting)
1. Privacy page text == these answers (T268 rule: change the app, change the page).
2. `EXPO_PUBLIC_CONTACT` == the contact on the listing == support@canifishthis.ca.
3. Crash reporting is default-on; if T242 flips that, the label stays (collected, optional).
