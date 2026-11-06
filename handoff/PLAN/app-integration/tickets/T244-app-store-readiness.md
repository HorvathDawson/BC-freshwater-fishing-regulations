# T244 — app store readiness

Status: IN PROGRESS — 2026-11-05: Claude's part current (see Check 2026-11-05); the rest is owner / T319 / T88 / T404. 2026-10-29: splash + listing/privacy-label drafts DONE (Claude); icon art, store accounts, builds, submissions are the owner's / wait on T268, T250.

Source: owner 2026-10-28 — approved proposal P10 in handoff/PLAN/proposed-tickets-2026-10-28.md ("approved ALL 16").

## Work
Icon + adaptive icon, splash, store screenshots (phone + tablet, light/dark), short + full description, keywords,
privacy policy page (canifishthis.ca/privacy: no account, no tracking, crash reports if T242 on), store data-safety /
privacy labels ("data collected: none" or crash data), age rating questionnaire, content rating, support URL + contact,
Play internal track + TestFlight build via eas.json `production` profile (T232).
## Acceptance
An internal-track Android build and a TestFlight build install from the stores; listing drafts saved; policy page live.
Depends: T232, T86/T240 (prod hosting), T242 decision, T250 (SDK). Size: M.
Owner: yes — developer accounts, listing text sign-off, contact address.

## Outcome 2026-10-29 (Claude part)
**Splash = the loading fish, handed off seamlessly (owner 2026-10-29).**
- Art is only what exists: frame 0 of the committed FishSpinner strip (`fish-sprite.generated.ts`, `tools/build-spinner.mjs`) + the same orbit ring,
  composited by `app/tools/build-splash.mjs` (`pnpm splash` checks it; wired into `pnpm check`) to `apps/mobile/assets/splash/splash-{light,dark}.png`
  (330 px = 110 dp x 3, colours read from the theme files; no colour literal anywhere).
- `apps/mobile/app.config.js` (dynamic part; app.json unchanged) adds the `expo-splash-screen` plugin (new dep `expo-splash-screen ~57.0.10`,
  noted in deps.md + layers.json): ground = each theme's `color.ui.card` (what the boot screen paints), dark variant, `imageWidth` 110.
- Hand-off: `src/splash.ts` — `holdSplash()` (index.js, before mount; 8 s safety limit) / `releaseSplash()` (idempotent, 150 ms fade). `App.tsx`'s new
  `BootScreen` draws the spinner at once (`immediate`, no 300 ms hold) at 110 dp, centred on the WINDOW (insets difference taken out, as the native splash is
  window-centred), pun + step + bars hang BELOW it so late arrivals never move it; the native splash is released two frames after the fish is laid out.
  A failed / ready start releases it too.
- Text: `SPLASH_WORDS` / `splashLineFor` in `@app/core` `uiWords.ts` (six puns, one per opening, no fish promised; test). The native picture is the fish only
  (a native splash cannot hold live text in the app's faces); the pun fades in under the fish as the splash releases.
- Verified on web (`T244-shots/web-boot-{light,dark}.png`, 390x844, data held back): fish centre (195, 422) = window centre. Native splash art:
  `T244-shots/native-splash-art-{light,dark}.png`. Android reasoned, not run: the plugin writes `Theme.App.SplashScreen` (Android 12 SplashScreen API,
  `windowSplashScreenAnimatedIcon` on a 288 dp canvas, our 110 dp art centred, well inside the 192 dp circle) and light/dark `splashscreen_background`;
  `MainActivity` already switches to AppTheme before create. No emulator shot: the heavy lock is held. iOS: storyboard, centred, `contain`.
- Needs a `expo prebuild` (the next `build-apk.sh`) to take effect natively; the old `android/` dir still holds Expo's default splash until then.
- Web before-JS paint is unchanged (blank until the bundle runs); not part of this.
**Listing + privacy drafts for sign-off:** `handoff/PLAN/app-integration/store/listing-draft.md` (name, short 79/80, subtitle, full, keywords 100/100,
categories, support/privacy URLs, rating answers, graphics table) and `store/privacy-labels-draft.md` (what the app sends, Play Data safety, Apple App
Privacy; the Location row is an OWNER judgement: a saved spot's coordinates go to Open-Meteo). Nothing submitted.
## Still open (owner)
App icon / adaptive icon / feature graphic (no icon art in the repo beyond Expo's default; suggestion in the listing draft), contact address, listing +
label sign-off, store screenshots from a production build, developer accounts + TestFlight (T88), Play upload (T267/T268), SDK window T250. Items
are in OWNER-TODO.

## Update 2026-10-29 (owner answers)
- **Icon = the favicon** (`archive/webapp/public/favicon.svg`, byte-identical to what canifishthis.ca serves; copied to `apps/mobile/assets/icon/favicon.svg`).
  `app/tools/build-icons.mjs` (@resvg/resvg-js, dev dep; run by hand, PNGs committed) places its own paths, in its own stroke colour, on the light theme's
  ground: `icon-1024.png` (iOS, app.json `icon`), `adaptive-foreground.png` + `adaptive-monochrome.png` (inside the 66/108 safe circle; background colour =
  the ground, set in app.config.js), `play-512.png`, `feature-graphic-1024x500.png` (base, no text), `favicon-48.png` (app.json `web.favicon`).
  Preview: `T244-shots/icons-preview.png`. Staging/SEO: `tools/seo/favicon.ts` adds `/favicon.svg` + `<link rel="icon">` to every hub page (staging had none, 404);
  water pages carry the app export's favicon. Not deployed: the staging site deploy waits on the coordinator.
- **Privacy labels:** Location declared collected + shared with Open-Meteo (weather stays on Open-Meteo), Esri for imagery tiles (owner).
- **Contact:** support@canifishthis.ca (listing; `EXPO_PUBLIC_CONTACT` in eas.json x3, build-apk.sh, export:web scripts; the app's Contact page reads it). Privacy page
  keeps privacy@canifishthis.ca. OWNER-TODO: set up the mailbox.

## Check 2026-11-05 (0.6.2)
- Done since: icons (favicon art), splash, listing + privacy drafts, contact `support@canifishthis.ca` in every build
  profile (and a production build without it is now refused: T86, e45841b85), Play internal track live (0.6.2, build 11),
  privacy page on staging, Play data safety redrafted with evidence (`play/data-safety.md`, T371).
- `store/privacy-labels-draft.md` brought up to the code: the install id (T352) and the report's map centre (T384) were
  missing from its table and Apple section; Apple → Identifiers → Device ID is a proposal (**OWNER**).
- Left, all owner or later: listing + labels sign-off, the support mailbox, store screenshots (T319), TestFlight (T88,
  needs the Apple account), the production build (T404), Closed testing (T371).
