# T319 — Store screenshots that sell the app, generated automatically (for the real launch)

Status: POSTPONED (the ledger's call, owner 2026-11-05; T437 aligned) — where it stood: IN PROGRESS 2026-11-05 — the generator is built (one command, every size); the shot list + titles wait for the owner; the saved-spot shot needs the phone build. Was: POSTPONED (owner 2026-10-31: "make it so the screenshots for promotion on app store are better … optimal pictures of
the app and how it is used. Making it look good? Then we can have a way to auto generate it? This is a ticket for later
don't start it now add to stuff needed for real deploy").

## Work (later, part of the production launch — T240)
1. A shot list that tells the story: find a water → today's answer → what you can keep → conditions → salmon → save a spot →
   offline. Real, attractive places (Lower Mainland / interior / north), good light/dark, no dev overlays, a clean status bar.
2. Framed store images: phone frame, short caption per shot in the app's fonts/colours, Play (phone + 7"/10" tablet) and App
   Store sizes (6.7"/6.5"/5.5" iPhone, iPad), feature graphic.
3. One command regenerates them all from the current app + release (emulator/simulator via the test bed, or the web app at
   device viewports), so each release can refresh the store listing.

Depends: T240, T244, T313. Size: M. Owner: picks the shot list/captions.

## Built 2026-11-05
- **One command:** `node app/tools/store-shots.mjs [--theme light|dark] [--only <targets>] [--no-shoot]` — runs
  `play-shots.mjs` on staging for every raw size the targets need, then frames each shot at the store's exact pixels:
  title + one line in the app's faces (`tools/seo/fonts`) and theme colours, the screen in a square card with the hard
  shadow. Refuses a missing raw shot, a missing font or token.
- **Targets** (`app/tools/store-shots.json`): Play phone 1080×1920, 7" 1200×1920, 10" 1600×2560; App Store 6.7"
  1290×2796, 6.5" 1242×2688, 5.5" 1242×2208, iPad 13" 2048×2732. `play-shots.mjs` gained `iphone67`, `iphone55`,
  `ipad13`, a `09-offline` scene, and seeds the mode-hint tours as seen ("Where you are" covered the bottom bar).
- **Shot list (PROPOSAL, owner picks):** map → search → today's rules → conditions → salmon → runs → offline, with titles
  in the json. 49 light images made 2026-11-05 in `store/graphics/store/light/` (gitignored, regenerate).
- **Left:** the saved-spot shot (the web Spots tab is a "get the app" notice → the emulator test bed, heavy); dark set;
  the feature graphic's text; the owner's pick of shots and titles.
