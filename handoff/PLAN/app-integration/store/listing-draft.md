# Store listing — DRAFT for owner sign-off (T244; nothing submitted)

Package `ca.canifishthis.app`. Same text serves Google Play and the App Store; limits noted per field. No line promises fish,
a catch or legal advice (the app's own disclaimer applies: it is a reading aid, the printed synopsis and DFO notices rule).

## Name
canifishthis  (App Store subtitle / Play short title: "BC fishing rules, by the water")

## Short + full description
Moved to `handoff/PLAN/play/listing.md` (2026-11-04, T371). That file is the one copy, and the App Store uses the same
text. It was updated for T384 (opens on your location), T385 (share) and T352 (install id).

## Keywords (App Store, 100 chars, comma-separated)
fishing,BC,regulations,rules,freshwater,river,lake,trout,salmon,steelhead,angling,license,flow,gauge

## Categories
- Play: Maps & Navigation (alt: Sports)
- App Store: Sports (alt: Navigation)

## Support
- Support URL: https://canifishthis.ca/about   (confirm; or a mailto page)
- Contact email: support@canifishthis.ca (owner 2026-10-29; the build's EXPO_PUBLIC_CONTACT is the same: eas.json profiles, build-apk.sh, export:web scripts; the privacy page keeps privacy@canifishthis.ca)
- Privacy policy URL: https://canifishthis.ca/privacy  (live when the prod site is up, T240 section 5)
- Marketing URL (App Store): https://canifishthis.ca

## Age / content rating (answers; owner to submit)
- Play (IARC): category Reference / Utility; no violence, sexual content, language, controlled substances, gambling, user-to-user
  sharing or purchases; the app mentions fishing (animals caught and released or kept by licence) as the subject only. Expected: Everyone / 3+.
- App Store: all "None"; no unrestricted web access, no gambling, no user-generated content shared with others. Expected 4+.
- Target audience (Play): 18+ — avoids the Families policy; not designed for children.
- Not a news, government, health or financial app. Ads: none. In-app purchases: none. Login: none (all features open).

## Graphics (owner to supply or approve)
| Asset | Need | State |
|---|---|---|
| App icon 1024 (App Store) / 512 (Play) | yes | DONE: the favicon (owner 2026-10-29), `app/apps/mobile/assets/icon/icon-1024.png`, `play-512.png`; preview `T244-shots/icons-preview.png` |
| Adaptive icon foreground + background (+ monochrome) | yes | DONE: `adaptive-foreground.png`, `adaptive-monochrome.png`, background = the light theme's ground (app.config.js) |
| Feature graphic 1024x500 (Play) | yes | BASE done (`feature-graphic-1024x500.png`: the mark on the ground, no text); owner adds words if wanted |
| Phone screenshots, light + dark | 2-8 each | take on the production build after T240 smoke; shot list below |
| Tablet screenshots | Play optional; App Store only if tablets supported (`supportsTablet: false` today, so none) | n/a |

Shot list (390 px phone, light then dark): map with colouring; a water's regulations; conditions chart; salmon notice; search;
spots; the loading screen is NOT a store screenshot.
