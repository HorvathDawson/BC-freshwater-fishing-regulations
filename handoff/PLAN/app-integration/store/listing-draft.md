# Store listing — DRAFT for owner sign-off (T244; nothing submitted)

Package `ca.canifishthis.app`. Same text serves Google Play and the App Store; limits noted per field. No line promises fish,
a catch or legal advice (the app's own disclaimer applies: it is a reading aid, the printed synopsis and DFO notices rule).

## Name, descriptions, keywords, categories, URLs
Moved to ONE source (T1072): `app/store-listing/listing.json` (+ `full-description.txt`); limits checked by `app/tools/store-listing.test.ts`.
Facts (not words) are in `handoff/PLAN/play/listing.md`. Contact email: support@canifishthis.ca (owner 2026-10-29; the privacy page keeps privacy@canifishthis.ca).

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
