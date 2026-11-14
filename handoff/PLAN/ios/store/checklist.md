# App Store Connect: ordered checklist (T727, PROPOSAL for the owner)

A proposal, nothing entered. Order is the order of the screens in App Store Connect as far as I know them (check against the
live screens). "Who": OWNER = only you can; DONE = already in the repo; ACCOUNT = needs the Apple Developer account first;
T86 = needs production data and hosting live; OTHER LANE = being made by another lane (screenshots, video).

## A. Before anything (account)
| # | Step | Value or page | Who |
|---|---|---|---|
| 1 | Enrol in the Apple Developer Program, pay, accept agreements | individual or organization is open question 1 in `handoff/PLAN/ios/START-HERE-ios.md` (an organization needs a D-U-N-S number and changes the seller name) | OWNER (Apple ID + 2FA) |
| 2 | Create the app record | Bundle ID `ca.canifishthis.app` (`app.json:36`), Platform iOS, Primary language English (Canada) (START-HERE-ios.md), SKU: your choice, proposal `canifishthis-ios` | OWNER, ACCOUNT |
| 3 | Team ID and API key to `~/.canifishthis/ios-team-id`, `~/.canifishthis/asc-api-key.json` (.p8, App Manager) | START-HERE-ios.md "What needs the owner physically" | OWNER, ACCOUNT |

## B. App information
| # | Field | Value or page | Who |
|---|---|---|---|
| 4 | Name (max 30 chars) | **Can I Fish This?** (16 chars; owner settled 2026-11-13 for both stores) | OWNER, ACCOUNT |
| 5 | Subtitle (max 30) | BC fishing rules, by the water (exactly 30 chars: check the count Apple shows) (`listing-draft.md`) | OWNER, ACCOUNT |
| 6 | Primary / secondary category | Navigation / Sports (decided): `review-notes.md` | OWNER |
| 7 | Content rights ("contains third-party content") | Yes: government data, OpenStreetMap, Esri imagery. Check that the licence page (More, Legal and the Licences page) covers each; the app wording is T365 "Government information" row | OWNER |
| 8 | Age rating | `age-rating-answers.md` | OWNER |
| 9 | Privacy policy URL | https://canifishthis.ca/privacy (live only when the production site is up, T240 section 5 / T86) | T86 |
| 10 | App Privacy | `app-privacy-answers.md` (decide its Mismatches table first) | OWNER |

## C. Version page (1.0 or the first version)
| # | Field | Value or page | Who |
|---|---|---|---|
| 11 | Promotional text (170 max, optional) | Play short description, 74 chars: see `listing.md` here | OWNER |
| 12 | Description (4000 max) | `handoff/PLAN/play/store-description.md` unchanged, 3924 chars incl. the disclaimer (fits). Check "approximately 500 MB" against the real size; review notes say about 480 MB | DONE (text), OWNER to paste |
| 13 | Keywords (100 max) | `fishing,BC,regulations,rules,freshwater,river,lake,trout,salmon,steelhead,angling,license,flow,gauge` (exactly 100 characters, counted today; Apple may count commas, check) (`listing-draft.md`) | DONE |
| 14 | Support URL | https://canifishthis.ca/about | DONE (page up when prod is) |
| 15 | Marketing URL (optional) | https://canifishthis.ca | OWNER |
| 16 | What's New | for a first version, proposal: "First release for iPhone." Check later versions against `app/packages/core/src/changelog.ts` | OWNER |
| 17 | Copyright | proposal: the year and the name of the seller entered at enrolment (not mine to choose) | OWNER |
| 18 | Screenshots, iPhone 6.9" and 6.5" | made by another lane from the simulator (`app/tools/store-shots.mjs`, `iphone67` and `iphone55`) | OTHER LANE |
| 19 | App preview video | `app/tools/store-video` run on iOS (T709 step 2) | OTHER LANE |
| 20 | iPad screenshots | none: `supportsTablet: false` (`app.json:37`) | n/a |
| 21 | App icon 1024 | `app/apps/mobile/assets/icon/icon-1024.png` (the Play icon, `app.json:6`). CHECKED: 1024 x 1024 but hasAlpha = yes; Apple wants no alpha: flatten a copy onto the icon ground before upload (proposal, not done) | OWNER / needs a small fix |

## D. App Review information and release
| # | Field | Value or page | Who |
|---|---|---|---|
| 22 | Sign-in required | No; leave demo fields empty | OWNER |
| 23 | Contact name, phone, email | your own; email proposal support@canifishthis.ca | OWNER |
| 24 | Notes | paste block in `review-notes.md` | OWNER |
| 25 | Build | a PRODUCTION-env build (`CONFIRM_PROD=canifishthis.ca` for production iOS builds, AGENTS.md "Staging"), uploaded with `deploy/staging/build-ios-testflight.sh --production` once the keys exist. Claude never sets the production confirm | OWNER, ACCOUNT, T86 |
| 26 | Export compliance | HTTPS only; `ITSAppUsesNonExemptEncryption` is false in the build (`app.json:39-41`) | DONE |
| 27 | Advertising identifier (IDFA) | No | OWNER |
| 28 | Version release | proposal: manual release after approval (so the production data is live first) | OWNER |
| 29 | Pricing and availability | Free; territories: your choice (proposal: Canada first, since the data is BC only; check) | OWNER |

## E. The name: where it differs (list only, nothing edited)
Owner settled "Can I Fish This?" on both stores (2026-11-13). Earlier decision (2026-11-06, `listing.md`): the short
"Can I Fish?" is kept only where the icon shows the name.

| String | Where | Today | What it controls |
|---|---|---|---|
| `expo.name` | `app/apps/mobile/app.json:3` | Can I Fish? | the iOS home-screen label (CFBundleDisplayName) and Android launcher label; not the store listing name. 12 chars fit under an icon |
| `expo.web.name` | `app.json:150` | Can I Fish This? | the web app's title and install name only |
| `expo.web.shortName` | `app.json:151` | Can I Fish? | web home-screen label |
| Permission prompts | `app.json` plugins (photos, camera, location) | "Allow Can I Fish This? ..." (literal text) | what the person reads in iOS permission dialogs |
| `APP_NAME` | `app/packages/core/src/menu/words.ts:9` | Can I Fish This? | the name inside the app (About, Legal, etc.) |
| Store name | App Store Connect field 4 above | to be entered | search and the listing |
| Listing text and subtitle | `store-description.md`, `listing-draft.md` | Can I Fish This? | description text |
| Changelog line | `app/packages/core/src/changelog.ts:201` | "New name on the home screen, Can I Fish?" | history text only |
| Slug / Xcode product | `app.json:4` slug `canifishthis`, product name | canifishthis / CanIFish | not shown to users except in crash logs (T365) |

DECIDED (owner 2026-11-14): the home-screen label stays "Can I Fish?" (`expo.name`, no app.json change); "Can I Fish This?" everywhere else, including the store name. The older generated `ios/` folder here shows the display name "canifishthis" and prompts
built from the product name: it is stale, rebuild before judging.

## F. Decided, and what is still open
Decided (owner 2026-11-14): home-screen label "Can I Fish?"; Precise location (and Coarse); install id purposes Analytics + App Functionality; category Navigation then Sports; Play texts are the source.
Open, default: email in the privacy answers is linked to the user (a) default, or (b) not linked.
Resolved 2026-11-14 (fa0d0a45c): `play/data-safety.md` now says production does send reports and that the crash switch is under More, Settings, Privacy. The Play form itself was already right (its answers were written for the build that sends reports); nothing submitted to Play changes.
See `listing.md` here for the field-by-field text.
