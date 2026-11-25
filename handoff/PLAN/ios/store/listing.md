# App Store listing fields from the Play items (T727 round 2, PROPOSAL)

**Superseded 2026-11-24 by `handoff/PLAN/ios/APP-STORE-SUBMISSION.md` (T1071), the page the owner pastes from. Where the two differ, that page is right (it was re-read against build 0.9.0 (45): push token, Precise vs Coarse, linked answers, the More menu paths). This page is kept as the evidence behind it.**

Source: the Play pages are the source (owner 2026-11-14): `handoff/PLAN/play/store-description.md` (owner's text),
`handoff/PLAN/play/listing.md` (fact sheet), `handoff/PLAN/app-integration/store/listing-draft.md`. Nothing is entered.
Limits are Apple's as the coordinator gave them; check them on the live screen.

| App Store Connect field | Limit | Value | Source | Fits |
|---|---|---|---|---|
| Name | 30 | Can I Fish This? (16) | listing.md "App name" | yes |
| Subtitle | 30 | BC fishing rules, by the water (30) | listing-draft.md | yes, exactly at the limit |
| Promotional text | 170 | Your pocket guide to BC fishing regulations. Less searching, more fishing! (74) | store-description.md "Short description" | yes |
| Description | 4000 | the "Long description" section as is, with its disclaimer and official-source links (3924 counted) | store-description.md | yes, 76 spare. Any added line must stay within 76 characters |
| Keywords | 100 | fishing,BC,regulations,rules,freshwater,river,lake,trout,salmon,steelhead,angling,license,flow,gauge (100) | listing-draft.md | yes, exactly 100 |
| What's New (first version) | 4000 | PROPOSAL: "First release for iPhone: the BC fishing regulations map, river conditions, salmon notices and saved spots." | mine, built from the description's feature names | yes |
| Support URL | n/a | https://canifishthis.ca/about | listing-draft.md | n/a |
| Marketing URL | n/a | https://canifishthis.ca | listing-draft.md | n/a |
| Privacy policy URL | n/a | https://canifishthis.ca/privacy | listing-draft.md | n/a |
| Category | n/a | Navigation, secondary Sports | `review-notes.md` | n/a |
| Icon | 1024 x 1024, no alpha, no rounded corners | `app/apps/mobile/assets/icon/icon-1024.png` (the Play icon) | listing.md graphics | **checked: 1024 x 1024, hasAlpha yes**. Flatten a copy onto the icon ground before upload (proposal, not done). Rounded corners: not checked visually |

No Play text was too long for an Apple field. Wording to check: the description ends "approximately 500 MB" and the review
notes say about 480 MB; the description's "Works Without Cell Service" and the rest has no Android-only claim that I saw.
