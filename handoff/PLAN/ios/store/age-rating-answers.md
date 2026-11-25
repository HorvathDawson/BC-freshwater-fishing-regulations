# App Store age rating answers (T727, PROPOSAL for the owner)

A proposal. Nothing is entered. Expected result: the lowest rating (4+ on the old scale; check which tiers Apple now shows,
since Apple changed its age tiers in 2025 and the question list changed with it). Apple's question wording below is from
memory of its categories: where a question may be worded differently on your screen, the answer still follows the "why".
Related earlier answer: `handoff/PLAN/app-integration/store/listing-draft.md` ("Age / content rating", all None, expected 4+).

## What the app is, and how I checked each claim (2026-11-14)
| Claim | Checked how | Status |
|---|---|---|
| Fishing regulations and maps; no game | app/store-listing/listing.json, listing.md fact sheet | verified (docs) |
| No accounts, no sign-in | T365 "Met" table; no sign-in code seen; `app.json` has no auth plugin | verified (T365 + app.json) |
| No chat, no messages between users, no user-to-user content | no such feature in the fact sheet; no server stores user content except problem reports sent to the owner | verified from docs; no screen-by-screen walk |
| Photos are the reader's own, kept on the phone | `app.json` photo and camera prompts say "stay on this device"; data-safety.md row 7 | verified (docs + prompts) |
| No web browsing inside the app | no WebView in `app/packages` or `app/apps/mobile/src` (searched for "WebView"); links go through `packages/ui-native/src/openLink.ts` to the phone's browser or mail app | verified by search |
| Links out exist (official sources, Google Maps for directions, mail for a report, share sheet) | `openLink` callers: `SpotHost.tsx:107`, `Licences.tsx:14`, `InfoPages.tsx:17`, `ReportPage.tsx:55`, `openBook.ts:24` | verified |
| No ads, no purchases, no gambling | listing-draft.md ("Ads: none. In-app purchases: none") ; T365 ("No payments, no ads") | verified (docs) |
| Fish are the subject (catch and release or keep by licence rules) | listing-draft.md | verified (docs) |

## Questions and answers
| Question (Apple's category) | Answer | One line why |
|---|---|---|
| Cartoon or fantasy violence | None | no violence shown |
| Realistic violence | None | no violence shown; fish limits and gear rules are text |
| Prolonged graphic or sadistic realistic violence | None | none |
| Guns or other weapons (if asked) | None | none shown (check: the question may list hunting and fishing; the app shows fishing rules, which is the subject, not an act shown) |
| Sexual content or nudity | None | none |
| Profanity or crude humour | None | none; water names come from official data (check: owner rule "never filter content" means a place name could be rude: see below) |
| Horror or fear themes | None | none |
| Mature or suggestive themes | None | none |
| Alcohol, tobacco, or drug use or references | None | none |
| Medical or treatment information / health or wellness topics | None | not a health app |
| Simulated gambling | None | none |
| Real-money gambling, contests, sweepstakes | None | none |
| Loot boxes | None | none |
| Unrestricted web access | No | no browser inside the app; links open the phone's own browser |
| User-generated content (visible to others) | No | saved spots and photos are private to the phone; reports go only to the owner |
| Messaging and chat | No | none |
| Advertising | No | none |
| In-app controls: parental controls, age assurance | None | no accounts or purchases to restrict (check whether Apple asks this) |
| Made for Kids | No | adult audience (listing-draft.md: 18+ on Play, avoids the Families policy) |
| Region-specific ratings (Brazil etc.) | Leave as calculated | follows from all "None" |

Note on place names: the map shows names exactly as sourced (owner 2026-10-28: never filter or rename). If a rude name exists
in the official data, the "Profanity" answer stays None only because a name is a place, not content the app writes. Check
whether Apple treats it so; this was not examined.

## Not verified
- I did not walk every screen of the app on a device; the "no chat, no user content" claims rest on the feature lists and the
  absence of such code, not a walk.
- Apple's exact current question list and tier names (check on the live screen).
- Whether `Notifications` copy or the alert text is a "user-generated content" surface: it is generated on the phone from
  the reader's own gauge settings, so No.
