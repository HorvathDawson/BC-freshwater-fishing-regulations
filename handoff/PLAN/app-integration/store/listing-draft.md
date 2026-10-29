# Store listing — DRAFT for owner sign-off (T244; nothing submitted)

Package `ca.canifishthis.app`. Same text serves Google Play and the App Store; limits noted per field. No line promises fish,
a catch or legal advice (the app's own disclaimer applies: it is a reading aid, the printed synopsis and DFO notices rule).

## Name
canifishthis  (App Store subtitle / Play short title: "BC fishing rules, by the water")

## Short description (Play, max 80)
BC freshwater fishing rules for any river or lake: map, seasons, limits, flows.

## Subtitle (App Store, max 30)
BC fishing rules, by the water

## Full description (Play max 4000 / App Store max 4000)
Can I fish this? Tap a river or lake on the map and see the freshwater fishing rules that apply to that exact water, in plain words.

WHAT IT DOES
- A map of BC's named rivers, lakes and streams, coloured by what the rules say today.
- The rules for the water you tapped: seasons, daily limits, size limits, bait and gear rules, closures, licences. Rules that
  change along a river are split by section, so you see the stretch you are standing on.
- Live river flow and temperature from the federal gauges, with how today compares with other years.
- Salmon notices and in-season changes, brought together when Fisheries and Oceans Canada publishes them.
- Search by water name, an old name or a nearby town.
- Save spots with notes and photos, and see the weather and river flow for the day you fished.
- Works offline once installed: the map and the rules live on your phone and refresh when you are on Wi-Fi.

PRIVATE BY DESIGN
No account, no ads, no tracking. Your location, photos and saved spots stay on your phone. The only things the app ever sends us
are a problem report you choose to send and, unless you switch it off, a short error report when the app itself fails.
Privacy policy: https://canifishthis.ca/privacy

FREE AND NON-PROFIT
canifishthis is free and not run for profit. It is not an official BC Government or DFO product.

IMPORTANT
The app is a reading aid built from the BC Freshwater Fishing Regulations Synopsis and DFO notices. Regulations change; always
check the current printed synopsis, the BC Fisheries website and DFO notices before you fish, and carry the licences you need.

## Keywords (App Store, 100 chars, comma-separated)
fishing,BC,regulations,rules,freshwater,river,lake,trout,salmon,steelhead,angling,license,flow,gauge

## Categories
- Play: Maps & Navigation (alt: Sports)
- App Store: Sports (alt: Navigation)

## Support
- Support URL: https://canifishthis.ca/about   (confirm; or a mailto page)
- Contact email: OWNER TO CHOOSE (the privacy page uses privacy@canifishthis.ca; the build's EXPO_PUBLIC_CONTACT must match what the listing shows)
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
| App icon 1024 (App Store) / 512 (Play) | yes | NO ICON ART IN THE REPO beyond Expo's default launcher; owner to approve a mark (suggest the loading fish in its ring on the live-water teal — the splash's art, redrawn larger) |
| Adaptive icon foreground + background | yes | same decision |
| Feature graphic 1024x500 (Play) | yes | needs the icon decision |
| Phone screenshots, light + dark | 2-8 each | take on the production build after T240 smoke; shot list below |
| Tablet screenshots | Play optional; App Store only if tablets supported (`supportsTablet: false` today, so none) | n/a |

Shot list (390 px phone, light then dark): map with colouring; a water's regulations; conditions chart; salmon notice; search;
spots; the loading screen is NOT a store screenshot.
