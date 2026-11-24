# T1038 — Carry a phone link's date, tab, sheet, scroll, age, res, horizon and whole (today they are reported and dropped)

Status: PARTIAL — 94c0a117d, 1a5574b00 (all eight keys carried on a water's link, `date`/`horizon` on a box's and on a day-only link (`/?date=`: the map on that day, as the web); read by the web start link's own readers and handed through the Shell's one open request; a link to the water already open re-applies its tab/sheet/scroll; tests per key, mutation-proved; web check of each key 2026-11-23); left (Claude): the device check below. Still reported, not carried (said with `reportFailure`): a page key on a link with no water (the web ignores it too) and `whole` where it does nothing.

Source: lane L15 on T1034, 2026-11-23. The web start link carries these keys (`parseStartLink`); the phone's link path
(`parseAppLink`) did not, and listed them as read while dropping them. T1034 now REPORTS each one (`reportFailure`: "<key>=
is not opened by the phone yet (T1038): dropped") and opens the water without it, so nothing is dropped silently.

## Work
1. Carry `date` (the Shell's day) and `tab` first (the most useful), then `sheet`, `scroll`, `whole`, `horizon`; `age` and
   `res` set the angler. Each through the same state the web start link sets (`StartLink` → Shell props), one load path.
2. Remove each key from `UNSUPPORTED_KEYS` (appLink.ts) as it is carried; a test per key.

Depends: T1034. Size: M. Owner: no.

## Done (lane L30, 2026-11-23)
- `startLink.ts`: `linkDay` (date, past, horizon), `linkPage` (tab, sheet, scroll, age, res) and `linkWhole` split out of
  `parseStartLink` unchanged; `parseAppLink` reads the same keys with them, so a value means the same on both and a bad
  value throws on both (the web then opens nothing; the phone reports and opens nothing). `UNSUPPORTED_KEYS` is gone.
- Phone: `applyAppLink` puts the day (null today), horizon (absent: Now), page start and `whole` on the open request;
  the angler goes to `setAnglerProfile` (a link with no `age`/`res` leaves the session's angler, as the web's "the
  session's default stands"); a past day shows the web's own notice line. The Shell's open-request effect sets `mapDay`,
  the horizon (`col.setHorizon`, the raw set `initialHorizon` makes, not a chip pick) and `start`, and opens the water
  through `opener.item(…, whole)`. A notification's tap (no day/horizon) leaves them as they are.
- Tests: `appLink.test.ts` (each key: phone == web; bad values; reported keys), `appLinkOpen.test.ts`,
  `Shell.appLink.test.tsx` (real Shell: route `whole`, `onAddress` day+horizon, the host's `start`). 23 mutations, all killed.
- (coordinator, 2026-11-23) `/?date=` / `?horizon=` with no water or box: `{ home }` → the Shell closes to the browse map
  on that day/horizon (the web's browse start). A new start per link request: `RegsScreen` applies each start it is handed
  (deps `[start]`; a start on an open page sets its sheet, stops a scroll memory still restoring and jumps, Keep at 0 the
  top), and the Shell hands a link's start only to the page of the water it names (`startForFrame`). The re-apply test
  failed before the change; a second bug it found (a page mounted with no start treated the first later one as its
  mount's) is fixed and pinned. 34 mutations, all killed.
- Web check (Expo web, staging data, 390x844): `date=2026-12-24` (Thu, Dec 24), `tab=gear` (Gear selected, scrolled),
  `tab=licence&age=u16&res=non_resident` (Licence, Under 16), `sheet=pos:pos` (Possession sheet open), `scroll=600` (600 px),
  `date=2026-10-01` (the "has passed" notice), `view=conditions&horizon=3` ("Forecast · 3 days ahead"), `view=map&whole=1`
  ("Whole water"); after the start change: `/?date=2026-12-24` (Map tab, Thu, Dec 24), `tab=gear` (Gear, 1139 px, 3 of 3
  warm loads; the first load after a Metro reload landed short at 937 px, the same on main's RegsScreen when compared).

## 0.9.0 device pass (Android)
Open each with another water on screen, then from a cold start; each must land as the same URL on the web:
- `https://staging.canifishthis.ca/?regs=gnis:3062&date=<a future day>&tab=gear` — that day in the day chip, Gear tab.
- `…/?regs=gnis:3062&sheet=pos:pos` — the Possession sheet open; `…&tab=licence&scroll=300` — Licence, 300 px further.
- `…/?regs=gnis:3062&age=u16&res=non_resident&tab=licence` — the licence answer for an under-16 non-resident.
- `…/?regs=gnis:3062&view=conditions&horizon=3` — Conditions on +3; `…&view=map&whole=1` — the Map on the whole water.
- `…/?regs=gnis:3062&date=<a past day>` — today, with "The day this link names has passed, so today is shown."
- `…/?regs=gnis:3062&tab=gear` with the Vedder's Regulations already open and scrolled: Gear again (a new start re-applied).
- `…/?date=<a future day>` (no water) with a water open: the browse map on that day.
