# T1038 — Carry a phone link's date, tab, sheet, scroll, age, res, horizon and whole (today they are reported and dropped)

Status: PARTIAL — 94c0a117d, 1a5574b00, 06b85846a (all eight keys carried on a water's link, `date`/`horizon` on a box's and on a day-only link (`/?date=`: the map on that day, as the web); read by the web start link's own readers and handed through the Shell's one open request; a link to the water already open re-applies its tab/sheet/scroll; tests per key, mutation-proved; web check of each key 2026-11-23); left (Claude): the device check below. Still reported, not carried (said with `reportFailure`): a page key on a link with no water (the web ignores it too) and `whole` where it does nothing.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1038 | Carry a phone link's date, tab, sheet, scroll, age, res, horizon and whole (today reported and dropped) | PARTIAL — all 8 carried (web readers, one open request), day-only links, start re-applied; left (Claude): 0.9.0 device pass | 94c0a117d, 1a5574b00, 06b85846a | lane L15 on T1034, 2026-11-23 |

Source: lane L15 on T1034, 2026-11-23. The web start link carries these keys (`parseStartLink`); the phone's link path
(`parseAppLink`) did not, and listed them as read while dropping them. T1034 now REPORTS each one (`reportFailure`: "<key>=
is not opened by the phone yet (T1038): dropped") and opens the water without it, so nothing is dropped silently.

Done (lane L30 + coordinator + review of #80, 2026-11-23; 94c0a117d, 1a5574b00, 06b85846a): `startLink.ts` `linkDay`
(date, past, horizon), `linkPage` (tab, sheet, scroll, age, res) and `linkWhole` split out of `parseStartLink`;
`parseAppLink` reads the same keys with them (a bad value throws on both); `UNSUPPORTED_KEYS` is gone. Phone:
`applyAppLink` puts day, horizon, page start and `whole` on the open request; the angler goes to `setAnglerProfile`; a
past day shows the web's notice line. `/?date=` / `?horizon=` with no water: `{ home }` → the browse map on that day.
`RegsScreen` applies each start it is handed (deps `[start]`); the Shell hands a link's start only to the page of the
water it names (`startForFrame`), and only once `opener.item` has routed (a link to ANOTHER PART of the water on screen).
`view`/`part`/`theme` on a link naming no water are reported. Tests: `appLink.test.ts`, `appLinkOpen.test.ts`,
`Shell.appLink.test.tsx`, `Shell.appLinkPart.test.tsx` (38 mutations, all killed). Web check (Expo web, staging data,
390x844) passed for each key; the first `tab=gear` load after a Metro reload landed short at 937 px of 1139, the same on
main's RegsScreen.

## Work (left, Claude): 0.9.0 device pass (Android)
Open each with another water on screen, then from a cold start; each must land as the same URL on the web:
- `https://staging.canifishthis.ca/?regs=gnis:3062&date=<a future day>&tab=gear` — that day in the day chip, Gear tab.
- `…/?regs=gnis:3062&sheet=pos:pos` — the Possession sheet open; `…&tab=licence&scroll=300` — Licence, 300 px further.
- `…/?regs=gnis:3062&age=u16&res=non_resident&tab=licence` — the licence answer for an under-16 non-resident.
- `…/?regs=gnis:3062&view=conditions&horizon=3` — Conditions on +3; `…&view=map&whole=1` — the Map on the whole water.
- `…/?regs=gnis:3062&date=<a past day>` — today, with "The day this link names has passed, so today is shown."
- `…/?regs=gnis:3062&tab=gear` with the Vedder's Regulations already open and scrolled: Gear again (a new start re-applied).
- `…/?date=<a future day>` (no water) with a water open: the browse map on that day.

Depends: T1034. Size: M. Owner: no.
