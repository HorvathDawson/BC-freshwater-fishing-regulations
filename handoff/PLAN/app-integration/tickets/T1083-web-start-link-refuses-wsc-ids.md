# T1083 — The web start link refuses `wsc:` item ids, yet the app writes `?regs=wsc:…` into its own URL (reload and share fail)

Status: DONE (e65bc413c, lane L24 2026-11-24): one list of id kinds, `@app/data` `ITEM_ID_KINDS`, read by the writer and every reader, web and phone.

Source: the 0.9.1 chat, 2026-11-24, PR #131 lane, web build from main: "start link: regs=wsc:… is not an item id". The app itself writes `?regs=wsc:…` into the URL, so reloading or sharing the page of a `wsc:` water fails. 197 of the parts T1052 touches are `wsc:` waters. Evidence: a Playwright console log of 2026-11-24 20:19Z in the main checkout's git-ignored .playwright-mcp directory. NOT yet checked on production 0.9.0.

CONFIRMED on production 0.9.0, 2026-11-24 15:11 MST (the 0.9.1 chat): opening the site with `?regs=wsc:100-019698-831000`
logs "start link: regs=wsc:100-019698-831000 is not an item id", the URL is rewritten to the bare site and the reader
lands on the bare map. A reload or a shared link of a `wsc:` water loses the water.

## Work
1. Check production 0.9.0 first: open a `wsc:` water on the web, reload, and open its share link on a phone. Write the result here.
2. The start-link reader accepts every id kind the app writes (one list shared by the writer and the reader, so they cannot drift).
3. A test that round-trips every id kind through write then read; a mutation that drops one kind fails it.

## Done (lane L24, 2026-11-24)
- Cause: `parseStartLink` (web: `regs=`, `show=`) and `parseAppLink` / `itemOfWaterPage` (phone) each typed their own
  `^[a-z]+:-?\d+$`; a `wsc:` key is dashed digit groups, so none of them read it. The writer (`routeSearch`, the address
  bar and Share) checked nothing. The phone was broken the same way: a shared `?regs=wsc:…` link and a `wsc:` water's
  `/water/<slug>` page (557 `wsc:` slugs in `app/seo/slug-map.json`; its `cift:item` was refused) both failed. The web's
  `/water/<slug>` page of a `wsc:` water failed too (`startLink.web.ts` puts its `cift:item` into `regs=`).
- Fix: `app/packages/data/src/itemId.ts` `ITEM_ID_KINDS` (gnis, wbk, wsc, blk: the kinds `registry/build.py` `item_id`
  mints), `itemIdKind`, `isItemId`. All four readers ask it; `routeSearch` reports (`reportFailure`) an id it writes that
  the reader would refuse. The kinds were read from the live release r-0aec88ed3edb `core.item` (19,786 ids: gnis 11,606,
  wbk 7,647, wsc 355, blk 178; wsc codes of 2 to 8 groups, wbk and blk both signs), not guessed.
- Test: `app/tools/item-id-links.test.ts`: one real id per kind round-trips through the address writer, a water page's
  query and `?show=`, read by the web start link, the phone app link and the water-page reader (runs everywhere); the
  same trip for every item id of the release under test (skipped by name with no release). Mutants killed: wsc dropped
  from the list; the old regex back in each of the four readers; the writer's report removed.
- Browser (Playwright, Expo web dev server from the branch on the live release r-0aec88ed3edb, 390 px):
  `/?regs=wsc:100-019698-831000` opened Crayfish Creek's Regulations page, a reload kept the address and the page, and
  the console held no start-link refusal. Not checked: a `wsc:` water's `/water/<slug>` page (needs the SEO build) and a
  phone tapping a link (covered by `parseAppLink` / `itemOfWaterPage` in the test only); both ride the next app build.

## Acceptance
a `wsc:` water's page reloads and its link opens on web and phone; the round-trip test covers every id kind.

Severity: HIGH (production: shared and reloaded links to `wsc:` waters are dead). Release: 0.9.1 under "broken" (the
0.9.1 chat takes it). Model: Opus. Size: S. Owner: no.
