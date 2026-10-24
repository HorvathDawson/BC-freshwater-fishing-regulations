# T167 — The More tab: simpler details, references apart, data freshness on the main tab, a real desktop layout

Status: DONE — 9770cf9b (lane-T167; merge into redesign/stream-sections)

Source: owner 2026-10-23: "details in the more tab could be simplified a lot too. right now it is hard to read. separate
out references to a separate thing. then this stuff [the data sources list: Synopsis, In-season changes, DFO salmon,
Freshwater Atlas, Map tiles, Basemap, HYDAT, Live gauge readings, River forecasts, Weather, Satellite imagery — each with
its source, date and Up to date / Not read / Likely out of date] could mostly be formatted into the main tab without
adding much bulk. seems silly to have it hidden. also that entire page doesn't feel very clean on desktop. mobile is good
but desktop it feels a little lacking".

Code: core/src/menu.ts (the words and the source list), ui-native/src/menu/MoreScreen.tsx, Fresh.tsx, InfoPages.tsx,
MenuHost.tsx.

## Work
1. **Details simplified:** cut the details to what a reader needs, in short plain sentences; one idea per line; no
   repeated qualifiers.
2. **References apart:** citations, credits and licence lines (the Synopsis, OGL-BC, OSM/Protomaps, Open-Meteo CC BY 4.0,
   ECCC, BC RFC, PSF where run timing ships) move to their own "Sources and credits" page, linked once.
3. **Data freshness on the main tab, compact:** one row per source, name + date/age + a status dot (Up to date / Likely
   out of date / Not read / Live), in a tight list or a two-column grid — no paragraph per source; the long description
   moves to the Sources page. The "Not read by this app" in-season changes stays prominent (a warning row with the link).
   "Map tiles" shows a date, not a hash (the hash in the Sources page).
4. **Desktop:** a proper desktop layout — a centred column with a max width, or two columns (freshness beside the
   settings/links) — consistent with the app's desktop shell; mobile stays as is.
5. Shots before/after: phone and desktop, light and dark. Render tests for the new list; words whole-value in core.
Ships: app. Size: S–M.

## Owner note (2026-10-23)
The BC River Forecast Centre gave the owner permission to use its data — credit it on the Sources page; HYDAT/ECCC
under the Open Government Licence – Canada (decisions.md "OWNER 2026-10-23 — data permissions").

## As built (lane-T167, 2026-10-23)

**Before → after, in words.** Before: the More tab's Data block listed 8 of the 11 sources under its own short names
("Regulations", "River flows", "Map"), with a "Details ›" footer opening *Data sources & dates* — 11 rows, each a
long name, a description sentence, a date line and a badge, then a footnote and the credits, full window width on
desktop. HYDAT, DFO and the atlas were only on that page, so the summary had to say "· see Details". On desktop the
tab was one 640 px column with empty wash either side.

After:
- **More tab, Data block (all 11 sources, compact).** The summary badge on the kicker line; the in-season changes as a
  ruled warning card ("In-season changes are not read" / "The Province can change a regulation during the season.
  Check its notices before you fish." / link "Province's in-season changes ↗" → the Province's fishing-regulations
  page `#In-Season-changes`, URL verified 2026-10-23); then ten one-line rows: dot · name · date or age. The state is
  drawn in words only where it asks the reader to look ("Live gauge readings — 4h ago · Likely out of date"); aloud
  every row says name, date and state. Names are the owner's: Synopsis, DFO salmon, Freshwater Atlas, Map tiles,
  Basemap, HYDAT, Live gauge readings, River forecasts, Weather, Satellite imagery. Every source counts in the summary
  (no "see Details" any more). Phone bulk: about +60 px against before (two more rows, the warning card; the Details
  footer is gone).
- **Map tiles** shows its state ("Matches the data" / "Out of step with the data" / "Not proven"), never the hash; the
  hash is on Sources and credits ("Version d768…"). It does NOT show a date: the release records no tile build date
  (see owner question 1).
- **Sources and credits** (renamed from "Data sources & dates"; a row among Glossary / About / Report, linked once):
  references only — each source's name, who publishes it and its licence, the edition title (Synopsis) and the tile
  build (Map tiles), then the fish-picture and icon credits. No dates, no badges. Credit lines: FWA "Contains
  information licensed under the Open Government Licence – British Columbia"; HYDAT and live readings "Environment and
  Climate Change Canada … under the Open Government Licence – Canada"; "BC River Forecast Centre. The CLEVER, COFFEE and
  ELF river forecasts, used with permission."; "© OpenStreetMap contributors. © Protomaps."; "Weather data by
  Open-Meteo.com, CC BY 4.0."; Esri's required words (one constant, `IMAGERY_ATTRIBUTION`, now under `IMAGERY_CREDIT`).
- **Desktop / tablet (≥ 900 px):** the tab is two columns in a centred 1040 px frame (`MORE_WIDE`): Data on the left
  (1.15 share), Display · pages · Contact on the right; every block a boxed card on the wash; a hairline between data
  rows (long name-to-date travel); the head's words on the columns' edge. Menu pages (Sources, About, Report,
  Glossary) read in a centred 680 px column. **Phone unchanged** in arrangement (one edge-to-edge column).

Code: `core/src/menu.ts` (one name table `MENU_WORDS.sources.names`, `from` credits, `INSEASON_URL`, `sourcesVM` →
references only, `SourceCreditVM`), `core/src/more.ts` (`dataVM` rows for every source but in-season, `DATA_ROWS`,
the in-season warning VM, `shown`/`label` composed whole; the 8-line `DATA_LINES` and the second "when" function are
gone), `core/src/imagery.ts` (`IMAGERY_ATTRIBUTION`), `ui-native/src/menu/MoreScreen.tsx`, `InfoPages.tsx`,
`MenuHost.tsx` (no longer takes feed/pair/notice props — nothing on its pages is dated), `MenuPage.tsx` (wide column),
`theme.ts` (`MORE_WIDE`), `Shell.tsx`.

Tests: `core/src/more.test.ts` (every row whole as drawn and aloud, the warning, the tiles-no-hash rule, synopsis /
HYDAT / forecasts / lapse / newer release, summary = stale dots drawn), `core/src/menu.test.ts` (the Sources page
whole; no date or freshness word on it), `ui-native/src/menu/Menu.test.tsx` (render: the new list drawn and aloud,
dot shapes, the warning's link, phone one column vs 1000/1280/1600 two columns, Sources page whole with no dots/dates,
the page's 680 px column), `tools/one-menu.test.ts` "one list of the data's sources" (one `SOURCE_ORDER`, each name
written once in core/menu.ts, `sourceFreshness(` in core only, `dataVM(` only in more.ts, `FreshBadge` only on the
More tab, `FreshDot` only there and on the Regulations lapse note; mutation pin).

**Shots** (`handoff/PLAN/app-integration/T167-shots/`, own Expo port 19731, live data server read-only): `before-*`
(phone / desktop 1280 / 1600, light + dark; `before-details-*` the old Details page), `after-*` (same set, plus
`after-phone-full-*` the whole phone scroll and `after-sources-*` the new page at every size and theme).

**Checks:** `REGS_STORE_REQUIRED=1 pnpm -C app check` via `scripts/heavy.sh` on lane-T167 rebased onto b5f0eef3 (the
rows-33 fixture re-cut): every gate green, 348 test files passed / 5 skipped, 8087 tests passed. (Before the rebase,
on fdb97a43, the only failures were the regs-fixture pairing — "rows section version 32, this app reads 33" — and the
water screens that need that store; none in the menu.)

## Adversarial review (against the owner's words)
- *"hard to read"*: the old Details page's 4-line-per-source paragraphs are gone; the tab's rows are one line, and the
  state word appears only on a stale row, so a fully fresh list is just names and dates. Credits read as one line each.
- *"mostly formatted into the main tab without adding much bulk"*: all 11 on the tab; phone +~60 px. The in-season card
  is the one bulky item — kept deliberately (ticket: "stays prominent").
- *"desktop feels lacking"*: checked at 1280 and 1600 (shots): two balanced columns, kickers on one baseline, boxed
  cards, no full-width stretch. Weakness left: at 1600 the frame is 1040 px, so ~280 px of wash either side — by design
  (the rows would be unreadably long wider). On web desktop with a classic scrollbar a menu page's head sits ~8 px right
  of its body (the scrollbar narrows the body's centring box); cosmetic, not fixed.
- Colour-only risk: fresh vs stale dots are both filled (teal vs attention); the stale row's words and bold ink carry the
  state, so it is not hue-only.
- **Mutations that bit:** drawing `shown` without the stale word (6 fail); a second, shorter row list (9 fail); the tiles
  row showing the hash (5 fail); two columns only on desktop, not tablet (1 fail); a `FreshBadge` imported into
  InfoPages (one-menu: 1 fail).

## Owner questions (defaults applied)
1. **Map tiles date.** The release states no tile build date (manifest `tiles.current` has id/sha/bytes only; the dev
   sidecar neither). Default: the row shows the pair state, the hash moves to Sources. To show a date, the pipeline
   would stamp e.g. `sources.tiles.date` in the manifest — a new ticket if wanted.
2. **Sources link placement.** Default: "Sources and credits" is a row with Glossary / About / Report (linked once);
   the Data block itself no longer opens a page. Alternative: a small "Sources ›" link under the data rows.
3. **Synopsis licence / DFO terms.** Default: credited by publisher only (no licence named) — say if the Province's or
   DFO's terms need a line.
