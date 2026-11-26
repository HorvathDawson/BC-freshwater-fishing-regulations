# T1148 — Satellite tiles are very likely kept on the phone; the privacy page says "not stored"

Status: PARTIAL — the words are reworded (PR of 2026-11-26); left (Claude): the look inside the cache on an Android phone and an iPhone, and T1122's store-form table

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1148 | Satellite tiles are very likely kept on the phone; the privacy page says "not stored" | PARTIAL — the words are reworded (PR of 2026-11-26); left (Claude): the look inside the cache on an Android phone and an iPhone, and T1122's store-form table | | the device check (step 1: what MapLibre's cache holds on an Android phone and an iPhone, how long, how big) when the Mac and phones are free; T1122's table (planning chat) |

Source: T1108's second scoping pass, 2026-11-25 (read-only). MapLibre Native keeps an ambient tile cache on disk, 50 MB
by default (upstream `include/mln/util/constants.hpp:53`), and the app never sets it (no call in `app/packages`
or `app/apps`). So Esri satellite tiles very likely go to disk on phones today, as far as Esri's cache headers
allow, while the privacy page says "not stored" (`app/tools/seo/html.ts:255`) and the style comment says "Never
stored" (`app/packages/map/style/layers.source.json:19`). NOT verified on a device.

Owner, 2026-11-25 (planning chat dialogs): to "turn the cache off (recommended) / keep the cache, reword the page":
**"Keep cache, reword the page"**. To "also say the imagery provider sees your IP address and the map area you
view (recommended) / only the cache rewording": **"Only the cache rewording"**.

Work: (1) verify on an Android phone and an iPhone: open satellite, then inspect the engine's cache database
(what is stored, for how long, how big); web: say what the browser cache does. (2) Reword the privacy page and the
style comment to what is true (proposal, his to clear: "Satellite pictures you have looked at are kept on your
device for a short time so they load faster; they are not sent anywhere else."). (3) Keep T1122's store-form
table in step. (4) A test pins the page's sentence to the cache setting, so a later change of one fails without
the other. Esri's terms do not say whether a device cache is allowed (see T1108); note it, no action.

Severity: low to medium (a privacy page that is not exactly true). Model: Sonnet (Opus for the device check).
Size: S. Owner: clears the sentence.

## Built 2026-11-26 (map engine chat; the words only, nothing checked on a phone)
The privacy page, More > Sources ("Shown only while online. Pictures you have viewed may be kept on this device so
they load faster.") and the licences table ("The Satellite ground, online only.") read core's `IMAGERY_KEPT_*`
(`app/packages/core/src/imagery.ts`); the style comment is reworded. `app/tools/imagery-cache.test.ts` holds the
words whole and fails when any source of ours names the engine's cache controls (step 4). Also in the PR, asked
by the planning chat: the privacy page's script list names the /gauges search and the gauge page's chart script.
Owed: step 1 on real phones (the sentence says "may be kept" until then); step 3, T1122's table.

## Owner

- [x] 2026-11-25 — Satellite tile cache → "Keep cache, reword the page" and "Only the cache rewording" (2026-11-25).
- [x] 2026-11-25 — Clear the reworded privacy sentence about the satellite tile cache when it is drafted. → "Shortest": "Imagery is shown only while online. Satellite pictures you have looked at may be kept on your device so they load faster; they are not sent anywhere else."; the two app lines that said "never stored" → "Fix both now (Recommended)" (2026-11-26, map engine chat dialog).
