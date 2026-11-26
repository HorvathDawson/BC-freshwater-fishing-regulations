# T1148 — Satellite tiles are very likely kept on the phone; the privacy page says "not stored"

Status: OPEN — the hardening release; verify first, then reword (owner: keep the cache)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1148 | Satellite tiles are very likely kept on the phone; the privacy page says "not stored" | OPEN — the hardening release; verify first, then reword (owner: keep the cache) | | owner 2026-11-25: "Keep cache, reword the page"; "Only the cache rewording" |

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
