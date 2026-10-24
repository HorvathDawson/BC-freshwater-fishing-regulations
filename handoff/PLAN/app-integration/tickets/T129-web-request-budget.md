# T129 — Web request budget: gate what a web visitor fetches, not only the release total

Status: DONE (3d0cc66a, 4183a2f4, 950dfaeb) — owner 2026-10-22: "as long as it isn't too much of a raise. we can be smart
on the webapp on how things are requested." (The release total gate went 14 → 15 MB, Q55.) Proposals below for the owner.

## Why
The release total is what a PHONE holds offline. The web app fetches lazily (MINIMAL-PACKAGING): paint first, then
core/water, a region's regs file only on a tap in that region, conditions/near on demand. Nothing gates those web
paths, so a growing file (regs-3 swung +130 KB on a dictionary change today) can quietly slow first paint or a tap.

## Work
- Measure, on the live release, the bytes a web visitor fetches for: first paint (map coloured), first search, first
  tap on a water (per region), opening Conditions, the Change horizon, a spot. Record each in a table.
- Gates (named constants, in the release build and an app test): first-paint bytes, per-tap bytes (largest region),
  Conditions open. Set them from the measured numbers with headroom; fail the build when crossed.
- Look for easy wins: is anything fetched before it is needed (e.g. `water` 2.7 MB before the first tap? `access` on
  the Map tab?), can a region file be split (regs-3 is the largest), HTTP caching headers on R2 (immutable, sha-named
  files), prefetch the likely region after first paint.
- A Playwright/network test on the web build records the request list for the walk above and pins it.

## Acceptance
The table, the gates in the build and the app, the pinned request walk; any lazy-load fixes with before/after bytes.

## Outcome

DONE 2026-10-24 (3d0cc66a, 4183a2f4, 950dfaeb). Measured on r-bb4331929428 served by the owner's :39217, with the app
on a dev server of this worktree (Expo web, 390 × 844), each walk a cold Chrome profile (no HTTP cache, no Cache
Storage), recorded over CDP (`app/tools/request-walk.mjs`).

### What a first web visit fetches

Release bytes are the manifest's gzip sizes (what R2 sends); "+" is what the moment adds to the walk so far. The
sql.js engine (sql-wasm, 658.7 kB) rides with the first SQLite file opened. The feed's `index.json` is 352.6 kB as the
dev server sends it (raw; 46.7 kB gzip). Not counted: tiles (≈ 0.46 MB at first paint on the default camera, about
1.1 MB more when search frames a water), glyphs/sprites, the app bundle and fonts (a dev build, not production's), the
fish pictures a Regulations page shows (≈ 0.48 MB of webp).

| moment | files it adds | release bytes | + wasm / feed |
|---|---|---:|---|
| first paint (map coloured) | manifest, paint, mask | 1,024,306 | + feed index |
| first search ("Thompson") | core, near | 3,083,131 | + wasm |
| search: show a water on the map | water (for its `part_geo` box) | 2,736,121 | |
| search: open it (Thompson, R3) | regs-3 | 337,761 | |
| first map tap, named water, cold (Gravel Slough, R2) | core, water, access, regs-2 | 4,308,592 | + wasm |
| first map tap, unnamed stream, cold (R2) | core, water, access, regs-2, regs-sid, **conditions** | 8,403,320 | + wasm |
| open Conditions (from the map, cold) | core, flow-area, conditions | 4,156,368 | + wasm |
| the Change horizon | nothing | 0 | |
| the Spots tab; open a spot | nothing from the release | 0 | open-meteo, 2 kB |

First tap on a water, per region (named, from the map, cold: core + water + access + the region file; a start link
`?regs=` fetches the same minus access):

| region | regs-<r> gz | tap total | unnamed tap (+ regs-sid + conditions) |
|---|---:|---:|---:|
| 1 | 316,089 | 4,371,173 | 8,465,901 |
| 2 | 253,508 | 4,308,592 | 8,403,320 |
| 3 | 337,761 | 4,392,845 | 8,487,573 |
| 4 | 348,252 | 4,403,336 | 8,498,064 |
| 5 | 323,178 | 4,378,262 | 8,472,990 |
| 6 (largest) | 381,047 | 4,436,131 | 8,530,859 |
| 7a | 284,994 | 4,340,078 | 8,434,806 |
| 7b | 153,511 | 4,208,595 | 8,303,323 |
| 8 | 125,438 | 4,180,522 | 8,275,250 |

### Gates (`pipeline/deliver/release/contract.py` `WEB_BUDGET`, emitted into `pack.generated.ts`)

| gate | files | measured | gate |
|---|---|---:|---:|
| `first_paint` | manifest, paint, mask | 1,024,306 | 1,150,000 |
| `first_tap` (largest region) | core, water, access, regs-{region} | 4,436,131 | 5,000,000 |
| `first_tap_unnamed` (largest region) | + regs-sid, conditions | 8,530,859 | 9,500,000 |
| `conditions_open` | core, flow-area, conditions | 4,156,368 | 4,700,000 |

- The release step fails over one: `build.gate_problems` → `web_moments` (manifest-only pricing; `regs-{region}` is the
  largest region file). `test_release.py::test_the_web_budget_prices_each_moment_and_fails_a_crossed_gate` (no data)
  pins the pricing and, by mutation, that one grown region fails both tap gates and nothing else.
- The app: `app/tools/release-budget.test.ts` — the pinned walk fetches exactly each gate's files at its moment (and
  only manifest/paint/mask/feed index before the first question, nothing on the Change horizon or a spot); the live
  release is under every gate (a mutation grows the smallest region past the headroom and it becomes the priced one
  and fails); a cold open of a water in each of the nine regions opens paint, core, water and that region's file only.
- No release writer changed (build()/ship() untouched; the gate reads the manifest after the files are written), so no
  side deliver run was made: the new code cannot move a byte.

### The pinned walk

`node app/tools/request-walk.mjs --url http://localhost:<port>` (Chrome + the running app; `--pin` rewrites the pin):
23 moments across visit/search, the two map taps, Conditions + the Change horizon, a seeded spot and a start link per
region; exit 1 when any moment fetches other files than pinned (a duplicate request counts). The map taps check the
header's name, so a moved camera or retiled map fails by name.

### Fix

- The manifest was fetched TWICE per web visit: the boot's read, then the sync icon's first check an instant later.
  `useSync` takes `named` (App passes the boot manifest's release on the web only; a phone's boot opened its held set,
  so its first check still asks). First paint on the wire 1,031.5 → 1,025.3 kB, one request fewer (3d0cc66a).

### Proposals (not done: each changes the release format or what the web holds)

1. **An unnamed tap fetches all of `conditions` (2.88 MB) for one line.** The header's "Tributary of …" walks
   `section_down`. Split `section_down` (628 kB gz) into its own file, or ship a per-section "first named water down"
   in regs-sid: the unnamed tap goes 8.53 → ≈ 6.3 MB (or ≈ 5.7). Largest single win.
2. **"Show on the map" fetches all of `water` (2.74 MB) for a box.** It needs `part_geo` (578 kB gz of water). A `geo`
   file (part_geo + part_mark) or the item box in core: show = 0.58 MB; the regs page then needs the remaining 2.16 MB.
3. **The first search fetches `near` (1.99 MB) for each result's "near Kamloops" line.** Ship each water's nearest town
   (+ distance) in core (≈ 20k rows, tens of kB); `near` then waits for a town search. First search 3.08 → ≈ 1.15 MB.
4. **Conditions opens `core` (1.09 MB)** for the lakes' gauges (`ITEMS_FOR_SECTIONS`); a `section → lake station` table
   in conditions would drop it. And `gauge_clim` is half of conditions (1.44 MB of 2.88): keyed per station already,
   a file of its own would let the map colour before the climatology lands — measure first.
5. **The feed index: 352.6 kB raw, 46.7 kB gzip.** The feed's R2 sync is not written yet; it must send
   `Content-Encoding: gzip` and a short `Cache-Control` (it changes every 30 min) — −306 kB at every first paint. The
   release's own headers are already right (publish.py: sha-named files `immutable`, the manifest `no-cache`); the dev
   server (`serve-tiles.mjs`) sends no Cache-Control, so a dev session refetches sprites on each map remount (20 kB
   each) where R2 would not.
6. **Splitting regs-3 / the largest region buys little now**: regions are 125–381 kB gzip (regs-6 is the largest
   today, not regs-3) inside a 4.2–4.4 MB tap that is 87 % core + water. Not worth a format change; the gates watch it.
7. **Prefetch after first paint** (core + wasm, 1.75 MB, during idle) would make the first search, tap and Conditions
   wait only for their own files, but costs a visitor who only looks at the map 1.75 MB. An owner call; if yes, the
   first-paint pin gains a separate "idle prefetch" moment so the gate still measures paint alone.
