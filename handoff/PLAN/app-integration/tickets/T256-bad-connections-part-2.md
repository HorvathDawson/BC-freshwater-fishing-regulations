# T256 — bad connections, part 2: fewer bytes per answer, charts before their envelope, the phone's installer

Status: DONE (5e1888580, 19fd5e25f, 51db891b8, 01d1aac07, 7d426803f, b214318e9, 1056d1de3) — request-walk pin re-cut waits for a release with slices

Source: T254's measurements (owner 2026-10-29, "make the app work well on BAD connections"). T254 made the app order,
survive and say what it is doing on a poor line; what is left is the BYTES an answer costs, which no ordering can hide.
Numbers: `node app/tools/bad-net.mjs` (T254 ticket, "Measured").

## Items
1. **The answer's bytes.** A named water's Regulations need core (1.27 MB gz) + water (2.22 MB) + regs-<region> (0.26 MB)
   + the sql.js wasm (0.33 MB): ≈ 4 MB, ≥ 80 s on Slow 3G and ≥ 5 min on 2G-ish however they are ordered. Split
   `water` / `core` by region (or a per-region "answer" slice), so a tap in Region 2 reads Region 2. Weigh against the
   request walk's pins (T129) and the release total gate.
2. **A start link waits for paint.** `bootRelease` decodes paint (0.68 MB gz) before the Shell draws, so a `?regs=` link
   (an SEO page's "Open in the app") waits ≈ 14 s (Slow 3G) / 55 s (2G) for a file its page does not need first.
   Let a regs-first start draw the page before paint, the map after.
3. **Conditions chart before its envelope.** `gaugeSeries` waits for `climate` (1.4 MB gz) before the chart draws a
   point; on a poor line draw the observations and forecast first and add the normal-range band when it lands (the
   answer store, `askOnce`, must not keep the envelope-less series as the answer).
4. **The phone's first install** (`install.ts`, `toDisk`) is not on `fetchResilient`: no per-size timeouts, retries or
   resume on a 1–2 bar first run. Wire it (expo-file-system's resumable download) and test on a device.
5. **The app bundle** is 1.5 MB gz: 45 s of Slow 3G before the map chrome (and the fonts). Look at code-splitting the
   menu pages / charts out of the first bundle.
6. **The request walk** (`app/tools/request-walk.mjs`) fails at the named tap on an exported build (before T254 too):
   it does not accept the first-run notice / walkthrough the way `bad-net.mjs` does. Make it.

Depends: T254. Size: L. Owner: none.

## Outcome (2026-10-29)
Measured with `app/tools/bad-net.mjs` on local production exports (`expo export`, served gzipped by `--serve`), the
named tap (Gravel Slough, R2), one run each. Item 1's before/after is ONE build (HEAD 37ad39fa0 + item 1, in a clean
worktree) over the same data: BEFORE = r-fa1915a56adb (whole files), AFTER = r-c141c4238c93 = the same release + slices
(`python -m pipeline.deliver.release.slices --from …`, scratch only; not promoted — the data lane's next rebuild carries
them, `build.py` now cuts them).

**1. Answer slices (5e1888580).** Per region, `core-<r>` + `water-<r>` (web only): the whole files' rows for that
region's waters (+ the waters they flow into / go on as / are cut into) and its sections, complete for what they cover
(`pipeline/deliver/release/slices.py`). The app routes each core / water read (`release/focus.ts`): a read whose every
key (ord, item id, sid, place, str, picker) the focus region's slice covers goes there; anything else — search, counts,
a key outside it — to the whole file, so a miss costs bytes, never a wrong answer. Focus = the map's region (`onRegion`).
The whole file wins once it is here (opened, prefetched, or in the release cache). A phone never installs a slice
(`withoutSlices`; regulations stay whole on the phone); the offline total ignores them.

| r-fa1915a56adb tables, gz | whole | R2 slice | largest (R6) |
|---|---|---|---|
| core | 1,378,875 | 197,628 | 327,095 |
| water | 2,221,528 | 777,894 | 858,978 |

| named tap (R2) | release bytes (bad-net) | Slow 3G | 2G-ish |
|---|---|---|---|
| BEFORE (core + water) | 5,057 kB | 51 s | **timeout** (600 s cap: "5.5 MB of 5.5 MB", wasm + 5 files retried) |
| AFTER (core-2 + water-2) | 2,432 kB | **22 s** | **82 s** |

Budget: `first_tap` = core (the idle prefetch's) + `water-{region}` … 4,987,754 → 3,839,954 (gate 5.5 → 4.2 MB); new
`first_tap_low` (nothing ahead: Save-Data / low mode) = `core-{region}` + `water-{region}` … 3,096,919 (gate 3.4 MB);
`first_tap_unnamed` water → `water-{region}` 6,715,000 → 5,552,246 (gate 7.2 → 6.2 MB; the T253 background prefetch reads
it as WHOLE files). Release total for a phone unchanged (slices excluded); +9.0 MB on R2. Proof: pipeline
`test_release_slices.py` (closure, str cut, bitmap, refusals; live: rows equal the whole's), app
`conformance/src/regs/slices.release.test.ts` (on a sliced release: R2 / R6 / R7B waters' page inputs, sheet and group
read with the whole core / water REFUSED deep-equal the whole-file reads; unnamed sids say "no water" from the slice; an
R8 water with focus R2 falls back to the whole files), `focus.test.ts`, `install.regs.test.ts` (no slice installed).
19fd5e25f: the sql.js wasm's deadline is sized by its raw 660 kB — an uncompressed host cut every 2G try at the gzip size.

**2. A start link draws before paint (01d1aac07, fork).** `bootRelease({ paintFirst: false })` for a regs-first start; the
Shell holds the map's status until the page is drawn. Slow 3G link: water title 45.9 → 26.2 s, Regulations 154 → 143 s,
release bytes before the page 4,546 → 3,867 kB. (A link has no map region yet, so it reads the whole core / water: the
link's region is not known before core — left.)
**3. Conditions chart before its envelope (7d426803f, fork).** `draftSeriesOf`: observations + forecast drawn first, the
normal-range band when `climate` lands; the draft is asked under its own key, never kept as the series (`askOnce`). Slow
3G: chart 229 → 199 s, band 232 s.
**4. The phone's first install (51db891b8).** The data files now come through `fetchVerifiedResilient` (timeouts by size,
retries with backoff, gzip Range resume, verify, one whole refetch); the big files were already on the native resumable
transport (T262/T272). Emulator: see the synopsis-in-install run (the install scenario exercises it).
**5. The bundle (b214318e9, fork).** pdf-lib out of the first bundle (loaded with the rule book): 1,147,465 → 952,084 B gz;
Slow 3G map chrome 46 → 42 s, 2G 181 → 165 s. maplibre-gl (~290 kB gz) is the next split (needs a lazy map wrapper) —
left. Menu pages (~20 kB) / charts (<10 kB) not worth a chunk.
**6. Request walk (1056d1de3, fork).** Already passed the named tap at HEAD (efd83d987); bad-net now seeds the first-run
notice / walkthrough / in-season notice as the walk does, and gained a `chart` step.

## Left
- Re-cut `app/tools/request-walk.pin.json` on the first live release with slices (`node app/tools/request-walk.mjs --url …
  --pin`); until then `release-budget.test.ts` reads an unsliced pin with each slice as its whole file (`PIN_SLICED`). A
  walk over r-c141c4238c93 showed the named tap fetching `water-2`, the Save-Data tap `core-2` + `water-2`.
- T286's uncommitted web prewarm (`useShellNav` `prewarmRegs`, 1.5 s after boot) fetches water before the map reports a
  region — the whole 2.2 MB, Save-Data / low mode included; told to that lane (should wait for the first region report and
  skip low mode; with a focus it reads `water-<r>`).
- A start link's region (a slug carrying it) would let a link read slices too.

