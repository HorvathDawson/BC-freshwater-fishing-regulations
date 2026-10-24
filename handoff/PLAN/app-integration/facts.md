# facts.md — the code as it is (verified 2026-10-20 @ 04765ab5 (prototype v33)), and where APP-INTEGRATION.md was wrong

Every ticket names files and signatures from this page. Paths are relative to `app/` unless they start with `data/`,
`pipeline/`, `handoff/`.

## A. Corrections to `~/.claude/projects/…/handoff/PLAN/APP-INTEGRATION.md`

| # | plan said | the code says | consequence |
|---|---|---|---|
| 1 | `BaitPick.excepted_from` missing from the generated TS | present (`answers.generated.ts:7`, `ExceptedFrom` :39); `python -m pipeline.deliver.answers.model --check` exits 0 | drop the gap |
| 2 | `store sample --items` tool | no such subcommand: store CLI = `build \| check \| measure`; answers CLI = `build \| tap \| page-view` | fixture tool is new work in app/tools (T08) |
| 3 | regs.sqlite 13.3 MB, json 35.6 MB | 14,237,696 B; answers json 38,032,637 B | — |
| 4 | blobs `0x01`/`0x02` | EVERY blob in the built file is `0x02` (dict-deflate, `meta.blob_codec='zdict'`, 23 `zdict` rows); reader must still accept `0x01` and raw JSON (`store/common.py:68-91`) | fflate with `dictionary` is required (T02) |
| 5 | `licence_frame = [holds, documents, under_16]` | blob is integer refs `[h, d, u?]` → `licence_hold`, `licence_doc` (list of `licence_answer` ids, 60 profiles), `licence_u16` | per-family codec (T04) |
| 6 | `frame<T>(family, id)` generic | `rows_frame` = `[spp, {S:[h,w]}, [rows_row ids], steelhead_line]`; `ladder_frame` = `[common, [[fix, v]\|[fix, vNone, vHatch, vWild]]]`; display/gear blobs are the frame | per-family codec (T04) |
| 7 | `parts(item)` | no secondary index; `item.item_id` is a `str` ref → build `Map<itemId, item_ord>` once (19,641 items, 22,863 strings) | T04 |
| 8 | versions in `answers.generated.ts` | not emitted; layout versions in `meta.layout.versions` = `{ladder:1, rows:26, gear:12, licence:6, display:16}`; glossary 5 in `static answers.glossary.version` | hard-code `KNOWN_VERSIONS` in core and refuse (T03) |
| 9 | `DateSheet` + "Which day?" | **no DateSheet** (deleted in 30c3e4d1; only stale `ui-native/dist/DateSheet.d.ts`). `MapDay.tsx` is a « ‹ [label] › » stepper | header date pill opens a new `DaySheet` or reuses `MapDay` (D7) |
| 10 | `mapDay: Date` | `Shell.tsx:97 useState<Date \| null>(null)`; null = today; only the Map tab's legend renders `MapDay` (Shell:663) | `useRegulations` takes `mapDay ?? new Date()` (T10) |
| 11 | `TYPE.answer`, `TYPE.lead` | do not exist; `type.ts` TYPE = title, name, screen, section, body, bodyStrong, small, micro, pill, figure, figureBig, tab | add (T23) |
| 12 | `SPACE` in theme | none | add (T23) |
| 13 | fish manifest: one `code:null` | 8 null codes (chum/coho/pink/sockeye × ocean/spawning, WDFW); schema `{file, code, caption, phase, stand_in_for, credit, type, source, width:800, height:400}` | fish.generated skips null codes (T22) |
| 14 | status index "beside the bundle" | beside the ATLAS: `statusIndexUrl(atlasUrl)` (`ui/src/hooks.ts:507`) | the regs store gets its own URL constant (T01) |
| 15 | `/bundle.sqlite` = production | `/bundle.sqlite` serves the FIXTURE `packages/data/dev/bundle.sqlite`; production is `/province.sqlite`; App.tsx `BUNDLE = "http://localhost:39217/province.sqlite"` (:57) | add `/regs.sqlite` → `data/generated/bundle/regs.sqlite` (T01) |
| 16 | `useRegsStore(bundleDb: Db)` | `Db` (`data/src/bundle/db.ts`) is not exported from any package entry; `loadBundle` only via `@app/data/bundle/open`; neither has a vitest alias | export + alias (T03) |
| 17 | Shell has the Db | App.tsx holds `LoadedBundle`, passes only `source: WaterSource` to `Shell` | regs store opened by a hook from a URL prop (T10) |
| 18 | `BundleInfo` pairs `rule_ids_sha256` | `BundleInfo = {version, validUntil, sectionHandles, reachDigest}` (source.ts:75–77); province bundle meta HAS `rule_ids_sha256 = 16be720be08b0fca`; the fixture bundle has none and `reach_digest = ''` | add `ruleIdsSha256: string \| null` (T03) |
| 19 | `pnpm check` = boundaries→…→test | root package.json: `boundaries → platform → style → colours → spinner → fixture → gauge-policy → calendar → deps → typecheck → test`; `answers.model --check` is NOT in it (only `.github/workflows/pipeline-ci.yml:69`) | add `answers` check (T05) |
| 20 | fflate a dep | transitive only (`fflate@0.8.3` via `pmtiles@4.5.0`, hoisted); `inflateSync(data, {dictionary})` supported | declare in `packages/data/package.json` + `deps.md` + `layers.json` data mayImport (T02) |
| 21 | `regulations.ts` header stale about verdicts | staler: lines 7–9 say `status.ts` and the date pill were "deleted outright" — both exist | rewrite (T00) |
| 22 | conformance runs the node driver | only `conformance/src/fixture.test.ts` (fixture source); bundle source tested in `packages/data/src/bundle/source.test.ts` | regs conformance = a new runner (T09) |
| 23 | page v36 reads `static display.strip` | page v36 computes strips client-side; the PROTOTYPE's runs are snap `runsOf` = per-day `display_frame.strip`, legend words = `SEC.display.strip` labels | the app follows the prototype (T13) |
| 24 | prototype line refs (1081–1279, 1577–1603, 1446–1532) | at 04765ab5 (v33): `buildKeep` 1027–1326, `shortOf` 1628, `pinUpdate` 1637, `buildGear` 1476–1572, `licence` 1381 (v32 @ 18bc2a11: 1020, 1588, 1597, 1454, 1359) | read refs from `port/golden/inventory.json` |
| 25 | — | **prototype bug**: `pinUpdate` throws `Cannot read properties of null (reading 'headline')` on 25 of 134 states (a ledger article whose row has no `group`) | app `pinChain` must skip; excluded from pin parity |

## B. App seams (exact)

- **Layers** `layers.json` (`tools/check-boundaries.mjs`): core imports nothing (bans react, react-native, expo, node:fs…);
  data → `@app/core` (bans react*); `data/src/bundle/drivers` → core, node:sqlite, node:fs/url/path, expo-sqlite, sql.js;
  ui → core, data, react; ui-native → core, data, ui, map, react, react-native, react-native-svg; apps/mobile → all +
  expo, expo-font, @expo-google-fonts/*. Only `apps/mobile` registers assets. testOnlyImports: vitest, @testing-library/react.
- **Gates** `tools/check-colours.mjs` (no colour literals under packages/ apps/ tools/ conformance/; ALLOW = tokens.json,
  themes/{light,dark,cvd}.json, style.json, style.meta.json; exports `findColours(src, path)`, `scan()`);
  `tools/check-platform.mjs` (SHARED = ui, ui-native, core, map; `.native`/`.web` pairs complete; BANNED_IN_SHARED:
  `Alert.alert`, `PermissionsAndroid`, `document.`, `window.localStorage`, `navigator.geolocation`, `accessibilityState` —
  use `aria-checked`/`aria-selected`/`aria-disabled`); `tools/check-deps.mjs` (every dependency has a
  "- `name` — why, cost" line in `deps.md`); `tools/check-style.mjs` (style.sha256 — run `pnpm style:build` after a token change).
- **vitest** `vitest.config.ts`: project `logic` (node; `packages/{core,data,ui,map}/**/*.test.ts`, `conformance/**/*.test.ts`,
  `tools/**/*.test.ts`), project `render` (jsdom, `tools/render-setup.ts`; `packages/ui-native/**/*.test.{ts,tsx}`,
  `packages/ui/**/*.render.test.tsx`). Aliases: `react-native`→`react-native-web`, `react-native-svg`→web build,
  `@app/core`, `@app/data/fixture`, `@app/data/spots`, `@app/data/bundle/node`, `@app/data/bundle`, `@app/data`,
  `@app/ui`, `@app/map`, `@app/ui-native`. **A new subpath needs an alias line** (e.g. `@app/data/regs`, `@app/data/regs/node`).
- **Render tests** (`packages/ui-native/RENDER-TESTS.md`): `@testing-library/react` through react-native-web in jsdom;
  imitate `packages/ui-native/src/WaterScreen.test.tsx` (`render`, `screen.getByRole("heading",{name})`, `afterEach(cleanup)`,
  `LIGHT` from `./theme`). No native renderer.
- **core** `src/statusIndex.ts`: `dayOfYear(on: Date | {month, day}): number` (:44, leap calendar, Feb 29 = 60, Mar 1 = 61);
  `decodeStatusIndex(bytes, expectHandles, expectReach)` refuses with `StatusIndexError("… — refused")`; private `utf8()`
  because Hermes has no TextDecoder (:66). Parity-fixture precedent: `src/statusIndex.fixture.ts` generated by
  `pipeline/tests/test_status_index.py` (`UPDATE_APP_FIXTURE=1`). Generated-file precedent: `calendar.generated.ts`
  (`python -m pipeline.tools.emit.calendar`, `--check` in `pnpm check`). `src/index.ts` does not export `answers.generated`.
- **core** `src/regulations.ts` exports `RegulationsAvailability`, `REGULATIONS`, `RegulationRecord`, `WaterRegulations`,
  `regulationsFor(_item): null` — zero importers besides `index.ts`.
- **data** `src/bundle/db.ts`: `type Cell = string | number | null | Uint8Array | undefined; interface Row {[c: string]: Cell};
  interface Db { all(sql, ...params): Promise<Row[]>; get(sql, ...params): Promise<Row | undefined>; close?(): void };
  str(v)`, `num(v)`. `drivers/engine.d.ts`: `interface LoadedBundle extends Db { bytes: number }`,
  `setSqlWasmUrl(url)`, `loadBundle(url, signal?, onStage?): Promise<LoadedBundle>` (generic SQLite; web = sql.js whole
  file; native = expo-sqlite `deserializeDatabaseAsync`, 15 s timeout). `drivers/node.ts`: `DEV_BUNDLE`, `hasDevBundle()`,
  `openBundle(path): Db` (node:sqlite `DatabaseSync` readOnly). package.json `exports`: `.`, `./fixture`, `./bundle`,
  `./bundle/node`, `./bundle/open`, `./spots`.
- **ui** `src/async.ts`: `type Async<T> = {state:"loading"…}|{state:"ready";value:T}|{state:"failed";error}`;
  `useAsync<T>(run, key: string, enabled = true)`. `src/hooks.ts`: `loadStatusIndex(url, handles, fetcher = fetch, reach = null)`
  (:521, module cache keyed `${url}#${handles}#${reach}`), `useStatusIndex(source, atlasUrl)` (:545), `useWater(source, item)`.
- **ui-native** `Shell.tsx:59 Shell({source, palette, theme, themeName, onTheme, tiles, spots, onSaveSpot, weather,
  onRefreshSpots, onDeleteSpot, refreshing, feed, attribution})`; state `tab` (:95), `mapDay: Date|null` (:97), `item` (:98),
  `picked` (:105) + `usePickFocus`; water mount (:423) `<WaterScreen source item palette onConditions onBack />`; bottom
  `TabBar` stays under the water screen (:675). `WaterScreen.tsx:32 WaterScreen({source, item, palette, onBack, onConditions})`
  — ScrollView: `FaceBar face="regulations"`, title (TYPE.title), private `Section title="Regulations"`,
  `RegulationsPlaceholder`, `CrossLink "and the conditions"`; it also exports a duplicate `type Face` (:30).
  `Faces.tsx FaceBar({palette, face, onFace, onBack, backLabel})`. `MapDay.tsx MapDay({palette, day: Date|null, onDay})`,
  `dayLabel(d)`, `shiftDay(d, n, months)` (not exported from the index). `Sheet.tsx Sheet({open, onClose, title, palette,
  children})` (Modal, grab bar, "Done"; **no ScrollView inside**), `Choice<T>({palette, options:{k,t}[], value, onChange,
  label})` (radiogroup, role radio, aria-checked). `TabBar({active, onChange, palette})`, `TABS` Map·Search·Conditions·Spots.
  `Credits({palette, lines, label="Sources"})`.
- **theme** `ui-native/src/theme.ts`: `Palette` = page, card, wash, tint, line, line2, ink, sub, faint, closed, restricted,
  open, quiet, waterStatus, accent, onAccent, live, place, placeBand, stock[5], donor[4], survey[2], water[2], imagery[3],
  scrim, lift{boxShadow, elevation}, r: Radii; `RADIUS` all 0; `paletteFor(theme)`, `LIGHT`, `DARK`, `CVD`, `THEMES`.
  `type.ts`: `FACE` = display Bricolage 700, displayHeavy Bricolage 800, text/textMedium/textSemi/textBold Archivo
  400/500/600/700, figure JetBrains Mono 500; `TYPE.body` 14.5/21, `TYPE.small` 12.5/18, `TYPE.section` 11/14 caps 1.4.
- **apps/mobile** `src/App.tsx`: `TILES` (:34), `BUNDLE` (:57), `FEED` (:75), `SQL_WASM` (:77); fonts loaded (:117–121): the
  seven FACE weights (no Archivo 800). `assets/fish/*.webp` 39 files + `manifest.json`.
- **serve** `tools/serve-tiles.mjs` `ARCHIVES` map (:19–37): `/atlas.pmtiles`, `/basemap.pmtiles`, `/bundle.sqlite`
  (fixture), `/province.sqlite`, `/status_index.bin` (env `STATUS_INDEX`), `/bc_outside.geojson`; port 39217; Range +
  Content-Length.
- **deps** present: react-native-svg 15.15.4 (ui-native peer), expo-sqlite 57.0.2, react-native-web 0.21.2, sql.js 1.14,
  vitest 3.2, @testing-library/react 16.3, typescript 5.9.

## C. Store (`data/generated/bundle/regs.sqlite`, `regs-store/4`)

meta: `format`, `blob_codec=zdict`, `section_handles=d768d6e135e62b99`, `reach_digest=cad1c21fe6eb9ba9`,
`rule_ids_sha256=16be720be08b0fca`, `store_digest=90a32c3ccedbcfb0`, `answers_sha256`, `answers_bytes`,
`export_subset_sha256`, `about_bundle`, `layout` (`{top, sections, versions}`), `counts`, `generated_by`. The three digests
equal `bundle.sqlite` meta and `status_index.bin`'s header.

Tables: `meta`, `str(id,text)`, `static(k,j)` (25 keys: answers.{about, fish, glossary, moments, schemas, spec, unnamed},
display.{licensing, reasons, strip}, export.{conduct, licences, species}, gear.{conduct_means, count_nouns, methods, moments,
parent, province_methods}, ladder.{reason_state, reasons}, licence.{profile_dims, profile_labels, profiles, residency_labels}),
`zdict(tbl,d)`, `seglist(seg, starts)`, `segmom(mom, ix)`, `place(place, regions, label, names)`, `akey(akey, ruleset,
licensing_set, steelhead_water, steelhead, steelhead_rules, province_except, home_region, kind, tidal, seg, mom, nseg)`,
`cell(akey, s, ladder, rows, gear, licence, display)`, frame families `(id, j)`, `item(item_ord, item_id, shown, picker,
unresolved, name, kind, sections, outside_bc, entries, steelhead, x)`, `picker`, `part(item_ord, part_ix, akey, ord, label_s,
short_s, runs_s, place_s, hint_s, km, closed_all_year, paper_licence, xp, sections, xruns)`, `runs`, `xpart`,
`section_akey(block, b[8192])`, `section_place(block, b[4096])`, export words `rule(rix,j)`, `lic(lix,j)`, `entry(e,id,j)`,
`ruleset`, `lset`. Python reference: `pipeline/deliver/store/decode.py` `Store(path, verify=True)` with `s(i)`, `val(tbl,b)`,
`static(k)`, `family(tbl)`, `place_of(h)`, `unnamed_name(h)`, `akey_of(h)`; blob `unpack` in `store/common.py:81-91`.
Tap: `pipeline/deliver/answers/encode.py:923 segment_index(starts, day, moments=None, weekday=None, inside=False)`:
`s = bisect_right(starts, day) - 1`; if no moments or `starts[s]` not repeated → s; else among the repeated run pick the
first i whose `moments[i].weekdays` has the weekday and (`hours` null or `hours.in == inside`). Weekday words are
"Monday"…"Sunday". `rule.j` = `{entry_id, rule_id, type, family, dimension, label, verbatim?, fields, parts?, prov}`;
the prototype's rule key is `` `${entry_id}::${rule_id}` ``.
