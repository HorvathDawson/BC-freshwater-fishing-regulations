# decisions.md — owner questions, each with a default so implementation never waits

Rule: a ticket implements the **default**. When the owner answers differently, the "if not" column names the ticket(s)
to reopen and how big the change is. Each default is chosen so the other answer is a local change, not a rewrite.

| id | question | options | **default** (why) | blocks / touches | if the owner picks otherwise |
|---|---|---|---|---|---|
| **D1** | Navigation shell when a water is open — **ANSWERED by the owner 2026-10-20: the prototype layout, but the bottom bar is HIDDEN while latched to a water (water mode, app-data/APP-PREP.md §0, P13/P14); the four header tabs stay. The default below is superseded; T23 mounts in P14's `WaterMode`.** | (a) the prototype: the app's four bottom tabs stay (Map · Search · Conditions · Spots); What you can keep · Gear · Methods · Licence are header tabs over ONE scroll. (b) the 2026-10-10 plan: the bottom bar is replaced by Map · Regulations · Gear & licence | **(a)** — the later design, passed every reader gate 20b–20h; `Shell.tsx` already keeps `TabBar` under `WaterScreen` (:675), so (a) needs no shell change | T23, T29 | (b): T23 swaps the header tabs for a bottom bar inside `RegsScreen`; tab sections stay the same VM — ~1 day |
| **D2** | Where "Conditions" goes — **ANSWERED by the owner 2026-10-20: no FaceBar; a "Conditions here" action in the regs header opens Conditions at the part's MIDPOINT section (`part_geo.mid_sid`, P09/P14), with "‹ Regulations" back. Placement: beside Map (owner). The default below is superseded.** | (a) keep `FaceBar` (Conditions \| Regulations) as the first row above the regs header, on both faces. (b) fold "Conditions" into the header's button group beside Map | **(a)** — nothing the app already settled moves; the regs header becomes a second row. Cost: ~44 px less list on the Keep tab; the pin bar sits under both rows (`PinBar` top = measured header height) | T23, T29 | (b): T23 removes `FaceBar` from the regs face and adds a "Conditions" `hgo` button; header height drops — pins re-measure automatically |
| **D3** | The palette's `sub` (the one secondary grey) | (a) adopt the prototype's (v33: `#555C62` / dark `#B0B8BF`) in `themes/{light,dark}.json color.ui.sub` (app-wide). (b) keep `#6C737A` / `#98A0A7` and re-run the 20g census on the app | **(a)** — the prototype raised it for glare (v33 `r20i-glare`, `--sub ≥ 6.5:1`): v33 `#555C62` on white 6.79:1 / dark `#B0B8BF` on the card 8.87:1, vs the app's `#6C737A` 4.80:1 / `#98A0A7` 6.72:1 (v32 had `#5F666C`/`#A3ABB2` — it moved once already, so always take the value from `port/golden/tokens.json` at sync time, never from this page). One token edit + `pnpm style:build` + the cvd/contrast gates. `faint`/`wash`/`quiet` stay in the palette for the rest of the app but are **banned as text colour inside `ui-native/src/regs/**`** (T39 gate) | T22, T39 | (b): T22 drops the JSON edit; T39's census expectation for `sub` changes — values only |
| **D4** | Fish pictures in dark mode with no tile (20h) | (a) accept the photos' own grounds; (b) pre-process the 39 assets to a transparent/neutral ground (asset step, keeps credits); (c) a `tint` tile in dark only | **(a) for phase 1–3, (b) before ship** — 20h says the manifest pictures are RGBA cut-outs, so (a) may already be right; T39 shoots every fish on the dark card and the owner looks once. (c) contradicts 20h `r20h-picture-surface` | T26, T39 | (c): one conditional `backgroundColor: palette.tint` in `FishStrip` + the picture-surface render gate excepts dark |
| **D5** | The frozen port target | (a) regs v33 @ `04765ab5` (round 20i, latest committed; its snapshot was taken from the data the built store holds: rows 26 · gear 12 · glossary 5). (b) v32 @ `18bc2a11` (where this spec started) | **(a)** — goldens, tokens, inventory and taps are generated at `04765ab5`; the next version flows in with `node app/design/regs-v11/port/sync.mjs --ref <commit>` (README §1). v32 is NOT an option in practice: its strings came from rows 25 / gear 11, which the built store no longer holds | all presenter tickets (goldens) | a later version: run sync at its commit; the inventory diff lists the functions to re-read (the v32→v33 bump changed `buildKeep, sizeOf, kt, bigWords, buildGear, note2, render, pinUpdate` + new `dutyWords`) |
| **D6** | Rule book links on native | (a) open the synopsis PDF URL with `Linking.openURL(url)` (no `#page=`; the link already says "Rule book p.N ↗"); web keeps `#page=<pdf>`. (b) an in-app PDF viewer that honours the page (new dependency) | **(a)** — no dependency; the printed page number is in the label. The PDF→printed offset rules (memory "synopsis PDF not byte-identical") stay in the data | T34, T43 | (b): `RuleBookLink.native.tsx` opens a viewer; one component — `deps.md` line + layers entry |
| **D7** | The day control in the regs header | (a) the header pill shows "Today / Fri" + "Oct 10" and opens a small `DaySheet` with the existing `MapDay` stepper inside (« ‹ day › ») — one date state (`Shell` `mapDay`) shared with the map. (b) no pill; the Map tab's `MapDay` is the only day control | **(a)** — the prototype's header pill passed the readers; `DateSheet` no longer exists, `MapDay` is reusable; ONE state (`mapDay`, null = today) so the map and the page never disagree | T11, T23 | (b): T23 drops the pill (header keeps the slot), T11 unchanged |
| **D8** | Angler profile (age, residency) | (a) per session, in memory (`useAnglerProfile` in Shell, default B.C. resident 16+) — the prototype keeps it in the hash. (b) a stored setting | **(a)** — no storage layer touched (`window.localStorage` is banned in shared code; native would need a new dep). The hook's signature is the same for (b) | T11, T33, T35 | (b): `useAnglerProfile` reads/writes `expo-secure-store`/AsyncStorage behind a `.native/.web` pair — 0.5 day |
| **D9** | Web delivery budget for `regs.sqlite` — **SUPERSEDED by D22: the regs tables ship in the water / regs-<region> / regs-sid release files (lazy on web, on disk on native); web first paint is the paint file only (0.88 MB gz vs today's 105 MB identity bundle).** | (a) whole file now (14.2 MB raw; br ≈ 4 MB) beside the 98 MB bundle, sql.js loads both. (b) wait for a range-read VFS / atlas split | **(a)** behind a measurement: T03/T09 log open time + heap on a phone browser; the store opens lazily on the first water screen, not at app start | T01, T03, T11 | (b): phase 0 still lands; T11 gates the hook on the VFS |
| **D10** | Hours-window moments (night closures, `Moment.hours.in`) | (a) show the day's answer at `inside=false` plus page v36's "At certain times" line (`timedHtml`), listing the other moment's answer. (b) both moments as two cards | **(a)** — the prototype has no design for it; `timedHtml` words are already page-v36 words; the segment index uses `inside=false` exactly as `encode.segment_index`'s default | T05, T42 | (b): a design round; T42 only |
| **D11** | Unnamed-section header | (a) as page v36 / the store say: "Unnamed stream · Region 5" (`place.names[kind]`), no part label (the empty slot keeps the header one size), no Map/Parts button. (b) its own design round | **(a)** — header-one-shape holds; nothing to choose between | T16, T42 | (b): T16 label rules + T23 header only |
| **D12** | Round 20h group head form | (a) the share leads ("2 a day per angler from streams" big, "of the 4 a day per angler across Region 2, lakes included" small); pin "Trout and char · 2 a day from streams" + "(of 4 across Region 2)". (b) the total leads + "↳ 2 of them from streams" | **(a)** — the prototype's default (`S.tot || 'a'`), picked by two readers in r20h | T17, T28 | (b): `buildRegsPage(…, {tot:"b"})` — both forms are implemented (the presenter takes the option), only the default flips |
| **D13** | Regs fixture for CI — **AMENDED by D22: (a) becomes the fixture RELEASE (`deliver release --items …`), which also replaces `packages/data/dev/bundle.sqlite`.** | (a) `app/tools/build-regs-fixture.mjs` subsets the province store to the fixture bundle's items (deterministic, committed under `packages/data/dev/`). (b) no fixture: regs tests run only where `data/generated/bundle/regs.sqlite` exists (skipped in CI) | **(a) for conformance + unit smoke, (b) for the 134-state golden parity** (the goldens need the province frames; parity runs locally and in the pipeline CI job that has the data) | T10, T12 | (b) only: T10 shrinks to the skip guard |
| **D14** | `answers.model --check` in `pnpm check` | (a) add it (Python in the app gate, like `calendar`/`gauge-policy`). (b) leave it to pipeline CI | **(a)** — the app now imports the generated types; drift must fail the app gate | T05 | (b): drop one script line |
| **D15** | Fish stand-ins and protected species | (a) ship stand-ins captioned as the manifest says (`stand_in_for`). (b) omit pictures of protected species | **(a)** with the permission gate (T43): no image ships without a cleared row in `fish_credits.tsv` | T20, T43 | (b): filter in `fish.generated` |
| **D16** | Licence tab on a closed day | (a) as the prototype: the Licence section shows the selectors and an empty answer (162 golden objects are empty). (b) show the day's licence answer anyway (anglers plan ahead) | **(a)** for parity — UR2 6 moved licence words off closed days on purpose ("no licence is needed while fishing is closed" read as "no licence needed") | T33, T35 | (b): T33 reads the licence frame regardless of status; one branch |
| **D17** | The classbox "Source" button (licence) | (a) open the `licsrc:` sheet of that profile. (b) drop it (it does nothing in the prototype) | **(a)** — every other "Source" opens its sheet; a dead control fails the a11y gate | T33, T35 | (b): remove one Pressable |
| **D18** | Bait `also_allowed` (e.g. dead fish as bait in a circumstance) | (a) hidden, as the prototype. (b) a note under the allowed item | **(a)** for parity; owner question for the next design round | T31 | (b): one note line in `baitList` |
| **D19** | klmb_1010's pooled row drawn on page v36's old card path (no `group`) | (a) a data-side `group` for that row (rows 27, a pipeline change). (b) port the old card path (page v36 `groupBlock`/`speciesBlock`, ~250 lines) for one row | **(a)**; the state stays excluded from Keep parity until the data lands (T19b) | T19b, T21 | (b): a new ticket, ~1.5 days |
| **D20** | Tidal water's Gear/Methods text | the golden (x31p_1010, tidal) says "No fishing here on any day of the year." — the prototype's `closedUntil` over a null strip; (a) parity, (b) page v36's `tidalHtml` words ("Tidal water" + the guide note) | **(b)** — the golden string is a prototype bug (tidal water is not closed); T42 excludes x31p_1010 from Gear/Methods parity with this reason | T30, T42 | (a): drop the exclusion |
| **D21** | Licence/answer patches the page adds over the data (v33: steelhead stamp dropped where steelhead are closed, "(not kokanee)") | (a) port them, isolated in `patchNeeds()`. (b) move them to the pipeline (licence 7) | **(b)**, under the owner rule 2026-10-21 (the backend answers, the front end only reads). SHIPPED in licence 7: the stored documents leave the Steelhead Conservation Surcharge Stamp out where the card at the same key and segment says steelhead are closed (`stamps.drop_closed_steelhead_stamp`; the prototype's 7 states compared, no exclusion), and an under-16's `buy` document carries its alternatives (`or`). `patchNeeds()` keeps only WORDING, no decision: the "(not kokanee)" words read from a stored requirement's own label, and the "Also, only if…" split of the stored "Only if" documents | T33 | — |

## OWNER RULE 2026-10-21 — the backend answers, the front end only reads (overrides any ticket text)

Every regulation answer is PROCEDURALLY GENERATED BY THE PIPELINE in the answer format developed for the prototype
(the data differs now, the shape is the same): ladder verdicts, rows (decided takes, caps, releases), gear, licence,
display. The app DECODES and PRESENTS them; it never decides a regulation. Concretely:
- open/closed, keep/release, the daily take, which rule governs, what is lifted/displaced/moot, which closures apply on
  a day — all come from the store (ladder_frame, ladder_verdict, rows_decided, rows_row, …). T06/T07/T08 (`describe`,
  `ladderAt`, `settle`, the rows model) may only decode, select by day/part/fish/origin, group and word them.
- If a screen needs an answer the store does not carry, ADD IT TO THE PIPELINE'S ANSWERS (and the release), never
  derive it in TS. A reviewer treats regulation logic in app code as a blocker.
- A regulation fix (e.g. the Kitimat rulings) is a pipeline change + `deliver all`; no app change should be needed.

## OWNER RULE 2026-10-21 — one water screen, whichever map you tapped
Tapping a stream on the Conditions map and tapping it on the Map tab (regulations) must open the EXACT SAME water screen —
same route, same header, same parts, same section kept — differing ONLY in which view is in front (Conditions vs
Regulations). "That should be impossible" to diverge: both maps' taps call ONE function (e.g.
openWater(item, tapSid, at, view)); the browse-mode Conditions reach panel is folded into the water screen's
Conditions view (no second Conditions screen); a test pins that the same tap from either map yields routes that are
equal except `view`. Applies to search's open action and saved spots too where they open a water.
IMPLEMENTED (one-water-screen workflow, 2026-10-21): `@app/ui` openWater.ts (`openWater`/`openTap`/`useOpenWater`) is the
only builder of an openWater action and the only caller of `itemForSection`; `ConditionsScreen` and the browse `cond`
drill are gone (the panel is drawn only by `WaterConditions`). Two consequences for the owner to confirm: (1) BACK IS
LATERAL — back from any view leaves the water (to the map it was tapped on), no longer Map/Conditions -> Regulations
first; (2) an UNNAMED reach (no registry item; ~90k of 108k gauged sections) opens as a frame with `item: null` offering
Conditions only — the SAME frame from the Conditions map and from the Map tab (review fix: the Map tab used to open
nothing for it, so most streams still diverged; `openWater` now puts Conditions in front whatever view was asked).

## OWNER 2026-10-21 — satellite imagery is ONLINE-ONLY, from Esri World Imagery
The satellite basemap is shown only when the device is online; it is never packaged or cached for offline use. Offline,
the drawn basemap is the ground (the Layers toggle says why satellite is unavailable). This lifts the offline-caching
constraint from the imagery choice; a provider still needs terms that allow MapLibre use, an acceptable cost/key model,
and BC coverage at high zoom (research: handoff/PLAN/app-data/satellite/README.md).
The owner chose **Esri World Imagery** for the online layer, as the original app used (0.3-0.5 m over rural BC, z17-18).
Its terms (ArcGIS Location Platform: client key, free tier then usage billing) are the owner's to set up; the 10 m base
stays underneath and shows through offline. The BC 1 m 1995-2004 colour mosaic is "Access Only" — the owner may request
permission (https://forms.gov.bc.ca/copyright-permission-request/).

## OWNER 2026-10-21 — one-part waters show their part label
A water with no part picker shows its part's label in the header ("Whole water", "Mouth → Source") wherever the label
is not the water's own name — the app frames every part from the tiles (D1). The prototype showed nothing there (it
had no water map of those waters); the 22 golden states stay keyed in exclusions.ts NO_MAP_LABEL_IDS by design.

## Added 2026-10-20 — app data + app prep (`handoff/PLAN/app-data/`)

| id | question | options | **default** (why) | blocks / touches | if the owner picks otherwise |
|---|---|---|---|---|---|
| **D22** | How data reaches the app | **ANSWERED by the owner 2026-10-20:** a few small gzip files in `data/release/<r>/` (each with a stated reason): `paint` (0.88 MB gz, first paint), `core` (1.11), `near` (1.99), `water` (regs-common + topology, 1.94), `regs-<region>` × 9 (2.74 all; 0.19–1.32 each est.), `regs-sid` (1.22), `conditions` (2.66). **Native is always offline** (holds everything; update check downloads only changed files); web fetches lazily, a tap in a new region fetches that region. Build artifacts stay in `data/generated/`. `regs.sqlite` is not shipped (MINIMAL-PACKAGING §1–§5) | — | T01, T03, T04, T09, T10, T11, T12 | — |
| **D23** | Section geometry for the water map / highlight | **owner 2026-10-20:** tiles only — feature-state on `part_section` sids; camera from `part_geo`; navigation from `part_mark` / `water_exit` (P09, P15). No geometry store | — | T16, T23, P15 | — |
| **D24** | "Conditions here" | **owner 2026-10-20:** next to Map in the header; opens the midpoint section (`mid_sid`) of the part currently being read | — | P09, P14, T16, T23 | — |
| **D25** | Feb 29 — **SUPERSEDED by D31** | | | T05, P06 | |
| **D26** | Card gate | **owner 2026-10-20:** runs on the SHIPPED answers inside `deliver all`; failing blocks packaging (P05) | — | P05, P08 | — |
| **D27** | Tiles vs data versions | **owner 2026-10-20:** tiles are geometry-only and versioned on their own (`<handles16>.<tilebuild12>` in the pmtiles metadata); each data manifest lists `tiles.compatible`; a data-only release never rebuilds tiles; the app keeps the last good pair on a mismatch (P18, P10, P11; MINIMAL-PACKAGING §6) | — | T01, T03, P08, P10, P11, P18 | — |
| **D28** | Platform priority | **owner 2026-10-20:** native-first (Expo/React Native); web from the same tree via Expo web. The native map (N1 spike, N2) lands before any map-facing ticket; every RN ticket is accepted on native first, then web parity | — | every RN ticket; T23, T29, T41 | — |
| **D29** | Middle-man JSON | **owner 2026-10-20:** `ui-rules-answers.json`, `ui-rules-export.json`/guide are debug-only (`--debug-json`, written to `data/generated/debug/`); `regs.sqlite` is not shipped (P20) | — | T12, P20 | — |
| **D30** | Codec | **gzip everywhere** (owner Q4: brotli saves 0.13 MB at web first paint and 0.2% of a native install — not "a LOT better") | — | P08, P10 | — |
| **D31** | Feb 29 (amends D25) | **owner 2026-10-20:** keep the 365-day year; Feb 29 reads Feb 28's regulations with its own real weekday (P06) | — | T05, P06 | — |
| **D32** | Tile size and the mask | **owner 2026-10-20:** the atlas zoom-13 overzoom and the basemap landuse filter (≈ −345 MB per native install, est.) are deferred to their own effort, scheduled before the first public native release; the outside-BC mask stays OUT of the tiles (it blew up the tile size) as a separate `bc_outside.geojson` in the tile version folder, listed in the manifest | — | P18, P11 | — |
| **D33** | The Licence tab on tidal water | the golden (x31p_1010, tidal) draws an empty Licence section — the prototype's closed-day path over a key that is not closed; (a) parity (the empty section), (b) page v36 licParts' "Tidal water" box with the stored tidal licence words (`licence_frame.holds.tidal.licence`) | **(b)** — as D20 for Gear/Methods: tidal water is not closed, and the stored answer names the licence a tidal angler needs; T33 excludes x31p_1010 from licence parity with this reason (exclusions.ts TIDAL_LICENCE_IDS) | T33, T35 | (a): `buildLicence` returns the empty section on a tidal key; drop the exclusion |

## Owner questions this spec could not settle from the code

- **Image permissions** (GoFishBC, WDFW, What To Fish, Photo Ark) — a legal matter, not a default; T43 makes it a ship gate.
- **The 8 `code: null` salmon images** in the manifest are unreachable from any VM (no code) — keep, recode, or drop?
- **Prototype bug** (T14): a closed-all-year chain line is rewritten to "." (chw0_1010 and the other closed-all-year states); ported for parity, flagged.
- **Prototype bug** `pinUpdate` throws on 25/134 states (a ledger row without `group`); the app's `pinChain` skips such a
  row. Owner/prototype agent: fix in the prototype so the goldens cover those states (re-run `sync.mjs` after).

## OWNER 2026-10-21 — lake Stocked / Depth modes: prepared, not fed yet
"we dont have the stocking and bathy map data in a feed yet. but we just want it prepared for it when we do have the feed.
we even might want the toggles grey/disabled for now."
- Keep the plumbing: the release's optional per-lake values slot (contract + reader), the colouring engine's stocked and
  survey channels, and the tests that a release carrying values recolours lakes with no tile change.
- Do NOT fetch or ship stocking or survey data now: no stocking download in the deliver chain; the release slot ships
  empty (or absent).
- The Stocked and Depth lake-mode toggles are DISABLED (greyed, with a short "no data yet" note) while the release carries
  no lake values; they enable themselves when a release does — no app change needed when the feed arrives.

## OWNER 2026-10-21 — Riffle is obsolete
"riffle html is obsolete we dont need to reference it anymore." app/design/riffle.html is no longer a design reference.
Regulations screens follow the regs prototype (regs-v11 / v38); everything else follows the app's own tokens and
components plus explicit owner direction. Older mentions of Riffle in comments and docs are history, not spec.

## OWNER 2026-10-21 (late) — layers menu, water map, imagery
Layers menu: build direction B ("map · colour the water · on the map"), with these owner changes:
- Data sources and credits leave the layers menu: they live in the hamburger menu (Data sources & dates) and in the
  map's own attribution. The colour palette choice (light / dark / colour-blind) moves to the hamburger menu too.
- NO SPLIT between stream and lake colouring: "it is dangerous to have the divergent layers for features". One choice
  colours ALL water features (streams, lakes, wetlands) by the same thing; a feature the chosen measure doesn't cover
  draws neutral and the legend says so. (B's separate stream "Plain" mode stays rejected.)
- Coming-soon items are shown greyed with "Coming soon".
- Keep B's "always shown" line, and it must include Indigenous land (reserves).
Water map: keep the 10 municipal continuations; the crossing button SAYS where it goes ("▲ Upstream into Chilliwack
River"); drop reversed continuations like Eagle Trib.5-3 → Trib.5 (fix the rule, not a hand list where possible).
Imagery: use Esri World Imagery WITHOUT a key, as the original app did ("this is a non profit app"); a key stays optional
config. No offline 10 m base ("10m is useless"). Add network-status detection on phones (netinfo). The Satellite choice
does not need to persist across launches.

## OWNER 2026-10-22 — side channels follow their own river (option A)
"a is what i want. just follows its own river at that point." A side channel's Conditions panel comes from a reach on
its OWN river (same FWA watershed code) — `mainstem_of` in pipeline/atlas/gauges/build_panels.py never adopts another
river's reach. Where its own river is silent, the channel is silent too. Options B (an explanatory line), C1 (share-of-
flow weighting) and D (the downstream-walk area rule) are NOT part of this. See handoff/PLAN/app-data/side-channels/REPORT.md.

## OWNER 2026-10-22 — a water's own gauge comes first
"the pitt could use its own gauge." The Pitt's own gauge, 08MH035 Pitt River near Port Coquitlam, records LEVEL only
(2017–2024), so in Flow mode the Pitt borrowed North Alouette (26 km²). Refined by the owner: "it can still use auxiliary
gauges but own gauge should be main (unless far from click) low magnitude compared to click spot." Rule: a water's OWN
gauge (matched to that water) carries the MAIN weight, in whatever it measures; other donors still count as auxiliary,
with a smaller share. The own gauge stops leading only when it is far from the tapped spot along the water, or its
catchment is much smaller than the spot's (low magnitude relative to the tapped spot); then the usual weighting applies.
NEVER MIX QUANTITIES (owner, same day: "we care about flow. so we dont want to mix flow and level for now. we could have
a way to separately view level"): the Flow view uses flow gauges only; the own-gauge rule applies within the chosen
quantity. A level-only own gauge (the Pitt's 08MH035) does not lead the Flow view — it leads the Level view. Level stays a
separate view of its own (today the Conditions legend's Flow / Level switch; the owner suggests it may move into the
layers menu — decide with the layers registry). In the Level view, check and
disclose tidal or backwater influence where the record shows it (Port Coquitlam is tidal, with Fraser backwater). Build after option A,
before the data rebuild; the estimate stays pipeline-generated with the app mirror held equal.

## T49 default (2026-10-22) — which days the date sheet offers (OWNER QUESTION)
The data is one 366-day calendar with no year, so a date in another year would read as the same month and day of this
one. Default implemented (`@app/core` `dayRange`): the picker offers TODAY'S CALENDAR YEAR (Jan 1 – Dec 31), cut earlier
when the release states `regulations.valid_until` (inside the year and not yet passed); today is always offered. Other
days are disabled and the sheet says why. If not: a rolling 12 months, or the synopsis edition (Apr 1 – Mar 31) — one
function (`dayRange`) and its tests change; the sheets do not.

## OWNER 2026-10-22 — answers (T39/T40, T47, T48, T41, process)
T39/T40: dark fish cut-outs with no tile are fine; the licence paper note's tint is Claude's call (the prototype is a
reference, not the end-all — improve on it where there is a better way); small inline links are fine; keep the
original "Who is fishing?" radiogroup labels; tabs wrapping at 2x text is fine unless there is a better way.
T47: region (and unit) boundaries grey is good, but the STATUS colours must be MORE different from each other than the
candidates — push own / closed / restricted / base / open further apart, then apply.
T48: keep a bottom bar, made smaller: Map · Conditions (option 1). The search field stays in the top bar, always open,
with the sync status in the top of the search area. Placeholders may also mention towns. The search transition and the
placeholder lines are approved. The owner doubts splitting things between a top hamburger, a bottom bar and a side rail,
and asked for Claude's view on (a) the hamburger moving into the bottom bar and (b) Spots as a tab vs on the rail.
CLAUDE'S RECOMMENDATION (built, easily reversed): bottom bar Map · Conditions · Spots · More — "More" holds what the
hamburger held (glossary, data sources & dates, display, about, report a problem, contact); Spots is a tab again (its
list, open, show on map, add) while long-press-to-add stays on the map; the side rail keeps Layers only; the top bar is
the search field alone, with the sync status line at its top. One place for each kind of thing.
T41: the prototype is a reference — Claude decides R1 spacing, sheet kicker style, the report link placement and the
sheet's Done word, improving on the prototype where there is a better way.
Process: add strict consistency auditors (Fable model) that check data and UI agree and that every route into a screen
reaches the SAME screen (e.g. a water's Regulations/Conditions opened from the Map tab vs the Conditions map), with no
duplicate menus; then finish the remaining implementation and do the mass cleanup.

## OWNER 2026-10-22 — icons: Lucide, rounded 1.75 (corrects a misread)
"no C. Open licence: Lucide (ISC), rounded 1.75." The "option C" the owner picked was the ICON direction, not a status
colour candidate: the app's icons (bottom bar Map · Conditions · Spots · More, the side rail's Layers, and search / sync /
back where they share the set) come from Lucide (ISC licence; record it with the other credits), drawn with rounded joins
and caps at a 1.75 px stroke on the 24 px grid. T47 stays as decided before: palette D derived from B, grey boundaries,
status colours more distinct than A/B/C.

## OWNER 2026-10-22 — the audit plan (handoff/PLAN/audit/PLAN.md)
- Waves 2 (merge old pathways, all items), 3 (split long files) and 4 (docs) approved as planned.
- Wave 1 with two corrections: the STOCKING and BATHYMETRY code (and SALMON / DFO) are NOT dropped — they are not fed or
  not implemented YET and will come back: keep them (tidy, document as "prepared", don't delete). And EVERY script is
  double-checked before deletion: prove it produces nothing anything still needs (outputs, consumers, docs, CI).
- Calls: (1) old outputs can go; dead code too, unless it is something planned to come back (bathy/stocking feeds,
  salmon). (2) Archive the prototype answers reference, keep its useful tests as golden fixtures — AND do a big test
  cleanup. (3) Keep only one Map entry (the view strip; drop the header's Map button). (4) One name normaliser, with a
  reviewed binding diff. (5) Ambiguous cuts — the owner asked what they are; explained (below); awaiting a decision.
  (6) Move the continuation links into the bundle. (7) Run the determinism proof in the background, mindful of memory
  (heavy.sh, alone). (8) Clean the ~78 GB of old scratch builds and releases (never the live release, the served tiles
  or anything a current file points at). (9) Archive the old prototype rounds and merged-ticket shots. (10) Keep the
  own-gauge constants. (11) Claude makes judgement calls on the ~30 smaller pending questions and records them.
  (12) Move heavy.sh into the repo.
- Ambiguous cuts, explained: 93 rule edges (86 rules) say "from <a split point> …" where that split point matches TWO
  places on the river line — e.g. "Harrison River … Harrison Lake" meets the lake at km 20 and km 84 (64 km apart). The
  builder takes the first and only a diagnostics file notes it. Median gap between the two candidates 12.6 km, p90 58 km —
  so a closure or rule may start at the wrong place.
- Ambiguous cuts, DECIDED (owner): "pick the end that is closest to the other extent, because this will never be a lake
  regulation and under-lake streams have no regs." When a split point matches two places on the line, take the candidate
  nearest the rule's OTHER extent (the stretch stays on the river and never spans the lake). Deterministic, so no review
  list; the choice is still recorded (a bundle diagnostic row with both candidates and the one taken) and pinned by tests on
  the real cases (Harrison, Ash, Mohun, Cayoosh). Needs a reach rerun + deliver; diff the rule bindings before promoting
  (memory: split fixes change the trib walk).
- Ambiguous cuts, refinement (owner OK'd 2026-10-22): a HAND-CURATED choice outranks the general "nearest the other
  extent" rule. Where a curated record states which crossing/end it means (e.g. skeena_river_2's review_reason LS-1: the
  reserve's LOWER mainstem crossing standing in for the Exchamsiks confluence), that pick stands; the general rule
  applies only where nothing curated says otherwise.
## T48 BUILT (2026-10-22) — the calls Claude made, and why (each a local change if the owner wants otherwise)
Built as recorded above: top bar = the search field alone + sync line; bottom bar Map · Conditions · Spots · More
(smaller: 20 px icons, 5/4 px padding, every tab still 44 px tall); rail = Layers alone; hamburger + drawer DELETED
(More lists the same six pages — `tools/one-menu.test.ts` pins one list, one spots list, one capture flow, one search
sheet, and no `@app/map` in the sheet or the bar). Decisions not spelled out by the owner:
- SEARCH IS ROUTE STATE, not a tab and not a sheet layer (`route.search`, only over Map / Conditions): back closes it
  first on every platform, a tab change closes it, and a water opened from it keeps it underneath, so back from the
  water lands on the same results (query, selection, map framing). Why: one back rule, and the browser's history counts
  it like any other step.
- THE SHEET HAS TWO HEIGHTS: full while choosing (nothing picked; map dimmed), half once anything is picked (a water
  framed OR a town's list) — typing again forgets the pick and raises it. The main map follows only PICKS, never the
  typing (the full sheet covers the map; moving it unseen is wasted work and a jumpy cancel). The old preview's
  "open on the best match" therefore no longer moves the map.
- NO BLUR behind the sheet, the scrim only: a real blur needs a native module (expo-blur — against "minimal prod
  packages") and on the web a backdrop-filter over a live WebGL canvas costs frame time on the phone profile.
- CANCEL RESTORES THE CAMERA EXACTLY, and only when search moved it: a new `fit.camera` on the map contract
  (`easeTo`, both renderers, pinned in parity.test and adapter-contract). Opening a spot from search restores too.
- THE EYE IS GONE: a place's tap shows it (owner); a water's tap frames it and its selected row carries a › "Open …"
  (the one opener, `openWater`, with the view the map in front implies: Conditions from the Conditions map).
- SPOTS "SHOW" IS ONE FUNCTION (`useMapRail().show`) for the Spots tab row and the spot screen: it goes to the Map tab and
  frames the spot; the framing survives that one move (it is what the move is for) and is forgotten by any later one.
- SYNC LINE TODAY: "Regs to <valid_until> · Flows checked <age>" (the Data sources page's own facts, shortened). The
  release carries no build date, so "data release age" is the edition's last day; the feed's age reads only the feed's
  index (cached), never the bundle counts, so a cold start opens no core for it.
- PLACEHOLDERS: the approved six + three naming towns ("Search a lake, river or town", "Name a town to find water near
  it", "Fishing near a town? Type its name"); one per app opening; ≤ 36 characters (fits 390 px, measured in Chrome).
- A REPORT FROM THE MORE TAB says "Screen: more" (where the reader is); a report from a regulations tab is unchanged.
OWNER QUESTIONS (defaults built): (1) should the search sheet keep the old "open on the best match while typing"
preview? (default no); (2) should Spots' "Add a spot here" from the Spots tab say "at the middle of the map as you left
it"? (default: the existing words).
## CLAUDE 2026-10-22 (owner delegated) — regs polish: R1 rhythm, Q3 report link, O4 sheet type, Q4 Done, T39 Q2 paper tint
The owner handed these to Claude ("the prototype is a reference, not the end-all"). Each decision, why, and how to undo it.
Measured with `app/tools/regs-heights.mjs` (new: prototype v38 staged at the goldens' ref vs the app on Expo web, 390 px,
the prototype's today); side-by-sides in `T41-polish-shots/` (`heights-before.json`, `heights-after.json`).

| state · section | prototype | app before | app after |
|---|---|---|---|
| vedder_1010 Gear | 652.4 | 731.5 (+12.1 %) | 650.0 (−0.4 %) |
| vedder_1010 Methods | 501.6 | 605.2 (+20.6 %) | 511.8 (+2.0 %) |
| vedder_1010 Licence | 800.3 | 926.5 (+15.8 %) | 839.8 (+4.9 %) |
| klwa_1010 Gear | 529.2 | 631.2 (+19.3 %) | 549.8 (+3.9 %) |
| klwa_1010 Methods | 559.6 | 673.3 (+20.3 %) | 569.7 (+1.8 %) |
| klwa_1010 Licence | 660.3 | 764.7 (+15.8 %) | 695.4 (+5.3 %) |
| bc1_501 Gear | 600.4 | 663.6 (+10.5 %) | 591.6 (−1.5 %) |
| bc1_501 Methods | 634.3 | 767.3 (+21.0 %) | 663.9 (+4.7 %) |
| bc1_501 Licence | 660.3 | 764.7 (+15.8 %) | 695.4 (+5.3 %) |

(“before” includes each section's own 52 px report line.) Kootenay LWA Methods now has 61 px to spare under
bait-ways-compact (was 9).

- **R1 rhythm — tightened to the prototype, by mechanism, not by shaving margins.** The measured cause was one thing: a
  note or price drawn as a NESTED run inside its item's name Text inherited the name's line box, so every ✓/✕ note line
  drew at 20.25 px and every licence "when"/price line at 21.3 px instead of the prototype's `--t4/1.3` (16.22). Fix:
  the item stays ONE Text (the golden lines read name + note as one line; a screen reader reads the item whole), its strut
  is the small run's size and line box, and the name is a run at its own size and height (`YesNoList`, `LicenceAnswer`
  `NeedItem`). Then Gear/Methods' own 24 px foot → 8 (the prototype's `.gpart` 8 + nothing): Vedder Gear lands on the
  prototype's 652 px. KEPT taller on purpose (measured reasons): the 44 px segmented "Who is fishing?" rows (T40 44 × 44)
  and the line wraps a pixel of column apart (Bella Coola "Spear or bow — …" wraps in the app). The section head's rule
  + 16 px is the prototype's own (`.gpart` padding-top 16 + its 1.5 px rule) — not changed. T40's 200 % layout test passes
  with the nested runs (they scale with the text). Undo: one style per component (rhythm.test.tsx pins each value).
- **Q3 report link — once, at the page's foot; the report names the whole page, NOT a tab.** Four identical links cost
  ~52 px a section, sat inside each section as if they were one of its rules, and a reader scrolling the one page met the
  same link four times. First cut prefilled "the tab in view when pressed" — the review measured that this is always
  Licence (the link is first fully on screen at y=4072 on vedder_1010 / y=3120 on klwa_1010 at 390 px, y=3622 at
  1280 px; bc1_501's report read "Tab: Licence" and subject "… Bella Coola River · Licence"), so it named the wrong
  section whenever the problem was in Keep/Gear/Methods. Now `regsReportWhere(page, null)` names no tab: "Where you
  were" is the screen, the water, its part and the day; the subject is "… problem report: <water>"; the reader says
  which rule in their own words (the What's wrong box). `ReportLink` takes no `tab`. Drawn after Licence, before the
  picture credits; the menu's Report a problem (ReportPage) is unchanged and still the other way in; T40's 44 × 44 kept.
  Rejected: per-section links back (the 52 px × 4 cost above); a tab picker on ReportPage (a form field for a fact the
  free-text box already carries). Undo: `RegsScreen` draws it inside `sec()` passing the section's tab (one line; the
  core function still takes a tab).
- **O4 sheet typography — sentence-case sheet headings; rule titles and licence documents stay at body size.** The
  prototype's own round-20 rule #7 says caps only on section kickers (`.sbody .lbl { text-transform: none }`) — the app's
  sheet `Head` ("To fish here", "These decide it (12)", "What to look for") drew caps against it; now bold sub sentence
  case (the Keep tab's style gate rule, applied to sheets). NOT ported: the prototype's 12.5 px rule titles and its small
  `.doc` lines in licsrc — in the v38 shots those lines run together ("…to fish.Province") and they are the legal
  requirements a reader opened the sheet to read; body size reads better. The kicker of a Sources group (`kicker:where`,
  the prototype's `.lvh`) stays caps (it is a kicker in the prototype too).
- **Q4 Done — "Done", sentence case, body size (14.5 semibold), accent, in EVERY sheet in the app** (the one shared
  `Sheet`: map, layers, spots, regs). The prototype settled sentence case (`.done { text-transform: none }`); 11 px caps
  was the smallest and loudest-shaped word on a sheet. Still grown to 44 × 44 (`hitProps`). Undo: `DONE_WORD` in Sheet.tsx.
- **T39 Q2 paper note tint — kept.** "Carry your paper licence … record it right away" is the one thing on the Licence
  tab an angler must DO at the water, so it is the one tinted box there (prototype `.gtopic .paper`, --wash = --tint at
  v38). The census keeps its keyed exception (`note:paper`, reason updated); ink on tint passes the light / dark / CVD gates.

Also fixed (environment, not design): Chrome 155's headless draws a classic 15 px scrollbar, which took 15 px from every
width `tools/wide-measure.mjs` measured and failed T45/T46 on an unchanged tree (another lane's baseline check failed the
same two). `wide-measure.mjs` now hides scrollbars (`Emulation.setScrollbarsHidden`), as `shoot.mjs` already did.

## OWNER 2026-10-22 — palette D stands
"could we just put into app and if I have an issue with palette I ask you to change it later?" Palette D (T47 part 2) is
the app's palette; its three open trade-offs (light restricted near-black aubergine, base periwinkle, open no longer
green) are accepted as built. The owner raises any change after using it. Not an open question any more.

## OWNER 2026-10-22 — refinement pass ("right now it just feels unrefined")
MAP: (1) the Conditions WATERSHED (basin) colouring starts at a slightly HIGHER zoom, and while it is visible the lakes,
streams etc. are hidden (no double layer). (2) Names, labels and icons tuned to read well in EVERY mode — regulations,
conditions, plain, satellite, light/dark — one universal label/icon style. (3) Private land is too abrasive when on: softer
styling, and visible from slightly more zoomed out. (4) Restricted and closed AREAS look nicer at every zoom. (5) A nicer
scale bar. (6) The Conditions map's legend/histogram bar at the bottom made nicer.
MORE TAB: data sources & dates always shown at the TOP, minimal, with a badge (freshness), tap for the detailed references;
nicer display options; Contact at the bottom inline (not a nested page); each feed's last update and whether it is likely
out of date. A back control at the top that returns to the previous tab.
TOP BAR: no "Flows checked 27h ago" text in the search bar. Instead a SYNC ICON up there that indicates when a refresh is
needed (stale feed / new release); the details live in More.
MOTION: when a menu/sheet closes, the dim backdrop must NOT slide down with it — it stays in place and fades out after the
sheet has slid away (and fades in in place on open).
- SPOTS (owner, same pass): "add a spot should be either where you are or select on map — 'add a spot here' is awkward
  because you need to centre it on the map first". Adding a spot offers two ways: AT MY LOCATION (asks for location
  permission first; on native via the location module when present) and PICK ON THE MAP (go to the map with a short
  "Tap where your spot is" banner + Cancel; a tap drops the pin and opens the existing drop sheet → the one capture flow).
  "Add a spot here" (map centre) is removed. Long-press on the map stays as the shortcut.

## CLAUDE 2026-10-22 (owner delegated) — the selected tab cue
Icons merged (Lucide v0.460.0, ISC; credited under More › Data sources; app/LICENSES/lucide.txt). The selected bottom-bar
tab keeps the ink underscore as its second cue besides colour, and gets no filled pill: the underscore already works
without colour, it is the app's one "selected" style (the water view strip and the regs tabs use it), and a pill would add
a second selection style and a new fill to gate. Ask to change it if it reads weakly on the phone.
