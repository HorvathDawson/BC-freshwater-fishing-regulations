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
- SPOTS ON THE MAP (owner, same pass): "we wanted a way to toggle our spots on and off in the layer menu." Saved spots draw
  on the map as a layer with a switch in Layers ("On the map"), on by default, on both the Map tab and the Conditions map
  (a registry entry in core layers.ts; one pathway; no second spots list).
- BUG (owner): "the refresh for week before isn't working" — a spot's lead-up (the week before the visit: weather + gauge,
  S1) does not refresh/backfill. Investigate and fix.
- NEW TICKET T51: spot similarity forecast (below).
- NEW TICKET T52 (owner 2026-10-22): an "Expected change" view on Conditions — a chip after +10 that flags rivers with a
  big expected change anywhere in the forecast window (spikes between the sampled days included), from each station's
  DAILY forecast series (CLEVER and the other models), flow only.

## OWNER 2026-10-22 — estimates use every gauge that could inform a section, chosen live
"make sure the section gauge percentage in prediction computes for all gauges we have possible so that even if some
drop/get added in that list it is still able to make a prediction using all available gauges on the current flow at the
location." Panels are built over EVERY station with a usable record that could describe the section (ranked, with their
weights' inputs), not only those the roster calls real-time; at READ time the estimate uses the members reporting now,
re-normalising the weights among them, and the own-gauge lead goes to the best-ranked own gauge reporting now. A station
joining or leaving ECCC's real-time list changes estimates on the next feed, with no rebuild. Never mix quantities.

## CLAUDE 2026-10-22 (refinement pass, lane: conditions legend + sheet motion) — what changed and why
- **SHEET BACKDROP: the sheet slides, the scrim stays and fades** (owner: "the screen opacity slides down with the menu").
  Cause: the shared `Sheet` used the Modal's own `animationType="slide"`, which translates EVERYTHING in the Modal, scrim
  included — the dim travelled down the window as a band (recorded: `app-data/sheet-motion/before-trace.json`, the
  scrim's top edge 844 → 0 on open and 0 → 844 on close). Now the Modal animates nothing and stays up while the sheet
  leaves; two Animated values on the platform driver (`NATIVE_DRIVER`): `slide` moves only the sheet panel, `dim` is only
  the scrim's opacity, and the scrim (now a full-window layer BEHIND the sheet, so no bright band opens up under a
  leaving sheet) never has a transform. Timings `SHEET_MOTION` (motion.ts): open — rise 260 ms ease-out, dim in 200 ms;
  close — fall 220 ms ease-in, dim HOLDS 160 ms then fades 180 ms (gone 120 ms after the sheet: the dim is the last thing
  to go, as the owner described). Reduced motion: no slide, sheet + scrim fade together in 120 ms. While leaving it takes
  no touches. A closed sheet slides out WITH what it showed (Sheet keeps its last title/sub/children; SheetHost keeps
  its last page sheet mounted closed) — before, a regs sheet vanished on Done. Reopened mid-close it turns round from
  where it is. The wide dialog (T45) keeps its Modal `fade` (nothing translates there). SearchSheet already had a
  separate fixed dim (`search-dim`); MenuPage is a full page with no scrim. Pinned: `SheetMotion.test.tsx` (react-native-
  web's REAL Animated, not its test double — every 20 ms through open/close the scrim's transform is none, it holds then
  falls monotonically, the sheet has left before it has faded; mutation-checked: a translating scrim or a zero delay
  fails 3 of 5). After: `after-trace.json` — scrim top 0 on every frame; on close the scrim is 1.0 until ~190 ms,
  0.69 when the sheet reaches the bottom at 255 ms, gone at ~380 ms. (`record.mjs` reproduces both; the PNG strips are
  coarse — a screenshot takes longer than a frame — the trace is the per-frame record.)
- **CONDITIONS LEGEND** (owner: "the histogram bar … could be made a little better"):
  - the ramp is the MAP'S, stop for stop — a gradient with each colour at the percentile its stop names (0/10/25/50/75/
    90/100, `flowRampStops` reading the style metadata the map's expression reads). It was seven EQUAL blocks: a gauge
    mark at the 20th percentile stood on f2 while the map painted it nearly f3, and "normal" was one seventh of the bar.
  - it says what the scale is: a title ("Flow vs. past years, this date" / "Water level vs. …"), Low · Normal · High,
    and a bracket over the middle half where core's `standing()` says NORMAL (read from that function, not restated).
  - it counts what the histogram is of ("39 gauges in view").
  - words in `sub` at 11.5–12.5 px (was `faint` 10.5 px: under the T40 text contrast floor); title in ink.
  - capped at 560 px and centred (desktop drew a 1250 px stripe).
  - the horizon chips are ONE joined segmented strip (one card, hairline dividers, chosen day filled) instead of five
    loose shadowed pills; each segment a 44 × 44 press. Same words, same labels, same order.
  Shots: `app-data/conditions-legend/{before,after}-{390,1280}-{light,dark}.png`, `after-360-light.png`.

## REFINEMENT BUILT (2026-10-22) — More tab + sync icon: the calls Claude made, and why
Shots: handoff/PLAN/app-data/more-tab/ (before-* / after-*, 390x844 light+dark, 1280x800).
- SYNC ICON, not a line: Lucide `refresh-cw` at the search field's right end (40 px slot, hairline divider), a red dot
  (`closed`, the "act on this" colour) when it NEEDS A REFRESH — the feed past its cadence or not reached, or the server
  naming a newer data release — and a slow turn while CHECKING (still under reduced motion). Fresh: the glyph alone.
  Said aloud: "Refresh data: river flows last checked 27h ago". It gives way to the clear ✕ while search is open. The
  bar is 20 px shorter, so the map's chrome starts 20 px higher.
- THE TAP refreshes what can be refreshed and nothing heavier: the feed forgets its 5-minute cache (`GaugeFeed.refresh`)
  and is re-read (the Conditions colouring re-asks through `epoch`), and the manifest is re-read (`no-store`). A newer
  release is SAID (dot + More's Data block "Reopen the app to use it"), never swapped under the reader.
- ONE stale line for the feed: 2 h (four missed 30-min ticks) = `STALE_AFTER_MS`, the line a reading's own age is held
  to (P02); a test holds them equal. Release-dated cadences: DFO 120 d, FWA 730 d, basemap 365 d, HYDAT 548 d (named in
  `STALE_AFTER`). Regulations: fresh through `valid_until`, stale after; no `valid_until` (today's live release) =
  Unknown. Tiles: the proven pair (`usePairing`). In-season: "Not read". Weather / imagery: "Live" (a 4th badge kind —
  they are fetched on demand, so "up to date" would claim an age they do not have).
- MORE TAB: ‹ <previous tab> at the top; DATA always shown (dot + name + short date per line, one summary badge in
  words, the whole block opens Data sources & dates — which now carries the same badge per source); DISPLAY as three
  preview tiles drawn in each palette's own tokens (no Display page any more); Glossary · About · Report a problem as
  rows; CONTACT inline at the bottom (no Contact page; its duplicate "Report a problem" button dropped — the row is
  right above). Wide screens: one 640 px column ruled at its sides.
- BACK FROM MORE goes to the tab it was opened from (route `prev`, only on More) for the ‹ AND the system back — one
  back rule; then the Map tab as before. More from the Map tab is the plain route (unchanged).

## OWNER 2026-10-22 (late) — silent gauges, and "use best judgement" on the open items
- Silent panel members are HIDDEN from the Conditions gauge table, but mentioned: one line naming them with each one's
  last data time (e.g. "Not reporting: Liard at Upper Crossing (last 14 Oct 09:00)"). Never-reporting stations say so.
- The rest is Claude's call. Owner: "gauge data is much faster turnover than other feeds". So:
  - More sync stale cadences are PER FEED: gauge readings go stale in hours, forecasts in about a day, regulations /
    map data on their release cycle. One table in core, not per screen.
  - Dark-mode water ink: bring it back to the blue family of light-mode water (it read sea-green); palette D otherwise kept.
  - The release stamps `valid_until` (the synopsis period end) so the app can say when the regulations lapse.

## CLAUDE 2026-10-22 (owner delegated) — staleness per feed, from one table
Owner: "gauge data is much faster turnover than other feeds"; "use best judgement". What was built and why:
- ONE TABLE: `STALE_AFTER` in `app/packages/core/src/staleness.ts`, a rule per feed (`age` + ms / `release` / `notRead`
  / `live`). Every staleness reads it: the More Data block + Data sources & dates (`sourceFreshness`), the sync dot
  (`syncVM`), a reading's age line (`STALE_AFTER_MS` is now `STALE_AFTER.feed.ms`, not a second constant) and a forecast
  run's line (`forecastRunLine`). `SYNC_CHECK_STALE_MIN` (more.ts) and the old `{feedHours, dfoDays, …}` (menu.ts) are gone.
- THE LINES, from the real publish cadences:
  - Gauge feed index (`fetchedAt`) **2 h**, unchanged: publish.py runs every 30 min over ECCC's hourly real-time CSVs, so
    2 h is four missed runs (two missed ECCC hours). A few hours was the owner's word; going longer would hide a dead cron
    for a whole morning. **This device's last check 30 min** (the sync dot): one publisher tick, what a tap can fix.
  - Forecast run (`issuedAt`) **36 h**: ELF / COFFEE issue daily, CLEVER once or twice a day (forecast.py); a day plus
    half a day's slack for a late morning run. Shown as "· likely out of date" after the run line on the Conditions chart
    (same words as the Data block's badge). 2 h would have called every forecast stale by lunch.
  - Regulations and map tiles: **release-ruled, never by age**. Stale only when a newer release is known (the sync
    icon's manifest re-read), the regulations' `valid_until` has passed, or the tiles and data are a mixed pair. New:
    a known newer release now marks both lines "Likely out of date" on the block AND the Details page (MenuHost gets
    `newRelease`), where before only the summary said it.
  - DFO 120 d, FWA 730 d, basemap 365 d, HYDAT 548 d: unchanged (upstream snapshot ages, each justified in the table).
- The sync DOT still means only "a tap or reopen fixes this" (newer release, feed not reached, device check > 30 min);
  a feed the server has not published lately and an old forecast are said, not dotted (a tap cannot fix them).
- Pinned: whole-string badges at a minute inside / past each line (menu.test, more.test, conditions.test); a mutation
  pin that the gauge feed and the regulations/map do not share a line (setting synopsis/tiles to the feed's 2 h age
  fails it; so does giving forecasts the feed's 2 h).
- NOT DONE: no "Forecasts" line on the More Data block — the feed index carries no feed-wide forecast issue time (only
  per-station files do). Adding one is a publish.py field (`forecastIssuedAt`, newest run) + the feed reader + a line;
  needs a feed republish, nothing heavier. The feed index's own `hydat.stale` (a newer HYDAT known) is read by
  @app/data but not yet used for the HYDAT row (still age-ruled).
## CLAUDE 2026-10-22 (owner delegated) — dark water ink back to blue
What read sea-green was the dark water-NAME ink, `color.label.water` (#5AB49C, LCh hue 173°). The refinement pass had put
it there to clear the label gate's floor (worst-reader ΔE00 12 from all five statuses). The stream and lake colours are
unchanged. The fix is one token at its single source, `app/packages/map/style/themes/dark.json`: **#5AB49C → #2498FC**
(hue 274°, the same hue as the light map's water #4A90E2 at 275°). `pnpm style:build` regenerated style.meta.json and
style.sha256. Palette D is otherwise untouched: no status, flow, lake or UI token moved.
- **The floor costs one pair.** No dark blue clears 12 from all five palette-D statuses. For protans, restricted's mauve
  falls toward blue; for tritans, open's mint falls toward cyan. Searching every blue at hue 250–290° that passes the
  other gates (4.5:1 contrast, the place-ink step, ΔE76 12 from every other map token), the best is 10.6–10.9. I chose
  #2498FC because its only pair under the floor is 10.6 from `status.restricted` under protanopia (Machado). Every other
  status pair stays 12+ (base 12.6, open 21.0, closed 31.5, own 43.4). The test lists that pair WHOLE
  (`UNDER_FLOOR` in tools/label-colours.test.ts), so a second pair or any further slip fails. A new test requires every
  theme's water name to be a blue: hue within 30° of 275° and chroma 30+. Mutation-checked: the old #5AB49C fails both.
- **Legibility (dark).** On its halo (`color.outside` #0A0B0D) the name is 6.54:1 (was 7.92; floor 4.5). Without the halo,
  on the lake fill #205579, it would be 2.64:1 (was 3.20). Labels always draw on the halo, so the halo carries
  legibility. Against place ink #D2D8E1 it is ΔE00 27.4 (worst reader 24.1; floor 15/10).
- **The flow ramp.** The name used to sit ΔE00 6.6 from `flow.f4` (normal, #3FCBAE), so a water's name looked like
  "normal flow". It now sits 6.3 from `flow.f5` (#63ACF2) and 10.5 from f6. Every ramp step is ΔE76 18+ from the new ink
  (the build gate's floor is 12), and the ramp itself is unchanged. On the Conditions map the names read as blue words
  beside the lines, not as one more line.
- **Shots** (phone, dark, from the running :39218 app with :39217 tiles; read-only. "After" repaints the three
  water-name layers `stream__label`, `lake__label` and `water_label_ocean` through `__map` with the new value, which is
  what the runtime theme paints): `shots/dark-water-ink-{before,after}-{map-z9,map-z12,conditions-z12}.png`.
- **Rebuild:** none for data. The app picks up the theme at runtime; the served :39218 app shows the new ink once
  it runs this branch.
## CLAUDE 2026-10-22 (owner delegated) — the release stamps `valid_until`
- SOURCE: the synopsis's own cover ("EFFECTIVE APRIL 1, 2025 THROUGH MARCH 31, 2027", repeated inside as "valid from
  April 1, 2025, to March 31, 2027"), read by ONE reader, `pipeline/common/synopsis_pages.synopsis_period`. No date is
  typed anywhere; statements that disagree, or a book that states none, or a missing PDF stop the build (an empty stamp
  would ship a release the app cannot tell has lapsed). `BOOK_TITLE`'s years are held to the read period by a test.
- ONE STAMP: the bundle's meta `valid_until` (`bundle/build.edition_valid_until`), which the release already copied into
  `manifest.regulations.valid_until` — so the bundle and the manifest cannot disagree. Lands with the next `deliver all`
  (not run here); today's live releases say `null`, and the app treats that as "no stamp".
- APP (core owns the words, one rule): `regsLapsed(validUntil, now)` — in force THROUGH the last day on the READER'S
  calendar (a B.C. day, like every day the app reads; the old UTC rule lapsed at 5 pm on Mar 31). The More tab's Data
  block line reads "Regulations · Valid to Mar 31, 2027" (the Data sources page says "Valid to" too); past it the badge
  says "Out of date" (not "likely": the edition has lapsed) and the Regulations page leads with "Out of date: these
  regulations were valid to <day>. Check the current Synopsis and the Province's notices before you fish." (the
  out-of-date dot, `accessibilityRole="alert"`). No stamp: no "Valid to" line (the edition's years, as before), never
  lapsed, no note.
- SUMMARY (review fix): a lapsed edition leads the Data block's summary as "Regulations out of date" — never folded into
  the "N likely out of date" count beside a line that says "Out of date"; other stale lines follow it
  ("Regulations out of date · 1 likely out of date", "… · 1 more in Details"). A newer release still outranks it.

## OWNER 2026-10-22 (late) — upstream updates the owner runs: HYDAT first
- Owner: "the feed should check for hydat releases then have a way for a notification to be sent that the hydat needs to
  be updated (user done) ... similar to bundle changes where the app checks when in service for updates. etc etc."
- Claude's design (owner delegated the details):
  1. DETECT: every feed run already asks HYDAT's listing (one GET). Keep it; record it as a MAINTENANCE NOTICE in the feed
     index (`maintenance: [{kind, since, current, latest, action}]`), one list for every owner-run upstream task.
     HYDAT is the first kind; ECCC roster drift (already detected) is the second. Others can join the same list.
  2. NOTIFY ONCE: a notice is sent to the owner the first time it appears (deduped by kind + latest, state kept beside
     the feed), through an optional webhook set by environment variable (works with ntfy / Discord / Slack style URLs).
     Off when unset: the console warning and the index notice remain. Nothing is sent to end users.
  3. THE OWNER RUNS ONE COMMAND (`scripts/update-hydat.sh`): fetch HYDAT, rebuild the climatology, republish the feed,
     and any bundle step HYDAT feeds. The notice clears itself on the next feed run once current == latest.
  4. THE APP PICKS IT UP LIKE A RELEASE: it already re-reads the feed while in service, so a new envelope reaches
     readers with no app change. The More "HYDAT" line reads the notice ("Newer release out · update pending") instead
     of guessing by age. No sync dot: a reader cannot fix it.

## OWNER 2026-10-22 (late) — back must leave a water, not replay every move inside it
- Owner: moving around sections and then hitting back takes many presses, "undoing" each move, before you reach the
  main view (Map · Conditions · Spots · More). Getting from a water's Regulations / Conditions / map page back to the
  main view is "extremely hard".
- Claude's rule (owner delegated the details): BACK STEPS OUT ONE LAYER, it does not undo moves.
  - Moving WITHIN one water (another section of it, its Regulations / Conditions / map tab, a sheet's own tabs)
    REPLACES the current history entry; it never adds one. Back from anywhere in a water goes to where the water was
    opened from (the main tab, a search, a spot).
  - Opening a DIFFERENT water from inside a water (a tributary, a receiving river) is a new layer: back returns to the
    first water once, then out.
  - Every water screen has a one-tap way out to the main view (a close control in its header), so even a deep chain
    is never more than one tap from the main tabs.
  - Same on the web (browser back), the in-app back control, and Android's system back (T50). The back-layers check
    holds this as a rule.
## CLAUDE 2026-10-22 — silent gauges hidden and named: the calls made
- **Silent = no reading AT THE HORIZON** (`donorSilent` in core, the same rule `donorReads` words): not on ECCC's
  real-time list, not reporting, or (a day ahead) no forecast reaching it. A gauge that reports today but cannot be
  ranked ("Reporting, but its reading cannot be ranked today.") SPEAKS and stays a row — hiding it would call a live
  gauge silent.
- **Hidden everywhere it is drawn**: table rows, route-map pins, routes, key entries, the route chain the map colours,
  and the camera/fit box. Not hidden from the chart's gauge switch: picking a silent gauge there still opens the quiet
  head ("not reporting today, last reported …"), which is the only place its own chart can be asked for.
- **One line under the table** (`silentLine`, core): "Not reporting: Liard River at Upper Crossing (last 14 Oct, 09:00
  PDT) · Coal River (not a real-time gauge)". Times are SHORT but still local and labelled (P16), so the zone is
  named; a day-only report says its calendar day ("14 Oct"); another year is named ("3 Mar 2024"). A not-real-time
  gauge adds its last data when known ("(not a real-time gauge; last 3 Mar 2024)"). A day ahead reads "No forecast this
  far ahead: …". Name = the route's station name, else the station id (before routes arrive).
- **"Has never reported"** is said only when the station's own file was READ and held nothing (`withLastReports` marks
  the row `noData`). A 404 or an offline fetch is NOT that (review 2026-10-22: `lastReport` now answers `undefined` for
  an unreadable file, `null` only for a read, empty one) — it and "not asked yet" say "(last report not known)".
- **Review 2026-10-22, one rule per member word:** the one percentile-less member still drawn reports with nothing to
  rank, so its pin, key and row label say "reporting, not ranked" / "reporting but not ranked today" (core
  `donorUnranked`), never "not reporting" beside a row that says it reports. The quiet head reads at the sheet's
  horizon (`quietHead`): a day ahead says "No forecast for this gauge that far ahead.", as the line does.
- **All silent** keeps the no-record (or no-forecast) refusal and the quiet head, and the refusal carries the same line.
- **Colour follows the member's index in the panel's weight order, not its shown position.** The gauge switch lists
  every member by that index, so the switch dot, the row and the pin are one colour by construction; silent members
  weigh 0 and sort last, so the speaking ones hold the first tones anyway and hiding one never shifts a shown hue.
  Pinned: `DonorPanel.silent.test.tsx` (a silent member between two shown ones; row dot == pin tone == switch tone).

## OWNER 2026-10-22 (late) — closing search on desktop
- Owner: on desktop it is confusing to close search: the X sits inside the search bar and the menu is far away. On
  mobile the current layout may make more sense.
- Claude's rule (owner delegated the details), WIDE layouts only (phone unchanged unless a defect is found):
  - Esc closes search, whether or not text is typed.
  - A click/tap on the map outside the search panel closes it.
  - The results panel carries its own visible close control at its top-right, next to the results, where the eye is.
  - The X inside the field is only "clear text", labelled so; it never doubles as close.
  - Search is a layer: back (browser / in-app / Android) closes it, matching the back-steps-out-a-layer rule.

## OWNER 2026-10-22 (late) — sloughs and canals look like streams in Conditions
- Owner: in Conditions, sloughs and canals with stream regulations get the SAME conditions look as streams. Nicomen
  Slough has both stream lines and lake polygons, but all of it is a stream — the book treats sloughs and canals as
  streams. Many such polygons are already marked; the rest are still to be found (the postponed canal/slough work,
  see BRAIDS-HANDOFF.md), and stays postponed.
- Claude's rule: the colouring follows the WATER'S KIND (a slough / canal item is a stream), never the geometry
  type. A polygon of a stream item paints with its stream's flow colour (its section / panel), exactly like the lines
  of the same water, on every Conditions view (map, legend, water's map). Lake-only treatment (level, lake fill) is
  for lake items only. Applies to the polygons already marked; finding the rest is a separate, postponed ticket.
## CLAUDE 2026-10-22 (late) — closing search on desktop: built
- WIDE (≥ 720 px, `searchPanelWidth`): the panel's ✕ at its top-right ("Close search") is the ONE close control; the
  field's left slot keeps the magnifier (no ‹ there), so the bar offers only the field's ✕. Escape closes from anywhere
  (a window listener in `SearchSheet`; a page dialog — `aria-modal` — keeps its own Escape). Escape typed IN the field
  is the field's own `onKeyPress` on every screen: react-native-web's input stops its keys at the field, so the window
  never hears them (review fix; pinned by "Escape typed IN THE FIELD"). A click on the map beside the
  panel closes search and does NOTHING else: no water opens under it, no pin drops (the Shell hands the map
  `onMapPoint = close` and no `onPressFeature` while wide search is open); a drag still pans. Closing restores the
  camera as every close does.
- The field's ✕ is "Clear search text" on every screen and only empties the field (pinned: mutating it to close fails
  `Search.shell.test.tsx` "THE FIELD'S ✕ ONLY CLEARS").
- PHONE UNCHANGED: ‹ in the field closes, ✕ clears, Escape in the field closes, the sheet has no close of its own, a map
  tap at half still opens the water. No defect found.
- Back: search was already route state (`route.search`, T48), so back already closed it; nothing in the back model was
  touched (a test now pins it on wide too).
## CLAUDE 2026-10-22 (late, owner delegated) — HYDAT notices, as built
- FEED (`publish.py`): `index.json` gains `maintenance` (always written; `[]` when nothing is due), sorted by kind.
  `hydat` when the listing names a newer release than `clim.json`'s (`current` = the envelope's release, `action` =
  `scripts/update-hydat.sh`); `roster` when ECCC's list drifts from the roster (`current` = the roster file, `latest` =
  `+joined -left` ids, `action` = rm + re-fetch + `pipeline build` via heavy.sh). `since` = the first run that saw the
  SAME kind+latest, carried from the previous index (a later HYDAT is a new notice, new `since`). Calls: a check that
  could not run (listing GET failed or `--no-release-check`; no station list) KEEPS the previous notice — one failed
  GET must not flip the app to "Up to date" and back; a kept HYDAT notice is dropped once the envelope reaches its
  `latest`. No envelope (`--clim` absent): no HYDAT notice either way. The old `hydat` block stays (nothing migrated).
- NOTIFY (`notify.py`): once per kind+latest; sent keys in `<feed dir>.notified.json`, BESIDE the feed dir (the dir
  itself syncs to the public origin). A seen key is never re-sent, so a roster set flapping back is not re-announced.
  Failure: not recorded, retried next run, never raises. Unset URL: one stdout line, only when a notice exists.
- SCRIPT: `scripts/update-hydat.sh` (not run). `pipeline build` is the whole rebuild (deliver reruns on the hydat
  stamp + clim hash; tiles are not keyed on HYDAT so skip themselves); upload of the new release is not in it.
- APP: `STALE_AFTER.hydat = {by: "notice", ms: 548 d}` — the notice decides, the age line only with no word.
  `hydatNotice(index)` (core): `due` = a hydat notice; `current` = notices present, none for hydat, and this run's
  `hydat` block names release AND latest; else null (older index, or a run that could not check) -> age. Read off the
  same index read as the sync state (poll + tap); not passed to `syncVM` (no dot).
- WORDS: the badge is **"Newer release out"** (`FRESHNESS_WORDS.newerRelease`). Not "Likely out of date" — the
  publisher SAW the release, nothing is guessed; not "Out of date" — the records in use are still correct, a release
  only adds the last year (that word is kept for a lapsed edition, which is wrong to fish by); "· update pending" from
  the design was dropped: a badge is one state, and "pending" promises a timing the reader cannot see or act on. The
  More block's summary still counts it with the other Details-only lines ("1 likely out of date · see Details") — a
  separate summary wording for one Details row was not worth a second rule.
- NOT DONE: the roster notice is owner-only (the app ignores it); no feed run/notification was made for real.

## OWNER 2026-10-22 (late) — Twin Lakes (MU 2-8): the closure must reach BOTH lakes
- Owner: "in region 2-8 only one twin lakes gets the closure it should get both the lakes."
- Cause (Claude): the curated override binds GNIS 30212 ("2 polygons"); both lake items carry gnis:30212, but the
  matcher's id index (`build_id_index`, pipeline/regs/matching/matcher.py) maps a ref to ONE item, first writer wins.
  That rule exists for side channels that inherit a mainstem's gnis (the Fraser case); for a lake drawn as several
  polygons that share their OWN gnis it drops all but one. The catalogue's `matched` then carries one wbk.
- Systemic: 15 catalogue entries curated to several polygons carry only the first (Illusion Lakes 1-6, Thetis 1-1,
  Twin Lakes 2-8 and 8-1, Garbutt 4-22, Tagish 6-27, Cameron Lakes 7-31, La Salle 7-3, Rainbow Lakes 7-52, Sundance
  7-32, Tatlatui 7-39, Yellowhead 7-1, Arlington Lakes 8-12, McCulloch Reservoir 8-10, Tee Pee Lakes 8-6).
- Rule: a typed id an override pins resolves to EVERY lake item whose own polygon carries it, when no item owns it as
  its id; side channels that only inherit a name's gnis still do not. Then backfill `matched` (dry-run first), reach
  rerun + deliver, binding diff reviewed before promoting.
