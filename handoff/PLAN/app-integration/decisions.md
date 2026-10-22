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
When the chosen quantity isn't recorded on the own gauge but the other is, the own gauge leads in that other quantity,
labelled ("Level — this gauge records level only"). Check and disclose tidal
or backwater influence where the record shows it (Port Coquitlam is tidal, with Fraser backwater). Build after option A,
before the data rebuild; the estimate stays pipeline-generated with the app mirror held equal.
