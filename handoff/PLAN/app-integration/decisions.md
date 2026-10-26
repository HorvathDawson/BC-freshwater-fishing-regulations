# decisions.md — owner questions, each with a default so implementation never waits

Rule: a ticket implements the **default**. When the owner answers differently, the "if not" column names the ticket(s)
to reopen and how big the change is. Each default is chosen so the other answer is a local change, not a rewrite.

| id | question | options | **default** (why) | blocks / touches | if the owner picks otherwise |
|---|---|---|---|---|---|
| **D1** | Navigation shell when a water is open — **ANSWERED by the owner 2026-10-20: the prototype layout, but the bottom bar is HIDDEN while latched to a water (water mode, app-data/APP-PREP.md §0, P13/P14); the four header tabs stay. The default below is superseded; T23 mounts in P14's `WaterMode`.** | (a) the prototype: the app's four bottom tabs stay (Map · Search · Conditions · Spots); What you can keep · Gear · Methods · Licence are header tabs over ONE scroll. (b) the 2026-10-10 plan: the bottom bar is replaced by Map · Regulations · Gear & licence | **(a)** — the later design, passed every reader gate 20b–20h; `Shell.tsx` already keeps `TabBar` under `WaterScreen` (:675), so (a) needs no shell change | T23, T29 | (b): T23 swaps the header tabs for a bottom bar inside `RegsScreen`; tab sections stay the same VM — ~1 day |
| **D2** | Where "Conditions" goes — **SUPERSEDED 2026-10-22 (owner call 3, T71): the view strip (FaceBar: back + Regulations · Conditions · Map) stays on every view and is the one Map entry; the regs header has no Map button and no "Conditions here" — see "D2 / D24 against the view strip" below. Was: ANSWERED by the owner 2026-10-20: no FaceBar; a "Conditions here" action in the regs header opens Conditions at the part's MIDPOINT section (`part_geo.mid_sid`, P09/P14), with "‹ Regulations" back. Placement: beside Map (owner). The default below is superseded.** | (a) keep `FaceBar` (Conditions \| Regulations) as the first row above the regs header, on both faces. (b) fold "Conditions" into the header's button group beside Map | **(a)** — nothing the app already settled moves; the regs header becomes a second row. Cost: ~44 px less list on the Keep tab; the pin bar sits under both rows (`PinBar` top = measured header height) | T23, T29 | (b): T23 removes `FaceBar` from the regs face and adds a "Conditions" `hgo` button; header height drops — pins re-measure automatically |
| **D3** | The palette's `sub` (the one secondary grey) | (a) adopt the prototype's (v33: `#555C62` / dark `#B0B8BF`) in `themes/{light,dark}.json color.ui.sub` (app-wide). (b) keep `#6C737A` / `#98A0A7` and re-run the 20g census on the app | **(a)** — the prototype raised it for glare (v33 `r20i-glare`, `--sub ≥ 6.5:1`): v33 `#555C62` on white 6.79:1 / dark `#B0B8BF` on the card 8.87:1, vs the app's `#6C737A` 4.80:1 / `#98A0A7` 6.72:1 (v32 had `#5F666C`/`#A3ABB2` — it moved once already, so always take the value from `app/fixtures/golden/tokens.json` at sync time, never from this page). One token edit + `pnpm style:build` + the cvd/contrast gates. `faint`/`wash`/`quiet` stay in the palette for the rest of the app but are **banned as text colour inside `ui-native/src/regs/**`** (T39 gate) | T22, T39 | (b): T22 drops the JSON edit; T39's census expectation for `sub` changes — values only |
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
| **D18** | Bait `also_allowed` (e.g. dead fish as bait in a circumstance) — **DECIDED (a) hidden, T81 2026-10-22 (owner delegated; see "CLAUDE 2026-10-22 (T81)")** | (a) hidden, as the prototype. (b) a note under the allowed item | **(a)** for parity; owner question for the next design round | T31 | (b): one note line in `baitList` |
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
| **D24** | "Conditions here" | **SUPERSEDED 2026-10-22 (owner call 3, T71): no header Map button, no "Conditions here"; Conditions is the view strip's.** Was — owner 2026-10-20: next to Map in the header; opens the midpoint section (`mid_sid`) of the part currently being read | — | P09, P14, T16, T23 | — |
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

- **CLOSED (owner 2026-10-20: permission is held; T81 records it)** **Image permissions** (GoFishBC, WDFW, What To Fish, Photo Ark) — a legal matter, not a default; T43 makes it a ship gate.
- **CLOSED (T81 2026-10-22: no longer true — the manifest's only `code: null` images are the two adipose drawings, which the hatch sheet reaches)** **The 8 `code: null` salmon images** in the manifest are unreachable from any VM (no code) — keep, recode, or drop?
- **CLOSED (T81, Q25)** **Prototype bug** (T14): a closed-all-year chain line is rewritten to "." (chw0_1010 and the other closed-all-year states); ported for parity, flagged.
- **CLOSED (T81, Q25)** **Prototype bug** `pinUpdate` throws on 25/134 states (a ledger row without `group`); the app's `pinChain` skips such a
  row. Owner/prototype agent: fix in the prototype so the goldens cover those states (re-run `sync.mjs` after).
- **D2 / D24 against the view strip (T23 review, 2026-10-21)** — the owner's D2 says "no FaceBar" and D24 puts a
  "Conditions here" button beside Map in the regs header, opening Conditions at the read part's `part_geo.mid_sid`. The
  later view-swipe work (68305a3d; G1-gestures.md "SWIPE CHANGES VIEW") made `FaceBar` the water's header on all three
  views (back row + strip "Regulations · Conditions · Map"), and the one-water-screen rule asks for the same header on
  every view. The build is therefore: the strip above the regs header, the header's Map button (Map offered twice),
  no "Conditions here", and the strip's Conditions opens on the TAPPED reach (or the first reach), not `mid_sid`. The
  strip itself is an implementer's reconciliation (waterViews.ts), not owner text, so T23 does not pick a side. Owner:
  (a) keep the strip on every view; drop the header's Map button (the strip's Map is the one), and say whether the
  strip's Conditions should open at `mid_sid` when the water was not opened by a tap (P14); or (b) D2/D24 as written:
  remove `FaceBar` from all three views (each needs another back and another way between views), add the header's
  "Conditions here" (`hgo` beside Map) → Conditions at `mid_sid`; the swipe keeps its order without a strip.
  **RESOLVED by the owner 2026-10-22 (audit plan call 3, "Keep only one Map entry (the view strip; drop the header's
  Map button)"): (a). The strip stays on every view and is the water's one Map entry; the regs header's action is Parts
  or nothing (core `headerOf`, no `canMap`), its 44 px slot kept. Built in T71; D2 and D24 are SUPERSEDED. Still as
  built, not ruled: the strip's Conditions opens on the tapped reach (or the first), not `mid_sid`.**
  **EXTENDED by the owner 2026-10-23 (T150, Q122: "dont we want to remove 'parts' from reg page since map is now at the
  top tabs?"): the regs header has NO action — no Map, no Parts, no 44 px slot (core `HeaderVM` has no `action` field).
  It keeps the part's NAME (the place label), so the reader always knows whose rules are shown. The parts are the
  water's Map view's: a collapsible "Show all N parts" list under the map (a radio group by name, the part shown
  checked; a pick is the map's part step, so the frame and the Regulations view follow). Tapping a part on the map
  still works. Golden: the header's Parts action is an exclusion with this reason (`GOLDEN_PARTS_ACTION_IDS`, beside
  `GOLDEN_MAP_ACTION_IDS`); header.parity asserts no action on every state. The Parts SHEET itself (core
  `partsSheet`, sheet key `parts`) is no longer opened by the app — only the web start link `sheet=parts` reaches it.**

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

## T49 default (2026-10-22) — which days the date sheet offers (OWNER QUESTION — DECIDED as built, T81 Q19)
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
OWNER QUESTIONS (defaults built) — DECIDED, T81 Q17/Q18 (below, "CLAUDE 2026-10-22 (T81)"): (1) should the search sheet keep the old "open on the best match while typing"
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
  5. FEED CLEAR IS NOT APP CURRENT (fix 2026-10-22): the feed clears once ITS envelope catches up, but the app's bundle
     (gauge_clim, gauge_stats, section_panel) moves only with the next data release — so HYDAT reads "Up to date" only
     when the notice is clear AND the release's `sources.hydat.date` is the feed's `hydat.latest` (same day); clear but
     the release older reads "Newer release out"; no release date or no feed word, the 548-day age line.

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
## CLAUDE 2026-10-22 — back steps out one layer: the calls made
- **One model, three inputs.** The rule lives in the route reducer only (`@app/ui` route.ts); the browser's history
  (`historySync`), the in-app ‹ and Android's BackHandler all count `backDepth`, which the reducer computes, so they
  cannot disagree. Sheets (fish, hatch, rule, day, legend, mark popup, fork chooser) stay back layers on top: back
  closes the sheet first, as before.
- **A move within a water replaces the frame**: a view (strip or swipe), a part step (▲ / ▼, a tap on the map), "Show
  regulations for this part", and a split lake's other closure area (it was an `openWater` push from "Show regulations";
  now the same `partAction` the step uses). The same water opened again from inside itself replaces too.
- **A water already in the stack is gone back to, not stacked again** (`enter`): A → B → A is A alone, so ▲ / ▼ to and
  fro across a name change (Vedder ↔ Chilliwack, the likeliest source of "many backs") never grows the stack. Crossing
  INTO a different water is still one layer (the brief: a different water). An unnamed reach is identified by its
  section.
- **The ×** sits at the right end of the header's ‹ row (the row keeps its 32 px height; 32 px drawn, 44 × 44 to the
  touch), on every water view, also at depth 1 (where it equals ‹), so it never moves. Icon: the set's existing
  `close` (Lucide `x`; no new licence line). Accessibility label: "Close, back to <tab>" — the tab the first water was
  opened from (`@app/core` `waterExit`, which also owns the ‹'s "Back" word now). It returns the browse state whole
  (search, open spot), like back from the first water.
- Pinned: `tools/back-layers.test.ts` "back steps out one layer" (walks through the real action builders; mutation pin:
  a part or view that pushes, a revisit that stacks, a close that only backs), `apps/mobile/src/back.test.tsx` (real
  Shell, Android and the browser: N moves → one back → the map, one history entry; A → B → back A → back map; × → map).
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

## OWNER 2026-10-22 (late) — a river that changes its name is ONE system for back
- Owner: "yes think of things like this as one system. bella coola atnarko is another example." (Vedder ↔ Chilliwack.)
- Rule: stepping ▲/▼ off the end of a water into the water it CONTINUES AS / FROM (a name change on the same
  mainstem, through a lake included) REPLACES the current step; back still leaves the whole system in one press.
  A true tributary (a creek's mouth on another river, "Flows into …" from a side stream) stays one extra step.
- CLAUDE (built): the cause was the pipeline, not the app. `build_panels` read the graph's NodeKind (a polygon is
  `lake:<wbk>`) as the water's kind, so all 146 stream-item polygons had no panel; 45 on 20 waters (Nicomen 4, the
  Vedder Canal on the Vedder River …) drew grey between coloured lines. Now `build_panels.stream_polygons` gives a
  STREAM item's polygon the panel + target area of the nearest panelled line of the same water (downstream first,
  never another water's), keyed on the item's kind — the one colouring engine then paints it like its lines, with no
  app change. Lake items untouched. Needs a `pipeline.deliver` rerun (no tile/atlas/reach rebuild). Lines boxed
  between polygons (Nicomen 59620) and finding the rest: T55.
- CLAUDE (review, measured on the REAL graph with the live `section_panel`, before any rebuild): the pass answers 38
  of the 45; the other 7 (Nation River 4, West Road (Blackwater) 2, Honna 1) sit among lines of their own water that are
  unanswered too — beyond lakes of other waters the walk may not cross — so they stay honestly grey with their lines.
  All 4 off-stem ones are answered; each answer is a (panel, area) pair of the polygon's own water; no lake or wetland
  section gains a panel. The bundle invariant is local: a stem polygon touching an answered stem line of its water.

## OWNER 2026-10-22 (late) — a polygon-only slough or canal gets gauges like a stream
- Owner: "if a slough or canal is fully polygon only it should still be able to get gauge info of neighbours if the
  propagation means it would have values if it was just a stream itself ... doesn't have to only be instances where it
  is stream and polygon, could just be poly."
- Rule: the panel walk reads the WATER'S kind, not the node's geometry. A polygon of a stream item is walked, sized
  and propagated through exactly like a stream line (its drainage area from the network it sits on), and is not a
  lake crossing. A polygon-only stream item (Seton Canal, Bowman, Lewis, Hansen, Taylor …) gets a panel whenever a
  line in its place would. True lakes keep lake treatment. Before/after panel diff on the real graph before shipping.
- CLAUDE (built, T55 point 2): `build_panels.flowing` — a node is a stream for the walk when it is a line, or a
  polygon of a STREAM item (`kind_of[water_of[sec]] == "stream"`, the mutation-pinned gate). Such a polygon is offered
  a panel, is not a lake crossing, can start a walk as its gauge's own section, is walked through by `own_distances`
  (at 0 m — a lake node carries no length), and is "same river" with sections of its own water (it has no `blk`).
  SIZE: its own `stream_magnitude`, which the graph sets to the largest magnitude of the network lines beneath it. On
  the real graph all 145 stream-item polygon nodes have one; against the line below it is equal on 92, smaller on 51
  (the line below takes in a confluence), larger on 1, and against the line above equal on 88, larger on 47 (tribu-
  tary mouths dip into it) — the through-line at the polygon, between its two lines, as a line there would be. Seton
  Canal: 3,314 (the Seton River line under it); Vedder Canal 2,198 (= the Vedder line below it).
  The old copy pass (`stream_polygons`) is KEPT AS THE FALLBACK for a polygon the walk leaves unanswered beside an
  answered line of its water — two on the real graph (West Road lake:329047577, Daly Slough lake:329522748; their
  lines answer through `mainstem_of`). Retiring it would take their answer away; with it nothing loses one.
- CLAUDE (measured, REAL GRAPH, read-only via heavy.sh; inputs rebuilt from the live bundle and checked: the old code
  reproduces all 246,619 live `section_panel` rows exactly, 0 differ). Old 246,657 answered sections → new 286,676:
  (a) stream-item polygons: answered 38 → 59 of 145 (21 gain: Camp Slough 11, Mountain 2, Hudsons Bay 2, Seton Canal,
  Stellako, Duncan, Bateson, Quaamitch, Lewis Slough; 33 change from the copied line answer to their own walk's).
  Polygon-only items: Seton Canal ANSWERED (Cayoosh Creek 08ME002 63%, Nicomen 08MH085/086 13% each, 08ME018), Lewis
  Slough ANSWERED (08NG049 60%, 08NG084 40%); Bowman Slough and Hansen Slough stay grey — the lines either side of
  them are unanswered too, so a line in their place would be grey. (b) lines gaining a panel: 79 beside a stream
  polygon (Nicomen 59620 now has 11 donors at 10.9 km²; Camp 11, Nelson 3, Zaitscullachan 3, Gravel 3 …) and 39,921
  elsewhere. (c) 31,768 other sections change members. LOST: 2 walk-only, 0 with the fallback. No lake or wetland
  section gains a panel; the simulated bundle passes both bundle invariants.
  WHY (b)+(c) IS SO BIG: 71,668 of those 71,770 changes are ONE cause — the two Nicomen Slough gauges (08MH085 at
  Deroche, 08MH086 at Dewdney) now walk up the slough, out of its head into the Fraser (modelled at ~27,000 km², inside
  MAX_AREA_RATIO of their 207 km²) and up every tributary within the ratio: the Stein, Cottonwood, McGregor, Bowron …
  Before, the slough's own polygons ended that walk as "lake crossings". Without those two stations the change is 85
  gains, 36 changes, 2 losses (the fallback's two). Biggest (c): Stein-basin creeks (Teaspoon, West Fickle, Shelter,
  Lean-to, unnamed) go from 08MF011 alone to 08MH085/086 ~42% each; Bell and Nelson Slough lines go from 08MF064 alone
  to ten small Fraser-basin gauges (Cale, Coldwater, Spius …) that walk down the Fraser and now through Nelson
  Slough's polygons (max share change 0.89).
  THIS IS THE EXISTING WALK, NOT A NEW RULE: a gauge on a side channel already climbs out of it into its parent river.
  Measured: forbidding the walk to step UP from a side channel into a bigger non-side-channel removes 29,520 answers
  from TODAY's panels (a blunter "never step up into something bigger" removes 32,877). With that guard in both, this
  change is +134 sections, 65 changed, 6 lost. OWNER QUESTION (not decided here): should a side-channel gauge speak
  for the river it branched from and that river's tributaries? Until answered, the slough behaves like every other
  side channel, as "a line in its place" would.
- CLAUDE (review, REAL GRAPH re-measured independently, heavy.sh): reproduced old 246,657 / new 286,678 (walk +
  fallback), 40,021 gained, 0 lost, 0 lake/wetland/unowned polygon gained; all 7,621 lake-item polygons stay lakes;
  Harrison Lake 08MG012 still lake-crossed on all 1,306 records. THE 71,668 IS LEVEL ONLY: 08MH085/086 report level
  and nothing else (5 years each), so Flow moves on 65 gains + 30 changes in all, none involving them; Level gains
  40,011 (39,907 answered by those two alone) and changes 31,747. The owner question is "should two 5-year level-only
  backwater slough gauges be the Level answer for 71,668 sections up the Fraser", not a flow question. The gauges stand
  on a LINE (355991886:0), and 140,598 of the 148,062 lake-flag flips were already lake-free over lines alone: the old
  walk's lake flag is ORDER-DEPENDENT (first visit wins in a DFS with a seen-set), and the slough polygons only hid
  the climb by being visited first. Still in the new walk: 2,074 records at 75 stations are flagged lake although a
  lake-free path exists (pre-existing, not fixed here).
  FIXED (review): 9 polygons of side-channel sloughs (Maria 3, Nicomen 2, McArthur Island, Gravel, West Road …) walked
  on their own 1.4 km² and took ten creek gauges up to 400 km away while the slough's lines either side carried the
  Fraser via `mainstem_of` — the Herrling Island artefact. `build_panels.side_channel_polygons`: a stream polygon whose
  nearest own-water line (downstream first) is an adopted side channel takes that line's answer, or none; 14/14 now
  agree with their lines. Pinned in `test_polygon_walk.py`.

## OWNER 2026-10-22 (late) — back from a searched water lands on the map, at the water
- Owner: after searching and opening a water (its regulations), back returns to the search list. It should land on
  the MAP at that location instead.
- Rule: opening a water FROM SEARCH consumes the search: the water's `from` is the map tab with search closed, and the
  map camera is framed on that water (the same framing the water's own map uses), so back and × both arrive on the
  map at the place just viewed. The search text is not kept. Opening from a spot or the map is unchanged.

## OWNER 2026-10-22 (late) — Main Lake / Mine Lake, and "See X" group entries
- Main Lake (Quadra) 1-15 ALSO applies to Mine Lake: "a gazette issue where it has both names really". Mine Lake gets
  both entries' rules (stamped f455228).
- Group entries that only say "See X" (Tee Pee Lakes "See Friday, Saturday, and Sunday Lakes"; Cameron Lakes "See
  North and South Cameron lakes"; Garbutt "See Norbury Lake") are POINTERS: the member lakes show their regulations
  under their own names; the group name leads the reader to those waters ("see X" with a way to open each). A "See X"
  pointer is never drawn as a regulation line on the lakes it points to. Group entries with real rules (McCulloch
  Reservoir) still apply to their member lakes; an identical rule from two entries reads once.
## CLAUDE 2026-10-22 (late) — "See X" groups, member lakes' own names, one rule from two rows: built (deliver-side)
- **Pointers draw nothing** (already true: a pointer-only row has 0 rules and binds nothing); now pinned on the member
  lakes themselves — Friday 8-6, North and South Cameron 7-31, Norbury 4-22 carry only their own rows, no "See …" label
  and no pointer entry in any of their rule sets (`test_see_groups.py`).
- **The group name leads to its waters**: the bundle's new `see_pointer` table (`rules.see_pointer_rows`): one row per
  named row of each POINTER-ONLY entry, in the book's order — the pointer's shown name, the named row, its shown name and
  the water opening it opens (its first match; NULL when it matched none). A TWIN (the same row in another region's
  table) is no group and is left out, as is a zone table's note (z4:kootenay_annual): 45 pointers, 51 rows. REVIEW: both
  names are the BOOK's heading without notes or quotes (`rules.see_name`), not `entry.name` — that read "Campbell River: see
  Campbell River" for LITTLE CAMPBELL RIVER and '"Jones" Lake', which search by "Jones Lake" never finds; Bighorn's two
  Wigwam River rows are one way in. The release copies it into `core` (additive, still `core/1`;
  NOCASE index). Search (`searchPointers`, both sources, one SQL) finds a pointer by its name or the START of it — never
  a word inside it ("lake" lists none) — ordered by core's `rankPointers`; the sheet draws a group "In the regulations"
  between Places and the waters: core's line ("Tee Pee Lakes: see Friday Lake, Saturday Lake, Sunday Lake", `seeLine`)
  and one button per named water ("Open Friday Lake") straight to it through `openWater`; a named row with no water is
  said in the line and gets no button. Not built: a pointer's "own page" (the 5 pointers whose waters have no other row
  show nothing there today) — none of the three named groups has one, its matches are its members.
- **Member lakes under their own names**: `entry.name` (so every "applies" line and `provenance.entry_name`) is
  `place_names.row_name`: the registry's name when it names the SAME water as the book heading (equal words; fewer —
  "Chilliwack River" for "CHILLIWACK / Vedder Rivers …"; a lake part's authored "X — Y"; or only words of the heading's
  note — "Williston Lake — Zone A"), else the heading without its notes, re-cased: Haynes Lake, Hydraulic Lake, Minnow
  Lake (not "McCulloch Reservoir"), South Cameron Lake (not "Cameron Lakes"), Peckhams Lake and Norbury Lake (not
  "Norbury Lakes"), Chuchi/Indata/Tchentlo/Tsayta Lake (not "Nation Lakes"), Witch Lake, Tee Pee Lakes. 77 water rows
  move (zone tables keep their authored scope); diff the answers' "applies" lines before promoting. Page TITLES are
  still the registry item's name (Friday Lake's page is titled "Tepee Lakes") — registry naming, not this.
- **An identical rule from two rows reads once**: `gear.once` — of vessel rules saying the same (`vessel_says`), the
  nearest (rank), then the page's order, is shown (REVIEW: the statement includes where it reaches —
  `extents`, `extent_text`, `unresolved`, `scope` — so a part's rule never folds into the whole-water one); the others stay in force and cited, in `repeats` ("All gear
  sources"). McCulloch's main polygon shows the engine limit once (Minnow's is the repeat), Norbury "No powered boats"
  once (Peckhams'), and after the reach rerun Haynes (McCulloch + Haynes) and Mine (Main + Mine) once. Non-vessel
  families already fold restatements (`rows.row_restates`). The app does not dedupe.
- **To ship**: `python -m pipeline.deliver` (bundle → … → store) then `python -m pipeline.deliver release`; then the app
  goldens/regs fixture follow the new store as for any deliver. Until then the 6 bundle tests in `test_see_groups.py`
  FAIL as predating (`see_pointer` absent) and `test_release.py`'s built-release tests stop at "b has no table
  see_pointer" — both by design.
## CLAUDE 2026-10-22 (late) — one system for back: built, and what the data says about the Atnarko
- **A route action, not an openWater flag**: `{ type: "cross", item, part }`, built ONLY by `crossAction` (waterMap.ts),
  reduced in route.ts: the frame is built as an open is (`opened`, Map in front) and REPLACES the top frame — the top is
  dropped, then `enter` (so a water already under it is still gone back to, never stacked). One back from either side
  of the Vedder ↔ Chilliwack leaves the system for where it was opened from; under a tributary (Jeperson → "Flows
  into" Chilliwack → ▼ Vedder) the crossings replace only the top, so back is the tributary. Nothing in browse mode.
  `openWater` / `exitAction` untouched: "Flows into" stays a push. The × is unchanged.
- Pinned: back-layers walks `cross` + `system`, mutation `crossPushes` (the cross as an `openWater` push again) fails
  both; route.test "one system"; waterMap.test "across waters"; WaterMode "across waters"; back.test "one system"
  (Android, and the browser: one history entry). one-water-screen: `"cross"` named only by route.ts + waterMap.ts.
- **The data (release r-66f94fa635c1, read-only):** Vedder/Chilliwack IS a continuation: `water_continue` row
  (Chilliwack ord 11095 part 1 → Vedder ord 7670 part 0), so ▼/▲ crosses it and the rule applies. The Atnarko is NOT:
  `water_continue` has no row for it (17 rows in all); it is reached only as `water_exit` (Atnarko ord 2628 →
  Bella Coola ord 561, `down_part` 0), i.e. "Flows into Bella Coola River", a push. WHY: the Bella Coola begins at
  the Atnarko + Talchako confluence (its part 0 "From the mouth up to the Talchako River confluence"; the Atnarko's
  lowest part "From the Talchako River confluence up to …"). Both rivers' main lines (blue lines 360879335 and
  360887426) begin at measure 0 at the top of the Bella Coola's main line (360885253, top 48 628 m), neither is on
  it, and FWA codes BOTH as tributaries of it (Bella Coola `910-275583`, Atnarko `910-275583-999439`, Talchako
  `910-275583-999074`). `continuation.py` refuses that on purpose ("two rivers meeting make a third, and neither is it"
  — the Somass/Thompson rule), so `item_down` alone links them.
- **No safe app-side rule in the release.** Tried: "the exit's receiving stream shares a regulations entry with this
  one AND the exit lands on its top-ranked part": 5 hits — Atnarko → Bella Coola (right), Hatzis → Lower Hatzic and
  Chilliwack → Vedder (already continuations), Fraser River → Annacis Channel (WRONG: and the Fraser's `water_exit`
  naming Annacis Channel is itself a defect worth a look) and Squamish Powerhouse Channel → itself. An entry that
  "includes tributaries" (the Atnarko/Bella Coola entry is held by the South Atnarko too) makes sharing no proof.
  Nothing implemented.
- **PROPOSED (not done; a pipeline change + release, owner's call):** let `continuation.py` pick ONE water at a
  two-river confluence instead of refusing it, when evidence names it: (1) the synopsis's own pairing — one entry
  that names both waters as one ("Atnarko/Bella Coola Rivers", "Chilliwack/Vedder Rivers"; the entry id carries both
  names), with `item_down` A → B and A's mouth at B's top (the existing STUB_M gate), the other river at the
  confluence then stays a tributary; or (2) without a pairing, the inflow whose FWA code joins B nearest its top
  (`-999439` over `-999074`) only when its upstream length / Strahler order is also the larger — else keep refusing.
  (1) alone covers both owner examples and cannot produce "continues as two waters". The app needs no change: a
  new `water_continue` row makes the Atnarko's ▼ read "▼ Downstream into Bella Coola River" and cross in place.
## CLAUDE 2026-10-22 — T53 built: an unnamed stream opens its Regulations (supersedes (2) under "one water screen")
- **App only, as the diagnosis found** (the pipeline already answers every unnamed section). A frame with `item: null`
  offers Regulations · Conditions (`@app/ui` route.ts `REACH_ONLY`); the water Map stays a named water's (it frames a
  water's parts). From the Map tab Regulations is in front, from the Conditions map Conditions; asked for Map, the
  frame's first view (Regulations). Back / × unchanged: a view move replaces the frame and an unnamed reach is matched by
  its section, so one back leaves it. Mutation pin: restoring `REACH_ONLY = ["conditions"]` fails 6 tests (route,
  openWater, gestures, WaterMode).
- **The page is the store's**: `WaterScreen` takes `item: null` + the tapped section; `useRegsTarget` answers
  `{kind: "section", handle}` for it (T09's section branch). No regulation is assembled in TS.
- **Title**: the store's name ("Unnamed stream · Region 1", `place.names[kind]`), on the Regulations header and now on
  the Conditions view too (`useUnnamedName`, the same `unnamedName` read): one water, one header. The header's label slot
  says where it flows when known — core `downstreamLabel`: "Tributary of <water>" for a stream, "Flows into <water>" for
  an unnamed lake or wetland (sentence case: it stands alone on its line). The water is the first NAMED section down the
  flow pointers (`@app/data` `downstreamWater`: `traceToGauge`, then `waterFor` past unnamed sections, at most 50). There
  is no line when the pointers end; they exist only inside gauged watersheds, so most unnamed reaches get no line.
  Geography only.
- Tests: the ticket's three examples run on the live release (`conformance/src/regs/unnamed.release.test.ts`, skipped
  without a release): 1408 shows the Eve River tributary closure, 64035 the Salmon River tributary closure, 59506 the
  base only. The committed fixture holds none of the three, so the render test uses its unnamed 634 (Harris Creek
  tributary rules) and 817454 (Region 1 base only).
## CLAUDE 2026-10-22 (late) — back from a searched water lands on the map, at the water: built
- **Route** (`@app/ui` route.ts, one line in the reducer): the `openWater` action carries `fromSearch` (set only by the
  opener's `searched`, i.e. a search result's open action, wide panel and phone sheet alike). From browse it makes `from`
  the map underneath with `search: false` — the tab search was over (the Map tab, or the Conditions map when searched
  there). Every water stacked above shares that `from`, so back from the first water and × from any of them land on the
  map. The back depth is the same as search alone (search open = 1, the water = 1), so the browser keeps its ONE entry:
  no orphan entry for the closed search, nothing pushed or dropped (back.test, web History).
- **Map tap beside the half sheet is unchanged** (a tap, not a result): search stays under the water, and the ‹ / ×
  now say so — `waterExit(depth, tab, overSearch)` names "Search" ("Close, back to Search") when `from.search` is open,
  so the label names where back actually goes. A water opened from a result names the tab ("Map"), as it should.
- **Camera** (Shell): the open from a result keeps the framing search gave the water (`session.focus`: its own outline
  box, `refine` as search had it; the bundle's `waterBox` when search had not placed it yet) as `arrive`, and does NOT
  restore the camera from before search. The browse map, coming back, is handed it as its `fit` (key
  `arrive:<item>:<n>`, after a spot's "Show" and before a close's restore). Forgotten at the next move of the route after
  landing, so a later water tapped on that map and left again does not drag the camera back. The search text is cleared
  (`NO_SEARCH`): the search is consumed.
- Pinned: route.test (whole route values both tabs, back / flow+back+back / close, depth), waterViews.test (wording),
  Search.shell (back, ×, wide; the fit the map receives == the box search framed it with == `waterBox`; sent once; map
  tap keeps search and says "Search"), back.test (Android: back → map → Android leaves; browser: one entry, back and ×).
  Mutation pins run: keeping `search` in `from` fails 7 tests across the three files; dropping the `arrive` fit or the
  label's `overSearch` fails 5 in Search.shell.

## CLAUDE 2026-10-22 (T81, owner delegated: audit call 11) — the smaller pending questions, settled
Owner call 11: "Claude makes judgement calls on the ~30 smaller pending questions and records them". Every row the
tickets README "Owner questions" table marks **delegated**, plus the stragglers left in progress notes, is decided here:
the call, why, how to undo it. Calls that need code name the ticket that carries the work; nothing here changed code.
Owner-only rows (Q1–Q7, Q9, Q10, Q13, Q26–Q44) are NOT decided here and stay in the README table.

| # | question | decided | why | undo |
|---|---|---|---|---|
| Q8 (T76) | A gauge across a `water_continue` link counts as the water's own? | **Yes, `water_continue` only** | a continuation is the same water under another name (owner: a river that changes its name is ONE system); tributary, side-channel and receiving links are other water | one link-kind set in T76's rule |
| Q11 (T80 item 3, D19) | klmb_1010's pooled row has no `group` | **(a) a pipeline-side `group`** | the backend answers, the front end only reads (owner rule 2026-10-21); (b) ports ~250 lines of old card path for one row | (b) as a new ticket |
| Q12 (T82) | O3: timed caveat look; O5: sheet height | **O3: the same caveat box as the other top caveats. O5: size to content, capped at the prototype's height** | one caveat look; a sheet taller than its words is empty space on a phone | O3 one style; O5 one max |
| Q14 (T83) | Region/MU boundaries above or under the status lines | **Under the status lines, above the fills** | the status line is the answer; a grey line across a closed stream reads as a different status. Fills under the boundary keep the region readable over lakes | layer order in the style |
| Q15 (T83) | Warm temperature dot reuses restricted plum | **Its own hue, not a status colour** | a plum dot beside a plum line reads "restricted"; one meaning per colour | one token |
| Q16 (T83) | Sub-20 contrast pairs (thin lines, permit/temperature, dark boundary 15, dark closed/base 19.1) | **Lift where cheap; else a recorded exception listed WHOLE in the gate test** | the dark-water-ink precedent (`UNDER_FLOOR`): a named exception list fails on any second slip | edit the list |
| Q17 (T48) | Keep "open on the best match while typing"? | **No** (as built) | the full sheet covers the map, so moving it under the typing is unseen work and a jumpy cancel; the map follows picks only | pass `follow = true` to `useSearchView` (ui `search.ts`) |
| Q18 (T48) | "Add a spot here" wording | **Closed** | the owner removed it (2026-10-22: "where you are or select on map"; core/ui `mapRail.ts`) | — |
| Q19 (T49) | `dayRange` | **This calendar year, cut at `regulations.valid_until`** (as built) | the data is one year with no year in it; another year's date would silently read as this one's | one function (`dayRange`) + its tests |
| Q20 (D18) | Bait `also_allowed` | **Hidden** (as built, D18 a) | parity; the circumstances are long and rare, and the bait list's source line already leads to the book | one note line in `baitList` (D18 b) |
| Q21 (T44) | Name size on Conditions/Map; desktop column; Map full-bleed; wheel outside the column | **As built**: one 17 px name row on every view (`WaterTitle`); one 1,280 px frame, with the regs reading column 560 px inside it (T45); the Map canvas full-bleed under the header, its words on the column; the wheel as built | the owner's complaint was views that moved; T44 measured nothing moves at 390/768/1440 | `WaterFrame` tokens |
| Q22 | The selected-tab cue | **As built** (already recorded: "CLAUDE 2026-10-22 (owner delegated) — the selected tab cue") | — | see that block |
| Q23 | Dark water names near `status.base` / flow f5 | **As built** (#2498FC, "dark water ink back to blue"): 6.3 ΔE00 from f5, every status pair 12+ but one listed exception | labels draw on their halo and read as words, not lines; T83's gates revisit the pairs | one token |
| Q24 | p15 continuations | **Answered by the owner** 2026-10-21 (late) — already in README "Answered" | — | — |
| Q25 | Prototype bugs: closed-all-year "."; `pinUpdate` throws on 25/134 states | **Closed: the prototype is a reference.** The app already does the proposed fix for both: core `chainText` keeps the sentence when the rewrite leaves no letters; `pinChain` skips a row without `group` | the goldens are not edited; the app's own tests pin its behaviour | — |

Stragglers (questions raised in implement_progress.md as merges landed, never in the table). Each **as built**, since
the owner's later word "the prototype is a reference, not the end-all … Claude decides" (OWNER 2026-10-22) covers them:
- T38 (hatch sheet): caption before the image, no tile behind the drawings, side by side on wide. D4 tile part already
  answered by the owner.
- T42 (edge states): tabs on a part outside B.C.; the tidal Licence box (D33 b). The caveat look is O3 (Q12 above).
- T43: D15's permission gate is moot (owner: permission held); the 18-line Fish pictures foot stays.
- T45/T46 (desktop): the 1,280 frame + 560 px reading column; the date pill in the side column; regs sheets as centred
  dialogs (map sheets stay bottom sheets); fish looks side by side in landscape where they fit; small notes at ~103
  characters a line.
- T39/T40: the Keep colour bound 7; the ⓘ label wording ("What does <term> mean?"). The rest was answered by the owner
  (OWNER 2026-10-22 — answers).
- T49: the calendar button stays in the strip; weeks start on Sunday.
- Map chrome (implement_progress.md "MERGED ~23:30: MAP CHROME … OWNER QUESTIONS"; T81 review), each as built:
  rail square vs round — superseded: the rail holds Layers only (T48, "OWNER 2026-10-22 — answers"), a box in the
  palette's `r.box` radius with Lucide icons; native "At my location" — the location module when present (owner
  2026-10-22, SPOTS); a long press on the Conditions map drops a spot too — yes, the Shell wires `rail.pressLong` on the
  one map whichever tab is in front; back at a tab root goes to the Map tab first, and from Map leaves the app (ui
  `route.ts` ONE BACK RULE; More returns to `prev`); back on Search closes search, and inside a town's list first
  closes the list (`SearchSheet` `useBackLayer` → `searchStep` "back") — a selected water row is not a layer of its
  own; the synopsis date is the release manifest's `sources.synopsis.date` (core `menu.ts`/`more.ts`); in-season
  changes are NOT ingested — Data sources says "Not read" and the menu says to check them first (ingesting them is a
  new feature for the owner to raise, not a default). App name and contact are owner-only Q26.
- "Level may move into the layers menu": done (476b06d8) — closed.
- decisions.md "Owner questions this spec could not settle": image permissions (owner: held), the 8 `code: null` salmon
  images (no longer in the manifest), both prototype bugs (Q25) — closed and marked there.

Undo for any straggler: say which; each is one component's change, and the ticket that built it names the files.

## CLAUDE 2026-10-22 (T76, owner call 10 + T81 Q8) — own gauge across a continuation; the five own-gauge constants
- **The rule**: a gauge on a `water_continue` predecessor or successor is a section's OWN gauge — one system (owner:
  a river that changes its name is ONE system, 87ba267c), `water_continue` only (T81 Q8): a tributary, side channel or
  receiving river is other water. Same quantity, same far/area gates (`panel.may_lead`); the distance runs along the
  water through the name change; a chain (A as B, B as C) is one system; a lake still ends the walk.
  `build_panels.own_distances(…, continues)`; mutation pin `test_the_system_is_what_holds_the_vedder_test`.
- **Ordering in the bundle**: the panels are built while the graph is loaded, before `entry` exists, so the
  continuations are settled there — `item_down` + `continuation.links` moved before `_gauges`, the book's pairings read
  from the catalogue (`rules.read_entries`, the same sources and registry `rules.write` reads). `continuation.write`
  settles again from `entry` and stops the build if the two differ (`built_with`).
- **The five constants, as values (call 10 kept them; `panel.py`, pinned by test_own_gauge.py)**: lead share
  `OWN_LEAD_SHARE` = 0.7; much-smaller ratio `OWN_SMALLER_RATIO` = 10; far fraction `OWN_FAR_FRACTION` = 0.2 of the
  Hack length; far floor `OWN_FAR_MIN_KM` = 10 km; area trust `OWN_AREA_TRUST` = 10 (published vs modelled catchment).
  (Also `OWN_LEADS_PER_QUANTITY` = 4.)
- **Real graph, before → after** (live atlas graph + live bundle donors, the 17 live `water_continue` rows plus T59's
  Atnarko → Bella Coola): 41 sections change, all on continuation waters (Chilliwack 15, Vedder 2, Atnarko 20, Bella
  Coola 4); 0 elsewhere; no member added or dropped (only `lead` ranks); 4,713 → 4,714 panels. Vedder sid 313838:
  08MH047 (its own, retired) rank 1, 08MH001 rank 2, 08MH055 rank 3 — the app leads with the best-ranked REPORTING,
  so 08MH001 leads; sid 313839: 08MH001 rank 1. Atnarko sections gain the Bella Coola gauges 08FB007 / 08FB002 / 08FB011 at ranks 2–4 behind 08FB006;
  the Bella Coola gains 08FB006. Undo: pass `continues=()` in build.py.

## CLAUDE 2026-10-22 (T128, owner wording; T127) — a sub-cap line names its total; where the total is named once
- **Owner (T128)**: "Of your 5 trout and char a day in streams, up to 2 may be hatchery brook, brown or cutthroat trout."
  Every kind line of a card's tree now names the total it is part of — the nearest total, stream (lake) share or water's
  own rule above it (a count or size tier is part of one): its number, the group's fish (the group pool's written name:
  "trout and char", "trout" for Region 1), "a day", and where it holds only where the total is limited to one ("in
  streams" / "in this stream" for a share; "under this river’s rule" for a water's own rule; "in streams" for a zone total
  printed for one kind of water — no live group has one today, z8 r5 is a one-kind row with no group). "only N" where the
  kind is held below that total, else "up to N". One decision: `tree.total_named` (which tier) + `tree.total_words` (its
  words); the gate proves the named number; `test_answers_t128_subcap_total.py` pins "Of your " in one place.
- **Where the total is named once (the decision the ticket asked for)**: IN FULL on the header and the tier lines only
  ("4 a day per angler across Region 2, lakes included", "Only 2 of the 4 may be from streams") — the definition: per
  angler, across whom, lakes included. A kind line names it SHORT, by reference ("Of your 2 trout and char a day in
  streams, …"), never in the header's words, so the two never repeat word for word. A kind that is the ONLY kind of its
  total at the total's number names nothing (whitefish "Up to 15 may be whitefish" under "15 a day per angler across
  Region 2"; Kootenay Lake — Main Body's rainbow 10; a kind folded into its share/own tier): its tier's line already says
  the whole of it. Supersedes ROUND UR7 2's "(part of the 2 from streams)".
- **Review (T128)**: a water's own rule holding only SOME of the group's fish is no "your N trout and char" — Kamloops
  Lake ("Rainbow trout daily quota = 2", "bull trout daily quota = 1") read "Of your 3 trout and char a day under this
  lake’s rule" right above "Of your 5 trout and char a day". Such a tier is named by its number and whose it is: "Of the
  3 a day under this lake’s rule, only 1 may be Dolly Varden/bull trout" (`tree.total_fish`; Adams Lake, the Duncan shape).
- **Kinds under a count tier** ("1 cutthroat or rainbow trout in all") name the total around the count tier, not the
  count ("Of your 2 trout and char a day under this river’s rule, only 1 may be hatchery cutthroat trout"): the count tier's
  own line says the 1 in all, directly above.
- **App**: the Keep tree reads the store's words as before; `tree.ts` `kindWords` only chooses which number is bold — the
  kind's ("up to 2" / "only 1"), not the total's. No section-version bump (the frames' shape is unchanged; Q below).
- **T127**: a pool the reader DISPLACED for a fish is no "counts toward this one" cross-reference. `rows.counts_toward`
  (the pool governs the fish, or holds it as `contains` — the reader's "who governs this kind here") now decides the
  cross-references, the group's kinds and the outer total; before, the cross-reference also took `replaced`. Perry Creek:
  the St. Mary's "1 on all tributaries" row lists no brook trout; Perry Creek's own "brook trout 20" row is held by nothing
  and is a one-kind group of 20 (T80 3). The gate's `_kind_n` needed no special case.
- **Owner questions (defaults built; in the T128 ticket)**: T128-Qa — a kind under a water's OWN total that is the root ("This creek’s rule:
  up to 1 a day per angler") also says "Of your 1 trout and char a day, up to 1 may be …" — keep, or name nothing when the
  kind's number equals the total? Default: keep (one rule everywhere). T128-Qb — bump the rows section version (32 → 33)
  for the wording? Default: no (shape unchanged; the app reads the words whatever they say).

## OWNER 2026-10-22 (late) — release size limit raised (Q55)
- "we can raise the size limit". The release total gate goes 14,000,000 → 15,000,000 B gzip (TOTAL_GATE,
  pipeline/deliver/release/build.py). Private land stays in the access file. Per-file gates unchanged.

## OWNER 2026-10-23 — side-channel gauges (Q44), the order guard (Q53), the eight unlocated waters (Q45)
- **Q44 / T101:** a gauge on a side channel or slough never answers **Level** for the river it branched from (level
  is local to the backwater). It answers **Flow** for that river only when the side channel carries a large share of
  the river's flow; otherwise no. The side channel and its own polygons still take their own gauge. T101 builds the
  rule in one function, measures it against the 29,520 live answers that rely on a side-channel gauge today, and ships.
- **Q53 / T102:** as built. The WSC code decides parent/child in the tributary walk; Strahler order only decides
  between equal or unrelated codes (28,359 floodplain/braid edges).
- **Q45 / T89:** the eight waters printed in an MU holding no registry water of that name stay unbound and listed
  (AWAITING_WATER). A same-name water elsewhere is never bound.
- **Q6 / T67 (braids):** not yet — braids stay paused; the owner reads the assessment first.

## OWNER 2026-10-23 — curation review plan approved (Q5, Q34–Q41)
"The review app changes seem good. We could also maybe make it do salmon review. But for now it seems good. Make sure we
could add splits and rebuild then continue review with the new splits etc."
- handoff/PLAN/curation-review/PLAN.md is approved with every §6 default (Q34–Q41): start link backs the pane (deep
  link first), reviewers mark only (fix mode owner-only), overrides/steelhead/cross-refs/DFO/name variants not in this
  pass, per-entry marks (per-rule for zone entries, per-point for splits), no sampling, ingest ledger retired, the repo
  PDF is the book (sha256 on every mark), no review before phase 3.
- Splits mid-review are a requirement: add/move a split in fix mode → printed rebuild → the tool picks up the new
  vintage; only marks whose binding moved go stale ("binding moved"); the new split is queued in S0. Built across T97,
  T133–T136.
- Salmon (DFO) review is a later pass: T132.

## CLAUDE 2026-10-23 (T82, Q12 carried out) — the timed caveat look (O3) and the sheet height (O5)
- **O3: the app's caveat box is the one look; the prototype's grey box is not a design.** The grey 56 px box around
  x15p2's hours is the prototype's location-bar rule `.where{display:flex;min-height:56px;padding:8px 16px;
  background:var(--wash);border-bottom:…}` (regs-v11.html l.408; no v38 page draws that bar) leaking into the caveat's
  `<span class="where">`, whose own rule `.caveats .where` sets only font-size and colour. Measured in the staged
  prototype: x14p2's "On one side of the channel only" item gets the same box; "Closed to some anglers" items have no
  where line. The app already drew every top caveat (timed, side, angler, check) in one `CaveatBlock` box — the 3 px
  restricted rule, the sub-grey label, the where line in sub grey — so nothing changed in code. Pinned:
  `Edges.test.tsx` "O3" (whole style of block, kicker, item and where equal across the four kinds; the where line has no
  background, padding or min height). Undo: give `.where` a box in `CaveatBlock` for every kind, never one kind.
- **O5: sheets stay sized to content, under the prototype's cap; the title line tightened.** `regs-heights.mjs
  --sheets` over every golden sheet of vedder_1010, klwa_1010 and bc1_501 (192; the 15 licsrc person sheets have no
  prototype sheet to compare): BEFORE (Vedder, 65) the median app sheet was the prototype's height (Δ 0 px), 26 of 60
  taller; AFTER (all 192) the median app sheet is 6.9 px SHORTER, 69 of 177 taller (rule sheets: median +15 px, their
  chrome +40). The app's chrome is +21 px median (the 28 px foot under the body, kept: the stand-in for the phone's
  home-indicator area the Modal does not inset); its bodies −26 px. The tallest app sheet is 722 px against the
  prototype's 743 (its cap, 88 % of 844); both cap the same 19 sheets. The app's cap is the body at 0.68 of the window
  (≈ 666 px with plain chrome), kept under the prototype's on purpose (0.82 covered the water's header, T37). What made a
  sheet open taller than its words was the title: 19 px on TYPE.screen's 27 px line, 4 px of air per line, and most
  rule titles wrap. Now `SHEET_TITLE` 19 / 23 (both Sheet branches; the app's other 19 px heading is 19 / 23): every
  measured app sheet moved 4–12 px shorter. The rest of a rule sheet's extra (a 19 px title wraps where the prototype's
  17 px does not; the sub line under the rule) is the one Sheet's style (T41 Q4: one Sheet for map and regs) and stays.
  Pinned: `SheetSize.test.tsx`. Undo: one constant.

## CLAUDE 2026-10-23 — owner-question triage (owner asked: "double check all the owner questions to see if some are not necessary")

Every open question was classified (owner-only / Claude can decide / answered), then each non-owner call was challenged by a second agent
(default to owner when unsure). The owner reads this record and can reverse any line. Format: Qn (ticket) — decision; undo.

- **Q1** (T54) — decided: permit-only areas stay Tier B with the 'where a permit is needed' note, as built; undo: move the permit kind into Tier A in access.py / core accessNotice.
- **Q3** (T54) — decided: lake fills inside Tier A areas take the fourth colour, as built; undo: exclude lake/wetland from the no_access paint rows.
- **Q46** (T57) — decided: no change; an unnamed lake at the top of a reach stops the walk like a named one; undo: let the tributary walk climb through unnamed lakes at the reach top (reach walk), then reach rerun + binding diff.
- **Q48** (T78) — decided: flag a gauge only through HYDAT remarks or a tidal.json row carrying a checkable reason, and add 08MH026 and others once a reason is found; undo: add rows to data/curated/gauges/tidal.json.
- **Q49** (T78) — decided: disclose 08HB074's standing backwater with its own Level sentence (not the tidal one); episodic ice, beaver and slide remarks stay unread; follow-up ticket; undo: drop the backwater row and the sentence.
- **Q50** (T80) — decided: search lists Kootenay Lake's three parts and has no parent result, as built; undo: allow pieces = 0 parents in SEARCH with a parts-list page.
- **Q51** (T80) — decided: a 'See X' row opens X's page and gets no page of its own, as built; undo: give pointer-only waters a stub page.
- **Q52** (T80) — decided: keep the focus fallback armed everywhere, as built, and recheck in T77 D1a; undo: arm it only where the input lacks a 'cancel' event.
- **Q54** (T54) — decided: keep the recorded per-theme exceptions for the no-access colour; undo: edit the UNDER_20 list and the token.
- **Q56** (T51) — decided: today + 4 days, as built; undo: shift the day list in core likeVisit.ts to +1..+5.
- **Q57** (T51) — decided: flow 45 %, air 20 %, rain before 20 %, wind 10 %, cloud 5 %, as built; undo: change the weight constants in core likeVisit.ts.
- **Q58** (T68) — decided: --stations copies the refreshed default roster over the given file, as built; undo: skip the copy for a non-default path in roster_action.
- **Q61** (T52) — decided: Change colours by the ratio to the model's own today, and the panel words give the percentile move; undo: switch the Change domain to the percentile move (app-only, no paint call)
- **Q64** (T104) — decided: when_words says 'noon'/'midnight' for 12:00/00:00 to match the book, shipping with the next export rebuild + deliver; undo: revert the when_words special case
- **Q65** (T104) — decided: leave dates.When.words until the licensing labels are reworked, then route it through when_words and pin it; undo: route it now and accept the 2 changed labels
- **Q66** (T120) — decided: the pipeline ships the leading origin per fish (common.lead_origin) in the store/release and the app's mainRes only reads it, tracked as a ticket, not left as a pinned TS mirror; undo: keep a TS mirror pinned by parity under T125
- **Q67** (T130) — decided: one answer per zoom, rivers from z8 and the heat map below, with no per-order field under the big rivers; undo: add a per-order basin field below z8 (tile window)
- **Q68** (T130) — decided: flow arrows keep their own z10 floor (as built); undo: lower the arrow layer's minzoom to the z8 handover constant
- **Q70** (T131) — decided: a tap on stacked spots opens the Spots list; undo: fan the group out on tap
- **Q71** (T131) — decided: a spot opened from a water's map sits over the water, and its tab bar leaves the water for that tab; undo: keep the water's tab context while the spot is open
- **Q72** (T131) — decided: drop a blocked selected name like any other label; undo: place a blocked selected name below its face
- **Q73** (T131) — decided: spot pins draw over gauge pills and win the tap (as built); undo: draw spots under gauge pills or offset the pill
- **Q74** (T126) — decided: gauge dots follow the map's horizon, and a day no run reaches draws no dot; undo: keep dots at Now with a legend note
- **Q75** (T126) — decided: the Change dot label is '<ratio>× today'; undo: reword changeDotLabel
- **Q76** (T106) — decided: a record with no declared quantity reads as discharge; undo: refuse undeclared records
- **Q77** (T107) — decided: a value exactly on p10/p90 reads 'at or below/above the 10th/90th percentile', including a dry creek at 0 when p10 is 0; undo: switch both percentile_of and percentileInBand to strict < / > and republish the feed
- **Q78** (T114) — decided: no raw-name fallback, and the app and release ship together; undo: add a withArea-style fallback for older bundles
- **Q80** (T97) — decided: the binding counts km to 0.1 per named water, plus sections in no named water; undo: add the section count back to the fingerprint
- **Q81** (T97) — decided: a split added or moved on a water stales every entry matched to that water ('binding moved'); undo: stale only rules whose reach changed
- **Q83** (T101) — decided: LARGE_SHARE = 0.5; undo: change the named constant in build_panels.py and redeliver
- **Q85** (T133) — decided: show the awaiting-build binding labelled 'preview', with P blocked; undo: hide the binding until the build
- **Q86** (T133) — decided: moved splits are detected from the next --atlas build, which records split anchors; until then only added split ids are checked; undo: a migration that records the current anchors
- **Q89** (T110) — decided: the 1,302 sections go silent ('no baseline') under the same MIN_TOTAL_WEIGHT as every panel; undo: a Level-specific floor
- **Q92** (T105) — decided: keep 'This lake has its own gauge, read here directly; nothing is estimated for it from other water.'; undo: change the one core NoEstimate 'own-gauge' sentence
- **Q93** (T105) — decided: no global fixture panel; tests that need a chart add one locally; undo: add a Vedder Crossing panel to fixture.ts (about 60 test call sites)
- **Q94** (T134) — decided: S0 = 150 (a leading auto-proposal tag or a 'Verify:' note, plus moved/stale splits); undo: count any note containing VERIFY in tiers.split_needs_verify
- **Q96** (T134) — decided: no partial 'text and rules verified' mark; while an entry awaits its build, the reviewer can record wrong, unsure or deferred; undo: build it as T142's leftover
- **Q97** (T134) — decided: T134 DONE; the ≤1.5 min human target is checked by the T136 pilot (Q114); undo: reopen T134 until pilot-report
- **Q98** (T115) — decided: 356364114:699253 drops zp:steelhead r1b/r2b/r4b and steelhead_targeting_known, as built; undo: revert ONE_REGION_MEMBERSHIP for the steelhead twin's area_id
- **Q101** (T117) — decided: 1150 stays neither main flow nor side channel for the gauge panels; undo: add 1150 to SIDE_CHANNEL in pipeline/common/fwa_edge_types.py and measure the panel diff
- **Q102** (T117) — decided: on an order tie every main-flow outlet counts as the lake's river (tributaries.lake_main_outlet); undo: pick one outlet by length or code in lake_main_outlet
- **Q104** (T117) — decided: candidates keeps its own propagation rules (classified in the up_adj reader test); undo: route it through may_enter as its own ticket with a panel diff
- **Q105** (T135) — decided: keep the 12 named codes (A = fact, B = heuristic); undo: if the owner finds the audit's A-list, map or add the missing codes in precheck.CHECKS
- **Q106** (T135) — decided: undrawn part is a tiers.RISKS factor; undo: drop it from tiers.RISKS (the pre-check stays)
- **Q108** (T135) — decided: a split added on a water stales every marked entry on that water ('binding moved'); undo: entry_binding_fp holds only the splits its rules name (also settles Q81)
- **Q109** (T135) — decided: a curated cut_pick is not an 'ambiguous cut' risk and stays a B pre-check; undo: tiers.risks reads RunFacts.ambiguous_of
- **Q110** (T123) — decided: the map word is 'no forecast' for both day-ahead and Change; undo: one label in the donor-state table
- **Q111** (T121) — decided: standing water for a gauge station means lake OR wetland; undo: one kind check in the water-kind helper
- **Q115** (T136) — decided: after a split, the printed chain ends with 'pipeline build --promote' including tiles (~40 min); undo: none useful, since --no-tiles fails the vintage check
- **Q116** (T136) — decided: a tool blocker is a deferred mark whose note starts 'TOOL:' (counted by gate.pilot_report); undo: add a 'blocker' field to Mark and pilot_report
- **Q117** (T136) — decided: keep the 9 twin rules shown as 'needs corpus (explained)' in the review app and do not make the app read the run; undo: a small review-app change to read the reach run's bindings for the twin extents (op steelhead_waters), best done in the later salmon/steelhead pass (T132).
- **Q118** (T125) — decided: yes, file it as its own ticket (a StationMatch field for the matched name, a match.json regenerate, then the owner runs promote with --only per station and reviews any demotions); undo: close the ticket unbuilt.
- **Q120** (T109) — decided: no KNOWN_VERSIONS bump; undo: bump the regs store section version in KNOWN_VERSIONS if a pre-T109 release ever reaches a device.
- **Q121** (T125) — decided: bump cache.POLICY_VERSION (26→27) with the next reach-code commit, per AGENTS 20; undo: none needed, a bump only invalidates the per-process cache.
- **Q122** (T150) — decided: a collapsible 'Parts of this water' list under the map in the Map view, shown only when the water has more than one part; undo: move the list's mount point (one component).
- **Q127** (T143) — decided: Level ranks ELF's stage while Flow ranks CLEVER's discharge for the same day, each model named; undo: restore the per-day break in publish.forecast_percentiles (pinned by test_each_quantity_takes_the_first_model_that_answers_IT).
- **Q128** (T144) — decided: equal weights (geometric mean of ratios); undo: FLOOD_WEIGHTS CLEVER 2.0 in pipeline/gauges/feed/change.py (one line, pinned by the policy test). Revisit once run archives allow a skill check.
- **Q129** (T144) — decided: past COFFEE's day 5 the Change is CLEVER alone, with the 'COFFEE runs to <day>; after that, CLEVER alone.' note; undo: cap the change window at COFFEE's reach in change_source.
- **Q131** (T144) — decided: keep CHANGE_LOW_WATER as built ('ELF is a low-water model: it carries the river's recent fall forward and has no rain or snowmelt in it, so it cannot see a rise coming.'); undo: edit the one constant in @app/core.
- **Q133** (T144) — decided: keep 'still up/down when COFFEE's forecast ends' (shape open) as built; undo: in _measure, judge the return after the handover on CLEVER's own line.
- **Q135** (T145) — decided: selected spot = a ring in the spot colour, no new colour; undo: add a selection token for spots (it must pass the status/highlight ΔE gates).
- **Q136** (T145) — decided: keep as built (names from z12 on one line cut at 120 px; invisible tap target 44 px); undo: SPOT_PIN labelZoom/labelMaxWidth/hit constants in core spotPins.ts.
- **Q141** (T119) — decided: canonicalise on each entry's next review-app save, with no bulk write now; undo: a one-off backed-up, diffed canonicalising pass over the 8 entries (15 extents) as its own reviewed change.
- **Q143** (T100) — decided: keep 'the maps on p.28' cite for r5x/r11x; undo: add the pier exemptions to ROW_MAP_DOES_NOT_DRAW so they cite no map.

Answered / obsolete (removed from the table):

- **Q29** (T89) — This is obsolete. T89's done section matched every row whose MU held a single candidate (the r8/r7 rows were already matched). The only rows left are the 8 with no in-MU candidate, and Q45 (owner 2026-10-23) settles those: leave them unbound and never bind a same-name water elsewhere. No 'several candidates' tie is left to ask about.
- **Q43** (T100) — This is obsolete. T100 is DONE with the whole lake bound plus an area caveat that cites the book's map. The remaining choice, drawing real lake parts as added_lakes rings at the cost of an atlas rebuild, is asked again word for word as Q142. Q142 stays open for the owner.
- **Q59** (T52) — The owner answered this on 2026-10-23 (app walk, T144): 'the change could use coffee and clever … combine both … use elf if nothing else is there'. T144 (DONE) built CLEVER + COFFEE combined with ELF as a last resort. The follow-ups are Q128–Q133.
- **Q60** (T52) — This is obsolete: Q132 (T144 Q5) asks the same thing in the current model. The question is whether the +1/+3/+5/+10 chips follow the Change source, which brings COFFEE in, or keep CLEVER → ELF. Q132 stays open with the default 'keep'.
- **Q82** (T97) — Answered by the owner's approval of curation-review/PLAN.md on 2026-10-23 (README Answered: Q5, Q34–Q41). PLAN §2 says 'any ? sends the entry to the unsure side queue', so unsure is an approved outcome.
- **Q88** (T108) — Already answered by the owner's p15 call (was Q24, owner 2026-10-21 late, README Answered): 'keep the 10 municipal ones'. RULINGS P10 now records that the T108 goes_on rule replaced the 2026-10-13 magnitude test 'after the owner kept the municipal continuations'.
- **Q90** (T105) — Obsolete: the T105 adversarial review (51f77e32) already makes new spot captures freeze the speaker's station (sheetLead(speakerFor(...))), and the T105 follow-up 'Spot capture still freezes the section_gauge station' is struck out as fixed. The README default 'unchanged for now (follow-up)' is stale.
- **Q95** (T134) — This is the same question as PLAN §6 Q2 (Q35). The owner approved it 2026-10-23 with the default: deep link in a new tab first, iframe only once he approves (decisions.md 'OWNER 2026-10-23 — curation review plan approved'; README Answered Q5, Q34–Q41). Nothing is left to ask until the owner wants the iframe.
- **Q99** (T115) — Obsolete. Built as T140 (DONE 41f541a9, its source cites 'owner Q99, 2026-10-23: yes, as T140 with a measured diff'). Measured: no bundle row changes today, so nothing is left to decide.
- **Q100** (T111) — Obsolete. Built as T141 (DONE 8c05e725, d5899459, its source cites 'owner Q100, 2026-10-23: yes, as T141'). Measured: no binding moved.
- **Q107** (T135) — PLAN §3 'Order: one queue across all regions, worked tier by tier' with S0 first. The owner approved it with every §6 default on 2026-10-23 (decisions.md 'curation review plan approved').
- **Q112** (T121) — Obsolete since T105, where section_gauge stopped being a reading source. Checked in the live bundle.sqlite: the Vedder Canal polygon (sid 1957095, shape 1) speaks through panel 3577, led by the Vedder's own 08MH047 and then 08MH001 (OWNER 2026-10-22 'a water's own gauge comes first'). 08MH024 is not a member, so it is neither read nor traced.

## CLAUDE 2026-10-23 — batch 7 small calls (owner reads; reverse any)
- T150 leftover: the old Parts sheet stays reachable only by the web start link (`sheet=parts`) as a test aid until T61's
  cleanup removes it with its golden comparisons; undo: delete it now.
- T153: the tributary walk still never takes ponds (p.80: tributaries are streams) — kept; undo: let `expand` collect
  unnamed ponds above the reach.

## OWNER 2026-10-23 — data permissions
- **BC River Forecast Centre:** the owner asked the RFC directly and has PERMISSION to use their data (the CLEVER,
  COFFEE and ELF forecasts the feed publishes). Credit the BC River Forecast Centre on the Sources page (T167).
- **HYDAT** (and ECCC's GeoMet hydrometric services) is under the Open Government Licence – Canada; attribute ECCC
  (T167; T170's online daily-mean lookup).

## OWNER 2026-10-23 — no public access on the map: dotted grey
Streams through no-access land draw as a dotted grey line (the under-lake style, a little stronger); no-access lakes grey;
the regulations page leads with the amber "No public access" block (T147); "own regulations" keeps amber. Supersedes
T146's spruce/cyan and the T165 amber swap; the dash is the owner's chosen second channel (was Q138). Ticket T165.

## CLAUDE 2026-10-24 — T167 small calls (owner reads; reverse any)
- Map tiles on the More tab shows its pairing state, not a date: the release records no tile build date. A real date
  needs the tile build to stamp one in the manifest — rides the tiles window (T64/T75); undo: show the hash.
- "Sources and credits" is linked once, as a row among the pages (Glossary / About / Report).
- The Synopsis and DFO are credited by publisher (no licence line): both are government publications the app reads and
  cites; a terms line is added if either publisher's terms ask for one.

- CLAUDE 2026-10-24 (T165): the "No public access" block heading stays ink — the light amber is 3.0:1 on the card,
  below what words need; the amber is in the bar and tint. Undo: a darker warning-text token.
- CLAUDE 2026-10-24 (T168): a lake with several stations counts the station that speaks (Shuswap: "1 gauge on this water"); undo: count every station on the lake.
- CLAUDE 2026-10-24 (T170/T171): `aheadDays` lives in the feed index (+9 KB gzip); ELF appears in the Like-your-visit strip
  only where no flood model reaches (37 stations), with its caveat; no offline history file in the release; a same-day spot
  whose gauge was silent at capture offers to fill the day's flow. Undo any by its ticket.
- CLAUDE 2026-10-24 (T175/T176): the opening zoom steps in until the water's own line draws (Fit brings far gauges back);
  with no tap the map frames the water's box plus its gauges; wetlands are not copied; the legend reads "N reaches
  coloured" (it counts reaches; a station count needs on-screen gauge dots — not built). Follow-up: no-access stretches
  should draw DOTTED in the copy too (owner's T165 look), today they leave a gap below the tiles' zoom — T175 Left.

## CLAUDE 2026-10-24 — T177 combining CLEVER and COFFEE (owner reads; reverse any)
- **One combination for every forecast day** (`change.combine_day`): where CLEVER and COFFEE both reach a day, the day is
  their weighted geometric mean (COFFEE first put on the gauge's scale); one alone where only it reaches; ELF only where
  no flood model does, with its low-water caveat. The Change, the +1/+3/+5/+10 horizons and `aheadDays` all use it
  (T171's first-model rule is gone). A joined day is named `CLEVER+COFFEE` in the feed.
- **Weights stay EQUAL.** Measured on this machine (T177 ticket): 5 paired stations at lead 1 on ONE issue day
  (2026-10-22). CLEVER log2 error 0.19, COFFEE 0.72, difference -0.53 +/- 0.56 (1 SE) — one coastal storm COFFEE
  overcalled 10x makes all of it; without it COFFEE 0.05, CLEVER 0.08. Too few to weigh.
- **The rule** (`skill.verdict`): equal until the archive holds >= 200 paired cases over >= 20 issue days at a lead;
  then equal if the mean paired difference is within 2 SE, else inverse-error weights. A person moves `FLOOD_WEIGHTS`
  with the table; the feed never re-weights itself.
- **Run archive**: `<feed dir>.runs/<YYYY-MM>.jsonl` beside the feed (never published), closed months gzipped, 14 months
  kept (~32 MB). Score: `python -m pipeline.gauges.feed.skill --feed data/generated/gauges/feeds`.

## OWNER 2026-10-24 — Q161: a spot records its TRIP day
"it should save the date that you set as the time of trip in the add a spot". A spot added for an earlier day reads its
lead gauge's daily mean and its section's panel (each member's daily mean, ranked with core `percentileInBand`) from
that day's record through the one history read (`flowOnDay`), never the live feed; offline it saves pending and a
refresh fills it. Spots saved before this for an earlier day are refilled by a refresh (a reading taken on the trip day
is never touched). T170's "read when this spot was saved" label is gone. Ticket T170 (Owner answer Q161), lane T177.
- CLAUDE 2026-10-24 (T177): CLEVER and COFFEE stay EQUAL weights until the run archive's verdict (one storm sample is
  not evidence; T178 re-scores in ~6 weeks); the archive keeps 14 months (~32 MB, beside the feed dir — the R2 sync must
  upload only the feed dir); COFFEE's day-5 stop is not marked on the strip; a trip-day panel member with no flow that day
  reads "Not reporting"; a day's flow read at capture still says "filled in from the day's record".

## OWNER 2026-10-24 — braids (Q6 / T67) and the T165/T131 looks

- **Q156** grey no-access lake fill: good as built. **Q157** dashes: good, keep. **Q134 / Q69** spot pins (square,
  neutral ink): "much better" — as built.
- **Q6 / T67 braids.** The first review page (artifact Lei1Bt4cCpnvEwessMVyQK, pre-gate) "very much missed the mark":
  e.g. South Atnarko → Atnarko cut a braid off far upstream. Answers to R14:
  - Q1: DROP EDGE_TYPE 2300-only ("manmade secondary flow") ditch lines from river items — side ditches, not the river.
  - Q2: (explained: automatic 25 km length cuts at junctions) — no braid sweep at length cuts; instead (owner refinement)
    `length_splits` never PICKS a junction a braid network spans (same both-sides gate); it takes the next eligible
    junction, or leaves the stretch uncut. Length cuts bind no rule, so only section ids / gauge colouring move.
    Then (owner follow-up) the braid sweep runs on EVERY split kind, no exemption by kind: at length cuts it must cut
    nothing, and that is a build check.
  - Q4: no cap on the cut line, but a cut may only take a braid that ATTACHES TO THE MAINSTEM ON BOTH SIDES OF THE CUT
    POINT (the topology gate). Nothing touching one side only (bulges, bends, South Atnarko) is ever cut.
  - The owner checks every proposed cut by hand in a review artifact before anything is promoted.

## OWNER 2026-10-24 (night) — promotion calls
- **T61 2.8 one name normaliser:** may be PROMOTED by Claude when its binding diff shows only explained changes
  (names now keying the same way); anything unexplained is held and listed for the owner.
- **T67 braids:** owner checks every cut on the review page (phone-friendly), sent by push notification when ready.

## OWNER 2026-10-24 (day) — braid review marks (artifact Et8B4Z6ZbKNNw9mMkJWzFH, collection "marks")
- 24 of 26 proposed cuts marked right; Chowade note only; 08NK016 "can't tell what is happening".
- **Tributaries on a braid** (Chowade, 08KH006, 08LF023): "don't assign tribs like this — only if they come from the
  side untouched". Rule: a tributary hanging off a braid takes the side of the cut where it JOINS the braid, and only if
  its own course never crosses the cut; otherwise it is cut (or left out of the braid network), never sided wholesale.
- **Gauge on a named side channel: YES, it still cuts its mainstem** (restores Fraser 08MH028/08MH032, Pemberton 08MG025).
- **Exchamsiks / confluence cuts:** "it should just cut straight across from the confluence across the mainstem" — a
  confluence cut is a line from the tributary's mouth straight across every channel to the mainstem. The proposed
  curated diff (exchamsiks_host.proposed.diff) is dropped.
- **Left alone tab:** the page must say in plain words why each braid was left alone and show which braid.
- **REVERSES T115: a piece that straddles a region line carries BOTH regions' rules** (owner answer to the conflict,
  2026-10-24). Supersedes "home region only" (T115, 2026-10-22). → T183. Moving a length cut then never moves a
  piece's region rules (q_length_home is moot).

## OWNER 2026-10-24 — Okanagan River oxbows are streams (slough review, T55/T149)
"All the oxbows should be made streams — make sure of that (Okanagan oxbow ones)." 41 slough candidates sit in the
registry item "Okanagan River Oxbows" (wbk polygons now kind lake/wetland; 38 UNSURE, 2 LIKELY_STREAM, 1 LAKE in v2).
All 41 become STREAM water. The book's own entry `r8:okanagan_river_oxbows@8-1` ("isolated oxbows adjacent to Okanagan
River downstream of McIntyre Dam and upstream of Vaseux Lake are EXEMPT from the spring closure") must keep binding
every one of them after the change — a binding diff proves it — so they read as streams with their own exemption, not
as Okanagan River mainstem. Applied with the slough fold (dry run first, per the bulk-reclassify rule).

## OWNER 2026-10-24 — no tool writes the catalogue
The curation review must not update files directly. Marks are the review's only output; changes reach curated entries
and splits as proposals the owner accepts (side rebuild + binding diff shown), applied as one commit per ticket. The
local tool's fix-mode writers are retired (T185). The review itself moves to a shareable artifact (T184).

## OWNER 2026-10-24 — order, PSF, and three small calls
- **Order:** braids (T67) → T183 → the tiles window (with the braid tile rebuild) → native (T50…) → the slough fold
  whenever the slough marks are finished (it takes a second tile rebuild; accepted).
- **T162 unblocked:** the owner asked PSF and permission is granted ("it is good"). Build run timing from PSF + DFO open
  data (best source per species: DFO for the salmon runs where it is better, PSF for steelhead and where DFO has none).
  PSF asked that their reply not be shared: it is NOT stored anywhere. The app credits "Pacific Salmon Foundation
  (Salmon Watersheds Program)" and DFO for run timing unless the owner gives other wording.
- **Q163 (owner: "we don't need to save PSF pages, just the data parsed"):** the three PSF pages are untracked
  (data/.gitignore) and stay local; only parsed data is kept. No history rewrite.
- **Q164 (owner: "prefetch reg data before a person clicks"):** prefetch on the web — always the regs file of the
  region in view; core + sql-wasm when the browser is idle and not on Save-Data / a slow connection. Gates (T129)
  measure first paint before any prefetch.
- **Q162 (owner: no preference; Claude's call: swap):** the typed ▾ / ▲ / ▼ in core words become the icon set's chevrons.

## OWNER 2026-10-24 — braids accepted
Every proposed braid cut, re-home and left-alone card marked right on round 3 (artifact Et8B4Z6ZbKNNw9mMkJWzFH). The
Ditches tab needs nothing (Q1 already decided: ditch-only lines leave their river). T67 merged and promoted with a
full `pipeline build --atlas --promote` (atlas, reach, tiles, deliver).

## OWNER 2026-10-24 — T183 follow-ups
- **Two regions' limits on one piece: stricter wins.** An angler on a straddling piece is under both regions' rules,
  so every cap (total and each sub-cap, e.g. "from streams") is the stricter of the two, and the card shows that
  combined group. Region 2 vs 8 hatchery steelhead over 50 cm → 1.
- **A touch is not membership: 5 m minimum** (SLIVER_M) inside a region polygon. Supersedes RULINGS E9's Contact Creek
  example (its 2.8 m in Region 6 does not bring Region 6's rules).
- **Skeena "section 2" starts at the Exchamsiks cut** (book p.53: Exchamsiks River to 1.5 km above Kitsumkalum),
  not the Ecological Reserve's crossing. Applied as one reviewed curated change with its binding diff (T185 rule:
  the owner approved this proposal here).

## OWNER 2026-10-24 — domain; DFO review app; water pages
- **Domain: canifishthis.ca** (primary); **canifishthis.com redirects to .ca**. App name stays "Can I Fish This?" (Q26
  domain part answered; R2 bucket/contact still open). Used by T86 hosting and T188 SEO (canonical URLs on .ca).
- **T164:** the DFO salmon extents get a review mode in the local review app — every scraped extent on a map with the
  proposed splits placed, using every anchor type (confluence, offsets, lake/MU/area edges), not only coordinates.
- **T188 water pages (owner: "what you think is best" → Claude's call): the FULL rules for that water in plain words**
  (crawlable text is what ranks, and an angler searching wants the answer on the page), today's open/closed line,
  the book page, a prominent "Open in the app" link and the "not a legal document — check the synopsis" line. Generated
  from the same release as the app, so the page and the app can never disagree; no interactive features on the page.

## CLAUDE 2026-10-24 — tiles window calls (owner reads; reverse any)
- (T85) The basemap drops OSM `stream` LINES (−242 MB of 918): the style draws them only from z14, under our FWA
  streams; OSM-only creeks carry no regulation. Undo: `basemap.WATER_LINES_KEPT += ("stream",)` and rebuild the basemap.
- (T85) Atlas archive to z13, top zoom at −S 5 (≈ the old z14's 4 m); `FIT_MAX_ZOOM` stays 14 so no fit moves; camera
  stays z18. Undo: `ladder.MAX_ZOOM` / `ATLAS_MAX_ZOOM` back to 14, `(2, 13), (0, 14)` on the ladder.
- (T85) The basemap keeps OSM `wetland` landuse (52 MB, not drawn today) as the possible stand-in for T64's dropped
  wetlands. Undo: drop it from `LANDUSE_STANDIN`.
- (T64) A wetland is drawn when it is in a registry item, stream-kind, or gazetted-named; everything else is left out
  (333,471), including the 460 under area-only rows (Liard) that carried the "own regulations" colour — as the owner
  asked. The bundle refuses a release that would need one drawn.
- (T152) Sewage basins, tailing ponds, dumps are NOT drawn (Q125 default was a muted fill; built undrawn, the layer
  `not_fishing_water` is in the tiles so a fill is one style entry).
- (T85, Q125 follow-up) The basemap STAYS z14: the owner's condition for z13 ("no feature may be lost") fails —
  `basemap.audit_zoom_cap` finds 6 kinds and 426 names (Squamish), 75 names (Lac Le Jeune) only at z14. Undo: accept the
  loss and build with `--max-zoom 13` after relaxing the audit.
- (T113) The 35 all-capitals `entry.name` values stay the book's spelling in the bundle (the export cases them and keeps
  `official_name`).

## 2026-10-24 — tiles window calls (owner)
- Q125: sewage basins, tailing ponds and dumps (T152 `not_fishing_water`) DRAW as a muted fill — never as a lake.
- Basemap to z13 too (≈ −289 MB), on two conditions: no feature may be lost (measure feature counts / kinds
  z14-sourced vs z13) and the map must still overzoom past z13.

## 2026-10-24 — T179 owner answers
- Gauge joins the nearest named cut within 400 m on either side: confirmed.
- "Curated cuts always exist in their location": an authored cut NEVER moves. Auto boundaries (gauge, area, border,
  length) within 5 m join the authored point, not the reverse. A lake edge / natural line end within 5 m cannot move
  either: the authored point stays and the pair is reported (split_conflicts.json) for the owner, never a silent
  snap. Exception: the 8 `at_edge` splits, whose curated location IS the lake edge/end (the owner's own setting).
- A curated cut within 5 m of a lake edge / line end is usually meant to BE there: besides staying put and being
  reported, each pair writes a PROPOSAL to `split_lake_anchor_proposals.json` (next to split_conflicts.json) to
  re-curate it as a `lake` anchor (wbk + gnis, inlet/outlet, distance) — or, at the line's own mouth, a
  `confluence` anchor on its blk; a source end is report-only. Never applied: the owner accepts each (T185).
- Follow-up 2: the PROPOSAL radius is 25 m (`sectionizer.PROPOSAL_M`) — a curated cut within 25 m of a lake edge or
  its line's mouth gets a re-anchor proposal with its distance `band` ("0-5m" | "5-25m"); placement unchanged, only
  under 5 m is it also a sliver conflict (a source end / minted cut stays sliver-only, no anchor). A MERGED PLACE
  CARRIES EVERY NAME AND EVERY ID: all ids in `aliases` (any binds), every other label in `SectionBoundary.names` →
  `RegistryBoundary.names`; the review app labels it "canonical (also: …)".

## 2026-10-25 — T179 split proposals (owner)
- ACCEPT lake anchors: Ladore dam (Campbell L.), Great Central Lake dam (Stamp R.), Revelstoke dam, Kootenay/Koocanusa,
  Peace Canyon dam (Dinosaur L.), Terzaghi dam (Carpenter L.), Caribou Creek Hwy 6 bridge (lake wbk 329262655).
- Cayoosh: lake wbk 329505890 is NOT a lake — it is part of Cayoosh Creek: a stream piece with stream regulations,
  bound by the Cayoosh Creek entry. Cayoosh Falls then stays a plain cut (no lake to anchor to).
- Caribou: lake wbk 329262655 is the Caribou Creek / Burton (Trout) Creek confluence wetland-slough: it carries BOTH
  creeks' regulations.
- REJECT the confluence proposals for Tweedsmuir Park upstream boundary (10) and Baker Creek / Pinnacles Park: park
  boundaries are not confluences; the proposal finder skips area/park boundary splits.
- Close pairs (owner 2026-10-25): park boundary splits need investigation and a revision so they work (Tweedsmuir
  eastern/upstream 0.02 m, Baker Creek / Pinnacles downstream/upstream 0.03 m — both ends on one point). Moyie River
  US border split and the Region 4 line (12 m) MERGE; the BC border is GENERATED from the region polygons (one source),
  so border and region lines coincide. Area-vs-area pairs 10–20 m (Clayhurst / 7B, Chilkoot / 6) are left as they are.
  The build reports every curated cut 5–25 m from any other boundary.
- Koocanusa (owner 2026-10-25): the split is the reservoir's INLET ("upstream of Koocanusa"). Rule: a lake-anchored cut's
  edge follows the extent's direction (upstream of the lake → inlet, downstream → outlet), and the lake itself binds
  only when it lies wholly inside the extent (as fixed before). The new explicit `edge` on lake anchors must agree with
  this; check it in the binding diff.
- Authored cuts at a polygon edge (owner 2026-10-25, replaces part of 974621af): one rule, both sides of a folding
  polygon's or a real lake's edge — an authored point within 1 m snaps onto the edge (`sectionizer.FOLD_SNAP_M`); farther
  inside a folded polygon, or 1–5 m outside any polygon/lake edge on its line, the build STOPS naming the split id, the
  wbk and the edge lon/lat to put in splits.json (a real lake also names the lake-anchor proposal). Never a sliver, never
  a silent move > 1 m. Cayoosh Falls re-curated to wbk 329505890's downstream edge [-121.9825923, 50.6572974].
- Overlapping rules retired (owner 2026-10-26, accepted all 4 of handoff/PLAN/curation-review/overlapping-rules.md):
  `z1:trout_quota.r3` and `z2:trout_char_quota.r3` ("2 hatchery steelhead over 50 cm allowed") are gone. The steelhead's
  2 is r2b's total (RULINGS Q16 unchanged), and "hatchery" comes from the wild-steelhead release. z1 r2b's verbatim now carries
  ", see table for exceptions)". `khartoum_lake.r3` / `lois_lake.r3` (hatchery rainbow = 6) are gone too, since the 6-in-the-aggregate rule
  implies them. Their `exempts` of `trout_char_quota.r3` were dropped. Khartoum's "(see diagram on page 13)" survives only
  in `regs_verbatim`.
- Zone daily quotas merged (owner 2026-10-26, the RECOMMENDED set of handoff/PLAN/curation-review/zone-entry-merges.md):
  each region's trout/char quota and "Other daily quotas" are ONE entry `zN:daily_quotas` (z1 z2 z3 z4 z5 z6 z7a z7b z8),
  named after the book's box ("Region N Daily Quotas" / "Zone A|B Daily Quotas"); 7B's Arctic grayling block joins its
  table. Every `rule_id` is kept, only the entry id moved (rule keys `zN:daily_quotas::trout_char_quota.rK`); every exempt
  `entry_id` was rewritten (R4 79, R7 62). Not merged: possession, the General Regulations block (entry-level
  `default_id` lifts), 7B bait + set lining, the Skeena advisories, and the 7B Dolly Varden NOTE (printed under
  Possession Quotas, not in the quota box). Page v36's oracle keys are renamed at load (`oracles.ts ORACLE_ENTRY_RENAMED`).
- Halfway cuts (owner 2026-10-26): both `halfway_river__*` cuts were cut on the PEACE (applies_to wsc 200-948755). The
  confluence point is now `peace_river__halfway_river_into_peace_river`; `halfway_river__…_u5000m` was the same anchor
  as `peace_river__…_u5000m` and merged into it. The Halfway's own closure ("from confluence with Peace River to
  fishing boundary signs approximately 5 km upstream, Mar 1-June 30") binds `downstream_of`
  `halfway_river__fishing_boundary_signs_5_km_above_peace_river` (the Halfway's mouth + 5000 m up the Halfway) instead
  of an undrawn part on the whole river.
- T206 (owner 2026-10-26): the page-v36 oracle is amended, not re-cut — each retired r3 key and each state it moves is
  a named, reasoned exclusion or re-pin (`oracles.ts ORACLE_RETIRED`, exclusions.ts T206).
- Splits to verify (owner 2026-10-26): every split in splits.json was placed by hand, so a `_note` (an "[auto-proposal L] … Verify:" one included) is history, never "to verify". `tiers.split_needs_verify` = `"verify": true` or a stale mark; corpus count 0 (was 150), split_to_verify pre-check 164 -> 0; tests re-pinned (Z 132 -> 122 and 1525 -> 1515 entries are the zone-quota merge 6335f73d, unresolved 29 -> 25 measured). Supersedes Q94's S0 rule.

## 2026-10-26 — unused curated cuts (owner)
- There are no orphans: every curated cut was added for a reason (often an unlabelled DFO place). Never delete one;
  find the rule or DFO scope that uses it, or show it as an open high-severity item in the review tool.
- Bind the 10 the T207 audit could match (Burton r1, Kootenay lower r1 navigation dolphin, 8 DFO scopes).
- Missing splits and unused curated cuts are HIGH severity in the review tool.
- Cranberry: the DFO falls point (55.56407, -128.68426) is the top of Cranberry River Canyon — add it as a curated
  split LABELLED AS THE FALLS ("Cranberry River falls (top of the canyon)"), confidence medium, note saying so and citing
  the 2026-08 needs-pin row `cranberry-river-699352-a` (its only link: OSM, Cranberry–Kiteen junction 55.51394,-128.8105,
  reference only); the downstream sign stays "needs a split (owner to locate)".
