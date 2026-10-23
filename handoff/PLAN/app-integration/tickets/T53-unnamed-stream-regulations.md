# T53 — An unnamed stream shows no regulations (only Conditions)

> **BUILT 2026-10-22, app only (decisions.md "CLAUDE 2026-10-22 — T53 built"):** an unnamed frame offers Regulations ·
> Conditions (no Map); the Map tab puts Regulations in front; the page is the store's answer for the tapped section
> (`RegsTarget` kind "section"); header "Unnamed stream · Region N" (the store's) + "Tributary of <water>" when the flow
> pointers reach a named water. No pipeline change. The ticket's three examples run on the live release
> (conformance/src/regs/unnamed.release.test.ts); REVIEW 2026-10-22 added the per-region real-data sample
> (conformance/src/regs/unnamed.sample.release.test.ts: per region 3 override-bound + 3 base-only unnamed sections, the
> store's rule set equal to the bundle's `section_ruleset` → `ruleset` binding, every cited rule one of them; skipped
> unless the bundle beside the release is its build) and a desktop-layout render.

Owner 2026-10-22: "clicking on unnamed streams shows no regulations right now only conditions. this is definitely an
issue that needs a ticket."

## The problem
Tapping a stream with no name opens Conditions only. The reader gets no regulations at all, though every stream in a
region is fished under that region's rules.

## What it should do
- An unnamed stream has regulations: its REGION'S BASE (zone rules are the base: region-wide rules, plus any water
  override that reaches it, e.g. a named river's tributaries rule). Base + deltas: every line attributable.
- Tapping it opens the same one water screen (openWater) with a Regulations tab, like a named stream. The title says
  what it is without inventing a name (e.g. "Unnamed stream", plus where it flows into when known: "tributary of
  Nicomen Slough").
- Answers stay pipeline-generated: if unnamed sections have no answer rows today, the pipeline produces them; the app
  never assembles regulations itself.
- Conditions for it keep working as today.

## Acceptance
- Real-data test: a sample of unnamed sections in each region opens a Regulations tab whose lines equal the region
  base plus the overrides the reach run binds to that section (from the bundle, not a copy).
- A tributary rule of a named river (e.g. "and tributaries") shows on its unnamed tributaries.
- No unnamed stream opens without a Regulations tab (whole-map count = 0), pinned by a test.
- Back / close behave per decisions.md "back steps out one layer".

## Cause
Diagnosis 2026-10-22 (read-only; bundle `data/generated/bundle/bundle.sqlite`, release `data/release/r-f734cbb93724`).

**Bottom line: this is an app gate, not missing data.** The pipeline answers every unnamed section, and the data and
core layers can already build a page for one. The UI route decides on purpose that a frame with `item: null` has no
Regulations view.

**1. Pipeline and release: every unnamed section has an answer.**
- Bundle: 1,893,082 sections have no item (`section_ruleset.up IS NOT NULL`). They fall into 577 `unnamed_part`s
  (stream 1,179,681 · lake 380,004 · wetland 333,397), and none of them is also in `item_section`. Each carries a
  ruleset that holds the region base (`z<r>:*` + `zp:*`, via `reach`) plus any bound override (via `trib`).
- Answers: `pipeline/deliver/answers/common.py:298` `unnamed_keys` adds one key for each distinct unnamed part (answers
  2.4 S1), and `answers.py:475,615` writes them to `unnamed{first, parts}`.
- Store: `pipeline/deliver/store/build.py:425-449` gives every unnamed section its part's akey in `section_akey`.
  `:440-446` refuses to build if any B.C. section is missing one. `:454-469` writes `section_place`.
- Release: `pipeline/deliver/release/shards.py:31-47` (`akey_regions`) puts each akey's cells into every region shard
  its sections touch.
- Measured in the release: all 1,893,082 unnamed handles have a non-zero akey in `regs-sid`, and that akey has `cell`
  rows in its region's `regs-<r>` shard. Per region (first region of the place), unnamed sections answered vs. missing:
  R1 129,252/0 · R2 67,520/0 · R3 129,627/0 · R4 114,201/0 · R5 261,891/0 · R6 572,085/0 · R7A 238,984/0 ·
  R7B 340,908/0 · R8 38,614/0.
- Sections whose set carries a water override (a trib rule or another non-zone entry), per region: R1 27,265 ·
  R2 27,060 · R3 27 · R4 68,935 · R5 40,699 · R6 118,743 · R7A 78,858 · R7B 201,544 · R8 16,749.
- The named side, for comparison: 63,263 sections, of which 63,072 have an akey (the other 191 lie outside B.C.).
- **No deliver, tile or atlas rebuild is needed.** The stream tiles carry only `{section_id, ord}`
  (`data/generated/tiles/layers/stream.geojsonl`), and that is all a tap needs.

**2. App: the tap drops into a Conditions-only frame by design.**
- `ui-native/src/Shell.tsx:458-465` `onPressFeature` → `opener.tap(sid, …, "regs")`.
- `ui/src/openWater.ts:48-53` `openTap` → `source.itemForSection(sid)`. For an unnamed section this is null, because
  `data/src/release/source.ts:163-166` reads core `item_section`, which lists named sections only.
- `ui/src/openWater.ts:37-42` → `frameViews({item: null})`, and `ui/src/route.ts:104-108` returns
  `REACH_ONLY = ["conditions"]`. So `openWater` swaps the asked `regs` view for `conditions`, and the reducer
  (`route.ts:116-121`) refuses any other view. The header comments state the intent: `openWater.ts:11-14` ("AN
  UNNAMED REACH … has no regulations page") and `route.ts:104`.
- `Shell.tsx:547` renders `WaterConditions` whenever `item === null`, and `Shell.tsx:698` gives the FaceBar
  `frameViews(frame)`, so the Regulations tab never appears.
- `WaterScreen` cannot take a null item either:
  - `ui-native/src/WaterScreen.tsx:44` types it `item: ItemId`.
  - `:73` calls `useRegsTarget(store, item, …)`.
  - `ui/src/regs.ts:78-81` returns null and stays disabled when `!item`.
- Tests that pin the current behaviour:
  - `ui/src/openWater.test.ts:30,66` ("an unnamed reach has only Conditions")
  - `ui/src/route.test.ts:195-208`
- What already works below the UI and is simply never called from a tap:
  - `RegsTarget {kind: "section", handle}` (`core/src/regs/model.ts:22`)
  - `loadRegsInputs` section branch (`data/src/regs/inputs.ts:76-88`): `sectionAkey` → `sectionPlace` →
    "Unnamed stream · Region N" → `keyed(...)`
  - core `edgeKind` "unnamed" (`core/src/regs/edge.ts:30`)
  - the loader opens `regs-sid` on the first unnamed tap (`data/src/release/loader.ts:7`)

**3. Examples** (section handle, midpoint lat/lon, what the release holds):
- **1408** · 50.27557, -126.36297 · R1 stream, flows into Eve River (sid 59010). akey 883, ruleset 20 = R1 base +
  trib rule `r1:eve_river@1-10 / eve_river.r1` "No fishing, Dec 1-May 31 — upstream of the fishing boundary signs
  near the South Main Bridge…". This is the tributaries case.
- **64035** · 49.08663, -122.50524 · R2 stream, flows into Salmon River (Langley, sid 305002). akey 469, ruleset 426 =
  R2 base + trib rule `r2:salmon_river@2-4 / salmon_river.r1` "No fishing — upstream of 232 Street".
- **59506** · 53.59316, -127.35023 · R6 stream (ord 4), flows into sid 59507. akey 18, ruleset 259 = R6 base only, with
  no water override.

**4. What the fix needs: app only.**
- Give an unnamed frame the Regulations view:
  - `frameViews` returns `["regs", "conditions"]` for `item: null` (the Map view stays named-only).
  - `openWater` and the reducer keep `regs` in front from the Map tab.
  - `WaterScreen` accepts `item: null` + `section` and uses target `{kind: "section", handle: tapSid}` instead of
    `useRegsTarget`.
  - Update the two pinned tests.
- Title: `inputs.place.name` ("Unnamed stream · Region 1") already comes from the store. A "tributary of X" line could
  read conditions' `section_down` (in the release, 108,363 rows, covering all 3 examples) → `itemForSection(down)`.
  That would be an addition, not a regeneration.
- No pipeline change and no deliver, tile or atlas rebuild.
