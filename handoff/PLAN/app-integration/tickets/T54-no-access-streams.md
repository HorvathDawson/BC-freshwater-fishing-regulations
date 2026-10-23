# T54 — Streams in no-access areas: a fourth colour, and the page says so

Status: PARTIAL — 991baa1c (pipeline: section_access, paint/3, access/1), 2fc72d9b (app: fourth colour, banner, note,
legend); ticket a43a7122. Built with Q1–Q3 defaults. Left: the rebuild below (deliver all + release), then the
needs_bundle checks; see "Built / left" at the foot.

Owner 2026-10-22: "the private land of coquitlam watershed, seymour watershed etc and rsa zones etc... basically no
access areas should color the streams a fourth color to show the user they can not access it. (regs are ok but access
is not) then in the regulation tab it should make it clear that this is in an area you arent allowed to access. (is an
issue for the no access stuff other things like private land or indigenous should not be treated as aggressively) for
those ones we just need to add a little note that the section they are viewing has potential access restrictions and
to look at map to see what it thinks the potential problem is."

## Two tiers — access is not regulation
Regulations stay as they are: a section's open / restricted / closed answer never changes because of access.

**Tier A — no access (strong).** The `no_access` layer, kind `closed` (pipeline/deliver/tiles/layers.py; 132 areas
today). Examples: Coquitlam, Seymour and Capilano watersheds, Greater Victoria Water Supply Area, Cannell Lake
Watershed, CFB / military ranges, "Restricted-access land" areas.
- On the regulations map, a stream section inside one is drawn in a FOURTH colour, "No public access". It sits beside
  open / restricted / closed, with a legend entry, and must clear the status-colour distinctness gate
  (T47 / `status-colours-distinct`, every theme, colour-blind checks).
- Its Regulations tab opens with a clear banner, worded in core. For example: "No public access: this section is
  inside Coquitlam Watershed. The regulations below are the Province's, but you may not enter this area to fish."
  "Show area on map" is a link.

**Tier B — possible restriction (gentle).** Private land (parcels), Indigenous land (reserves), and the `permit` kind
of `no_access` (4 areas, e.g. UBC Malcolm Knapp Research Forest). These areas don't change the stream's colour.
- The Regulations tab carries one small note, for example: "Access may be restricted here: this section crosses
  private land. See the map." Naming the cause from the data: private land / an Indian reserve (name) / permit-only
  land (name).
- Tapping it shows the area on the water's map.

## Where the facts come from
- Pipeline-generated, not computed in the app at run time (the bundle is the only source). This is a new
  `section_access` fact: section → area id, name, tier, share of the section's length inside. Build it from the same
  polygons the tiles draw: one vintage, never a second copy.
- STRADDLING: a section partly inside is marked, not cut (see the straddling-sections note). Tier A colour only when
  most of the section is inside. A section only partly inside gets the Tier A banner worded "partly inside". Pick the
  threshold with real data and record it.
- Display names drop the OSM id suffix ("Coquitlam Watershed [13480215]" → "Coquitlam Watershed").
- The paint file carries the access tier so the colouring engine (one pathway) draws it. Check the release size gate.
- Needs: pipeline (section_access + paint), `deliver all`, app (colour, legend, banner, note), fixtures.
  Probably no tile rebuild, because the polygons are already in the tiles.

## Open questions for the owner
1. Permit-only areas: Tier B with "permit needed" wording (Claude's suggestion), or Tier A?
2. Conditions map: keep the flow colour there (suggested: yes, Tier A gets only the banner on its Conditions tab
   too), or mark no-access there as well?
3. Lakes inside Tier A areas (e.g. Coquitlam Lake): same fourth colour for the lake fill? (Suggested: yes.)

## Acceptance
- Real data: every stream section mostly inside a Tier A area draws the fourth colour. Seymour, Coquitlam and Capilano
  examples are pinned by name. No Tier B area changes a colour.
- The banner and note have whole-string tests, from core. A mutation pin fails if private land turns a stream to the
  fourth colour.
- The legend shows "No public access" only in the regulations view.

## Built / left (lane wf_c3f10dea-7b4-1, 2026-10-22)

Built (defaults: Q1 permit-only = gentle tier, "where a permit is needed"; Q2 Conditions keeps the flow colour and
shows only the no-access banner; Q3 lakes take the fourth colour):
- `pipeline/deliver/bundle/access.py` → bundle `access_area` + `section_access` (permille), measured against the tile
  layers' own geojsonl (no_access, indigenous_land, parcel; sections from stream/lake/wetland), refused for tiles of
  another atlas. `pipeline build`: deliver now depends on tiles. Measured on the live tiles: 4,098 no-access rows
  (4,028 sections > 50%), 109 permit, 37,878 reserve, 106,609 private.
- THRESHOLD (recorded, core `MOSTLY_INSIDE` = 0.5, `TOUCH_ONLY` = 0.01): of 4,098 no-access rows 3,966 are 100%, 36 in
  90–99%, 41 under 10% (edge grazes); Coquitlam 785/789, Seymour 544/544, Capilano 875/880 mostly inside.
- paint/3 = paint/2 + the no-access rows (950,316 B gz of the 1,000,000 gate, simulated on the live bundle);
  `access/1` = the gentler tiers (225,488 B gz, gate 300,000). Release total ≈ 13.92 MB of the 14.0 MB gate —
  82 KB headroom (see Q-T54b).
- App: status `no_access` (token `color.status.no_access`: light #2A00E0, dark #985400, cvd #705864) through the one
  colouring pathway (`profileLabel` / `dayProfiles` / `mapStatusOn`, paint's rows on `index.access`); legend "No public
  access" in the regulations key only; banner / note worded in core `accessNotice`, drawn first on the Regulations
  page (`AccessNoticeBlock`), link → the water's Map view; Conditions view: the no-access banner only.

Left:
1. REBUILD (human / heavy.sh, one job): `python -m pipeline.deliver all` (bundle gains section_access; reads
   data/generated/tiles/layers — no tile rebuild), then `python -m pipeline.deliver release`. Diff: bundle tables
   `access_area` / `section_access` counts against the numbers above; release sizes (paint < 1 MB, access < 300 KB,
   total < 14 MB). Then run `pytest -m needs_bundle pipeline/tests/test_section_access.py pipeline/tests/test_release.py`
   (the named-watershed pins and paint == bundle rows; they FAIL on today's bundle, which predates T54).
2. "Show area on map" opens the water's Map view; it does not frame or highlight the area polygon itself.
3. A water opened from search (no tapped section) shows no notice: the notice is per tapped section.
4. The dev-bundle path (`status_index.bin` beside the tiles, no paint) draws no fourth colour.
5. Native map (T74) reads the pushed `status`: `mapStatusOn` gives it `no_access`, untested on a device.

New owner questions (default built with):
- Q-T54a: the fourth colour per theme is a compromise the gates record (dark ochre 12.7 from closed as a thin line;
  colour-blind mauve 3.1 from the grey boundary as a line). Keep, or trade the boundary/permit floors for another hue?
  Default: keep (status-colours-distinct UNDER_20 lists each with its reason).
- Q-T54b: the release is 82 KB under its 14 MB total gate after `access` (225 KB, private land is 168 KB of it).
  Raise the total gate, or drop private land from the note? Default: ship as built.
