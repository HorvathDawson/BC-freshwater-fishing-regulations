# T54 — Streams in no-access areas: a fourth colour, and the page says so

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
