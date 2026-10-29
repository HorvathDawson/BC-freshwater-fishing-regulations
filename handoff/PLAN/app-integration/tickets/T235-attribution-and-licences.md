# T235 — data attribution: credits on the map, on the sheets, and an About → Sources & licences page

Status: IN PROGRESS (lane T235/T236, main checkout)

Source: owner 2026-10-28 — approved proposal P1 ("credits must live in the proper location, like attribution on the
map").

## Built
1. **Map credit control names what is drawn (web + native)** — `app/packages/map/src/credits.ts`.
   - Always-drawn sources carry their own credit: basemap `© OpenStreetMap contributors` (linked to
     openstreetmap.org/copyright, ODbL wording) · © Protomaps; atlas `Province of British Columbia (OGL-BC) · Freshwater
     Atlas & Freshwater Fishing Synopsis · some names and channels © OpenStreetMap contributors`; gauges ECCC (as before);
     Esri while satellite is drawn (as before).
   - A mode/overlay that paints on the atlas gets its own empty GeoJSON source holding only its credit plus a layer that
     draws nothing, made visible by `applyLook` while the look draws that data: Salmon → DFO ("not an official
     version"), Run timing → PSF (with permission) + DFO (OGL-Canada), Stocked → Province (OGL-BC), Fish obstacles →
     FISS/WRIS/PSCIS (OGL-BC). Web: MapLibre lists sources a visible layer reads (verified: salmon credit appears on
     switching to Salmon). Phone: `nativeStyle` drops a hidden credit (MapLibre Native lists every source), as it does Esri.
   - The compact ⓘ stays collapsed on arrival (unchanged); one tap expands it.
2. **Sheets**: already credited and checked — salmon card ("Source: … DFO", page dates), Conditions fold (ECCC, the RFC
   verbatim notice, weather), obstacle sheet (source row + OGL-BC line), run timing (`runCredit`, T199 lane), name source
   labels (T224). No change needed.
3. **Sources and licences page** (More → "Sources and licences", and a link on About): the existing source list + a new
   Licences section (`core/menu/licences.ts`, `ui-native/menu/Licences.tsx`): 17 datasets, each with publisher, what the
   app shows, licence (linked) and the licence's required wording (OGL-BC, OGL-Canada, ODbL, DFO non-endorsement, RFC
   verbatim, Esri, Open-Meteo CC BY, PSF permission, Lucide ISC), plus the ODbL offer line.
4. Tests: `map/src/credits.test.ts`, `core/src/menu/legal.test.ts` (licences part), `ui-native/src/Legal.test.tsx`;
   updated menu/surfaces pins for the new label. Shots: `handoff/PLAN/app-integration/T235-shots/`
   (map-credits desktop/phone, map-credits-salmon, sources-licences desktop/phone).

## ODbL share-alike — what we must offer (OWNER CALL)
- **Produced Work** (the map, tiles, screens): attribution only — "© OpenStreetMap contributors" linked to the copyright
  page, visible. Done (map control + Sources page).
- **Derivative Database**: we adapt OSM data into our own database: `data/source/municipal/osm_streams.geojson`
  (Northrop Channel + Northrop Creek geometry, T222) and **177 of the 183 accepted mined names** in
  `data/curated/waters/mined_names.json` are `source: "osm"` (T224; thousands more candidates pending review). These
  ship inside the release (water names) and the atlas tiles. 177 features is above the OSMF "Substantial" guideline's
  100-feature threshold, and the names were extracted systematically, so treat it as substantial.
- ODbL §4.6: when a Derivative Database is Publicly Used (a Produced Work from it is shown publicly — the app), we must
  offer the Derivative Database itself, or a file of the alterations, under the ODbL. Only the OSM-derived part needs it
  if the rest is a Collective Database (OSM-derived records kept separable — they are: `source: "osm"` rows + one
  geojson).
- **What we must offer (default, recommended):** publish with each release a small `osm-derived.json` (ODbL 1.0, with
  the attribution line): every OSM-sourced name (target FWA blk / item id, name, OSM date) and the two OSM stream
  geometries; link it from Sources and licences in place of today's "ask through Contact" line. Our FWA/regulation data
  does NOT become ODbL. Until then the page says the data is available under the ODbL on request.
- Owner to confirm: (a) the published file (default yes, from the release step), (b) whether to keep auto-accepting OSM
  names at all (alternative: only keep BC GNIS / municipal names and drop OSM-sourced ones — no share-alike).
- Also verify (owner): the four city stream layers' licences (Abbotsford, Burnaby, Port Moody, Squamish — listed as
  "each city's open data licence"; add their exact wording if they ask for one); stocking/lake-survey data assumed OGL-BC.

## Not done / follow-ups
- Native attribution not checked on a device (MapLibre RN shows the style's source credits behind its ⓘ; the style
  logic is unit-tested). Rides T77 phone testing.
- `osm-derived.json` file + link: after the owner's call (above).

Depends: none. Size: S. Owner: yes (ODbL file + OSM-names policy; city licence wording). Default: the page offers the
OSM-derived data on request.
