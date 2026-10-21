# Pools and riffles from open lidar: feasibility spike (Vedder/Chilliwack)

**Verdict: it looks like it works, but it is not validated yet.** The HRDEM 1 m DTM is **not hydro-flattened into a smooth ramp** on this river. The water-surface profile is a clear staircase of flat treads and steep risers, spaced the way pools and riffles usually are. The weak point is ground truth: I found no published coordinates for named pools. Before building anything, it needs about 10 hand-checked pools.

## Method (about 60 WCS requests, nothing installed)
- Line: the repo's FWA sections 313838, 313839, 1235779 and 1235780 from `stream.geojsonl`. That is 20.2 km, from the Vedder Canal end up to about -121.84 W, projected to EPSG:3979, with a station every 10 m.
- DEM: `datacube.services.geo.ca/ows/elevation`, WCS **1.1.1** GetCoverage `identifier=dtm`, as 500 m GeoTIFF tiles at 1 m resolution.
  - The 1.0.0 and 2.0.1 requests answer "layer does not exist".
  - The tiles are big-endian float32, and PIL reads them byte-swapped, so call `.byteswap()`.
- At each station: the channel low is the minimum within 25 m. Then force it monotone (it may not rise going downstream), apply a light median/mean smoothing, and take the slope over about 70 m.
- Classes use the profile's own quartiles: flat < 0.13 %, steep > 0.83 %, run in between.

## Evidence
- **Not flattened.** In a flattened DEM each water polygon sits at one constant level.
  - Inside the flat reaches, the detrended DTM varies by a median of **1.4 cm** (only 15 % of reaches are under 5 mm).
  - Cross-sections show a wetted floor carrying cm-scale noise beside gravel bars (`work/xs.png`).
  - So the surface comes from real returns, or from a TIN drawn from water-edge points. Either way it carries the water level at the time of the flight.
- **Pool-riffle signal.** See `profile.png`. The mean slope is 0.54 %. Along the reach, 54 flat units alternate with 53 steep ones; the median unit is 70–80 m long and the steep units are about 380 m apart. That matches the textbook 5–7 channel widths for a channel about 50–60 m wide. The steps get sharper upstream (Chilliwack, slope up to 2.7 %) and fade in the low-gradient lower Vedder.
- `map.png` shows the classes along the line.

## Accuracy against named pools (weak)
- Two quick searches turned up no named Vedder pools with locations. Forum, blog and geocache sources also say floods keep filling and moving the holes.
- The only usable point is the "hole under the Vedder Crossing bridge" from a 2008 forum post. The station there (km 8.18) is **flat** for ±50 m.
- That is one point, consistent with the result, and proves nothing.
- A real check needs: a guide's map, or about 10 pools marked by the owner on imagery, or comparison with a DFO/Fraser Valley habitat survey.

## Caveats
- **Stage dependence.** The lidar captures one flow on one date (date unknown here). At high water, riffles drown out and the steps flatten.
- **Channels move.** The Vedder reworks its bed every freshet, so a layer built from a 2015–2020 flight may be stale. Label it with the flight year.
- **Line offset.** The FWA centreline sits up to tens of metres off the wetted channel; the 25 m min-window hides this. Braided reaches and side channels will confuse it.
- **Thresholds.** The flat/steep cutoffs are relative per river, not physical.
- **Classes are guesses.** Pool vs glide (both flat) and run vs riffle cannot be told apart without depth. "Flat" means slack water surface, not depth.

## Coverage and licence
- **HRDEM** comes under the Open Government Licence – Canada. Derived products are allowed, with attribution.
  - Its lidar-based DTM covers only part of BC: the lower Mainland, the Okanagan, and parts of Vancouver Island and the corridors. This list is from memory and was not checked in this spike.
- **LidarBC** comes under the Open Government Licence – BC, also with derivatives allowed.
  - It releases raw LAZ point clouds as well as DEMs.
  - Released coverage is about 86,000 km², roughly 9 % of BC (third-party figure, not confirmed). It is skewed to populated valleys, which is where most fished rivers are.
  - Where a DEM is flattened, the LAZ ground and water returns (class 9) could rebuild the water surface, but that is heavy processing.
- Remote rivers will have **no lidar at all**. Plan for "no data" as the normal case.

## Sketch of a pipeline layer (if worth doing)
Run a one-off offline build step, `habitat_units`, outside the atlas rebuild:
1. For each stream section with order 4 or more inside HRDEM/LidarBC coverage, sample the DTM every 10 m along the FWA line. Use the WCS, or better, cached COG tiles.
2. Take the channel low in a window sized to the river's width. Force the profile monotone, smooth it, and compute slope over about 70 m.
3. Classify flat/run/steep against thresholds scaled to the reach's gradient. Merge runs shorter than 30 m.
4. Emit one point per unit at its midpoint: `{section_id, class, slope_pct, len_m, lidar_year}`.
   - That is only a few points per km, so a few MB for the whole province. It goes into the bundle next to the gauges and works offline.
5. Freeze it as one reviewed file, like `gauge_match.json`, and never regenerate it silently.
6. Label it in the app as "slack water / fast water (lidar YYYY)", not as "pool".

Run steps 1–2 on 2–3 more rivers (one braided, one confined canyon) and do the 10-pool hand check before committing.

Files: `profile.png`, `map.png`, scripts `fetch.py` / `sample.py` / `prof.py`, intermediates in `work/`.
