# T813 — An added stream that continues an FWA line past its MOUTH is minted at the top measure (Port Moody's Schoolhouse Creek North)

Status: DONE (0435b445e; merged into redesign/stream-sections 2026-11-17).

Source: the Sheldens Creek extension (`lane/cuts-1116`, 998b4e6ab) found that the added-streams resolver treated a
hand-drawn line past an FWA line's mouth as an extension past its SOURCE: minted at the top measure while the geometry
sits at the bottom. That lane fixed it for hand-drawn (`manual`) sources only, so no city stream moved. Port Moody's
"Schoolhouse Creek North" (source id 3) shows the same fault in today's data. To do: apply the mouth-tail rule to the
city sources too, regenerate `added_streams.json`, check the stream's place on the map and every binding that reads
it (changed answers, if any, to the owner first).

## Built (lane/t813-t825, 2026-11-17, 0435b445e)
- One rule for every source: `fwa_match._tail_past_end` no longer takes a trust flag; which end is the mouth is read
  from the FWA line (mouth first), never from the source line's vertex order. `DOWNSTREAM_TAIL_IS_NOVEL` stays the
  mutation pin. A line past a mouth is classified like any other line (re-draw = duplicate, own line = novel).
- Side regen of `added_streams.json` (burnaby squamish port_moody osm manual; the old code regenerates it
  byte-identically first): exactly ONE change, Port Moody src 3 (channel 3/4/15/16, a 104 m hook at the tidal mouth of
  FWA 360827018) is now a duplicate of the FWA line, so its `extension` record (base 1,663 m, geometry at the mouth)
  is gone. No other stream, name variant or exclusion moved.
- Port Moody bbox graph built with both files: sections, section geometries and edges identical (360827018 is one
  section; no cut, tidal row, gauge or curated file names it). No binding or answer changes.
- Seen, not changed: two OTHER extensions (src 10 and 46) are both minted on 360827018 at the same top measure, and an
  extension on a one-section FWA line makes a self-loop connector edge (360827018:0 -> itself). Pre-existing, same
  before and after.
- Owner page (no decision needed): scratchpad t813-t825/T813-schoolhouse-creek-north.html.
