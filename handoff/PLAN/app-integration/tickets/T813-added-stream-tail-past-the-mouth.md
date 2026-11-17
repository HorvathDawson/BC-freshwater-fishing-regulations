# T813 — An added stream that continues an FWA line past its MOUTH is minted at the top measure (Port Moody's Schoolhouse Creek North)

Status: OPEN (found 2026-11-16 by the cut-fixes lane; not started).

Source: the Sheldens Creek extension (`lane/cuts-1116`, 998b4e6ab) found that the added-streams resolver treated a
hand-drawn line past an FWA line's mouth as an extension past its SOURCE: minted at the top measure while the geometry
sits at the bottom. That lane fixed it for hand-drawn (`manual`) sources only, so no city stream moved. Port Moody's
"Schoolhouse Creek North" (source id 3) shows the same fault in today's data. To do: apply the mouth-tail rule to the
city sources too, regenerate `added_streams.json`, check the stream's place on the map and every binding that reads
it (changed answers, if any, to the owner first).
