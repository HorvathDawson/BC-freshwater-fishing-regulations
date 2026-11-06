# T383 — Per-water time zones (later)

Status: DROPPED (owner 2026-11-06: the one B.C. clock stays). Was: OPEN — after 0.5.0 (owner 2026-11-04: "this seems too confusing could be make fixing time zones to be a ticket then we just set it to constant bc time for now? with labelled timezone in the sunrise/se

For 0.5.0 (T382): one constant B.C. clock, UTC−7 ("PT"), for the regulations' day and for sunrise / sunset, the zone
labelled on the sun line.

## Later
- Each water's zone from its coordinates at build time (Python `timezonefinder` → an IANA name per water in the
  release; only America/Vancouver, Edmonton, Creston, Dawson_Creek, Fort_Nelson occur), the offset from the app's own
  rule per zone (device tz data can be stale: tzdata 2026b, 2026-04-22, carries B.C.'s change; many devices may not).
- The south-east (East Kootenay, Golden): sources conflict (UTC−6 with Alberta vs joining UTC−7); verify Alberta's 2026
  status before writing its rule.
- Sunrise / sunset on the water's local clock; whether the regulations' day turns at local midnight is the owner's call.
