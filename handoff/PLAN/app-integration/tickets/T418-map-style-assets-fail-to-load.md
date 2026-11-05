# T418 — Map style assets failed to load on the phone (sprite, glyphs, the `outside` source)

Status: OPEN (found reading reports 2026-11-05). Every 0.5.0 report from 11-04 17:07 PST carried the same MapLibre
Native errors: "Failed to load source outside", "Failed to load sprite" ×2, "Failed to load glyph range 0-255" for Noto
Sans Italic / Regular / Medium. The one 0.6.0 report (11-05 20:21) carries none.

Find whether it was a one-off (network drop, a staging upload mid-way, the old release T403 fixed) or a real gap: a
self-hosted asset (memory self-host-everything) missing from the release or not cached for offline use. If real: the
asset ships with the install, and a failed style asset is reported once (reportFailure), not only logged. Check the
0.6.0 crash/report feed again after 2–3 days of testing. Size: S. Owner: no.
