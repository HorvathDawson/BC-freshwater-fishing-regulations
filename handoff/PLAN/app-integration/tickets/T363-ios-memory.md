# T363 — iOS memory footprint

Status: OPEN (owner 2026-11-03: Android 0.5.0 first, iOS next; from the ticket check's iOS gaps).

Run T360's per-screen measurement on the iOS Simulator (boot, pans, water sheet, Conditions, Salmon, rule-book
PDF), then on a real iPhone once T88 gives a signed build. Simulator showed RSS 420 MB, 665 MB peak panning (T313).
Apply T360's fixes and check they help on iOS too. Size: S–M.
