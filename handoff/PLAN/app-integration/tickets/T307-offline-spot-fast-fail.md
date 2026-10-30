# T307 — Adding a spot offline is instant (no waiting on network that isn't there)

Status: OPEN (owner 2026-10-31: "When offline adding a spot takes a long time because it doesn't fast fail the trying to get data").

## Work
Find every fetch the spot capture waits on (weather/Open-Meteo, gauge reading, day flow, hydat history, place lookup) and
fail fast when the device is offline (NetInfo says no connection → don't even try; flaky → short timeout ~2–3 s) — save
the spot immediately with what's on the device, mark the missing parts "fills in when you're back online", and backfill
them later (the existing refresh path). Same for adding a visit (T294). Emulator check in airplane mode: save < 1 s.

Depends: T294. Size: S. Owner: no.
