# T1012 — Staging report 2026-11-22: Serpentine River shows ditches on its mainstem

Status: OPEN — next release batch (owner 2026-11-19: small finds go into the next release).

Source: `GET /reports` on the STAGING feeds worker, read 2026-11-22 18:15 (report `e552b9f0b31e2698`). Android 0.8.5
(build 43, 558c527), staging, release `r-741815c6090d`, tiles `4ae7b62955bc9990.fc51e82f7c91`; water · map, salmon
mode, Serpentine River (`gnis:24978`) part 2, zoom 11 at 49.112, -122.788. Reason "rule". Verbatim: "Has ditches on
mainstem? Seem like they shouldn't be included".

## Work
1. List the lines in `gnis:24978` part 2 that FWA codes as ditches (edge type 2300 / named "ditch"), with their gnis.
   `registry.build.DITCHES_LEAVE_RIVER_ITEMS` drops a 2300-only ditch with no gnis of its own; say why each one shown
   stayed (own gnis = the river's? a mixed edge type? a salmon-mode DFO place drawing it?).
2. If they should leave: fix where the item is built (registry), never in a reader; diff the reach run's rule bindings
   (rule 63) before it ships. If they are the river by FWA's own name, show the owner a picture and ask.

Depends: none. Size: S–M. Owner: maybe (a picture if FWA names them the river).
