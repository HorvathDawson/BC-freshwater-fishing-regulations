# T122 — One station liveness: "active", "realtime" and "ECCC-listed" used interchangeably

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding gauges 5
(PLAUSIBLE).

## Decision being unified
"May this station speak at all, and is it speaking now?"

## Where it is decided today
- `pipeline/deliver/bundle/build.py:619-625`: "active, not traffic" (deliberate; stays the rule).
- Elsewhere in `pipeline/gauges/`: `realtime` flags and the ECCC station list are each used as "live".

How they differ: a station can be active but missing from the ECCC list, or flagged realtime but inactive, and the
readers disagree on whether it speaks.

## Work
- `pipeline/gauges/liveness.py` with `may_speak(station)` (the build.py rule) and `speaks_now(station, feed)`; every
  reader calls them.
- Pin: a grep test that `realtime` / the ECCC list are read only inside `liveness.py`.

## Acceptance
List the stations whose liveness changes (expected few, each named). Bundle diff on the gauge tables reviewed.

Rebuild: deliver. Depends: none. Size: S. Owner: no.
