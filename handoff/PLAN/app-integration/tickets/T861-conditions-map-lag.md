# T861 — The Conditions map feels laggy like the Salmon map did (same cause as T848/T855?)

Status: OPEN (app chat; being measured).

Owner 2026-11-18 ~17:10 (to the main chat, relayed 17:12), verbatim: "conditions seems like it was laggy like salmon map
was maybe it had same issue?"

T848/T855's layer split (lane A, in the 0.8.1 integration branch app/integrate-081) moved the lit/closed widths to a
runtime `stream-heavy` layer so MapLibre stops re-evaluating a big line-width expression on every feature-state change.
The staging site the owner is testing runs 0.8.0, which does not have that split. Work: check whether the Conditions
tab's map layers get the same split, measure its zoom-out and pan the same way (long tasks, staging 0.8.0 vs the 0.8.1
web build), and fix it if it is left out.

Owner 2026-11-18 ~17:15 (relayed), verbatim: "also check run timing to while at it". So the Run timing map is measured
and checked the same way; any fix rides the 0.8.1 web release.
