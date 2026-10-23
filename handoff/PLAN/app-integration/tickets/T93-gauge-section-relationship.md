# T93 — The data records how a gauge relates to each section it speaks for

Status: OPEN. Owner needed (policy).

Source: memory `gauge-representativeness-is-policy`; owner 2026-09-02: the gauge rule should be a tunable policy.
`section_gauge` is still `(sid PK, station, trust, mag)` — no up/downstream, channel distance or same-watercourse;
`pipeline/hydro/shed.py`'s drainage gate is the only rule.

## Work
A table of each section's relationship to each candidate gauge; today's gate stays the default behind a pluggable
policy; prove two policies on the Vedder/Chilliwack and Slesse Creek cases.

## Acceptance
Schema + tests; today's answers byte-identical under the default policy.

Depends: pairs with T76; needs a deliver. Size: L. Owner: yes. Default: keep the current gate as the default.
