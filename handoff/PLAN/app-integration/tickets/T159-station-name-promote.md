# T159 — Gauge auto-promote checks a station's name against every name the matcher accepted

Status: OPEN

Source: T125 item 8 "gauges 9"; Q118 decided by Claude 2026-10-23 (decisions.md owner-question triage).

## Work
promote.evidence checks a station's name only against the match's display name, while the matcher accepts any of the
node's names (Arrow Reservoir vs Lower Arrow Lake), so such stations never auto-promote. Add a StationMatch field for the
name the matcher used, regenerate match.json, and list the stations that would promote; the OWNER runs promote with
--only per station and --accept-demotions review (memory gauge-regenerate-drifts). Size: S.
