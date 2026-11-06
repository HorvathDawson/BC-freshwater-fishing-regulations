# T445 — Map error: "'…' 2298 resolved to '…' null which is null or a wrong type" (Android)

Status: OPEN.

Source: staging worker `GET /crashes` (read 2026-11-06), kind 655fec4ac8cca138: 0.6.1 (build 10) and 0.6.2 (build 11),
Android 37 (owner's phone), route map, `java.lang.Throwable`, no stack, 2 reports. The words are MapLibre Native's
expression error ("… resolved to … null which is null or a wrong type") with the names scrubbed to '…' by the report
filter — so we cannot tell which property/layer.

## Work
1. Find which style expression can evaluate to null: feature-state / paint-index lookups on the stream layers (e.g. a
   `["feature-state", …]` or `["at", …]` with no `coalesce`) for feature id 2298-ish handles; reproduce on the emulator
   (pan + mode switches) with logcat (MapLibre logs the full text locally).
2. Fix the expression (an explicit value for "no state yet" that draws as unknown, not a silent default colour).
3. Make the scrubber keep MapLibre's property/layer names (they are not user data) so the next one names itself (T352).
Size: S.
