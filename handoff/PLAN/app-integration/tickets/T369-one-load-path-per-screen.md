# T369 — One load path per screen: same output whatever the way in (in 0.5.0)

Status: OPEN — batch 6c of RELEASE-PLAN-0.5.0, beside T368 (owner 2026-11-03).

## The rule (owner 2026-11-03)
"Make sure all pages have single path to load and that it produces same output regardless of the item that triggers
the load whether a refresh or tab switch in regs pane or search etc. Should always be consistent regardless of
navigation pathway. This is an important audit to prevent bugs like this again."

## Why (bugs found 2026-11-03, all one class)
- Coquitlam Map view: a part picked from "Show all N parts" lit a 440 m sliver; ▼ to the same part lit it whole (T345).
- ▲/▼ moved the camera but the highlight stayed on the old part (web).
- A reload on `?regs=…&part=2&view=map` drew the river "Base rules" blue instead of "Own rules" orange, framed wider.
- Entering by a map tap (frame holds `tapSid`) vs search: only the tap route crashed the 0.4.0 app (T345).

## Scope
Every screen a route can show (map tabs, a water's Regulations / Conditions / Map views, More pages, sheets):
1. **List the ways in** for each: cold start / reload on its URL, tab switch, view-strip switch, search result (Show /
   Open), map tap, ▲/▼ step, parts-list pick, flows-into / continues links, back, deep link, offline start.
2. **One load path:** each screen's state is derived from the route (+ the release) by ONE function; no way in may
   carry extra state (a held ref, a stepped index, a tapped section) that changes what is drawn.
3. **A net:** a test harness that opens each screen by every way in and compares what it draws (the view model, the
   map's highlight/colouring/feature-state, the camera's target) — they must be equal. Fails on a new divergence.

Method: as T368 — one read-only pass (screens × ways in, where state can diverge), owner sees the list once, then fixes
with the net. Web and native one logic path.
