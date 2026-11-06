# T443 — Stepping ▲/▼ between a big part and a tiny one jumps the camera (disorienting)

Status: OPEN.

Source: owner problem report b48c0e449372c34b, 2026-11-06, app 0.6.2 (staging worker `GET /reports`): "When navigating
a river like the Fraser regulation map and you click up or downstream and it goes from a big section to a tiny section or
reverse it is very disorienting. Fraser is a good example in region 2.. we should have a minimum zoom in maybe and just
make it overall cleaner."

## Work
1. Reproduce on the Fraser (Region 2) water map: step ▲/▼ across its parts, record each fit's zoom (web + emulator).
2. Bound the camera change per step: a zoom floor/ceiling for a part fit (do not zoom in past what keeps the part's
   neighbours in view; do not zoom out to the whole river for one big part), and an animated transition that keeps
   the shared boundary on screen. One rule for every water (no per-water special case).
3. Shots of before/after on the Fraser and Coquitlam steps.
Size: S–M. Owner: look after.
