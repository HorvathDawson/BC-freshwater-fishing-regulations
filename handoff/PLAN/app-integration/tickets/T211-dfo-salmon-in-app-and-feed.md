# T211 — DFO salmon regulations in the app, and the DFO feed (plan + build)

Status: OPEN

Source: owner 2026-10-27.

## 1. Salmon regulations in the UI — OPTIONS for the owner
Lay out 2–4 designs (with mock shots) for showing DFO tidal/freshwater salmon rules alongside the provincial regs: e.g.
a separate "Salmon (DFO)" section on the water page, merged into the day's answer with a DFO source badge, a salmon
map layer, or a per-species sheet. For each: what an angler sees on a sample day, how DFO's frequent changes and
effective dates show, how conflicts with provincial rules read, data/release size. Owner picks.

## 2. The feed — plan how it works
DFO notices change often (memory `dfo-salmon-churn-asymmetry`: waters ~never move, rules turn over ~50%/yr, scope text
drifts). Plan: fetch cadence, diffing, where the feed lives (like the gauge feed: local + R2 sync, owner-run upload),
how a rule change reaches the app without a full rebuild, failure handling, and the owner's role.

## 3. Extents must match every historic location
Build the DFO locator → extent table from ALL historic DFO scrapes (not only today's page), so a location that comes back
or reappears with drifted wording is already matched. Normalise scope text (the 99 dormant locations are part of this);
report coverage: historic locations matched / total.

## 4. New-location alert
When the feed sees a DFO location (or scope wording) that matches no known extent, it must NOT silently drop its rules:
hold them unplaced and NOTIFY the owner (FEED_NOTIFY_URL, like HYDAT) so the new locator → extent can be added to the
matches and the rules propagate. Include a test feed with an unseen location.

Order: 1 and 2 are plans for the owner; 3 and 4 are code once the plan is accepted.


## Owner pick (2026-10-27)
Option A. All salmon closed → "Salmon — closed"; else species grouped. Main-map toggle Standard / Salmon in standard regulation mode (salmon paints differently). Next: build A + the toggle, then the feed (parts 3–4).
