# T580 — Audit: places where the same water can show differently (web vs phone, Flow vs Level, Now vs a day ahead)

Status: DONE — f1acba096 (findings; fix tickets T581–T586)

Source: owner 2026-11-10, on T568 / T567: "stuff like this should never happen and shows a divergence in code too so
look for other things like that."

## What "like this" means (from T567 / T568)
- The sheet's block ORDER changed with the state (map above or below the chart, or gone on a refusal).
- The LEAD gauge changed with the quantity asked.
- A figure was in one quantity over a chart in another; today's reading stood unlabelled on a forecast day.
- Web and phone showed different things for one water from one feed.

## Work (read-only audit first, then fix tickets)
1. Web vs native: every `.web` / `.native` pair, every `Platform.select` and `INLINE_ALLOWED` entry: do both halves give
   the same answer and the same layout? Any logic that lives in one half only.
2. State-dependent layout: every screen / sheet whose block order, or whether a block exists, depends on data (a
   refusal, a missing reading, a quantity, a day, a loading state). One fixed order per screen; a block with nothing to
   say keeps its place and says why.
3. One choice, asked once: anything picked per quantity / per day / per surface that should be picked once per water
   (lead gauge, default quantity, default day, selected part, units), and any value composed in a component instead of
   core (AGENTS rules 23, 25, 33; memory "one load path per screen", T369).
4. Kept state: what each platform keeps between visits (day chip, quantity, span) and whether a cold start, a tab
   switch and a deep link land on the same state on both.
5. Output: a short findings list ranked by what an angler would see wrong, each with file:line and a proposed fix;
   file fix tickets; add conformance tests (one fixture, both platforms, every quantity and day → same lead, same
   order) and a cheap gate where a class can be checked mechanically.

Start after T568 / T567 report their causes: they define the classes to look for.

Depends: T568, T567. Size: M. Owner: no.
