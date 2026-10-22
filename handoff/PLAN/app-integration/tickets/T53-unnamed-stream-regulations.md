# T53 — An unnamed stream shows no regulations (only Conditions)

Owner 2026-10-22: "clicking on unnamed streams shows no regulations right now only conditions. this is definitely an
issue that needs a ticket."

## The problem
Tapping a stream with no name opens Conditions only. The reader gets no regulations at all, though every stream in a
region is fished under that region's rules.

## What it should do
- An unnamed stream has regulations: its REGION'S BASE (zone rules are the base: region-wide rules, plus any water
  override that reaches it, e.g. a named river's tributaries rule). Base + deltas: every line attributable.
- Tapping it opens the same one water screen (openWater) with a Regulations tab, like a named stream. The title says
  what it is without inventing a name (e.g. "Unnamed stream", plus where it flows into when known: "tributary of
  Nicomen Slough").
- Answers stay pipeline-generated: if unnamed sections have no answer rows today, the pipeline produces them; the app
  never assembles regulations itself.
- Conditions for it keep working as today.

## Acceptance
- Real-data test: a sample of unnamed sections in each region opens a Regulations tab whose lines equal the region
  base plus the overrides the reach run binds to that section (from the bundle, not a copy).
- A tributary rule of a named river (e.g. "and tributaries") shows on its unnamed tributaries.
- No unnamed stream opens without a Regulations tab (whole-map count = 0), pinned by a test.
- Back / close behave per decisions.md "back steps out one layer".

## Cause
(to be filled in by the diagnosis — see below)
