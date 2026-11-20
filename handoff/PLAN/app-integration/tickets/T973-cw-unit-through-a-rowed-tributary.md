# T973 — Classified Waters: does a unit carry up through a tributary whose own row prints no unit? (ask a CO)

Status: BLOCKED (owner) — a question for a Conservation Officer; the release keeps today's rule (LI-2).

Source: owner 2026-11-20 11:44 (dialog, on T944's changes): "We should maybe just make this a ticket and a gotcha that
needs to be asked to a CO. Give info in the ticket. Doesn't hold release (keep what we have but on ticket reset we can
measure it better etc and make a decision)".

## The question (a gotcha)
The book prints "[Includes Tributaries]" on a Classified Water. One of those tributaries has its OWN row in the book,
and that row prints no Classified Waters unit. Is that tributary, and everything above it, inside the classified water
or outside it?

## How it works today (the owner's 2026-10-03 rulings, `pipeline/atlas/reach/licensing.py first_classified_downstream`)
- **RU-13 (one unit per section).** A section walked by two designations takes the unit of the FIRST classified water it
  flows into. Two units always conflict: an angler can't need two different day licences for one spot. T944's moves
  are this rule:
  - three unnamed Morice tributaries go from the Bulkley to the Morice;
  - Brownie Creek goes from the Elk to the Fording.
- **LI-2 (a rowed tributary with no unit is not classified).** If the walk down from a section first meets a stream
  that has its own row in the book and no unit, the section is DROPPED, and so is everything above it. This is the
  2026-09-30 confluence ruling ("whose own row prints no CW — the Iltasyuko — does not inherit") applied to every walked
  tributary. Examples:
  - the Endako, under the Stellako's "[Includes Tributaries]";
  - Gosnell Creek, under the Morice's;
  - Harmer Creek, above Grave Creek's row (Elk);
  - the unnamed Horsefly creek, above Little Horsefly's row;
  - the unnamed Stellako creek 60818, above the Endako's row.
  T944 only made the dead ends of those streams agree with the rest of their water. It changed no policy.

## Why it's a question
The owner's general tributary rule (2026-11-20): all downstream tributary rules carry up. Where two conflict, the
nearest wins; where they are compatible, they stack. Read that way, a plain row with no unit doesn't CONFLICT with a
unit. Under it, Harmer Creek would keep the Elk's unit and Gosnell Creek would take the Morice's. LI-2 instead reads the
row as the book's word on that water, which says nothing about a licence, so the water is not classified. Only a CO
(or the Ministry) can say which reading is enforced.

## What to ask the CO
The question is in `pipeline/docs/GOTCHAS.md`, "Questions the owner will put to BC Fish and Wildlife" → Classified
Waters licence (one example: the Stellako and the Endako). Longer draft:
"A river is listed as Classified Waters with '[Includes Tributaries]'. One of its tributaries has its own line in the
synopsis that does not mention Classified Waters (e.g. Grave Creek under the Elk, Gosnell Creek under the Morice, the
Endako under the Stellako). Does a non-resident fishing that tributary, or a creek above it, need the river's
Classified Waters licence?"

## Work (when the ticket is reset after the answer)
1. Measure: every section LI-2 drops today (switch `own_row_not_classified` off vs on, in memory through heavy.sh, as
   T944 did), counted by water, with the biggest waters listed and rules digest unchanged.
2. If the CO says "carries through": stop treating a rowed no-unit tributary as a stop in `first_classified_downstream`
   (RU-13's nearest-unit rule stays), update the docstring, AGENTS and the rulings, run reach + bundle, diff by place.
3. If the CO says "not classified": record the answer beside LI-2; no code change.

## Acceptance
The CO's answer is quoted here with the date. The measurement is in this ticket. The code matches the answer, with a
mutation pin on the stop.

Depends: a CO's answer (owner). Size: M. Owner: yes (ask a CO). Default: keep LI-2, as shipped.
