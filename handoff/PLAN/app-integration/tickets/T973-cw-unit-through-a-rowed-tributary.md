# T973 — Classified Waters: does a unit carry up through a tributary whose own row prints no unit? (ask a CO)

Status: IN PROGRESS — owner 12:11: build the carried unit with its path IN THIS RELEASE, shown as a "may need" note; the
CO question stays open (it decides the note later).

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

## Owner's plan (2026-11-20 12:10): carry the unit through, with where it came from
Verbatim: "Maybe we should propogate classified waters through the separate listed streams still just make it a tuple
or something with what class, where the source of that classified waters is and anither item saying if it passed
through anither listed stream without classified water title etc. still have nearest classified water level with so
downstream cwi is overruled by a more upstream cwii etc. make sense? Then we can at least have the info throughout".

What that means:
- **The walk no longer stops** at a tributary with its own no-unit row (LI-2's stop). The unit carries on up.
- **Each placed unit carries a record:**
  - `class`: the unit's class, e.g. Class I or Class II;
  - `source`: the classified water it comes from, e.g. the Stellako;
  - `through`: the rowed no-unit waters it passed on the way up, e.g. [the Endako]. It is empty when it passed none.
- **RU-13 stays:** one unit per section, and the nearest classified water wins. An upstream Class II overrules a
  downstream Class I for the sections above it.
- **The data is kept everywhere.** Whether a unit with a non-empty `through` counts as "you need this licence" or as
  "you may need it, ask" is a display choice; the CO's answer settles it later without a rebuild.

## Owner answers (2026-11-20 12:11, dialog)
- Display until a CO answers: **"\"May need\" note (Recommended)"**. A unit that passed through a rowed no-unit water
  shows with a short note, e.g. "Stellako River Class II may apply here: the Endako has its own line without Classified
  Waters. Check with a CO." A unit that passed through no such line shows as today.
- When: **"In this release"** (not after 0.8.4). The release waits for this lane and its review.

## Work (this release)
1. Reach: carry the unit through rowed no-unit waters with `class`, `source`, `through`; RU-13's nearest-unit rule
   stays. A unit with an empty `through` is placed exactly as today (byte-identical where nothing passes a rowed water).
2. Bundle + release: the carried units with a non-empty `through` go in an OPTIONAL new table/file, so apps before 0.8.4
   show nothing new (as today) and never show them as a plain requirement.
3. App 0.8.4: the licence card shows the "may need" note from that table.
4. Measure and list: sections and waters that gain a "may need" note, the biggest first.

## Work (when the CO answers)
1. Measure: every section LI-2 drops today (switch `own_row_not_classified` off vs on, in memory through heavy.sh, as
   T944 did), counted by water, with the biggest waters listed and rules digest unchanged.
2. If the CO says "carries through": stop treating a rowed no-unit tributary as a stop in `first_classified_downstream`
   (RU-13's nearest-unit rule stays), update the docstring, AGENTS and the rulings, run reach + bundle, diff by place.
3. If the CO says "not classified": record the answer beside LI-2; no code change.

## Acceptance
The CO's answer is quoted here with the date. The measurement is in this ticket. The code matches the answer, with a
mutation pin on the stop.

Depends: a CO's answer (owner). Size: M. Owner: yes (ask a CO). Default: keep LI-2, as shipped.

## Owner 12:24 (2026-11-20): nearest wins both ways; only a tributary-applying unit claims what is above it
Verbatim: "this also happens invereted (Nearest still wins: a stream gets only one unit, and an upstream Class II overrules a downstream Class I above it.) for example a upstrem class i overrules a downstream class ii too. (assumign that the classified water sis applied to tributaries) usually it says it?"
- Nearest wins whatever the class: an upstream Class I overrules a downstream Class II above it (RU-13 already does).
- A classified water claims the sections above it only when its row applies to tributaries ("[Includes Tributaries]",
  or a tributaries-only row). Today a walked section is re-homed to ANY designation whose own reach it meets. Default
  built (L10, to confirm with the owner): a classified water whose unit does not reach its tributaries is a
  pass-through like a rowed no-unit water: it goes in `through`, and the card shows the "may need" note. L10 counts the
  cases and names up to 5.


## Owner 12:54 (2026-11-20): the 5 Classified Waters units that stop at their own water are right
Checked against the book by the owner: Thompson River (R3), Skeena River sections 2 and 4 (R6, mainstem only), West Road
(Blackwater) River (R5, "Mainstem only"), Deena Creek (R1, no tributary mark on its Class II line). The other 60
waters' units reach their tributaries. Owner verbatim: "yes i missed those both are correct too." (after confirming
Thompson and Skeena from the book: "so only a few classified waters streams in regulations dont apply to the
tributaries").
