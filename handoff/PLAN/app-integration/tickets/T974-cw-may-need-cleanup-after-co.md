# T974 — Classified Waters "may need" note: clean up once a CO answers

Status: BLOCKED (owner) — waits for a Conservation Officer's answer to the T973 question (GOTCHAS.md, "Classified
Waters licence").

Source: owner 2026-11-20 12:38: "we want to ticket cleaning this up once we get contact with a CO", about T973's
"may need" note ("Stellako River Class II may apply here: the Endako has its own line without Classified Waters.
Check with a CO.") and its separate optional data file, which apps before 0.8.4 ignore.

## Work
Once the CO's answer is quoted in T973 with its date:
- **If the CO says the unit carries through:**
  - Carried units become plain units: placed like any other designation in the licensing data, with no note.
  - The optional "carried" file and the app's note path are removed.
  - Re-measure which sections change against the T973 build.
- **If the CO says it does not:**
  - Drop the carried units and the note.
  - Remove the optional file, its contract entry, the app reader and the note. Keep the `class` / `source` / `through`
    record only if something else still reads it.
- **Either way:**
  - Record the answer beside RU-13 and LI-2 (RULINGS, AGENTS, the `first_classified_downstream` docstring).
  - Remove the GOTCHAS question.
  - Update the T973 mutation pins to the settled rule.
- **The second T973 case:** a classified water whose unit does not reach its tributaries, which the walk passes
  through. Settle it the same way if the CO's answer covers it; otherwise ask the owner.

## Acceptance
No "may need" note is left in the app or the data. The CO's answer is quoted in T973. The binding diff by place
against the T973 build is in this ticket. The tests pin the settled rule.

Depends: T973 (built), a CO's answer (owner). Size: M. Owner: yes (contact a CO). Default: keep the "may need" note.
