# T104 — One "when" in words: titles and plain lines drop hours and weekdays

Status: OPEN.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding 2
(CONFIRMED on `ui-rules-export.json` / `ui-rules-answers.json`).

## Decision being unified
"How is a rule's `when` (dates, weekdays, hours, unparsed seasons) said in words?"

## Where it is decided today
- `pipeline/deliver/answers/display.py:272` `_plain`: dates only.
- `pipeline/deliver/answers/display.py:382-387` `_title`: dates and weekdays, no hours.
- `pipeline/deliver/answers/labels.py:301` `_when_words`: everything.
- `pipeline/deliver/answers/rows.py:2204-2234`: four more weekday wordings.

How they differ:
- Campbell River's export has `when.hours` 21:00–05:00, but the shipped title and plain both say "No fishing, Aug 1–Dec
  31". Nicomekl and Serpentine are the same. A night closure reads as all-day.
- Kootenay Lake Lower West Arm: the title says "…, Sat–Sun" but plain says "Keep up to 5 kokanee a day". The other
  rule's title says "Release kokanee, Mon–Fri" but plain says "Release every kokanee".

## Work
- One `when_words(when, style)` in `pipeline/deliver/answers/common.py` that covers dates, weekdays, hours and
  unparsed seasons. `style` covers the title/plain/label variants. `_plain`, `_title`, `_when_words` and the four
  `rows.py` wordings call it.
- Gate: any rule with `hours` or `weekdays` names them in both `title` and `plain`.
- Pin: a grep test that no other module in `pipeline/deliver/answers/` formats weekday or hour names itself.

## Acceptance
Proof is a reviewed behaviour change. The answers diff lists every changed title and plain. Expected: the hours rules
(Harrison, Pitt, Campbell, Nicomekl, Serpentine, Slocan) plus every plain whose rule has weekdays, and nothing else.
The gate fails on a mutation that drops hours from `_title`.

Rebuild: deliver. Depends: none. Size: M. Owner: no.
