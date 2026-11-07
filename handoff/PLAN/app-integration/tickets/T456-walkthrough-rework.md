# T456 — The guided walkthrough, reworked: show the real use, never cover what it is about, teach reading

Status: OPEN (owner 2026-11-06, chat 5, from a tester's feedback with a screenshot).

Owner: "The tutorial needs work. Make it actually show the use and make sure it doesn't cover items like in the
example. Also it should show examples of how to read the rules, how to read the conditions etc."

## What the tester hit (0.6.x, Vedder River → Conditions, step "The next few days", 14 of 20)
1. **The card covers the control it is about.** The day chips (Sat 7 … Thu 12) sit under the card. The tester could not
   change the day while the step was up ("the menu stays in the way").
2. **The words don't match the screen.** The step says "Tap +1d to +10d … the arrow at the end shows if it's going up or
   down". The screen shows `Now · Thu 12 ▲ · Change` and a row of day chips. The step text is stale.
3. **The water page's "‹ Map" stepped the walk back, to the wrong place.** That button leaves the water page. The walk
   took it as a Back (`useBackLayer` → `guide.back`), moved one step back, and did not put the app where that step
   expects. Leaving the page during a walk needs a clear rule (guardrail): e.g. the walk pauses, or the step that needs
   the page brings the reader back to it. It must never land half-way.
   **More from the tester:** "it all started when the tutorial said click back on the map button; it just brought me
   back a step instead." So the step ASKED for "‹ Map", and pressing it was caught as the walk's Back. The walk's back
   handler (`useBackLayer(guide.open, guide.back)`) sits on top of the page's own back. When the ringed control IS a
   back/close control, pressing it must do its own job (leave the page) AND complete the step, never go back a step.
   The owner's direction: "make it more interactive, so the user clicks it to go next, instead of it doing the normal
   action under the hood then borking it". Pressing the ringed control is how a step moves on. The walk never also runs
   the action behind the reader's back. Test: on that step, press "‹ Map" → the water page closes, the walk is on the
   NEXT step, the log holds the page's own close once.

## Work
1. **Never cover the target:** the card is placed beside the ringed control and never over it (above/below, or docked
   at the opposite edge). The ringed control and what it opens stay usable while the card is up. A test checks that for
   every step on phone and desktop sizes the card's box does not meet the ring's box, including after a sheet slides
   or the page scrolls.
2. **Show the real use (do, not read):** each step about a control asks the reader to use it ("Pick Thursday") and
   moves on when they do (the DO-step mechanism, `RULES[*].done`). "Show me" does it for them visibly. No step only
   describes a control.
3. **Teach reading, with real examples:** short steps that point at real lines on the screen.
   - Regulations: how to read a rule line (status, the fish, dates, quota/size, where it applies, the rule book link),
     a closure, and a "both hold" note.
   - Conditions: what the reading and its percentile/words mean, the forecast days and the trend arrow, the gauge's
     trust line, Flow vs Level, 72 h vs Year.
   The example waters must be ones the release holds (pick by stream key + start, never a raw section id, rule 46).
4. **Guardrails:** every way out of a step's screen (‹ Map, the tabs, the system back, closing a sheet) has one
   defined result: back to the previous step with the app put where it expects, or the walk pauses with a "Resume
   walkthrough" chip. Never a half state. Tests for each exit on each step that has a screen of its own.
5. **Words in one place:** step titles/bodies stay in `WALKTHROUGH_WORDS`. A test checks that every control a step names
   by label exists on the screen it rings (catches "+1d to +10d"-style drift).
6. Emulator walk (test bed `walkthrough` scenario, updated) with shots per step; send the owner a short recording.

Related: T293 (the walkthrough), T455 (tap races in the card). Size: M–L. Owner: phone check; the reading steps' wording
is Claude's draft, shown to the owner in the recording.
