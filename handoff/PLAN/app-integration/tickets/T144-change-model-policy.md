# T144 — Change: use CLEVER and COFFEE (combine when both), ELF only as a last resort with a low-water warning

Status: OPEN

Source: Owner 2026-10-23 (app walk). "the change could use coffee and clever but no elf (can combine both if available too for more confident
guessing) clever is better model i think but you can research them to see how you should use them. it can use elf if
nothing else is there. but maybe have a warning for it being a low water forecast". Related: T52 (Expected change),
T126 (CLEVER summary), Q59–Q61 (model order).

## Work
1. Research the BC River Forecast Centre / ECCC products named CLEVER, COFFEE and ELF (what each models, horizon,
   skill, which stations, low-flow vs freshet use); write the findings into this ticket with sources.
2. One function decides the Change source per station and horizon: CLEVER + COFFEE combined when both (state the
   combination rule and why; CLEVER weighted higher if the research supports it), one of them alone, else ELF with a
   visible "low-water model" caveat; never ELF beside CLEVER/COFFEE. Feed side (pipeline/gauges/feed) if the choice must
   be made where the data lives; app words and caveat.
3. Whole-value tests per case, mutation pins, web shot.

Owner questions with defaults. Size: M. Rebuild: feed republish (owner) if the feed changes.
