# T293 — One guided walkthrough: find a water, read its rules, read its conditions, save a spot

Status: OPEN (owner 2026-10-29: "improve the little tutorial … it wasnt multiple parts but instead walked the user through
finding a regulation using the map and search and then how to read the regulations and how to look at conditions / what
change means for it. also walking through adding a spot and why … then the app can have info on syncing and offline etc.").

## Work
1. Replace the separate first-run coach marks (T248 tour + T276 Runs/Conditions tours) with ONE guided, hands-on walkthrough
   the user does with real taps on a real water (a well-known one, e.g. Vedder River, chosen by the app; skippable at any
   step, resumable, re-openable from Help):
   1. **Find it** — search for the water (type a few letters), then the same water on the map (tap it).
   2. **Read the rules** — today's answer (open/closed, the colour), the day picker, what you can keep (quota, sizes),
      gear/methods, where a rule only holds on part of the water, the rule book link.
   3. **Conditions** — the gauge reading vs normal, the forecast, and what a change means for fishing there (rising/
      dropping, high/low for the date), in plain words.
   4. **Save a spot** — drop a spot, add a note/photo, and why: it keeps the day's conditions so you can see which coming
      days look like it.
   Mode-specific tips (Salmon, Runs colour key) stay as short one-time hints on first use of that mode.
2. Help (More → Help) gains short pages: syncing / updates (the refresh button + bell), offline (what is kept on the phone),
   and re-run the walkthrough.
3. Words in the core words tables; existing coach-mark component/look; works on phone + desktop web; accessible
   (screen reader announces each step; never traps focus; Android back steps back/out).

## Acceptance
A first-run user completes it end to end on the emulator and web; screenshots of every step (phone + desktop); skip/resume
works; a11y gate covers it.

Depends: T248, T276. Size: M. Owner: no.
