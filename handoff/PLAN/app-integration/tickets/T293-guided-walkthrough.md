# T293 — One guided walkthrough: find a water, read its rules, read its conditions, save a spot

Status: DONE on web (headless, 390 + 1280); emulator run owed with the next APK (owner 2026-10-29: "improve the little tutorial … it wasnt multiple parts but instead walked the user through
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

Owner add-on (2026-10-29): after "Save a spot", open the saved spot (Spots → the spot) and read "The next few days" — the
score (flow and lead-up rain first, then cloud/pressure, then air/wind), the reasons and "Closest to your visit". Worded
"a day you fished here" so T294's several visits per spot do not need a rewrite.

## Done (branch t293-walkthrough, merged after the APK cut)
- ONE walk, 21 steps, phases Find a water · Read the rules · Conditions · Save a spot · Your spot · That's it
  (core `guideSteps`, `GUIDE_WATER` = Vedder River found by name in the release; any water if absent). DO steps advance
  by themselves when the app gets there (search typed, row selected, water open, view, capture, spot open); "Show me"
  does them through the route/search/opener. SHOW steps: Next.
- Non-modal card in the coach-mark look (`GuideCard`, Walkthrough.tsx) with the accent ring on the control: search
  field, view tabs, regs year strip / date pill / species table / section tabs / part line / rule-book link, Conditions
  headline / horizons / chart, Spots tab, a spot's next days / closest day (`useCoachTarget`; registry now holds several
  views per key). The web scrolls an off-screen target into view.
- Once per device after the first-run notice (`walkthrough-3`); Skip at any step; resumes where left (`guide-at:` line in
  the same store); More → Help replays it. Back (system/browser/Escape) steps back one step, out from the first; stepping
  back puts the app on the step's view. Non-modal dialog, live-region title + body (phone: announced), no focus trap.
- Mode hints cut to one step each: Runs (colour key), Conditions map (gauge colours), new Salmon (on first Salmon).
- Help: short pages Walkthrough (replay + the three hints) · Syncing and updates · Offline (app/web words; Offline maps
  button on the app).
- Shots: T293-shots/ phone-* and desktop-* (every step, capture, Help, replay, back, Salmon hint); `shots.mjs` drives it.
- Tests: Walkthrough.test (walk end to end, auto-advance, skip, resume, back, ring, a11y role), help.test, a11y.shell
  (card + Salmon hint), back-layers entry. Pre-existing fails NOT from T293: back-layers SynopsisLoading BackHandler,
  NotFishingSheet missing from the surfaces list (T292).
- Emulator: not run — no emulator up and the APK is being cut; verify on the next APK (first run, Android back).
