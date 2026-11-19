# T874 — The walkthrough has only Back and Next (and Skip); Next does what "Show me" did

Status: OPEN (proposal; lands only on the owner's yes). Built on branch `app/t874-tour-next` (from
`app/integrate-081-web`), not merged.

Source: owner, 2026-11-18 ~20:34, verbatim: "The tour should have only next and back and every next should be show me
logic. Person can skip if they want. Otherwise it is weird".

Today (T293 / T456 / T824 / T842): a DO step's card had Back, Next and a "Show me" button; Next skipped the step
without doing it, Show me did it (`GuideActs`), and the words said "tap it, or tap Show me". A SHOW step had Back and Next.

## Work (built on the branch)

1. The card's buttons are Skip (top right, as before), Back and Next. No "Show me", no "Done". The last card's Next
   reads "Start exploring" and ends the walk.
2. Next on a DO step makes the step's move through the app's own route (what Show me did) and the walk moves on as
   soon as the app is there (no 0.7 s pause). On a SHOW step it moves on. The reader may still tap the highlighted
   control; that moves the walk on as before. A second Next while a move is on its way moves on; it never makes the
   move twice or skips a step.
3. Back undoes the step's move: a DO step's set-up (`bring`) is now the app as it was before its move.
   - "Search for it": the search closed, the map in front.
   - "Open its page": the water closed, the search up with its name.
   - "Pick another day": the day back to today (new `GuideActs.today`, the Shell's `setMapDay(null)`).
   - "How is the river?": the Regulations in front (it used to bring Conditions back, already done).
   - SHOW steps ("Open or closed", "You're all set"): nothing to undo; their screen is put back as before.
4. Words (core `help.ts`, whole strings pinned in `help.test.ts`): "I'll walk you through the Vedder River: Next searches
   for it, or tap the search bar and type “ved” yourself. On every step, Next does what is highlighted." / "Next opens
   its page, or tap Vedder River in the list yourself." / "Next picks tomorrow, or tap the date and pick the day you plan
   to go." / "Next opens Conditions, how high the river runs; its dot says low, normal or high for this date."
   `WALKTHROUGH_WORDS.showMe` and `doneStep` removed. `WALKTHROUGH_VERSION` not bumped: same steps, so a device that saw
   the walk is not shown it again.

<<<<<<< HEAD
What does not fit cleanly:
- "Pick another day" undone sets today, even if the reader had a day picked before the walk began (that step is then
  passed going forward, so this needs Back from the step after it).
=======
Independent review (after the owner's "Walkthrough OK"), three defects fixed with tests:
1. Back undid a day the walk never picked (a day set before a replay from Help: the day step is passed, Back landed on it
   and set today). Back now undoes only a move the walk itself made (`made`); the walk picks only when no day is picked,
   so its undo restores today exactly, and a reader's own day is never touched.
2. On the data's last day Next found no "tomorrow" and did nothing: `guideDay` (core) picks the day before there, and a
   move that cannot be made returns false, so Next moves on. The words still say "tomorrow" (the last day only).
3. Next on "Open its page", then Back before the water opened: the late open marked "Search for it" done and the walk
   jumped two steps. A move Next made on a step Back left is now put back if it lands (within 5 s), and its "done"
   moves nothing on.

What does not fit cleanly:
>>>>>>> app/t874-tour-next
- "Open its page" with no chosen water (a release without the Vedder River): there is nothing for Next to open, so Next
  moves on and the next step pauses behind "Resume walkthrough" until a water is open (as Next did before).
- The brief named a Conditions toggle step and a spot step; T842 cut both, so the pictures show the six-step walk's
  search, pick, day and conditions steps.

## Pictures (390 px, dark, the web export of each branch, `t874/shoot.mjs` in the scratchpad)

Before: `t874-before-{1-search,2-pick,3-day,4-conditions}.png`; after: `t874-after-{1-search,2-pick,3-day,4-conditions}.png`
and `t874-after-5-back-to-day.png` (Back from "How is the river?": the day is today again). In the session scratchpad.

## Acceptance

- `Walkthrough.test.tsx`: the buttons are Skip / Back / Next (no Show me); Next on a DO step makes the move and moves on
  at once, on a SHOW step only moves on; a done step's Next makes no second move; a slow move and a second Next; a move
  that cannot be made; Back undoes every step's move to the first; Skip from a DO step makes no move.
- `help.test.ts`: each DO step's whole sentence; no word says "Show me".
- The testbed walkthrough scenario taps `guide-primary` (still the Next button's id) through the walk.

Depends: the owner's yes. Size: S. Owner: yes (look at the pictures; yes / changes). Default: not merged.
