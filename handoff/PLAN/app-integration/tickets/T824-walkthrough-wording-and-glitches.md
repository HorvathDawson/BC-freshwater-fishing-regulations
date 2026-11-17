# T824 — The walkthrough: "rings what to tap" wording, and glitches with the keyboard and a lagging highlight

Status: DONE (f7b41c208; landed 2026-11-17 in land/app-1117 (10 steps); owner re-tests on the 0.7.12 build).

Source: owner 2026-11-16 ~21:45, in the app chat (on iPhone build 36), verbatim: "in the tutorial it says on first
slide each step rings what to tap (should say something like highlight instead? the tutorial is really glitchy too with
keyboard and the highlighting following the items with a delay etc etc".

## Wanted
1. The first card's words: "Each step rings what to tap" says "highlights" (or plainer) instead of "rings".
2. The walkthrough is steady: no fight with the keyboard (cards and highlight not covered or jumping when it opens or
   closes), and the highlight sits on its target at once, not trailing it with a delay as the screen moves.
Note: the trout redesign's ticket O also rewords a walkthrough step ("Tap Details") and bumps WALKTHROUGH_VERSION; keep
the two in step.

## Owner addition, 2026-11-16 ~21:47 (app chat), verbatim
"also the tutorial should be much much small 41 is way to many cut it down to like 8 or 10 items and make it super clean".
So: 8 to 10 steps, one short sentence each; the kept and dropped steps go to the owner to adjust.

## 2026-11-16 22:10 — app-fix/t824-walkthrough (not merged)

- **Words.** First card: "We'll look up the {water} together. Each step highlights what to tap: tap it, or tap Show me."
  No card says "ring" (a test reads every card and button word).
- **Cut to ten** (`@app/core` help.ts; `WALKTHROUGH_VERSION` walkthrough-5 → walkthrough-6, the existing rule for a
  rewrite: a device that saw the long walk sees the short one once; an old `guide-at:` line for a cut step starts at the
  top). Kept: welcome · search · pick (the row, then its arrow) · answer · day · conditions · reading (optional: passed when
  no live reading arrived) · leave (‹ Map) · modes (rail Trout / Salmon) · done (Spots + More → Help; the web's names Help
  alone). Cut: the rules folds (trout, rule, salmon, both, part), the conditions extras (forecast, gauge, quantity, span,
  change), the closed stretch (partsmap, upstream, closedregs, closed; `guideClosed.ts` deleted), Layers (layers, ground,
  obstacles, restore), the spot making (spot, add, pin, drop, water, when, name, save, show, open, days, reasons), and
  "Tap the arrow" merged into "pick". The trout redesign's ticket O reworded "Tap Details" (the cut trout step) and bumps
  the version too: whichever lands second takes the next number.
- **Lag.** `GuideCard` looked at its control every 400 ms (a timeout chain), so the highlight trailed anything that moved.
  Now each of the page's scroll events moves it (`CoachScroller.onScroll`, the regs page, Conditions and sheets already
  report them), and anything else that moves things (the step coming up, the keyboard, the window) starts a second of
  looks 32 ms apart, then 400 ms; no frame loop (tools/no-per-frame-animation.test.ts forbids one). The web reads the box at
  once (`windowRect.web.ts`: react-native-web's `measureInWindow` waits a task); the layer and the controls are measured
  together; the card stays hidden until its step's first look (no foot-then-jump); a window resize no longer resets the
  highlight. Web probe, 390 × 844, "Open or closed" while the page smooth-scrolls: before up to 160 px off for ~400 ms;
  after one frame behind the scroll (React renders after the paint), back on it as the scroll ends.
- **Keyboard.** The card's foot room ignored the keyboard (iOS does not shrink the window): `useKeyboardTop`
  (native: keyboard will/did show/hide; web: the visual viewport) + `footRoom` keep the card above it, whatever the step.
  Web stand-in keyboard on "Open its page": card bottom 539 under a keyboard top at 508 before; above it after.
- Tests: help.test.ts (ten keys, the words, one sentence a card); Walkthrough.test.tsx (the walk end to end, frame-by-frame
  scroll-event follow and the quick second — both fail with the 400 ms chain put back —, the keyboard via a stubbed visual viewport, `footRoom`, the exit
  guardrails per step). Shots: scratchpad `t824-shots/`.
- Only a real iPhone can confirm: the card riding up with the keyboard's own animation (will-show), and the highlight
  following a native scroll (measureInWindow per frame on the native side).
