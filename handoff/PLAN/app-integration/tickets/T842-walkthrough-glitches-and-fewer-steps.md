# T842 — The walkthrough on a phone: the highlight lags a scroll, the keyboard hides the target, and it has too many steps

Status: DONE 45fff5e63 (CI green 2026-11-17 22:28; Android benches: walkthrough PASS in the batch-1 bench; iPhone look waits with iOS benched; in 0.8.0).

Source: owner 2026-11-17 14:55, in the app chat, with two Android screenshots of the Play internal 0.7.12 build (step 3
of 10, "Open its page", search "ved"), verbatim: "On the last release the walk through is still a bit glitchy. First
image is a screen shot I caught when scrolling and the box has a weird delay tracking the box. It just feels
unprofessional. The other one is how the keyboard covers vedder in the walk through. The interactive walk through just
isn't perfect. The Qp steps are good otherwise. But could reduce steps even more. This is a user first look so we need
to make sure it is clean and not buggy".

## Faults (from the screenshots)
1. **The highlight trails a scroll.** Scrolling the search list, the purple ring around the target row lags behind the
   row (screenshot 1: the ring sits on "Vedan Creek" half a row off). The ring is placed from a measured position that
   is re-read after the scroll, not moved with it.
2. **The keyboard hides the target.** On "Open its page" the keyboard is still up; the Vedder River row is under the
   card and the keyboard (screenshot 2: only its name line shows, the ring cut off), so the step asks for a tap on
   something the reader cannot see.
3. **Too many steps** for a first look (10 today, T824).

## Fix (app chat's call, standard coach-mark practice)
- The ring moves with its target on the UI thread (native driver / layout-attached), or hides while the list moves and
  fades back when it settles; never a visible trail.
- A step whose target is in a list closes the keyboard first and scrolls the target into view above the card; the card
  sits on the side away from the target and never covers it.
- Step count: owner pick (below).
- Proof: Android test bed (walkthrough scenario with a scroll during the pick step and the keyboard up), iPhone
  simulator, web; frames sampled during a scroll show the ring on the row.

## Owner pick, 2026-11-17 15:00 (app chat dialog), verbatim label: "6 steps (Recommended)"
1 Search for it (the welcome folded in: type "ved") · 2 Open its page (tap Vedder River; the keyboard closes) ·
3 Open or closed (the big word + year bar) · 4 Pick another day (tap the date) · 5 How is the river? (tap Conditions;
the dot says low / normal / high, the old "reading" step folded in) · 6 You're all set (back to the map, the Trout /
Salmon switch and Spots / Help under More said in words, no taps). WALKTHROUGH_VERSION bumps so a phone that finished
the 10-step tour is not asked again; one that stopped part-way restarts at step 1.

2026-11-17 15:02, owner (app chat, verbatim): "Use industry standard ideas" / "Should be in 0.8". So the standard
coach-mark / product-tour pattern (Intro.js, Shepherd, react-native-copilot, Material feature discovery, Apple TipKit):
scrim with a spotlight cutout that moves with the target; the card anchored to the target with a pointer, auto-placed
(flips to the side with room, never over the target or under the keyboard); step indicator, Back / Next, Skip always
shown; taps pass through to the spotlighted target only; a short transition between steps (a fade under Reduce Motion);
the target scrolled into view before its step. In 0.8.0.

## Built (2026-11-17, branch app-fix/t842-walkthrough)
The standard coach mark (owner: "Use industry standard ideas"), the owner's 6 steps, `WALKTHROUGH_VERSION` walkthrough-7.
- **The ring never trails.** Each scrolled page's coach scroller (`coachScroll.ts`) carries its offset as an Animated
  value, driven by the scroll view's own native scroll event on a phone (`Animated.attachNativeEvent`, a FlatList's inner
  scroll view) and set by the scroll handler on the web. The cut-out, the ring and the card ride it
  (`spotFollow`: offset when measured − live offset); nothing is measured while a page moves, only once it is still,
  and only to check. Search's results list now provides a scroller (it had none: the ring was re-measured on a timer).
  A move a scroll does not explain (search's sheet dropping to half after a row tap) hides the ring and shows it where
  two checks agree; never chased. No per-frame JS (tools/no-per-frame-animation green).
- **A list target is never covered.** Steps say `keyboard: "closed"` (pick): the keyboard is dismissed as the step comes
  up (reached, resumed or stepped back to); the row is scrolled into view with room for the card; the card goes on the
  side with the room (below or above), never over the ring, above the keyboard.
- **Coach-mark parts adopted:** dim with a cut-out (four panes; taps in the hole reach the control, elsewhere nothing);
  card anchored with a pointer toward the ring, below/above, flipped by room; "n of 6", Back / Next / Skip on every
  card but the last; rise-and-fade between steps (fade only under Reduce Motion); scroll into view before showing.
- **6 steps** (help.ts): Search for it (welcome folded in) · Open its page · Open or closed · Pick another day · How is
  the river? (the dot folded in) · You're all set (back to the map, Trout / Salmon, Spots and Help said in words; the
  web's card names Help alone). Resume lines name the walk's version (`guide-at:walkthrough-7/pick`), so a phone left
  part-way through the 10-step walk starts at step 1.
- **Tests:** Walkthrough.test (6 steps; the pick step dismisses the keyboard: reached, resumed, stepped back to; the
  ring's transform follows the page's Animated value with no re-measure while it moves), Walkthrough.t842.test (real
  card size, keyboard up: dismissed, row scrolled up, card box ∩ ring box = ∅; the dim's panes cover all but the hole
  wherever the scroll carries them), help.test (6 steps, keyboard flag, old resume lines). Mutation-checked: removing
  the dismiss, the follow, the still-page guard, a pane or the keyboard flag each fails a test.
- **Test bed:** `node app/tools/testbed/run.mjs walkthrough` (6 steps; step 1 typed by the reader so step 2 comes up with
  the keyboard up and must close it; a slow drag inside the ring during step 2 with frames out/t842/pick-scroll-N.png
  and the ring still on the row after; the keyboard raised again; back pauses / Resume). Not yet run (Android).
- **Shots** (web 390 px, light + dark, six steps + mid-scroll on step 2): the lane's scratchpad t842-shots/.
- Left: the Android test-bed run, an iPhone look.
