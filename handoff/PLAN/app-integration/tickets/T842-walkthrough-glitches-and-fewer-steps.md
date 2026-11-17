# T842 — The walkthrough on a phone: the highlight lags a scroll, the keyboard hides the target, and it has too many steps

Status: OPEN (app chat; Android + iOS + web; part of 0.8.0: a new user's first look).

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
