# T516 — "Handling fish" reference at the bottom of the regulation pane

Status: DONE — b7ce44b40; proved by `RegsScreen.test.tsx` (the order pinned) and in build 22 (3d807829d).
Earlier: IN REVIEW — next APK. Owner 2026-11-08: "Reference handling fish in the regulation pane should be at the bottom of the page I think".

## What
- The Reference fold ("Handling fish · 21 rules that apply everywhere") closed the Methods section, so it sat between Methods and Licence. It is background reading, not a tab: `RegsScreen` now draws it last, after Licence and before the legal foot (`regs-reference`). `MethodsTab` no longer draws it. Still folded by default, still null on a closed day.
- Proof: `RegsScreen.test.tsx` pins the order (Keep, Gear, Methods, Licence, Reference, legal foot) and that Methods does not hold it; `methods-licence.render.test.tsx` mounts the fold on its own.
- Shots: `shots/T516-after-dark.png`, `shots/T516-after-light.png`.
