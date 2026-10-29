# T258 — accessibility leftovers (after T243)

Status: OPEN.

Source: T243 (2026-10-29 overnight): what the pass did not reach.
## Work
1. Into `a11y.shell.test.tsx`: the states T257 was rewriting during T243 — a water's Conditions panel with gauges
   (ConditionsPanel, DonorPanel, Hydrograph), the day calendar sheet, the bell sheet, the sync error sheet, the first-run
   notice, the walkthrough / coach marks. Give their presses `hitProps` and their pictures roles.
2. Labelled TEXT rows on the web: the data rows (More → Data: "Synopsis: No date recorded. Unknown.") and the Help
   legend rows carry their status words in the label only; react-native-web drops a label on a role-less view, so a web
   screen reader hears "Synopsis · Valid to Mar 31" without "Up to date / Unknown". Say the state in visible words, or
   give the row a role that keeps its label.
3. Web targets: react-native-web ignores `hitSlop`; the rail (40 px), back links (32 px) and similar stay at their drawn
   size on the web (≥ 24 px, WCAG 2.5.8 AA). Decide whether to draw them at 44.
4. Owner phone walk: TalkBack + VoiceOver, map → water → regs → conditions → salmon → menu; 200 % text (the tab-bar
   1.3× cap, T243 call).
## Acceptance
The extra states in the shell gate with `[]` findings; axe walk (`tools/a11y-walk.mjs`) still clean; the owner walk's
findings fixed or ticketed.
Depends: T257 merged. Size: S–M. Owner: yes (the phone walk).
