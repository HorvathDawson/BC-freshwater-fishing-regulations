# T258 — accessibility leftovers (after T243)

Status: PARTIAL — items 1-3 done (see Built); item 4 is the owner's phone walk.

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

## Built
1. **Shell gate** (`a11y.shell.test.tsx`, "(T258) the leftover states", phone + desktop, each must also show its words): the bell sheet,
   the first-run notice, the day calendar sheet (`DaySheet`), the sync error sheet, the coach marks (a measured registry), a water's
   Conditions with a gauge (ConditionsPanel + DonorPanel). They found real gaps, fixed: day grid cells / month and day steps, the
   hydrograph zoom buttons, donor rows and the "Notes and sources" fold, the chart and gauge chips, the coach Skip / Back now take
   `hitProps`; the coach card is a `role="dialog"` named by its title (its old label hid "1 of 6"; the axe walk then caught
   `aria-modal` on a role-less div, fixed). The Hydrograph picture itself is a labelled image (its label rightly stands for the SVG
   words) and has no press: it rides inside the Conditions state, not a state of its own.
2. **Labelled text rows on the web**: the Data rows and the updates row (More), the Help legend rows, the sync error rows and the day
   grid are `role="group"` so react-native-web keeps their label (a role-less div's `aria-label` is prohibited and skipped). The gate
   now fails any `div[aria-label]` with words and no role (planted violations pinned).
3. **Web targets - decision: keep as drawn.** The rail (40 px), back links (32 px) and similar are >= 24 px (WCAG 2.5.8 AA) on the web;
   react-native-web ignores `hitSlop`, and drawing them at 44 would move the layout (T260's rule: no look change). Native keeps the
   44 x 44 reach by `hitProps`. Owner may decide otherwise; it is a size change, not a hit-area one.
4. **Axe walk** (`tools/a11y-walk.mjs`, 22 scenes x desktop/phone x light/dark): clean except the coach `aria-modal` finding above, fixed.

## Owner only
- The TalkBack + VoiceOver phone walk (map -> water -> regs -> conditions -> salmon -> menu) and 200 % text (tab-bar 1.3x cap): not
  something a lane can do; findings become tickets.
