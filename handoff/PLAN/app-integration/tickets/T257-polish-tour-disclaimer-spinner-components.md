# T257 — polish pass: coach-mark tour, "Before you fish" sheet, one delayed spinner, shared components

Status: IN PROGRESS — lane polish-1029 (2026-10-29)

Source: owner 2026-10-29 polish pass (four items + add-ons: delayed spinner, generic components, pixel-diff proof).

## Work
1. First-run tour (T248) as coach marks: spotlight on the real element (date bar, Standard·Salmon, Conditions tab, a
   water, sync, bell), callout anchored to it, progress dots, Next/Back/Skip, reduced motion, phone + desktop, resumable
   from More → Help.
2. "Before you fish" (T236): calm sheet, clear title, three short points, "I understand", link to Sources & licences.
3. Every loading state is a FishSpinner through one delayed-show wrapper (shows after ~300 ms, stays ≥ 400 ms); guard.
4. Consistency sweep: extract generic components from the best existing instances (look unchanged), migrate call sites,
   guards against ad-hoc re-implementations; before/after shots + pixel diff per pair.
## Acceptance
Render tests + guards; tsc; shots in `T257-shots/` (desktop + phone, light + dark) with a diff table.
Depends: T236, T248. Size: M. Owner: no.
