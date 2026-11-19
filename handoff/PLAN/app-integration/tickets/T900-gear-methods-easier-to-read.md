# T900 — Gear and Methods: cleaner, easier to read (next chat)

Status: IN REVIEW — approved proposal on staging (a3a982a17); owner: OWNER-TODO "Test 0.8.2 on staging (2026-11-19 morning)".

Owner 2026-11-19 ~01:12 (app chat), two desktop shots of a Fraser water's Gear and Methods
(/Users/dawson.horvath/.claude/uploads/699dcd1b-ff73-4ed1-a69c-69feb621d839/9fe767d8-image.png and the one before it in that folder; if gone, re-shoot any Lower Fraser water's Gear/Methods),
verbatim: "gear and methods i feel like we could clean up a little bit and make it easier to read. this can be a future
ticket for next chat tho".

What the shots show (notes for the plan, not decisions):
- Gear: each row (Lines, Hooks, Weight on the line) repeats a "Province ›  Rule book p.N ↗" source line; Hooks has two
  bold lines ("One hook, lure or fly" / "Single barbless hook, no trebles ⓘ") with three sources.
- Bait: "Allowed / Not allowed" lists; "Fish or fish parts" carries a long run-on exception ("Dead fin fish may be used,
  when fishing for white sturgeon — upstream of the CPR Bridge at Mission · Dead fin fish may be used, when fishing for
  white sturgeon — Fraser River; Pitt River, … — lifts "No fin fish other than roe"") — the same exception twice.
- Methods: "Ways to fish / Allowed / Not allowed"; Angling's conditions in one long grey line; five ✗ rows.
Ideas to weigh with the owner: sources once per section (not per row), the exception said once and short, plain
grouping, word density (T890). Pictures first.

## Proposal (2026-11-19 night)

Branch `app/t900-proposal` (off 0.8.2), NOT merged; Status stays OPEN until the owner judges the pictures. Water: Fraser River (gnis:39325), Today Nov 19, web app built from the branch against the live release.
Pictures (`/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/1db4ea05-d81d-4b07-a925-fc0b9cdbe701/scratchpad/t900-shots/`): `before-fraser-{gear,methods}-{390-light,390-dark,1440-light}.png`, `after-fraser-…` same names; contact sheets (before | after, gear then methods) `contact-sheet-390.png` (light + dark), `contact-390-light.png`, `contact-390-dark.png`, `contact-1440.png`.

What changed:
1. Gear: ONE source line under the whole section ("Province · Region 2 · This water's › Rule book p.8 p.21 p.23"), not under Lines, Hooks, Weight and Bait. Core: `GearVM.src` (`mergedSource`, gear.ts; built in page.ts `gearOf`); new sheet key `srcs:gear` lists every rule of the section. The per-row `src` stay in the VM and their sheets (`srcs:gl|…`, `srcs:bait`) still resolve, but no row draws them. Moved, not dropped: every page a row cited is in the one line.
2. The white sturgeon exception: it was printed twice because TWO rules carry it (the Region 2 entries for the Fraser mainstem, one scoped "upstream of the CPR Bridge at Mission", one "Fraser River; Pitt River, …; Harrison River …"), each with the same head label "Dead fin fish may be used, when fishing for white sturgeon". `noteWithAllowances` joined the two labels with " · ". Data duplication of wording (two entries, same allowance), not fixed. Now core's `labelsOnce` (bait.ts) prints the head once and each rule's own "— where" after it. Every word of both scopes and the "lifts" tail stays.
3. Methods: Angling's conditions one per line (was one grey run joined with " · "); the bare ✕ ways (Set lines, Spear or bow, Nets, Snagging, Chumming) draw as one wrapped line (`YesNoVM[].inline`, set in core `yesNo` for 3+ bare ✕ names); Ways keeps its one source line.
Not changed (for the owner to weigh): the two bold Hooks lines, the Allowed / Not allowed heads, the Vessel restrictions section, the day line, the Bait notes' wording, the data's own words (T890 pipeline side).
Tests: typecheck clean; vitest core/regs + ui-native + conformance/regs (fixtures regenerated with UPDATE_REGS_FIXTURES=1: only the new `gear.src` and `inline` fields). Not run: full tier, Android/iOS, native render.


## Owner 2026-11-19 ~03:25: approved
Owner, verbatim (main chat): "T900 looks good you can add side channels style too". The proposal (a3a982a17) lands in 0.8.2 as built.
