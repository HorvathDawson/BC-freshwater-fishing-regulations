# T858 — Web: keyboard navigation in lists, starting with search (arrows move the highlight, Enter opens)

Status: OPEN (app chat; web only; after 0.8.1).

Owner 2026-11-18 ~16:50 (to the main chat, relayed to the app chat), verbatim: "in the webapp we shopuld have keyboard
navigation like in search bar using arrows to move highlight etc."

Today (web): search opens on focus / typing / Enter / ↓ in the field (TopBar, T69), and Escape closes search and the
dialogs (`escapeCloses.web.ts`, `escapeBack.web.ts`). Nothing moves through the RESULTS from the keyboard: ↑/↓ do nothing
and Enter does not open a result.

Build (web only; phones unchanged):
1. Search results: ↓ / ↑ move a visible highlight through the results (from the field into the list and back), Enter
   opens the highlighted one (the same path as a click), Escape still closes; the highlighted row scrolls into view and is
   announced (aria-activedescendant / listbox roles). Typing again keeps the focus in the field.
2. The same pattern where a list sits under a field or in a sheet and it fits: the water's parts list, Spots, the
   menu pages. List each one done here; leave out what does not fit.
3. Render tests for the key handling, and a check on the staging web at desktop width.
