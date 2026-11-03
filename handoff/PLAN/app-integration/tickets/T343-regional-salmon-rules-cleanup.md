# T343 — Clean up the regional salmon rules (DFO header notes) on the water page

Status: OPEN (owner 2026-10-31). Depends on T332 (salmon in the shared quota tree) — do on top of it.

The region header notes (`regs.dfo_salmon.header`, shown as "region rules" on the salmon card) are mostly not worth
showing on their own. Work each into the place it belongs, or drop it:

1. **"No fishing within 100 m of a government fish facility"** — the freshwater regs already carry the same rule and the
   page already shows it. Drop the DFO copy (no duplicate line).
2. **Daily salmon limit, fresh + tidal combined** — say it where the card states the daily total at the top of the salmon
   breakdown (the bucket line), not as a separate note.
3. **"Adult chinook" size** — make it a per-water value: default 50 cm, except the waters DFO names otherwise (at least in
   Region 2; R6 says over 65 cm except the Fraser watershed). Feed it into the size tiers/lengths where "adult" matters,
   not as a note.
4. **How-to-measure notes** ("tip of nose to tail fork", etc.) — drop.
5. **Every other header note** — list them all (every region's page), and for each: fold into a structured place (bucket,
   size, closure, season, gear) or drop. Nothing stays as a free-floating note unless it changes what an angler may do
   and has no structured home — list those for the owner.
6. **Salmon images (owner 2026-10-31):** the COLLAPSED salmon section shows one image, the ocean-phase chinook
   (`chinook-salmon-ocean-phase.webp`). When OPENED, each species row uses its own spawning-phase image
   (`<species>-salmon-spawning-phase.webp`; every species has one). Assets in app/apps/mobile/assets/fish/.
7. **DFO source-page info is one collapsible line, collapsed by default** (page link, date modified, notice numbers).
   Opened, it shows the full source details.
8. Tests: the five T320 spot-check places still read correctly; no header text lost without being folded or listed as
   dropped (a dropped-notes list in the ticket).
Size: M.

Unblocked 2026-11-03: its dependency (T332 merged 0c276aa10 / T350) is DONE.
