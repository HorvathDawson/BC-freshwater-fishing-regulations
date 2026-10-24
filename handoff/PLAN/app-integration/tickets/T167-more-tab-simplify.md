# T167 — The More tab: simpler details, references apart, data freshness on the main tab, a real desktop layout

Status: OPEN

Source: owner 2026-10-23: "details in the more tab could be simplified a lot too. right now it is hard to read. separate
out references to a separate thing. then this stuff [the data sources list: Synopsis, In-season changes, DFO salmon,
Freshwater Atlas, Map tiles, Basemap, HYDAT, Live gauge readings, River forecasts, Weather, Satellite imagery — each with
its source, date and Up to date / Not read / Likely out of date] could mostly be formatted into the main tab without
adding much bulk. seems silly to have it hidden. also that entire page doesn't feel very clean on desktop. mobile is good
but desktop it feels a little lacking".

Code: core/src/menu.ts (the words and the source list), ui-native/src/menu/MoreScreen.tsx, Fresh.tsx, InfoPages.tsx,
MenuHost.tsx.

## Work
1. **Details simplified:** cut the details to what a reader needs, in short plain sentences; one idea per line; no
   repeated qualifiers.
2. **References apart:** citations, credits and licence lines (the Synopsis, OGL-BC, OSM/Protomaps, Open-Meteo CC BY 4.0,
   ECCC, BC RFC, PSF where run timing ships) move to their own "Sources and credits" page, linked once.
3. **Data freshness on the main tab, compact:** one row per source, name + date/age + a status dot (Up to date / Likely
   out of date / Not read / Live), in a tight list or a two-column grid — no paragraph per source; the long description
   moves to the Sources page. The "Not read by this app" in-season changes stays prominent (a warning row with the link).
   "Map tiles" shows a date, not a hash (the hash in the Sources page).
4. **Desktop:** a proper desktop layout — a centred column with a max width, or two columns (freshness beside the
   settings/links) — consistent with the app's desktop shell; mobile stays as is.
5. Shots before/after: phone and desktop, light and dark. Render tests for the new list; words whole-value in core.
Ships: app. Size: S–M.
