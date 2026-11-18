# T841 — Every classified water the book marks (by its symbol, or as "tributaries of …") is captured, bound and requires the Classified Waters Licence; a badge in the card header

Status: OPEN (0.8.0; data audit first, then the app's badge).

Source: owner 2026-11-17 ~20:27, verbatim: "Also we need to make sure the classified water symbols are maintained properly and modify the license requirement. Also if a water is classified it should have a badge in the card I header I think. Make sure we capture all classified waters whether labeled by symbol or by tributary of something"

## Data (main chat)
1. List every classified water in the 2025-27 synopsis: each water printed with the classified symbol (Class I / Class
   II), each "tributaries of X" classification, each period (e.g. Skeena sections, Elk + tributaries, Dean). Cite printed
   pages.
2. Compare with what ships: each one bound to the right water and stretch (tributaries included where the book says
   so), with its class and period, and the Licence tab's Classified Waters Licence requirement on it, only there.
3. Every gap and every extra listed; fix the pipeline so the symbol and the tributary rule are both read; a check that
   fails the build if a book-classified water ships without its requirement.

## App (app chat, after T802's look)
A Class I / Class II badge in the card header of a classified water (owner's direction; T802 was "looks first"). One
picture to the owner before it ships.

## Data audit, 2026-11-17 (lane/t841)
- The book: 73 classified rows — 72 with the CW glyph (69 print their class: 74 designations; Bighorn (Ram) Creek,
  the Fording above Josephine Falls and Kwinamass River print none and are classified through the Wigwam, the lower
  Fording and Ksi X'anmas) and the West Road's glyph-less Class II (Li10). The Elk River's tributaries row and every
  "including tributaries" / ✱ clause are the "tributary of" classifications (69 of 74 designations walk them).
- Shipped (r-46e4355a7277): all 73 right against the corpus and the rulings. On every one of 86,963 designated section
  bindings the resident adult's Licence tab asks for the Classified Waters Licence on the first day of the period
  (the Atnarko and Burnt Bridge asleep "until reopened", as printed), and never outside one.
- No data change. Gate added: `python -m pipeline.regs.classified_waters` — `--check` (corpus vs the book's 73 rows,
  in fast-fail and the release preflight) and `--check --bundle` (the licence on every bound section, release checks
  stage on the candidate).
- For the owner (RULINGS Li5 / Li5a, not changed): tributaries with a row of their own that prints no class stay
  unclassified — open in their parent's period: Wahpeeto (p.19), Whiteswan Lake's outlet (p.40), Little Horsefly,
  McKinley, Moffat (p.46), Glacier (Redslide), Gosnell (p.51), Thautil, Two Mile (p.54), Station (p.53), Hevenor
  (p.51), Magar (p.52), Endako (p.51/59); closed all year: Line, Grave (p.37-38: the Elk's "ALL tributaries"), Sandown,
  East Fork White, Nichyeskwa, Lean-to, Deep, Bear (Sustut).
- For the app's badge: the licence frame of a day lists the designations in force (`holds.designations`, each a
  licensing record with `classified` I/II, `unit_name` and `period.says`); out of the period it lists none, so a
  year-round badge reads the key's other frames (no pipeline change needed).
