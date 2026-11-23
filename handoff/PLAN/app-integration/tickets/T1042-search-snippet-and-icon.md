# T1042 — Google shows the licence quote as the snippet and a generic icon

Status: DONE in PR (independent review pending) — d8a8580aa, 8a5810532

Source: owner 2026-11-23: "the search for canifishthis looks like this says license stuff first? and has the wrong icon".
Google's result for /water/coquitlam-river showed "From the rule book: If you are a resident of B.C. and disabled: You may
qualify for a basic freshwater angling licence at the reduced fee of $1.14." and a default icon.

## Cause (found on the live page, GET only)
- The meta description was one template on every water ("<Water> fishing regulations, B.C. (...): the season, what you can
  keep, gear and limits, in plain words."). Google ignores identical descriptions and takes a passage; the static HTML's only
  full rule sentence was the Licence block's.
- `/favicon.ico` held only 16 and 32 px and was the only icon link; `/apple-touch-icon.png` was 404.

## Built (0.9.0)
1. Description (and og:description, the same string): the name and place, then facts that are TRUE ON EVERY DAY OF THE YEAR, while
   they fit in 155 characters, a whole fact or none (`tools/seo/html.ts` `factsOf`, `descriptionOf`). Owner 2026-11-23
   ("Season windows (Recommended)", option b of `T1042-shots/live-samples.txt`): the water's open windows from its year strip
   ("Open Jul 1–Apr 30"), the vessel rules (Boats), and the strip's marks ("fly fishing only", "bait ban") only when they hold
   on every open day. NOT the build day's Other fish rows or Hooks line (independent review: 18 of 58 fixture waters changed
   description over 24 days), no licence, no dated line, no day word (T622 G2). A water whose stretches have different windows
   has no single window and keeps only what all stretches share. A page with no fact says so in whole words (never cut) and
   `budget.json` counts it (`descriptionsWithoutFacts`): 277 of 19,766 live waters, from 448 under the first version.
   The "Other fish" line no longer reads "Kokanee · 5 a day · · size limit" (the hint carries its own dot).
2. `data-nosnippet` on the words every page repeats: the licence block, the Handling-fish reference and the legal footer of
   the water pane (`NO_SNIPPET`, `WaterInset noSnippet`, RegsScreen; react-native-web `dataSet`, so the prerender AND the
   app's live pane carry it: Google renders the page), and the legal lines of every hub page and the site footer (site.ts,
   html.ts). Region, topic, letter and gauge descriptions were already page-specific (counts, names); only their shared
   legal lines needed the attribute.
3. Icons: `tools/web-icons.mjs` draws `favicon-48x48.png`, `favicon-96x96.png`, `favicon-192x192.png` from the app's own
   `favicon.svg` (the art `favicon.ico` carries) and `apple-touch-icon.png` (180) from `icon.svg`, with resvg; the links go
   in the home page (`web-shell.mjs` `shellExport`), in every water page (`appDocument`) and every hub page (`favicon.ts`).
   `favicon.ico` stays.
4. Tests, each proved by mutation (facts from a day's rows / size-hint dot back / licence mark removed / icon sizes wrong /
   touch icon transparent): `site.test.ts` sweeps the fixture's year for identical descriptions, and the touch icon's alpha is read: `tools/seo/description.test.ts`, `favicon.test.ts`, `site.test.ts`, `web-shell.test.ts`,
   `packages/ui-native/src/regs/RegsScreen.test.tsx`.

## Google's guidance (read 2026-11-23)
- developers.google.com/search/docs/appearance/snippet: a meta description is "a short, relevant summary of what a
  particular page is about"; identical descriptions on every page "aren't helpful"; no length limit (Google cuts to the
  device); `data-nosnippet` keeps a part of the page out of a snippet.
- developers.google.com/search/docs/appearance/favicon-in-search: a square image of at least 8 px, larger than 48 px
  recommended; PNG, ICO, ICO, PNG among them; found by `<link rel="icon">` or `apple-touch-icon` in the HOME page's head;
  one favicon per host; stable URL; crawlable. (The brief said "a multiple of 48 px": the page no longer says so; 48 / 96 /
  192 satisfy both.)

## Limits, and what is the owner's
- Google updates the snippet and the icon only when it recrawls (days to weeks). After the production site deploy (the
  owner's), Search Console can request a recrawl of `/` and a few water pages.
- Owner answer 2026-11-23: season windows (above). The brief wanted "whether it's open today" in the description: T622 G2 forbids a day word (a description is made once a
  release), so it is not there. The page's year-stable facts are thin: "what you can keep" (trout lines) changes with the
  build day, so it is not used. Question for the owner: accept this, or make the description from the year strip?
- Pictures: `T1042-shots/` (descriptions before / after on the fixture waters; the 48 / 192 px icon; the 180 px touch icon).
