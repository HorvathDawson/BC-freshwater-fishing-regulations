# T1140 — A water's plain link opens the rules of one part without saying which: the Squamish reads "Closed"

Status: OPEN — HIGH, on production; the first release on the new org repo, a patch of its own that also tries the new release steps (owner 2026-11-25)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1140 | A water's plain link opens the rules of one part without saying which: the Squamish reads "Closed" | OPEN — HIGH, on production; the first release on the new org repo, a patch of its own that also tries the new release steps (owner 2026-11-25) | | owner 2026-11-25 with two pictures; owner picks where a multi-part link lands |

Source: owner 2026-11-25 14:55 (planning chat), with two pictures of the web app on production, verbatim: "big app
bug we will want to fix asap.. basically if the user searches something liek squamish river regs and clicks the
link https://canifishthis.ca/water/squamish-river then it opens to regulatiosn tab which is fine. but it doesnt say
which section it leads with and in the case of squamish it is the eco resevre on it.. and it isnt labelled... so
maybe from the basic links it should go to the map and the user can then select which section they want to view
regulations for? also this case doesnt even say it is in the ecological reserve sectionb..."

## Owner, same day 15:00 (planning chat), verbatim
"it should happen as first thign on new org repo maytbe? and it will be the thing used for testing releases? also
if it is a single part then it could open regulations directly"

So: (1) this patch is the first release cut on canifishthis/canifishthis, and the release that tries the new
release steps (T1062's release candidate; T1133's stage A files if they exist by then). Planning chat's guard on
that, his to overrule: the fix is HIGH, so it does not wait for tooling. If the release-candidate build is not
merged when the fix is reviewed, the patch ships by the runbook as today and the next release tries the new steps.
(2) A water with a single part opens its Regulations tab directly, as now; only a water with more than one part
gets the new landing. That is option A's rule and is taken as settled for B and C too.

## Step 1 done 2026-11-25 15:08: the list (the 0.10.0 chat's read-only lane, on candidate r-71d09ad82e81)
The file is `handoff/PLAN/app-integration/T1140-files/multi-part-waters.md` (every water, each part's status and
span; game fish, adult resident, the day's statuses; read with the app's own page code, not re-derived).
- **The rule today:** a plain link, the pre-rendered search page (`app/tools/seo/pane.tsx`, about line 64) and the
  app's open-by-item all reach `targetFor(store, item, null)` (`app/packages/data/src/regs/inputs.ts`, about lines
  184 to 198): the FIRST part in river order, mouth first, that has an answer. Size and open or closed play no part.
- **478 waters** have two or more parts (456 show two or more on the Map tab). In 471 the lead is not the longest.
- **(a) lead part closed, another part open: 89** (the Squamish is one).
- **(b) the unsafe direction, lead part OPEN while another part is closed today: 156.** Among them the Capilano
  (checked by hand on production: it heads with the lower reach and reads Game fish "Open"; nothing says parts 2 and
  3 are closed all year), Chilliwack, Fraser, Skeena, Cowichan, Puntledge, Campbell, Nanaimo, Chehalis, Alouette,
  Pitt, Coquihalla, Brunette, Kanaka, Lynn, Bulkley, Babine, Kootenay, Columbia, Shuswap, Peace, Liard.
- What softens (b): most of those lead parts name their stretch in the head ("From the mouth up to ..."), so the
  page is true for the stretch it names. What is missing everywhere: no Regulations head says "part N of M", and
  nothing says other parts exist with other rules.
- Only 11 lead parts head with the water's bare name (all side-channel parts); five of them are closed all year:
  Squamish, Gordon, Thelwood, Rees, Comox. These five are the plain wrong-looking pages.
- Not determined: the Salmon card's own open or closed per part.

**What this changes (planning chat):** the fix has two speeds. First and small, the same on every platform: every
Regulations and Conditions head of a multi-part water says "Part N of M" with the stretch's name, and one line
under it, "Other parts of this water have different rules", that opens the parts list; the five bare-name closed
leads get their area's name. That is a wording and one-component change and can ship alone, web first. Second, the
landing (A, B or C), after the owner's pick.

## What the pictures show
- `/water/squamish-river`, Regulations tab: the head says only "Squamish River". Salmon "Closed today", Game fish
  "Closed today", "Closed all year. No fishing for any game fish. Federal / parks", source B.C. Synopsis p.9
  ("Fishing is prohibited in Ecological Reserves in B.C."). Nothing on the page says this is one part of the river,
  which part, or names the reserve.
- The Map tab of the same water: "PART 1 OF 2 · SQUAMISH RIVER", and part 1 is the short reach inside Baynes Island
  Ecological Reserve (named only by the basemap's own label). The rest of the river is part 2.

## Why it is high
Someone who arrives from a web search reads "Squamish River: Closed all year" for a river that is open outside a
few hundred metres of reserve. Here the error is on the closed side. The same fault with the parts the other way
round (the lead part open, another part closed) would tell a visitor a closed reach is open; whether any water does
that is step 1. It is the page search engines send people to (T193, T1045), so it is the first thing a new visitor
sees. Related: T369 (`one-load-path-per-screen`), T517 (stretches combine), T795 (a reserve covering a sliver of a
lake closed the whole lake), T1004, T1010, T1083, T1052.

## Work
1. The list, read-only, from the live release: every water whose plain link opens on a part that does not stand
   for the whole water: how many waters have more than one part, which part the link leads with and why (the
   lowest? the first in order?), and for each whether the lead part's open/closed answer today differs from the
   longest part's. Flag every case where the lead part is OPEN and another part is CLOSED. This list sets the
   urgency and is the test bed.
2. The landing, the owner picks from pictures (two or three options on the Squamish and on a river with many
   parts, the Skeena):
   A. his proposal: a plain water link for a water with more than one part opens the Map tab with the parts
      listed, and the visitor picks the part before any rules show;
   B. the Regulations tab, led by a summary of the whole water ("2 parts: rules differ", each part one line with
      its open/closed today), and the part's rules only after a pick;
   C. the Regulations tab on the part that covers most of the water, with the head saying which part and how many
      others differ.
   Whatever is picked: a link that names a part (`?part=` or a section id) still opens that part.
3. In every case, wherever one part's rules are shown, the head says so: the part's name and "part N of M", on the
   Regulations and Conditions tabs as the Map tab already does, on web, Android and iPhone, by every way in
   (search result, start link, tap on the map, a saved spot, the step buttons).
4. A part that exists because of a named area says the area's name: "inside Baynes Island Ecological Reserve".
   The closure block names it too ("Closed all year: Baynes Island Ecological Reserve"), not only "Federal /
   parks". Where the name comes from (the reserve's polygon in the atlas) and what shows when it has none.
5. The pre-rendered page that search engines read (`app/tools/seo/`) says the same thing as the app: title,
   description and the first lines must not say "Closed" for a water that is mostly open. Check what Google shows
   today for the Squamish page.
6. Tests: a multi-part water's plain link never shows one part's answer without the part's name (render test, each
   platform's shared component); the Squamish as a fixture; mutation-proved. Device check on Android and iPhone.

## Acceptance
From a search result for the Squamish River nobody can read "closed" without also reading that it is the reserve
reach, and the open part is one tap or less away; the list of step 1 has no water whose landing shows a single
unnamed part; the search page's snippet agrees.

Severity: HIGH (wrong-looking rules on the page search sends people to; possibly the unsafe direction on other
waters, unknown until step 1). Release: a patch right after 0.10.0 is on production; the list (step 1) and the
pictures (step 2) can start now beside the release, in a lane that builds nothing in the main checkout. The web
ships by the site deploy alone; the phone apps follow in the same patch's builds. Model: Opus (page model and
load paths), Sonnet for the pictures and the search page. Size: M. Owner: yes (picks A, B or C from pictures).
