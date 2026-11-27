# T981 — Municipal no-fishing waters (Lost Lagoon, Beaver Lake in Stanley Park, and others) shown as closed

Status: IN PROGRESS — Lost Lagoon SHIPPED in 0.8.5 (local-closures source); round 2 of the inventory listed 2026-11-22 (5 proposal rows); left (owner, next release): review the other inventory rows.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T981 | Municipal / park-bylaw no-fishing waters (Lost Lagoon, Beaver Lake in Stanley Park, others) shown closed with the bylaw cited | IN PROGRESS (Lost Lagoon shipped 0.8.5; round 2 inventory listed 2026-11-22, 5 proposal rows; left: other rows, next release) | | owner 2026-11-20; 11-21: build the curated path + Lost Lagoon only (0.8.5). Built on lane/085-t981: `data/curated/regulations/local_closures/` (a second entry source, guard vs … (detail: ticket file) |

Source: owner 2026-11-20: "Add a ticket to add lost lagoon and beaver lake in Stanley Park and any other municipal closed
to fishing water bodies into the regulations".

## Why
Some waters are closed to fishing by a city or park bylaw, not by the provincial synopsis. Lost Lagoon and Beaver Lake
(Stanley Park, Vancouver Park Board) are examples. The app shows them under the region's base rules, as if fishing were
allowed.

## Work (left)
- The owner reviews the inventory rows below (a later release: owner 2026-11-22 23:55 "Later release we will do this").
  Each accepted row is one `write_region` row in the curated path; a water not in the atlas (Fountain Lake, Salish Pond)
  needs an added lake first (like the municipal added streams).
- Still to check: the sources listed as not checked / could not load in both rounds.

Done (0.8.5, lane/085-t981): the curated path `data/curated/regulations/local_closures/region-<n>.json`
(`CURATED.regulations.local_closures`; a second ENTRY SOURCE beside `synopsis/`; `pipeline/regs/local_closures.py`
`load` / `check_entry` / `check_against_book` / `write_region` / `--check`; the guard stops the run on a water the
synopsis has a water row for). The one shape: a water row, one `matched` item, whole, `authority {body, instrument,
section?, url, quote}`, one rule "No Fishing" with `authority: superior` (outside the ladder, rank -1). Entry id
`r<n>:<slug>_local@<mu>`. Through bundle `entry.authority`, export and answers `RuleFacts.cited {link, url, quote}`; the
app's `web` Line run (`core/regs/line.ts`, `book.ts`). Tests `pipeline/tests/test_local_closures.py`, `test_mini_release.py`
(28 entries), `book.test.ts`. Lost Lagoon (wbk:329532481) `r2:lost_lagoon_local@2-8` shipped in 0.8.5.

## Acceptance
- Lost Lagoon and Beaver Lake show closed, with the Park Board bylaw cited and linked.
- The inventory is in this ticket, with each row's source.
- Every accepted row ships in a data release.

Depends: —. Size: M. Owner: yes (accept the inventory rows).

## Owner 2026-11-21
- 12:45: joins the 0.8.5 batch ("Yes, add it"). The owner accepts each row before it ships.
- Dialog, verbatim: "https://stanleyparkvan.com/stanley-park-van-location-lost-lagoon.html is only one I know for sure. Maybe add other info to a ticket but only create the way to ban lost lagoon for now with a nice curated data path to list these. Then put all the info found for the other water bodies in the ticket and we will review for the next release."
- Source check: the Parks Control By-law bans fishing only at posted bathing beaches (s.22(f)); the Park Board's Wildlife
  page (vancouver.ca/parks-recreation-culture/wildlife.aspx) says fishing is permitted except at bathing beaches; no
  official page closes Lost Lagoon. Asked keep, pull, or keep and say unofficial? Owner: "Keep it, current source".

## Inventory, 2026-11-21 (Claude, lane/085-t981): PROPOSALS for next release (Lost Lagoon accepted)
Full rows (quotes, URLs, matches, conflicts): `data/generated/review/t981_local_closures_proposals.json` (removed from git 2026-11-27, it is in git history at 9337319f7). Read nowhere.

| row | water | item | authority, bylaw, section | status |
|---|---|---|---|---|
| victoria-goodacre-lake | Goodacre Lake (Beacon Hill Park) | wbk:329676480 | Victoria Parks Regulation Bylaw 07-059 s.14(1)(h): "must not … catch a bird, animal, or fish" in a park | proposed, high |
| victoria-fountain-lake | Fountain Lake (Beacon Hill Park) | none (not mapped; needs an added lake) | same | proposed, high (rule) |
| saanich-swan-lake | Swan Lake (Saanich sanctuary) | wbk:329676327 | Sanctuary's Visitors' Guide "NO HUNTING / FISHING / BOATING"; Saanich bylaw 7753 s.2.1(a) gives the Society the authority | proposed, medium |
| burnaby-park-waters | every lake/stream in a Burnaby park | Deer Lake wbk:328976459, Burnaby Lake wbk:329291868 | Burnaby 7331 s.4: no fishing "in any lake or stream in any park without the special permission of the Administrator" | owner call: the synopsis prints open rules for both lakes |
| surrey-park-waters | every lake/stream in a Surrey park except designated, posted areas | Green Timbers Lake wbk:360887617 | Surrey 13480 s.34(a) | owner call: designated areas not published; Green Timbers is stocked |
| vancouver-lost-lagoon | Lost Lagoon | wbk:329532481 | StanleyParkVan.com: "Boating, swimming, and fishing are prohibited in Lost Lagoon." (the owner's source; the Parks Control By-law names no lagoon: only 22(f), posted bathing beaches, and s.10, posted Board notices) | **ACCEPTED** (owner 2026-11-21), shipped as `r2:lost_lagoon_local@2-8` |
| vancouver-beaver-lake | Beaver Lake (Stanley Park) | wbk:329532620 | no source found: the Parks Control By-law names no lake (22(f), s.10 as above) | needs a source (sign photo / Park Board page / 3-1-1) |

Checked, not a closure: Langley City 3048 s.12(x), View Royal 986 s.2.1(e), North Saanich 1470 s.8 (they say "disturb … trap or snare … fish", not catch or angle);
Metro Vancouver 1420 s.34 (posted areas only). No fishing clause: Saanich 7753, District of North Vancouver 8310 (shellfish only), Richmond 8771, City of North Vancouver 6611,
Central Saanich 804, West Kelowna 0184. Not yet checked: CRD 4225, Coquitlam, Port Moody, New Westminster, Delta, Township of Langley, Abbotsford, Chilliwack, Nanaimo, Kelowna,
Kamloops, Prince George.

## Inventory round 2, 2026-11-22 (Claude, lane L6): PROPOSALS for the owner (none accepted, no curated row written)
The owner's 2026-11-21 decision covers Lost Lagoon only and puts every other row up for review, so these are listed
here and nowhere else. Web research, each source read in full unless it says otherwise. Item matches are checked
against the live registry (`atlas/full`) and the synopsis rows.

| row | water | item | authority, instrument, section | words | source | status |
|---|---|---|---|---|---|---|
| chilliwack-salish-pond | Salish Pond (Salish Park) | none: not in the registry (needs an added lake) | City of Chilliwack, Parks, Recreation and Culture Bylaw 2006 No. 3290 (consolidated to 2026-09-01), s.24(1)(a) | "no person shall: (a) wade, swim, bathe, fish or attempt to catch fish in any manner ... in Salish Pond" | https://www.chilliwack.com/main/attachments/attachView.cfm?attachID=3306 (listed on https://chilliwack.com/main/page.cfm?id=363) | proposed, high (rule); not mapped |
| chilliwack-natural-areas | every water in Browne Creek Wetlands, Peach Creek Spawning Channels, Lickman Ponds and Channels, Lexw Qwo:m Park, Great Blue Heron Nature Reserve | Browne Creek `gnis:9713`, Peach Creek `blk:360830493` (whole items NOT checked against the park outlines) | same bylaw, s.26(2) (the properties are the bylaw's "Natural Area" / "Nature Reserve") | "No person shall: ... (2) fish or attempt to catch fish in any Natural Area or Nature Reserve" | same PDF | owner call: a part of a stream (the park's reach) is not the one shape (`local_closures` is whole-water only) |
| port-moody-parks | every water in a Port Moody park (Rocky Point Park; Noons Creek ponds if city parkland) | not matched | City of Port Moody, Parks and Community Facilities Rules and Regulations Bylaw No. 3321 (section NOT seen) | "No fishing, casting or hauling nets or other fishing equipment is allowed from, or adjacent to, any Park (Bylaw No. 3321)" | https://www.portmoody.ca/parks ("Park restrictions"); the bylaw text was not found | proposed, medium: needs the bylaw text and section |
| fvrd-cheam-lake | Cheam Lake (Cheam Lake Wetlands Regional Park) | `wbk:-4` | Fraser Valley Regional District parks bylaw (not read) | "boating and fishing not permitted" (a news item, from a search summary, page not opened) | https://fraservalleynewsnetwork.com/2021/05/27/reminder-boating-and-fishing-not-permitted-in-cheam-lake-wetlands-regional-park/ | a lead only; owner call: the synopsis prints a row (`r2:cheam_lake@2-3`), which the guard refuses beside a local row |
| vancouver-beaver-lake | Beaver Lake (Stanley Park) | `wbk:329532620` | — | no source found (the Park Board's restoration page, https://vancouver.ca/parks-recreation-culture/beaver-lake-restoration.aspx, says nothing on fishing; the consolidated Park By-laws PDFs did not load) | — | still needs a source (unchanged) |

Not a closure, worth a look:
- **Sardis Park Pond** (`wbk:329083384`, synopsis row `r2:sardis_park_pond@2-4`): Chilliwack bylaw 3290 s.25(2) PERMITS
  fishing in designated areas for anglers 12 and under or 60 and over, single barbless hooks, no lead weights. Compare
  with the synopsis row; the province's proposal page
  https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/ahte/angling/milllake-sardisparkpond-cranberrylake-gardenbay-rubylakes
  was not opened.
- **Posted areas only** (no water named, so no row): Port Coquitlam Parks, Facilities and Public Places Bylaw 2022
  No. 4274 s.6.4(h) "fish in any area posted to prohibit fishing"
  (https://www.portcoquitlam.ca/sites/default/files/2024-10/4274%20-%20Parks%2C%20Facilities%20and%20Public%20Places%20Bylaw.pdf);
  Nanaimo Parks, Recreation and Culture Regulation Bylaw 2022 No. 7340 s.4.1(d)/(e), a catch-all for signed or
  undesignated activities (https://www.nanaimo.ca/bylaws/ViewBylaw/7340.pdf); the city promotes fishing at Westwood
  Lake and Colliery Dam.

Checked, no fishing ban: CRD Regional Parks Bylaw 4225 (consolidated June 2023; the wildlife clause exempts "fishing
and crab-catching done in accordance with all enactments", https://www.crd.ca/media/file/bylawno4225consolidatedjune2023;
the 2026 amendment 4787 not checked); New Westminster Parks Regulation Bylaw 3646 (consolidated 2020, no "fish");
Coquitlam Bylaw 3617 (2004 text; may not be current; Lafarge and Como are stocked fishing lakes); Prince George Parks and
Open Space Bylaw 7370 (consolidated 2026-03-23); Reifel sanctuary's Canada.ca page (no fishing rule stated); the City
of Burnaby's Burnaby Lake page.
Could not load / not found (still to check): Kelowna 10680 (403), Abbotsford 2456-2015 (certificate error; Mill Lake has
a fishing pier), Kamloops 35-66, Township of Langley 5298, Delta 7206 (404), the Vancouver Park Board consolidated
by-laws PDF (would settle Beaver Lake and Trout Lake), Metro Vancouver's Burnaby Lake page, the FVRD parks bylaw.
