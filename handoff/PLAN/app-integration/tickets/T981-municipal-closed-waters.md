# T981 — Municipal no-fishing waters (Lost Lagoon, Beaver Lake in Stanley Park, and others) shown as closed

Status: IN PROGRESS — Lost Lagoon SHIPPED in 0.8.5 (local-closures source); left (owner, next release): review the other inventory rows.

Source: owner 2026-11-20: "Add a ticket to add lost lagoon and beaver lake in Stanley Park and any other municipal closed
to fishing water bodies into the regulations".

## Why
Some waters are closed to fishing by a city or park bylaw, not by the provincial synopsis. Lost Lagoon and Beaver Lake
(Stanley Park, Vancouver Park Board) are examples. The app shows them under the region's base rules, as if fishing were
allowed.

## Work
- **Inventory:** list BC waters closed to fishing by a municipal or park bylaw. Start with Vancouver Park Board (Stanley
  Park: Lost Lagoon, Beaver Lake), then other Metro Vancouver, Victoria/CRD and big-city park boards. Each entry gives
  the water, the authority, the bylaw with its section and URL, and the extent (whole water or part).
- **Model:** a curated source of "closed by local bylaw" (its own `source`, never mixed with the synopsis). It binds to
  the water like an override: closed all year, with the bylaw quoted and linked. Per memory
  `everything-added-is-a-proposal`, each row lands as a proposal for the owner to accept.
- **App:** the water shows closed. The reason names the authority and bylaw ("Closed to fishing by Vancouver Park Board
  bylaw …"), with the link, apart from the synopsis rules.
- Map the waters in the atlas (both are small ponds; check they are in FWA, or add them like the municipal added
  streams).

## Acceptance
- Lost Lagoon and Beaver Lake show closed, with the Park Board bylaw cited and linked.
- The inventory is in this ticket, with each row's source.
- Every accepted row ships in a data release.

Depends: —. Size: M. Owner: yes (accept the inventory rows).

## Owner 2026-11-21 12:45
Joins the 0.8.5 batch ("Yes, add it"). Inventory drafted as proposals; the owner accepts each row before it ships.

## Inventory, 2026-11-21 (Claude, lane/085-t981): PROPOSALS for next release (Lost Lagoon accepted)
Full rows (quotes, URLs, matches, conflicts): `data/generated/review/t981_local_closures_proposals.json`. Read nowhere.

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

## Owner 2026-11-21 (dialog)
In 0.8.5. Verbatim: "https://stanleyparkvan.com/stanley-park-van-location-lost-lagoon.html is only one I know for sure. Maybe add other info to a ticket but only create the way to ban lost lagoon for now with a nice curated data path to list these. Then put all the info found for the other water bodies in the ticket and we will review for the next release." Build: the curated local-closures path + Lost Lagoon only. The other rows stay here for review next release.

## Built, 2026-11-21 (Claude, lane/085-t981)
- **The curated path:** `data/curated/regulations/local_closures/region-<n>.json` (`_about`, `region`, `entries`; folder README),
  `CURATED.regulations.local_closures` (`config.yaml`). It is a second ENTRY SOURCE beside `synopsis/`
  (`parsing.io.entry_sources`), so the reach run and the bundle read it with no other change. `pipeline/regs/local_closures.py`:
  `load` (RAISES on a missing source), `check_entry` (the one shape), `check_against_book` (the guard), `write_region` (through
  `curated_write`), `--check`. `read_all_entries` (the reach run's corpus read) runs `check_source`: the source must be there,
  every entry in shape, and none on a water the synopsis has a water row for (that stops the run, naming both).
- **The one shape:** a water row, one `matched` item, whole, no pages, `authority {body, instrument, section?, url, quote}`
  (`CatalogueEntry.authority`, `LocalAuthority`; an entry carries pages OR an authority), and one rule "No Fishing"
  (`retention_limit`, ALL_GAME_FISH, take 0, may_target false) with `authority: superior`. A superior authority's rule is outside
  the ladder (rank -1, `bundle.read.source`): nothing in the book reopens it, as a national park's closure stands.
- **Entry id: `r<n>:<slug>_local@<mu>` (a water row).** The least invasive choice: the water kind is what every reader already
  treats as "this water's own row" (reach binding, ladder scope, export kind). A new kind would mean revisiting the ~70 kind tests
  in the readers. The `_local` suffix keeps the id apart from a row the book might print later (`r2:lost_lagoon@2-8`), which the
  guard then refuses.
- **Through to the app:** bundle `entry.authority` (schema.sql, `rules/tables.py`; the export requires the column), the export
  entry's `authority` (`read.py`, field dictionary), answers `RuleFacts.cited {link, url, quote}` with `book: []`, no `row`, and
  `applies` "Closed to fishing by the Vancouver Board of Parks and Recreation: Stanley Park rules, as StanleyParkVan.com gives
  them." (`display.decided.add_sources`). `cited` is optional within display 20 (T892's precedent): an older app shows the rule
  with no link.
- **App change (minimal):** a `web` Line run (`core/regs/line.ts`). `ruleBook` / `ruleBookLine` (`book.ts`) give a cited rule
  that one "StanleyParkVan.com ↗" run in place of the book pages, so every place that shows a rule's book link shows the
  citation. `LineText` draws it as a link through `openLink`, which shows and reports a refused open.
- **The one row:** Lost Lagoon (wbk:329532481), `r2:lost_lagoon_local@2-8`, written by `write_region`.
- **Proof:**
  - `pipeline/tests/test_local_closures.py`: loader, shape, guard, the live row, the corpus read, the answers facts.
  - End to end on the committed mini release: the mini world has an invented Bylaw Pond closed by an invented parks board.
    The real chain (atlas, reach, bundle, export, answers, card_check, release) answers it `closed_all`, with the citation in
    the shipped answers.
  - `test_mini_release.py` inventory: 28 entries.
  - App: `book.test.ts`.
  - The live bundle was not rebuilt. Lost Lagoon reaches the app with the next data release, whose binding diff will show
    the one new water (attributed: T981).

## Owner 2026-11-21 (answer, source check)
Claude looked for an official source to replace StanleyParkVan.com. Found:
- The Parks Control By-law bans fishing only at posted bathing beaches (s.22(f)).
- The Park Board's Wildlife page (vancouver.ca/parks-recreation-culture/wildlife.aspx) says fishing is permitted except
  at bathing beaches.
- No official page closes Lost Lagoon. The Park Board's by-law PDF and its Lost Lagoon page refused automated fetches.

Asked: keep, pull, or keep and say unofficial? Owner: "Keep it, current source". It ships in 0.8.5 as built.
