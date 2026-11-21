# T981 — Municipal no-fishing waters (Lost Lagoon, Beaver Lake in Stanley Park, and others) shown as closed

Status: IN PROGRESS — inventory + mechanism proposal on `lane/085-t981`; left (Claude): owner accepts rows

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

## Inventory, 2026-11-21 (Claude, lane/085-t981) — PROPOSALS, none accepted
Full rows (quotes, URLs, matches, conflicts): `data/generated/review/t981_local_closures_proposals.json`. Read nowhere.

| row | water | item | authority, bylaw, section | status |
|---|---|---|---|---|
| victoria-goodacre-lake | Goodacre Lake (Beacon Hill Park) | wbk:329676480 | Victoria Parks Regulation Bylaw 07-059 s.14(1)(h): "must not … catch a bird, animal, or fish" in a park | proposed, high |
| victoria-fountain-lake | Fountain Lake (Beacon Hill Park) | none (not mapped; needs an added lake) | same | proposed, high (rule) |
| saanich-swan-lake | Swan Lake (Saanich sanctuary) | wbk:329676327 | Sanctuary's Visitors' Guide "NO HUNTING / FISHING / BOATING"; Saanich bylaw 7753 s.2.1(a) gives the Society the authority | proposed, medium |
| burnaby-park-waters | every lake/stream in a Burnaby park | Deer Lake wbk:328976459, Burnaby Lake wbk:329291868 | Burnaby 7331 s.4: no fishing "in any lake or stream in any park without the special permission of the Administrator" | owner call: the synopsis prints open rules for both lakes |
| surrey-park-waters | every lake/stream in a Surrey park except designated, posted areas | Green Timbers Lake wbk:360887617 | Surrey 13480 s.34(a) | owner call: designated areas not published; Green Timbers is stocked |
| vancouver-lost-lagoon | Lost Lagoon | wbk:329532481 | **no source found**: the Parks Control By-law's only fishing clause is 22(f) (posted bathing beaches); s.10 covers a posted Board notice | needs a source (sign photo / Park Board page / 3-1-1) |
| vancouver-beaver-lake | Beaver Lake (Stanley Park) | wbk:329532620 | same | needs a source |

Checked, not a closure: Langley City 3048 s.12(x), View Royal 986 s.2.1(e), North Saanich 1470 s.8 (they say "disturb … trap or snare … fish", not catch or angle);
Metro Vancouver 1420 s.34 (posted areas only). No fishing clause: Saanich 7753, District of North Vancouver 8310 (shellfish only), Richmond 8771, City of North Vancouver 6611,
Central Saanich 804, West Kelowna 0184. Not yet checked: CRD 4225, Coquitlam, Port Moody, New Westminster, Delta, Township of Langley, Abbotsford, Chilliwack, Nanaimo, Kelowna,
Kamloops, Prince George.

## Mechanism (proposed, not built)
- **Curated source, accepted rows only:** `data/curated/regulations/local_bylaws/region-<n>.json` (`_about`, `region`, `entries`). It is a
  second ENTRY SOURCE: `parsing.io.entry_sources` already takes every directory of region files that carry `entries`, the reach run
  reads every source, and the bundle's `rules.read_entries` globs the same root. A small accept tool copies accepted proposal rows in
  through `curated_write.write`; proposal rows are read by nothing.
- **Entry = a synopsis water row:** `matched: [item]`, `extents: [{op: whole}]`, one rule `retention_limit`, `ALL_GAME_FISH`, `take 0`,
  `may_target false`, verbatim "No Fishing" (Bear River's shape). As a water row it already beats the region base. One new
  `CatalogueEntry` field `authority {kind: local_bylaw, body, bylaw, section, url, quote}`, in place of `source_pages`. The entry
  id needs a decision in `common/entry_id.py`: reuse `r<n>:` with a `__bylaw` slug, or add a new kind.
- **Through to the app:** the bundle `entry` row carries `authority`, and the export/answers put the reason on the closure, worked
  out in Python ("Closed to fishing by City of Victoria Parks Regulation Bylaw 07-059 s.14(1)(h)" + URL). The app only shows a link
  it is given.
- **Guards:** refuse a bylaw entry on an item that has a synopsis water row (those rows stay owner calls), refuse any rule but the
  closure, and keep the book-only gates (classified waters, extraction baseline, word reads, page map) on the synopsis source.
  Not built: it needs a schema field, a bundle column, an answers line and an app link, and no row is accepted yet.
