# T981 — Municipal no-fishing waters (Lost Lagoon, Beaver Lake in Stanley Park, and others) shown as closed

Status: OPEN

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

## Owner 2026-11-21 (dialog)
In 0.8.5. Verbatim: "https://stanleyparkvan.com/stanley-park-van-location-lost-lagoon.html is only one I know for sure. Maybe add other info to a ticket but only create the way to ban lost lagoon for now with a nice curated data path to list these. Then put all the info found for the other water bodies in the ticket and we will review for the next release." Build: the curated local-closures path + Lost Lagoon only. The other rows stay here for review next release.
