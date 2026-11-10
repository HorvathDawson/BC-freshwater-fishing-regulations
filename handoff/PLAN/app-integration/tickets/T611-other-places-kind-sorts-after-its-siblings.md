# T611 — Salmon chain: a place of the "other places" kind sorts after the places it rides on

Status: OPEN — the deliver lane

Source: T607 (2026-11-10), the one-sibling tie; coordinator: "file a ticket for the deliver lane … do not edit the
chain".

## Today
`pipeline/deliver/salmon.py`, `chain_order`:

    return lambda lid: (-precedence.get(lid, 0), size.get(lid, 1 << 30), lid)

Tier, then FEWER SECTIONS, then the location id. A place saved as "all the other placed places on this water"
(`op: other_places`, T607) binds the union of its siblings' sections. With two or more placed siblings the union is
larger than each of them, so every sibling answers first and a sibling's closure stands. With exactly ONE placed
sibling the two cover the SAME sections: the size ties, and the id decides. If the other-places place's id sorts
first, it answers before the named place — and where the named place is closed, the water could read as open.

No place is in that state today: the Little Qualicum's "all open portions" has three placed siblings (and is not yet
re-saved as the new kind); the Qualicum's has none placed.

## Work
A place whose extent is `other_places` ALWAYS sorts after the siblings it rides on — in `chain_order`, a key part
between the size and the id (0 for every other place, 1 for an `other_places` place), fed from the places' extents
the same way `size` is; and the same in the worker's chain if it re-sorts (`inseason` / live compose: check).
Test: two places of one tier covering the same sections, the `other_places` one with the smaller id — it sorts last,
and the closure of the other one answers. Mutation pin: drop the key part and the test fails.

## Meanwhile
The review app warns on such a place and in the kind's preview, naming this ticket (`dfo.other_places_of`,
`one_sibling`).

Depends: T607. Size: S. Owner: no.
