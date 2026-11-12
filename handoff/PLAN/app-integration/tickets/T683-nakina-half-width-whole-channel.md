# T683 — Nakina River: the half-width DFO rule binds the whole channel between the signs; the card carries DFO's centre-line-to-north-bank words

Status: DONE (62afe1d38)

Source: owner 2026-11-12, Owner To-Do row `nakina-half-width`, "Whole channel + note". RULINGS E19 (E4: a cut cannot take half a river's width). Proposal patch: `T664-pending/`.

## Work
- The Nakina River DFO rule binds the whole stretch between the two signs (stricter than DFO on the south half).
- The card says, in DFO's words, that the rule applies "from the centre line to the northern bank" only.

Depends: the next atlas build (T664 patches). Size: S.

## Done 2026-11-12 (62afe1d38)
The Nakina place (`dfo_salmon/places/region-6.json`) binds the whole channel between the two signs (it already did:
`between` the two sign splits) and now carries DFO's words "from the centre line to the northern bank" as a note with
`spatial_caveat: true`; `dfo_salmon.locations` reads that wording as a half-width note for any place
(`test_t683_half_a_rivers_width_is_a_note_that_narrows_the_extent`). The card shows it from the next release; the feeds
worker reads `places/` from R2 after `python -m pipeline.deliver.sync_inputs` (Mac only).
