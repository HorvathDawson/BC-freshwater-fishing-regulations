# T632 — Owner: four decisions from the reach run (T546 items 4 and 5)

Status: IN REVIEW — 8fdcb79d4

Source: T546 report 2026-11-11.

1. DFO placement: subtract tidal water from a DFO place, or not? 18 places are affected.
2. Should every unplaced-piece reason stop the run, with a per-piece accept list?
3. A curated "expected to match nothing" note for overrides (today 1 of 492: Okanagan River Oxbows, 26 dead ids).
4. Six entries whose `matched` disagrees with the matcher; four hold ids a curator added. Keep the curator's, or the matcher's?

## Acceptance
- The owner answers each; T546 items 4 and 5 are finished to match.

Depends: T546. Size: S. Owner: four answers.

## Owner answers 2026-11-11
1. Tidal: always subtract, unless there is a stated reason not to for a place. Send the 18 examples for a quick double check.
2. Unplaced pieces: stop the build unless accepted (per-piece accept list).
3. Dead overrides: no "expected" note. A check makes it impossible to build with one (fast-fail should run it), then fix Okanagan River Oxbows: it matched before; find why it stopped (owner's guess: lake became a stream polygon).
4. Matched disagreements: the curator's saved match wins; send the six with both versions for a tick.

## Run 2026-11-11
Landed: 8fdcb79d4 (code, curated files, tests). Owner files: `T632-tidal-examples.json`, `T632-matched-six.md` (beside
this folder). Reach rerun into a scratch dir against `data/generated/atlas/full`: rules digest unchanged
(1a11ea11a5b3fb3e). Binding diff against the T546 rerun on the same atlas: 0 rules, 0 unresolved, 0 licensing, 0
diagnostics, 21 DFO places moved. All 21 lost tidal sections only (141 in total) and gained nothing. Nothing promoted.

1. **Tidal.** Every DFO place now loses the run's tidal sections, through the same `build_reach` limit the provincial
   rows use. A place keeps them only if `data/curated/dfo_salmon/tidal_calls.json` states a reason; that file is empty
   today. The run stops on a place the subtraction would empty, and on a keep row that names no place. The issue
   counted 18 places; the curated DFO edits since then make it 21 (new: Kanaka Creek, Little Campbell River, Nicomekl
   River, 1 section each). All 21 are in the examples file. I would subtract on every one of them: each place's text
   is freshwater wording, and none says "tidal" or "to the mouth". One oddity, not about tidal water:
   `1:somass-river-tributaries:whole` binds a 5.5 km Somass mainstem piece below the Papermill Dam. Nothing renders
   the removed pieces: they are not in the tile layers, and drawing them needs `geometries.pkl` (a heavy job, not run).
2. **Unplaced pieces.** Any unplaced piece, of any reason, now stops the reach run (`build.unplaced_gate`), unless the
   owner accepts it in `data/curated/waters/unplaced_piece_calls.json` `accepted`. Five pieces wait there as PROPOSALS:
   Noons Creek -2200000392:0 (2.2 km) and Seymour River -2000000390:0, both `flows_to_the_sea_unjoined`, and three
   Skeena side channels 360220078/098/099:0, `straddles_the_cut`. The three Skeena pieces are unplaced because the
   atlas braid sweep refused the cut `skeena_river__kitsumkalum_river_into_skeena_river_u1500m`: `braid_sweep.json`
   records "refused: line misses" (its perpendicular at m 123,961 crosses none of the channels), and
   `residual_straddles` lists 098 and 099. The real fix belongs in the atlas: the sweep, or the cut's position.
3. **Dead override ids.** The reach run and fast-fail (`python -m pipeline.regs.matching.matcher`, about 2 s) refuse
   any override id that names no water, the DFO-tagged ones included. There is no "expected" note.
   **Okanagan River Oxbows cause.** The 26 dead ids are FWA watershed codes of unnamed Okanagan side channels
   (`300-432687-…`). The v2 registry gives no item to an unnamed line, so these codes never resolved. In git, the
   entry's `matched` never held one of them, from its creation on 2026-09-10 to today. The matcher hands nameless ids
   to a "resolver" that was never built. The owner's guess (the lake became a stream polygon, T322) is not the cause:
   the 40 oxbow polygons (`wbk`) resolve both before and after that change. A 27th code, `300-432687-516449`, does
   resolve, but it is Park Rill's own code. The matcher adds Park Rill for that reason; it was dropped from `matched`
   on 2026-10-07 and the owner kept it apart on 2026-11-03.
   **Proposed fix (owner applies; not applied):** in `data/curated/regulations/book_name_links.json`, entry
   `OKANAGAN RIVER OXBOWS` (8-1), delete the 26 dead `fwa_watershed_codes` and `300-432687-516449`, and keep the 40
   `waterbody_keys`. Bindings do not change: none of those ids ever bound. The oxbow channel LINES stay outside the
   exemption, as they are today. To exempt them, they need a registry item; that is atlas work for a new ticket.
   Not verified: that the 26 lines exist as unnamed graph sections. The graph probe was queued behind another lane's
   suite and stopped.
4. **Matched.** The curator's saved match wins. The six are in `T632-matched-six.md`, ready for the owner's tick.
   Nothing changed.

**Fails by design until the owner acts:** the reach run, and so `pipeline build` (the reach stage key now moves),
refuses on the 5 unaccepted pieces and on the Oxbows dead ids. `scripts/fast-fail.sh` fails on the Oxbows dead ids,
so every release or APK build that runs it stops there too.
**Waits for the owner:** accept or place the 5 pieces; apply the Oxbows diff; tick the six; skim the 21 tidal examples.
Leftovers (no ticket made): place the Skeena channels in the atlas (the braid sweep); check whether the Noons and
Seymour pieces lie in their closures; mint items for the oxbow channel lines if the owner wants them exempt; DFO
`owned=` (joining water at a confluence cut) is still not passed; render the tidal images from `geometries.pkl`.
