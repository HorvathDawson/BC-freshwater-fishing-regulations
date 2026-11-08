# T510 — A channel joining the Fraser to Strawberry Slough is filed under the Fraser

Status: OPEN — owner picked rule A for all (2026-11-08); next: pin tests, side atlas build, bindings diff (expect 13 sections), moved list to owner before promote; proposal list `shots/T510-moved.md`. Follow-up of T497. Part of T495 gate 1.

Owner 2026-11-08: "there is a little stream that connects between Fraser and Strawberry Slough that I feel like should
be Strawberry, not Fraser."
Owner 2026-11-08 (rule A): "Yes the strawberry slough thing should apply to all".

## Found (r-6dce77a24e6a)
- **The channel:** blk **355991900**, one section `355991900:0`, 1,543 m, unnamed in FWA, on the Fraser's watershed code.
  Its mouth is on Strawberry Slough (blk 355991897, 2.7 km, `gnis:16210`), its source on the Fraser mainstem. It is in the
  Fraser item `gnis:39325`, part "Upstream of the CPR Bridge at Mission" (stretch 1).
- **Why T497 left it:** `names.follow_connections` gives an unnamed channel another water only when everything its ends
  reach is ONE named water that is not the main river. One end reaches the Fraser, so the magnitude rule stays and the
  channel is the Fraser's. As designed: T497's rule was "if an end reaches the main river … the channel stays with the
  main river".
- **Rules on it today vs Strawberry Slough:** it carries the Fraser's `fraser_river_upstream_of_mission` r1 (dead-fin-fish
  bait for sturgeon only), r3 (no fishing 1 h after sunset to 1 h before sunrise), r5 (steelhead closed Aug 15–Dec 31), r6
  (do not impede commercial vessels) and bait.r3. As Strawberry Slough it would lose those and gain strawberry_slough.r1
  (8 km/h speed limit). Everything else (zone, provincial) is the same. **Moving it opens it at night and to steelhead in
  the fall** — a loosening, so the owner should agree.
- **Same pattern elsewhere** (end-walk on the FWA gpkg, whole province; `shots/T510-moved.md`): 11 connectors (13
  sections, 22.0 km) have one end only on the main river and the other only on one other named water: Fraser → Strawberry,
  Williamson Slough ×2, Annacis Channel; Pitt → Widgeon Slough; Columbia → Baldy Channel ×3, Back Channel, Hotsprings
  Channel ×2. Rules change on 2 (Strawberry, and Widgeon: loses Pitt's night closure + 10 km/h, gains 5 km/h); the other
  9 are a name change only. 15 more touch both waters at one end and stay either way.

## Guards
- **Receiving river is not a tributary (Strahler).** The rule only reads same-code named waters, and the main river is the
  highest magnitude on the code, so it never files a main river under its slough; a channel already named is never
  renamed. It moves unnamed channels only.
- **Lake must not answer to its river.** The rule names stream lines only. A polygon is affected only through the
  slough-fold (`registry/flowing.py`: an unnamed polygon a channel item enters and leaves becomes that channel), which is
  how T497's 11 ha Baldy polygon moved. None of the 11 is checked for that yet; the side build must.
- **Prune.** 13 candidate blks are pruned today; a rename must not revive them (`prune._has_own_name` covers T497's case).

## Options
- **A. Connector rule (recommended).** In `follow_connections`, judge each END on its own: when one end reaches only the
  main river and the other only one other named water, the channel takes that water. Name-agnostic, small, the 11 above.
  Needs: pin tests (355991900 → gnis:16210, 355993053 → gnis:27696, a both-at-one-end channel stays), a side atlas build
  via `scripts/heavy.sh`, bindings + braid_sweep diff vs live (expect exactly the 13 sections), then the owner list, then
  the next release (atlas rebuild).
- **B. Curate just this one.** A `name_variants` entry putting blk 355991900 in Strawberry Slough. No rule change, same
  atlas rebuild; leaves the Widgeon case (same shape, also loses a night closure) for another report.
- **C. Leave it.** Arguable: a channel that leaves the river is the river's water, and the night/steelhead closures are
  the safer reading.

Recommendation: A, because the Widgeon connector is the same shape, and both rule changes are the slough's own rules
replacing the river's — what the owner said for Strawberry. Not done: no atlas change, nothing promoted.
