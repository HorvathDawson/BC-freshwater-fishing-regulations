# T59 — One river system where two rivers meet (Atnarko/Bella Coola); Fraser exit into Annacis Channel

Status: OPEN. Owner 2026-10-22: "think of things like this as one system; bella coola atnarko is another example."

- The app already treats a `water_continue` row as one system (cross replaces the back step, 077677d9). Atnarko →
  Bella Coola has no such row: the Bella Coola starts where the Atnarko and Talchako meet, and `continuation.py`
  refuses a two-rivers-meet point (the Somass/Thompson rule).
- Fix in the pipeline: where two rivers meet, pick the one a synopsis entry names together with the receiving river
  ("Atnarko/Bella Coola Rivers", "Chilliwack/Vedder Rivers") as the continuation, still requiring `item_down` and the
  stub gate at the receiving river's top. The other stays a tributary. Fallback only when length/order agree.
- Defect found on the way: Fraser River's `water_exit` points into Annacis Channel. Find why and fix.

Acceptance: water_continue gains Atnarko → Bella Coola (and no false pairs: list every new row for review);
Fraser exit is the sea/estuary, not a side channel. Needs deliver.
