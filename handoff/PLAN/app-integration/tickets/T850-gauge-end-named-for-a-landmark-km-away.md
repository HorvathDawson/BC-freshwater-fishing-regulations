# T850 — A part that ends at a gauge cut is named for a landmark up to 18 km away ("Canyon City bridge → Canyon City bridge")

Status: OPEN, owner: big issue (2026-11-18 13:43). Main chat's area: pipeline/deliver/bundle; found 2026-11-18 by the app chat's Nass salmon check.

On the staging release r-f1f2e8da9666 the Nass's open coho stretch (Canyon City bridge → water survey station 08DB001,
~11.8 km) is titled "Canyon City (Gitwinksihlkw) bridge … → Canyon City (Gitwinksihlkw) bridge", and the next part
"Canyon City bridge → Meziadin". The bundle's `split_near` row says gauge 08DB001 is 0.0 m from the bridge.

Cause (read, not yet proved by a test): `gauge_near` in `pipeline/deliver/bundle/topology.py` (~733-737) measures the
distance to the whole section, not to the section's end at the cut; the part's main channel is one section from the
bridge to the gauge, so the bridge reads 0 m from the gauge. 69 of 236 gauge-end names point at a landmark 1.5-18 km
from the cut (Halfway, Zymoetz, Telkwa, Atnarko, Squamish among them). Names only: where rules bind is unchanged.

Fix: measure to the cut point (the section's end vertex at the gauge), name the end for the gauge (or a landmark within a
small radius of it), and pin a test on the Nass and two others.
