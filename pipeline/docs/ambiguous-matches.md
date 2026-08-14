# Ambiguous matching cases + graph-correctness invariants (curation watch-list)

Salvaged from the retired `10-matching-and-invariants.md` (old match-table model). The matching
*model* is superseded by [`DESIGN-regs-to-sections.md`](DESIGN-regs-to-sections.md); what remains
below is the **hard-won specific-case knowledge** that must survive: real ambiguous entries to watch
for during curation, and two graph-build filters proven necessary by the spike.

## Watch-list — ambiguous matching cases (flag during curation)

- **Bowron Lake 5-16 — "Park waters other than Bowron Lake"** (flagged 2026-08-09): resolves by an
  **admin boundary** (Bowron Lake Provincial Park polygon) **minus** the named lake, not a clean
  gnis/name match. Curated `n/a` as a *split* (no reach cut), but the reg still has to attach to the
  *park-waters set* via `admin_targets`/park-polygon → sub-extent. **Look out for this:** verify it
  lands on the park's other waters and does NOT accidentally match Bowron Lake itself (the named
  lake is excluded). This is the "admin bound but not clearly one" pattern — watch for similar
  "Park waters other than X" / "all lakes in the park except Y" entries.
  *(New model: an `area` registry item minus a named-lake item; the exclusion is a claimed-reach carve-out.)*
- **Sumallo River 2-2 — "includes 'Cedar' Lake, at Sunshine Valley"** (flagged 2026-08-10): NOT a split
  (curated n/a), but an **include** clause — the Sumallo River water must ALSO match/attach to "Cedar"
  Lake (a name-variant / co-located waterbody at Sunshine Valley). Ensure the matcher pulls Cedar Lake
  in under the Sumallo entry (name-tuple / co-membership), don't drop it. Same class as other
  "includes X Lake/Creek" extent clauses (Seton canal, Vaseux lagoons, McArthur slough).
  *(New model: entry `matched: [sumallo, cedar_lake]` — multiple registry items on one entry.)*
- **Bull River 4-22 — "Quinn Creek [Includes Tributaries]"** (flagged 2026-08-10): the trout/char C&R
  applies to Quinn Creek AND its tributaries as a whole set — an **include**, NOT a point split
  (curated n/a). The matcher must assign that reg to Quinn Creek's whole WSC subtree via tributary
  inheritance. FWA GNIS name is **"Quinn (Queen) Creek"**, WSC `300-625474-636250-492930` (a name
  variant — the reg says "Quinn", FWA says "Quinn (Queen)"). Ensure the name-tuple match resolves
  "Quinn Creek" → this WSC. (The Galbraith→Van and Aberfeldie Dam→Tie Mill Dam reaches ARE curated
  point splits; only Quinn Creek is a tributary include.)
- **Thorn Creek 7-39 → FWA "Thorne Creek"** (flagged 2026-08-10): the reg/gazette spells it **"Thorn
  Creek"** but FWA GNIS is **"Thorne Creek"**, `BLUE_LINE_KEY 359557375`, WSC
  `200-948755-999851-889551-445636-…`. Ensure the name-tuple match resolves "Thorn Creek" → this blk.
  The split ("from Attichika Creek to a point 500 m upstream") is the lowermost 500 m of Thorne Creek:
  the confluence with Attichika **is Thorne's mouth** (56.92636,-126.68256), split end 500 m up
  (56.92409,-126.68522).
- **Trepanier Creek 8-8 → "Trépanier Creek"** (flagged 2026-08-11): reg says "Trepanier", the gazette/FWA
  spelling carries the accent **"Trépanier Creek"** (Peachland). Ensure the name-tuple match is
  accent-insensitive. Split = Hwy 97C (Okanagan Connector, OSM way 155232470, 49.80873,-119.7447) down
  to Okanagan Lake.

## Graph-build filters KEPT after the spike (do NOT delete — `12` proved these necessary)

These are graph-correctness facts (not matching), still fully in force:

- **The WSC-hierarchy (excluded-WSC / parent-WSC) filter** — reframed as "a tributary is a
  WSC-descendant reachable upstream." SCC condensation is a no-op on FWA; this filter is what
  stops the Chehalis→Harrison confluence-parent leak.
- **The `EDGE_TYPE=2300` connector barrier** — lake-collapse does not replace it (canals bypass lake
  polygons); required to stop the Kootenay↔Columbia leak. Keep the strict missing-edge_type guard.
