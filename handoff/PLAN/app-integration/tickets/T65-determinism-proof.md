# T65 — Determinism proof (audit N12)

Status: DONE (2026-11-04, atlas; HASH). Prove whether two builds from the same inputs give the same atlas/reach/bundle (docstrings say no; the
2026-10-06 fixes may have cured it). Heavy: scripts/heavy.sh, background, mindful of memory. Acceptance: a needs_source
test or a recorded diff with the cause of each difference.

## Result (2026-11-04): the atlas IS deterministic
Built twice from the same inputs (`python -m pipeline build --atlas` into `full_next` beside the promoted `full`, both
27 min, under scripts/heavy.sh): promote's parity says IDENTICAL (21,317 items, 4,595,628 sections, 27,419 boundaries;
0 added / removed / changed). File by file, every artifact is byte-identical (geometries.pkl 2 GB, registry.json,
section_handles.txt, splits, tidal, waterbody_polys, …) except:
- `summary.txt`, `*.meta.json`: stage timings and `build_ts` only;
- `graph.pkl`: 36 bytes shorter from byte 468,616,076 on — Python's per-run string hashing orders a pickled set
  differently. Loaded, the two `StreamGraph`s compare EQUAL (`==`).
So the "~50 blocks differ" memory (atlas-build-not-deterministic) is out of date: the 2026-10-06 fixes cured it.
Reach and bundle carry their own content digests (reach_digest, store_digest; test_release reassembles the store at its
digest), so a drift there shows on every build. The side atlas was deleted after the diff (never promoted).
