"""THE WATER RULES MAP (T853): one command draws the "Nass-style" labelled map of any water's rules — for salmon, the
B.C. freshwater rules, or both — from a release on disk.

    .venv/bin/python -m pipeline.tools.water_map "Nass River" [--release DIR] [--date YYYY-MM-DD]
                                                [--mode salmon|freshwater|both] [--out DIR] [--focus KM-KM]

  release.py  the release's files, each proved against its manifest sha256 (core, water, geo; the regs store via
              `province_store`; the salmon file via `salmon.decode_files`)
  model.py    the water (name / alias / item id; ambiguous names list the choices), its system (the watercourse it is a
              piece of, `water_continue`; tributaries by `water_exit` that carry a BC row or a DFO place of their own;
              a lake's river near it), the numbered units per mode (a water's parts joined where they read the same)
              and each unit's cascade rows: region base -> the water's own rows (book pages) -> DFO places -> TODAY
  geo.py      lines and polygons from the tile build's layers (`stream`, `lake`, `tidal_water`, `basin`)
  layout.py   callout / label placement without overlaps (pure; tested)
  render.py   matplotlib: map, zoom insets, panels, legend, scale, north arrow, ticks

PYTHON DECIDES WORDS, THE APP DRAWS: every word on a panel is the release's — `display.rules[].title` and `book`,
the display frame (status, strip, closers), the trout table's top line, the gear summary, `display.ends`, the salmon
chain's places and verdicts. Nothing re-derives a rule. Read-only on data; writes PNGs to --out only.

A big system is capped and says so in the image: at most `MAX_TRIBS` rule-carrying tributaries are numbered (the rest
named in a note); a main water with more than `MAX_MAIN_UNITS` stretches in a mode needs `--focus KM-KM`.
"""
