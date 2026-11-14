# The mini world (T667 slice 8)

Every water here is invented. The ids are in ranges of their own: blue lines from 900000001, waterbody keys from
800000001, GNIS ids from 990001, and the code root is `920-123456`.

- `world.json`: the waters as lines and boxes. `python -m pipeline.tools.mini_release` writes an FWA-shaped
  GeoPackage and the coast pair from it into scratch.
- `curated/`: a whole curated tree in the layout of `data/curated`, holding only invented rows. The build reads it
  through `CURATED_ROOT`. Two files are copied as they are: the book's `definitions.md` and `licensing.md`, and the
  DFO limits wording. `zp:steelhead` is the province's entry, with its regions narrowed to the mini world's two.

Build it into scratch only. `--twice` builds it two times and checks that the digests agree:

    scripts/heavy.sh mini-release 30 -- .venv/bin/python -m pipeline.tools.mini_release --build <scratch> --twice

To change the world, edit these files. Never copy live data in.
