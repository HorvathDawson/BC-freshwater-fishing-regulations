"""python -m pipeline build [...]  ->  the one rebuild command (pipeline/build.py).

It is the ONLY entry (plan 2.11): every stage — the gauge envelope, the reach run, the tiles, the delivery — runs from
it, in order, when its inputs changed. A bare `python -m pipeline` used to run the atlas build; it now refuses, naming
both commands (the atlas alone is `python -m pipeline build --atlas`, or `python -m pipeline.atlas.build`)."""
import sys

USAGE = ("usage: python -m pipeline build [--dry-run] [--atlas [--promote]] [--force STAGE ...] [--no-tiles] [--labels]\n"
         "  (a side atlas alone: python -m pipeline build --atlas, or python -m pipeline.atlas.build)")

if __name__ == "__main__":
    if sys.argv[1:2] == ["build"]:
        from pipeline.build import main
        raise SystemExit(main(sys.argv[2:]))
    raise SystemExit(f"python -m pipeline: say `build`.\n{USAGE}")
