"""python -m pipeline build [...]  ->  the one rebuild command (pipeline/build.py).

`python -m pipeline <atlas flags>` (no subcommand) still runs the atlas build, as it did before
the rebuild command existed (pipeline.atlas.build), with a warning: say
`python -m pipeline.atlas.build` for that."""
import sys

if __name__ == "__main__":
    if sys.argv[1:2] == ["build"]:
        from pipeline.build import main
        raise SystemExit(main(sys.argv[2:]))
    print("python -m pipeline: no subcommand — running the ATLAS build (pipeline.atlas.build). "
          "For the whole rebuild say `python -m pipeline build`.", file=sys.stderr)
    from pipeline.atlas.build import main
    main()
