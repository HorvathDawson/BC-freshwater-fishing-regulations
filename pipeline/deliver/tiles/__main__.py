from __future__ import annotations

import argparse
import time
from pathlib import Path

from project_config import get_config
from pipeline.deliver.tiles import export, tippe
from pipeline.common.curated import CURATED, SOURCE


def main() -> None:
    cfg = get_config()
    ap = argparse.ArgumentParser(prog="python -m pipeline.deliver.tiles")
    ap.add_argument("--build", default=str(cfg.builds_dir / "full"),
                    help="a completed build dir (graph.pkl, geometries.pkl, registry.json)")
    ap.add_argument("--gpkg", default=str(cfg.fwa_data_gpkg))
    ap.add_argument("--places", default=str(SOURCE / "bc_places.json"))
    ap.add_argument("--out", default="output/tiles")
    ap.add_argument("--minzoom", type=int, default=4)
    ap.add_argument("--maxzoom", type=int, default=14)
    ap.add_argument("--limit", type=int, default=None,
                    help="only the first N sections — for a fast smoke test")
    ap.add_argument("--skip-water", action="store_true")
    ap.add_argument("--geojson-only", action="store_true",
                    help="write the layer files but do not run tippecanoe")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--write-contract", action="store_true",
                    help="regenerate pipeline/deliver/tiles/tile-contract.json and stop. The app's "
                         "style is checked against it, so a layer rename cannot silently "
                         "stop drawing.")
    a = ap.parse_args()

    if a.write_contract:
        import json as _json
        from pipeline.deliver.tiles.layers import contract
        p = Path(__file__).with_name("tile-contract.json")
        p.write_text(_json.dumps(contract(), indent=2) + "\n")
        print(f"wrote {p}")
        return

    build_dir, out_dir = Path(a.build), Path(a.out)
    layer_dir = out_dir / "layers"
    layer_dir.mkdir(parents=True, exist_ok=True)
    tippe.check()                       # fail before an hour of work, not after

    t0 = time.time()
    counts: dict[str, int] = {}
    print("admin geography")
    counts |= export.export_admin(a.gpkg, layer_dir)
    counts |= export.export_contours(a.gpkg, layer_dir)
    if not a.skip_water:
        print("water")
        counts |= export.export_streams(build_dir, layer_dir, limit=a.limit)
        counts |= export.export_waterbodies(build_dir, a.gpkg, layer_dir)

    print(f"\nfeatures: {sum(counts.values()):,}")
    for k in sorted(counts):
        print(f"  {k:<16} {counts[k]:>9,}")
    print(f"export took {time.time() - t0:,.0f}s")

    if a.geojson_only:
        print(f"layers in {layer_dir}")
        return
    out = tippe.build(layer_dir, out_dir / "atlas.pmtiles",
                      minzoom=a.minzoom, maxzoom=a.maxzoom, verbose=a.verbose)
    mb = out.stat().st_size / 1e6
    print(f"\n{out}  {mb:,.1f} MB   total {time.time() - t0:,.0f}s")


if __name__ == "__main__":
    main()
