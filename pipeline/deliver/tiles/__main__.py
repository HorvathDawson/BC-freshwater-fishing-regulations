from __future__ import annotations

import argparse
import time
from pathlib import Path

from project_config import get_config
from pipeline.deliver.tiles import export, tippe
from pipeline.common.curated import CURATED, GENERATED, SOURCE


def main() -> None:
    cfg = get_config()
    ap = argparse.ArgumentParser(prog="python -m pipeline.deliver.tiles")
    ap.add_argument("--build", default=str(GENERATED.build()),
                    help="a completed build dir (graph.pkl, geometries.pkl, registry.json)")
    ap.add_argument("--gpkg", default=str(cfg.fwa_data_gpkg))
    ap.add_argument("--places", default=str(SOURCE / "bc_places.json"))
    ap.add_argument("--out", default=str(GENERATED.tiles))
    ap.add_argument("--minzoom", type=int, default=4)
    ap.add_argument("--maxzoom", type=int, default=14)
    ap.add_argument("--limit", type=int, default=None,
                    help="only the first N sections — for a fast smoke test")
    ap.add_argument("--skip-water", action="store_true")
    ap.add_argument("--geojson-only", action="store_true",
                    help="write the layer files but do not run tippecanoe")
    ap.add_argument("--tiles-only", action="store_true",
                    help="run tippecanoe over the layer files ALREADY in --out/layers and "
                         "skip the export. For changes to the tippecanoe invocation, where "
                         "re-deriving the same geometry is ~40 min of wasted work.")
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
    if a.tiles_only:
        # The layer files are the export's whole output and nothing here changes them, so a
        # tippecanoe-side change can be validated against the ones already on disk.
        if not any(layer_dir.glob("*.geojsonl")):
            raise SystemExit(f"--tiles-only: no layer files in {layer_dir}. "
                             "Run without it once to export them.")
        out = tippe.build(layer_dir, out_dir / "atlas.pmtiles",
                          minzoom=a.minzoom, maxzoom=a.maxzoom, verbose=a.verbose)
        print(f"\n{out}  {out.stat().st_size / 1e6:,.1f} MB   "
              f"tiles took {time.time() - t0:,.0f}s")
        return

    counts: dict[str, int] = {}
    print("admin geography")
    counts |= export.export_admin(a.gpkg, layer_dir)
    counts |= export.export_contours(a.gpkg, layer_dir)
    # THE FIELD, for the zooms where a river is a hairline — see tiles/basins.py. Grouped
    # with admin geography rather than with water because it is neither: it is the ground a
    # reading is a claim about, and it draws only below the zoom the rivers take over at.
    from pipeline.deliver.tiles.basins import export_basins
    counts |= export_basins(a.gpkg, layer_dir)
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
    _write_sidecar(build_dir, out_dir)


def _write_sidecar(build_dir: Path, out_dir: Path) -> None:
    """`atlas.meta.json` — the vintage the bundle has to match.

    A SEPARATE FILE AND NOT PMTILES METADATA, because the app has to read it on both
    platforms: the web map goes through the `pmtiles` protocol and the device map through
    maplibre-react-native, and only one of those gives a page access to the archive header.
    A 100-byte JSON file next to the archive is readable by both with no library at all.
    """
    import json

    from pipeline.common.section_handles import digest_for

    meta = {"section_handles": digest_for(build_dir), "build": build_dir.name}
    p = out_dir / "atlas.meta.json"
    p.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    print(f"{p}  {meta}")


if __name__ == "__main__":
    main()
