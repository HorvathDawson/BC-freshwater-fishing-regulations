"""T419: stream lines smoothed per zoom keep every shared node and line end byte for byte, keep ids and properties,
and flatten a meander narrower than the blur while a wide bend keeps its shape."""

from __future__ import annotations

import math

import orjson

from pipeline.deliver.tiles import smooth


def _meander(n=400, amp_m=60.0, wave_m=300.0, length_m=6000.0, lat=49.1):
    ky = 111_320.0
    kx = ky * math.cos(math.radians(lat))
    pts = []
    for i in range(n):
        x = length_m * i / (n - 1)
        y = amp_m * math.sin(2 * math.pi * x / wave_m)
        pts.append([round(-122.6 + x / kx, 6), round(lat + y / ky, 6)])
    return pts, kx, ky


def _feature(sid, coords):
    return {"type": "Feature", "id": sid, "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {"name": "Salmon River", "ord": 4}, "tippecanoe": {"layer": "stream", "minzoom": 9}}


def test_shared_nodes_and_ends_stay_exact_and_ids_props_unchanged(tmp_path):
    main, _, _ = _meander()
    trib = [[-122.55, 49.15], main[200]]  # a tributary mouth on the main stem's INTERIOR
    p = tmp_path / "stream.geojsonl"
    p.write_bytes(b"".join(orjson.dumps(f) + b"\n" for f in (_feature(1, main), _feature(2, trib))))
    smooth.smooth_file(p, 10)
    a, b = [orjson.loads(ln) for ln in p.read_bytes().splitlines()]
    assert (a["id"], a["properties"], b["id"], b["properties"]) == (1, {"name": "Salmon River", "ord": 4}, 2,
                                                                    {"name": "Salmon River", "ord": 4})
    out = a["geometry"]["coordinates"]
    assert out[0] == main[0] and out[-1] == main[-1]
    assert main[200] in out, "the tributary's mouth must stay a vertex of the main stem"
    assert b["geometry"]["coordinates"] == trib
    assert len(out) < len(main) / 4


def test_a_tight_meander_flattens_a_wide_bend_stays():
    tight, kx, ky = _meander(amp_m=60, wave_m=300)       # ~1 px amplitude at z10, 6 px wavelength
    wide, _, _ = _meander(amp_m=1500, wave_m=12000)      # 30 px amplitude, 240 px wavelength
    for line, keep in ((tight, False), (wide, True)):
        out = smooth.smooth_line(line, set(), 10)
        amp_in = max(abs(p[1] - 49.1) for p in line) * ky
        amp_out = max(abs(p[1] - 49.1) for p in out) * ky
        if keep:
            assert amp_out > 0.9 * amp_in, (amp_in, amp_out)
        else:
            assert amp_out < 0.2 * amp_in, (amp_in, amp_out)
