"""(T1156) The region-PDF fetch (data/fetch_region_pdfs.py) and the one-book guard (pipeline/tools/one_book.py).
No network: the download is injected. Trees are built in tmp_path."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from data import fetch_region_pdfs as F
from pipeline.tools import one_book as OB

NAMES = ["region_1_a.pdf", "region_2_b.pdf"]


def _bytes(name: str) -> bytes:
    return f"%PDF fake {name}".encode()


def _pins(tmp_path: Path, extra: dict | None = None) -> Path:
    rows = {n: {"sha256": hashlib.sha256(_bytes(n)).hexdigest(), "bytes": len(_bytes(n)), "date_fetched": "2026-11-25",
                **(extra or {})} for n in NAMES}
    p = tmp_path / "pins.json"
    p.write_text(json.dumps({"pdfs": rows}))
    return p


def _serve(changed: dict | None = None):
    changed = changed or {}

    def dl(url: str, dest: Path) -> None:
        name = url.rsplit("/", 1)[1]
        dest.write_bytes(changed.get(name, _bytes(name)))
    return dl


def _run(tmp_path, dl, **kw):
    out: list[str] = []
    stops = F.run(tmp_path / "official", _pins(tmp_path) if not (tmp_path / "pins.json").exists() else tmp_path / "pins.json",
                  dl, say=out.append, today="2026-12-01", **kw)
    return stops, out


def test_all_match_is_ok_and_files_land(tmp_path):
    stops, _ = _run(tmp_path, _serve())
    assert stops == []
    assert sorted(p.name for p in (tmp_path / "official").iterdir()) == NAMES


def test_a_changed_checksum_stops_naming_the_file_and_writes_nothing(tmp_path):
    stops, _ = _run(tmp_path, _serve({"region_2_b.pdf": b"republished"}))
    assert len(stops) == 1 and "region_2_b.pdf" in stops[0] and "republished it" in stops[0]
    assert sorted(p.name for p in (tmp_path / "official").iterdir()) == ["region_1_a.pdf"]


def test_http_404_is_a_failure_naming_status_and_file(tmp_path):
    def dl(url, dest):
        if url.endswith("region_1_a.pdf"):
            raise F.DownloadError(404, "HTTP 404 Not Found")
        _serve()(url, dest)
    stops, _ = _run(tmp_path, dl)
    assert len(stops) == 1 and "region_1_a.pdf" in stops[0] and "404" in stops[0]
    assert "unchanged" not in stops[0]
    assert not (tmp_path / "official" / "region_1_a.pdf").exists()


def test_a_partial_download_leaves_nothing(tmp_path):
    def dl(url, dest):
        dest.write_bytes(b"%PDF half")
        raise F.DownloadError(None, "short read: 9 bytes of 99")
    stops, _ = _run(tmp_path, dl)
    assert len(stops) == 2 and all("short read" in s for s in stops)
    assert list((tmp_path / "official").iterdir()) == []


def test_repin_rewrites_exactly_that_row_and_keeps_other_keys(tmp_path):
    pins = _pins(tmp_path, {"text_sha256": "keep"})
    before = json.loads(pins.read_text())
    stops, out = _run(tmp_path, _serve({"region_2_b.pdf": b"republished"}), repin="region_2_b.pdf")
    after = json.loads(pins.read_text())
    assert stops == []
    assert after["pdfs"]["region_1_a.pdf"] == before["pdfs"]["region_1_a.pdf"]
    row = after["pdfs"]["region_2_b.pdf"]
    assert row["sha256"] == hashlib.sha256(b"republished").hexdigest() and row["bytes"] == 11
    assert row["date_fetched"] == "2026-12-01" and row["text_sha256"] == "keep"
    assert any("REPIN" in o and before["pdfs"]["region_2_b.pdf"]["sha256"] in o for o in out)


def test_a_changed_local_file_is_a_stop(tmp_path):
    (tmp_path / "official").mkdir()
    (tmp_path / "official" / "region_1_a.pdf").write_bytes(b"edited")
    stops, _ = _run(tmp_path, _serve())
    assert len(stops) == 1 and "region_1_a.pdf" in stops[0] and "local file differs" in stops[0]


# ---- the guard ----
def _pdf(path: Path, pages: int, cover: str = "FRESHWATER FISHING REGULATIONS SYNOPSIS") -> None:
    """A minimal PDF of `pages` pages; the first prints `cover`."""
    objs = ["<< /Type /Catalog /Pages 2 0 R >>",
            "<< /Type /Pages /Kids [" + " ".join(f"{4 + 2 * i} 0 R" for i in range(pages)) + f"] /Count {pages} >>",
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    for i in range(pages):
        text = cover if i == 0 else f"page {i}"
        stream = f"BT /F1 12 Tf 20 100 Td ({text}) Tj ET"
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] /Resources << /Font << /F1 3 0 R >> >> /Contents {5 + 2 * i} 0 R >>")
        objs.append(f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream")
    body, offs = "%PDF-1.4\n", []
    for n, o in enumerate(objs, 1):
        offs.append(len(body))
        body += f"{n} 0 obj\n{o}\nendobj\n"
    xref = len(body)
    body += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n" + "".join(f"{o:010d} 00000 n \n" for o in offs)
    body += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body.encode())


def _tree(tmp_path: Path) -> Path:
    src = tmp_path / "source"
    _pdf(src / "fishing_synopsis.pdf", 25)
    return src


def test_clean_tree_passes(tmp_path):
    assert OB.second_books(_tree(tmp_path)) == []


def test_a_book_copy_in_official_fails_naming_it(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "official" / "copy.pdf", 25)
    _pdf(src / "official" / "2025-2027_freshwater_fishing_regulations_synopsis.pdf", 2)
    bad = OB.second_books(src)
    assert len(bad) == 2 and any("official/copy.pdf" in b for b in bad) and any("synopsis.pdf" in b for b in bad)


def test_a_dated_archive_copy_passes(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "fishing_synopsis.2025-27.pdf", 25)
    assert OB.second_books(src) == []


def test_the_nine_region_pdfs_pass(tmp_path):
    src = _tree(tmp_path)
    for n, pages in zip(json.loads((Path(__file__).parents[2] / "data/source/region_pdfs.pins.json").read_text())["pdfs"], [8, 7, 6, 8, 7, 9, 6, 4, 5]):
        _pdf(src / "official" / n, pages)
    assert OB.second_books(src) == []


def test_a_long_pdf_that_is_not_the_book_passes(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "dfo_tidal_areas" / "waves.pdf", 74, cover="DFO tidal waters")
    assert OB.second_books(src) == []


def test_an_unopenable_pdf_is_an_error(tmp_path):
    src = _tree(tmp_path)
    (src / "official").mkdir()
    (src / "official" / "x.pdf").write_bytes(b"not a pdf")
    with pytest.raises(ValueError, match="x.pdf"):
        OB.second_books(src)


def test_the_real_source_tree_has_one_book():
    """Wired into CI through the pipeline tests: passes trivially where source/official/ is absent."""
    from pipeline.common.curated import SOURCE
    assert OB.second_books(Path(SOURCE)) == []
