"""(T1156) The region-PDF fetch (data/fetch_region_pdfs.py) and the one-book guard (pipeline/tools/one_book.py).
No network: the download is injected. Trees are built in tmp_path."""
from __future__ import annotations

import hashlib
import json
import os
import urllib.error
from pathlib import Path

import pytest

from data import fetch_region_pdfs as F
from pipeline.tools import one_book as OB

NAMES = ["region_1_a.pdf", "region_2_b.pdf"]


def _bytes(name: str) -> bytes:
    return f"%PDF-1.4 fake {name}".encode()


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
    stops, _ = _run(tmp_path, _serve({"region_2_b.pdf": b"%PDF- republished"}))
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


def test_repin_rewrites_exactly_that_row_and_drops_keys_that_described_the_old_bytes(tmp_path):
    pins = _pins(tmp_path, {"text_sha256": "keep"})
    before = json.loads(pins.read_text())
    stops, out = _run(tmp_path, _serve({"region_2_b.pdf": b"%PDF- republished"}), repin="region_2_b.pdf")
    after = json.loads(pins.read_text())
    assert stops == []
    assert after["pdfs"]["region_1_a.pdf"] == before["pdfs"]["region_1_a.pdf"]
    row = after["pdfs"]["region_2_b.pdf"]
    assert row["sha256"] == hashlib.sha256(b"%PDF- republished").hexdigest() and row["bytes"] == 17
    assert row == {"sha256": row["sha256"], "bytes": 17, "date_fetched": "2026-12-01"}
    assert any("REPIN" in o and before["pdfs"]["region_2_b.pdf"]["sha256"] in o for o in out)
    assert any("DROPPED region_2_b.pdf" in o and "text_sha256" in o and "recompute" in o for o in out)
    assert after["pdfs"]["region_1_a.pdf"]["text_sha256"] == "keep"


def test_a_changed_local_file_is_a_stop(tmp_path):
    (tmp_path / "official").mkdir()
    (tmp_path / "official" / "region_1_a.pdf").write_bytes(b"edited")
    stops, _ = _run(tmp_path, _serve())
    assert len(stops) == 1 and "region_1_a.pdf" in stops[0] and "local file differs" in stops[0]


def test_an_html_200_body_is_not_a_pdf_and_is_never_repinned(tmp_path):
    for body in (b"<html>maintenance</html>", b""):
        pins = _pins(tmp_path)
        before = pins.read_text()
        stops, _ = _run(tmp_path, lambda url, dest, b=body: dest.write_bytes(b), repin="region_1_a.pdf")
        assert len(stops) == 2 and all("not a PDF" in x and "republished" not in x for x in stops)
        assert pins.read_text() == before
        assert not any(p.suffix == ".pdf" for p in (tmp_path / "official").iterdir())


def test_served_with_repin_is_refused(tmp_path):
    with pytest.raises(SystemExit):
        F.main(["--served", "--repin", "region_1_a.pdf"])
    assert F.run(tmp_path / "o", _pins(tmp_path), _serve(), served=True, repin="region_1_a.pdf", say=lambda s: None)[0].startswith("--served and --repin")


def test_served_checks_without_creating_the_folder(tmp_path):
    stops, out = _run(tmp_path, _serve(), served=True)
    assert stops == [] and not (tmp_path / "official").exists() and len(out) == 2
    stops, _ = _run(tmp_path, _serve({"region_2_b.pdf": b"%PDF- republished"}), served=True)
    assert len(stops) == 1 and "region_2_b.pdf" in stops[0] and not (tmp_path / "official").exists()


def test_an_unknown_repin_name_is_refused(tmp_path):
    stops, _ = _run(tmp_path, _serve(), repin="nope.pdf")
    assert len(stops) == 1 and "nope.pdf" in stops[0]


def test_a_leftover_part_file_is_removed_and_said(tmp_path):
    (tmp_path / "official").mkdir()
    (tmp_path / "official" / ".region_1_a.pdf.x.part").write_bytes(b"x")
    _, out = _run(tmp_path, _serve())
    assert any("leftover partial download" in o for o in out)
    assert not list((tmp_path / "official").glob(".*.part"))


def test_the_pin_file_is_written_atomically(tmp_path, monkeypatch):
    pins = _pins(tmp_path)
    seen = []
    real = os.replace
    monkeypatch.setattr(os, "replace", lambda a, b: (seen.append((Path(a).name, Path(b).name)), real(a, b))[1])
    _run(tmp_path, _serve({"region_2_b.pdf": b"%PDF- new"}), repin="region_2_b.pdf")
    assert (".pins.json.tmp", "pins.json") in seen


class _Resp:
    def __init__(self, body: bytes, length):
        self._b, self.headers = body, ({} if length is None else {"Content-Length": length})

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self, n=-1):
        b, self._b = self._b, b""
        return b


def _fake(resp=None, err=None):
    def opener(req, timeout=None):
        assert req.get_header("User-agent") == F.USER_AGENT
        if err:
            raise err
        return resp
    return opener


def test_download_url_maps_http_errors_and_bad_lengths(tmp_path):
    dest = tmp_path / "x"
    with pytest.raises(F.DownloadError) as e:
        F.download_url("http://x/u", dest, opener=_fake(err=urllib.error.HTTPError("u", 503, "Unavailable", {}, None)))
    assert e.value.status == 503
    with pytest.raises(F.DownloadError, match="network failure"):
        F.download_url("http://x/u", dest, opener=_fake(err=urllib.error.URLError("dns")))
    with pytest.raises(F.DownloadError, match="short read"):
        F.download_url("http://x/u", dest, opener=_fake(_Resp(b"%PDF-abc", "99")))
    with pytest.raises(F.DownloadError, match="malformed Content-Length"):
        F.download_url("http://x/u", dest, opener=_fake(_Resp(b"%PDF-abc", "12x")))
    F.download_url("http://x/u", dest, opener=_fake(_Resp(b"%PDF-abc", "8")))
    assert dest.read_bytes() == b"%PDF-abc"


def test_a_malformed_length_is_one_files_failure_not_a_traceback(tmp_path):
    stops, _ = _run(tmp_path, lambda u, d: F.download_url(u, d, opener=_fake(_Resp(b"%PDF-", "zz"))))
    assert len(stops) == 2 and all("malformed Content-Length" in x for x in stops)


# ---- the guard ----
MiB = 1 << 20
REGIONS = ["region_1_vancouver_island.pdf", "region_2_lower_mainland.pdf", "region_3_thompson.pdf", "region_4_kootenay.pdf",
           "region_5_cariboo.pdf", "region_6_skeena.pdf", "region_7a_omineca.pdf", "region_7b_peace.pdf", "region_8_okanagan.pdf"]


def _pdf(path: Path, pages: int, cover: str = "FRESHWATER FISHING REGULATIONS SYNOPSIS", size: int = 0) -> None:
    """A minimal PDF of `pages` pages; the first prints `cover` (empty = no text); padded to `size` bytes."""
    objs = ["<< /Type /Catalog /Pages 2 0 R >>",
            "<< /Type /Pages /Kids [" + " ".join(f"{4 + 2 * i} 0 R" for i in range(pages)) + f"] /Count {pages} >>",
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    for i in range(pages):
        text = cover if i == 0 else f"page {i}"
        stream = f"BT /F1 12 Tf 20 100 Td ({text}) Tj ET" if text else ""
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] /Resources << /Font << /F1 3 0 R >> >> /Contents {5 + 2 * i} 0 R >>")
        objs.append(f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream")
    body, offs = "%PDF-1.4\n", []
    for n, o in enumerate(objs, 1):
        offs.append(len(body))
        body += f"{n} 0 obj\n{o}\nendobj\n"
    xref = len(body)
    body += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n" + "".join(f"{o:010d} 00000 n \n" for o in offs)
    body += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
    data = body.encode()
    data += b"%" + b"." * max(0, size - len(data) - 2) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _tree(tmp_path: Path) -> Path:
    src = tmp_path / "source"
    _pdf(src / "fishing_synopsis.pdf", 25, size=2 * MiB)
    return src


def test_clean_tree_passes(tmp_path):
    assert OB.second_books(_tree(tmp_path)) == []


def test_a_book_copy_in_official_fails_naming_it(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "official" / "copy.pdf", 25, size=2 * MiB)
    _pdf(src / "official" / "2025-2027_freshwater_fishing_regulations_synopsis.pdf", 2)
    bad = OB.second_books(src)
    assert len(bad) == 2 and any("official/copy.pdf" in b for b in bad) and any("synopsis.pdf" in b for b in bad)


def test_a_big_many_page_book_is_caught_whatever_its_text_says(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "official" / "book.pdf", 70, cover="", size=6 * MiB)
    _pdf(src / "official" / "image_cover.pdf", 70, cover="Hello", size=6 * MiB)
    assert sorted(b.split(":")[0] for b in OB.second_books(src)) == ["official/book.pdf", "official/image_cover.pdf"]


def test_the_suffix_is_read_in_any_case(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "official" / "COPY.PDF", 70, cover="", size=6 * MiB)
    assert [b.split(":")[0] for b in OB.second_books(src)] == ["official/COPY.PDF"]


def test_a_copy_in_a_symlinked_folder_is_found(tmp_path):
    src = _tree(tmp_path)
    _pdf(tmp_path / "elsewhere" / "book.pdf", 70, cover="", size=6 * MiB)
    os.symlink(tmp_path / "elsewhere", src / "linked")
    os.symlink(src, src / "loop")
    assert [b.split(":")[0] for b in OB.second_books(src)] == ["linked/book.pdf"]


def test_a_broken_link_is_an_error_naming_it(tmp_path):
    src = _tree(tmp_path)
    os.symlink(tmp_path / "gone.pdf", src / "dangling.pdf")
    with pytest.raises(ValueError, match="dangling.pdf"):
        OB.second_books(src)


def test_a_dated_archive_copy_passes(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "fishing_synopsis.2025-27.pdf", 70, size=6 * MiB)
    assert OB.second_books(src) == []


def test_the_nine_region_pdfs_pass(tmp_path):
    src = _tree(tmp_path)
    for n, pages in zip(REGIONS, [8, 7, 6, 8, 7, 9, 6, 4, 5]):
        _pdf(src / "official" / n, pages, cover="Region", size=int(1.7 * MiB))
    assert OB.second_books(src) == []


def test_a_long_small_pdf_that_is_not_the_book_passes(tmp_path):
    """The two tracked DFO evidence PDFs: 74 pages, 1.7 MB, no cover words: under the size to be a book by pages."""
    src = _tree(tmp_path)
    _pdf(src / "dfo_tidal_areas" / "waves.pdf", 74, cover="DFO tidal waters", size=int(1.7 * MiB))
    assert OB.second_books(src) == []


def test_the_cover_rule_catches_a_small_unnamed_copy(tmp_path):
    src = _tree(tmp_path)
    _pdf(src / "official" / "x.pdf", 25, size=2 * MiB)
    assert len(OB.second_books(src)) == 1


def test_a_small_file_is_never_opened(tmp_path):
    src = _tree(tmp_path)
    (src / "bathymetry_pdfs").mkdir()
    (src / "bathymetry_pdfs" / "corrupt.pdf").write_bytes(b"not a pdf")
    assert OB.second_books(src) == []


def test_an_unopenable_big_pdf_is_an_error(tmp_path):
    src = _tree(tmp_path)
    (src / "official").mkdir()
    (src / "official" / "x.pdf").write_bytes(b"not a pdf" + b"." * (2 * MiB))
    with pytest.raises(ValueError, match="x.pdf"):
        OB.second_books(src)


def test_the_pin_file_names_the_nine_frozen_here():
    from pipeline.common.curated import SOURCE
    assert sorted(F.load_pins(Path(SOURCE) / F.PINS_FILE)["pdfs"]) == sorted(REGIONS)


def test_the_tracked_source_tree_has_one_book():
    """What CI holds: the tracked files only. Passes trivially without a second book (source/official/ is absent)."""
    from pipeline.common.curated import SOURCE
    assert OB.second_books(Path(SOURCE), OB.tracked_pdfs(Path(SOURCE))) == []


@pytest.mark.needs_source
def test_the_whole_source_tree_has_one_book(request):
    """A machine with the fetched files (source/official/, bathymetry_pdfs/): the whole folder."""
    from pipeline.common.curated import SOURCE
    from pipeline.tests.conftest import need
    assert OB.second_books(need(request, "source", Path(SOURCE), "bash scripts/bootstrap.sh")) == []
