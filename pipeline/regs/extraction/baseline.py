"""THE EXTRACTOR CHANGE GATE (T426): a changed extractor must still read the current book the same way.

The annual runbook fixes the extractor for a NEW book's layout, then diffs the new book against last year's. Nothing
proved the changed extractor still read the CURRENT book as before, so a regression showed up as a fake "the book
changed" row (T415, Heber River). This module is that proof.

* THE BASELINE (`baseline/<book>.json`, committed): every row of the current book as the accepted extraction reads
  it — the PDF's sha, the extractor commit, and per row its key (PDF page + row box, in PDF points), image key,
  water, a sha of the whole row and a short sha per field. The rows file itself stays generated.
* THE ACCEPTED DIFFS (`baseline/accepted_diffs.json`, committed): each row a deliberate extractor change moved —
  its key, the sha it had (`was`, null for a new row) and has (`now`, null for a gone row), and the reason. An entry
  explains exactly that move: the same row changing again is unexplained again.
* THE CHECK: re-extract the book with the code as it is now (no row images are written: the crop is recorded,
  never rendered — the trick `row_boxes` uses), compare row by row, exit 1 naming every unexplained row.

    python -m pipeline.regs.extraction.extract_synopsis --check-against pipeline/regs/extraction/baseline/2025-27.json
                     # (no PDF argument: the baseline's own book, by sha — `book_pdf`)
    python -m pipeline.regs.extraction.baseline check --pages 18 24        # single pages (seconds)
    python -m pipeline.regs.extraction.baseline cut --book 2025-27 --commit <sha> --note "…"   # step 9 of the runbook
    python -m pipeline.regs.extraction.baseline fixture --rows page_018_row_021.png …        # the fast tier's chars

A whole-book check is a heavy job: run it through `scripts/heavy.sh` (runbook step 2).

What a row is compared on (`canonical`): water, mu, raw_regs, symbols, page, pdf_page, region, image. `symbols` is
compared as a SET (sorted): the 09-24 extraction's order followed the hash seed (T239: 22 rows), and the order carries
nothing. Anything else is compared exactly — whitespace included: a line break where a space was is a change.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

VERSION = 1
HERE = Path(__file__).resolve().parent / "baseline"
#: the current book's baseline and its accepted diffs. One book at a time: the latest `<years>.json` here is the
#: current book (a new cut lands beside the old one; the old one is then `git rm`ed — runbook step 9)
DEFAULT_MANIFEST = max((p for p in HERE.glob("*.json") if p.name != "accepted_diffs.json"),
                       key=lambda p: p.name, default=HERE / "2025-27.json")
DEFAULT_ACCEPTED = HERE / "accepted_diffs.json"
FIELDS = ("water", "mu", "raw_regs", "symbols", "page", "pdf_page", "region", "image")
#: the fixture file's char fields, in order (pdfplumber's names); `width` is x1 - x0
CHAR_FIELDS = ("text", "x0", "x1", "top", "bottom", "size", "fontname")


# ---------------------------------------------------------------- rows, keys, shas

def canonical(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {f: row.get(f) for f in FIELDS}
    out["symbols"] = sorted(out["symbols"] or [])
    return out


def _h(obj, n: int) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()[:n]


def row_sha(row: Dict[str, Any]) -> str:
    return _h(canonical(row), 16)


def field_shas(row: Dict[str, Any]) -> Dict[str, str]:
    c = canonical(row)
    return {f: _h(c[f], 8) for f in FIELDS}


def row_key(pdf_page: int, box: Iterable[float]) -> str:
    """`018:36.14,86.88,539.94,96.48` — the PDF page and the row's box in points, 2 decimals (`row_boxes`)."""
    return f"{int(pdf_page):03d}:" + ",".join(f"{float(v):.2f}" for v in box)


def file_sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# ---------------------------------------------------------------- re-extraction (no images)

def page_rows(args) -> List[Dict[str, Any]]:
    """Extract one PDF page with the current code; one {key, box, row} per row the extraction KEEPS
    (`extract_synopsis.keep_page`: the sample table and empty pages give none). The row crop is recorded,
    never rendered."""
    pdf_path, pdf_page = args
    import pdfplumber
    from pipeline.regs.extraction.extract_synopsis import FishingSynopsisParser, keep_page
    boxes: Dict[str, List[float]] = {}
    with tempfile.TemporaryDirectory() as tmp:
        parser = FishingSynopsisParser(output_dir=tmp)

        def record(page, page_num, row_index, section, x0, x1, div_x):
            key = f"page_{page_num:03d}_row_{row_index:03d}.png"
            boxes[key] = [round(float(x0), 2), round(float(section["y0"]), 2),
                          round(float(x1), 2), round(float(section["y1"]), 2)]
            return key

        parser.save_row_crop = record
        with pdfplumber.open(pdf_path) as pdf:
            result = keep_page(parser.extract_rows(pdf.pages[pdf_page - 1], page_num=pdf_page), pdf_page)
    if result is None:
        return []
    return [{"key": row_key(pdf_page, boxes[r.image]), "box": boxes[r.image], "row": r.to_dict()}
            for r in result.rows]


def extract(pdf_path: Path, pages: Optional[List[int]] = None, workers: int = 4) -> List[Dict[str, Any]]:
    import pdfplumber
    if pages is None:
        with pdfplumber.open(pdf_path) as pdf:
            pages = list(range(1, len(pdf.pages) + 1))
    workers = max(1, min(workers, 4))                    # heavy jobs run <= 4 workers
    if workers == 1 or len(pages) == 1:
        found = [page_rows((str(pdf_path), n)) for n in pages]
    else:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            found = list(ex.map(page_rows, [(str(pdf_path), n) for n in pages]))
    return [r for page in found for r in page]


# ---------------------------------------------------------------- the baseline

def cut(raw_path: Path, boxes_path: Path, pdf_path: Path, *, book: str, commit: str, note: str) -> Dict[str, Any]:
    """The baseline from an extraction on disk and its `row_boxes.json` (which must be cut from that very file and
    PDF — `row_boxes` refuses a box for another row)."""
    raw_sha, pdf_sha = file_sha(raw_path), file_sha(pdf_path)
    boxes = json.loads(Path(boxes_path).read_text(encoding="utf-8"))
    if boxes.get("raw_data_sha256") != raw_sha or boxes.get("pdf_sha256") != pdf_sha:
        raise SystemExit(f"baseline cut: {boxes_path} was cut from another extraction or PDF "
                         f"(raw {boxes.get('raw_data_sha256')} vs {raw_sha}, pdf {boxes.get('pdf_sha256')} vs "
                         f"{pdf_sha}) — re-run `python -m pipeline.regs.extraction.row_boxes` first")
    pages = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    rows: Dict[str, Dict[str, Any]] = {}
    counts: Dict[str, int] = {}
    for p in pages:
        for r in p.get("rows") or []:
            b = boxes["rows"].get(r["image"])
            if b is None:
                raise SystemExit(f"baseline cut: no box for {r['image']} in {boxes_path}")
            k = row_key(r["pdf_page"], b["box"])
            if k in rows:
                raise SystemExit(f"baseline cut: two rows at {k}")
            rows[k] = {"image": r["image"], "water": r["water"], "sha": row_sha(r), "fields": field_shas(r)}
            counts[str(r["pdf_page"])] = counts.get(str(r["pdf_page"]), 0) + 1
    return {
        "_about": ("T426 extractor change gate: the current book's rows as the accepted extraction reads them, one "
                   "sha per row keyed by PDF page + row box. Written by `python -m pipeline.regs.extraction.baseline "
                   "cut` when a book is accepted (runbook step 9); read by `extract_synopsis --check-against`. "
                   "Never edited by hand: a deliberate extractor change goes in accepted_diffs.json."),
        "version": VERSION, "book": book, "pdf_sha256": pdf_sha, "raw_data_sha256": raw_sha,
        "extractor": {"commit": commit, "note": note},
        "compared": ("canonical(row) over " + ", ".join(FIELDS) + "; symbols as a sorted set, everything else "
                     "exact (whitespace included)"),
        "rows_count": len(rows), "pages": dict(sorted(counts.items(), key=lambda kv: int(kv[0]))),
        "rows": dict(sorted(rows.items())),
    }


def write_json(doc: Dict[str, Any], path: Path, one_line_key: Optional[str] = "rows") -> None:
    """Indented, but each row of `one_line_key` on one line: a diff of the baseline reads one line per row."""
    if one_line_key and isinstance(doc.get(one_line_key), dict):
        head = {k: v for k, v in doc.items() if k != one_line_key}
        body = json.dumps(head, indent=1, ensure_ascii=False)[:-2]
        lines = [f" {json.dumps(k, ensure_ascii=False)}: {json.dumps(v, ensure_ascii=False, sort_keys=True)}"
                 for k, v in doc[one_line_key].items()]
        text = body + f',\n "{one_line_key}": {{\n' + ",\n".join(lines) + "\n }\n}\n"
        json.loads(text)                                  # the writer must round-trip
    else:
        text = json.dumps(doc, indent=1, ensure_ascii=False) + "\n"
    tmp = Path(path).with_suffix(".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def rows_digest(manifest: Dict[str, Any]) -> str:
    return _h(manifest["rows"], 16)


# ---------------------------------------------------------------- the comparison

def compare(manifest: Dict[str, Any], got: List[Dict[str, Any]], accepted: Dict[str, Any],
            pages: Optional[Iterable[int]] = None) -> Dict[str, Any]:
    """Row by row. `pages` limits the comparison to those PDF pages (a single-page check). Returns
    {unexplained, explained, stale, compared}; each diff is {key, kind: changed|gone|new, image, water, was, now,
    fields}."""
    want = manifest["rows"]
    if pages is not None:
        keep = {int(p) for p in pages}
        want = {k: v for k, v in want.items() if int(k.split(":")[0]) in keep}
    now = {g["key"]: g for g in got}
    if len(now) != len(got):
        raise ValueError("two re-extracted rows share one key")
    diffs = []
    for k in sorted(set(want) | set(now)):
        w, g = want.get(k), now.get(k)
        if w and g:
            sha = row_sha(g["row"])
            if sha == w["sha"]:
                continue
            fs = field_shas(g["row"])
            diffs.append({"key": k, "kind": "changed", "image": g["row"]["image"], "water": g["row"]["water"],
                          "was": w["sha"], "now": sha, "fields": [f for f in FIELDS if fs[f] != w["fields"][f]]})
        elif w:
            diffs.append({"key": k, "kind": "gone", "image": w["image"], "water": w["water"], "was": w["sha"],
                          "now": None, "fields": []})
        else:
            diffs.append({"key": k, "kind": "new", "image": g["row"]["image"], "water": g["row"]["water"],
                          "was": None, "now": row_sha(g["row"]), "fields": []})
    acc = {(a["key"], a.get("was"), a.get("now")): a for a in accepted.get("accepted", [])}
    unexplained, explained, used = [], [], set()
    for d in diffs:
        a = acc.get((d["key"], d["was"], d["now"]))
        if a is None:
            unexplained.append(d)
        else:
            explained.append(dict(d, reason=a["reason"]))
            used.add((d["key"], d["was"], d["now"]))
    in_scope = (lambda k: True) if pages is None else (lambda k: int(k.split(":")[0]) in keep)
    stale = [a for t, a in acc.items() if t not in used and in_scope(t[0])]
    return {"unexplained": unexplained, "explained": explained, "stale": stale, "compared": len(want)}


def load_accepted(path: Path, manifest: Dict[str, Any]) -> Dict[str, Any]:
    doc = json.loads(Path(path).read_text(encoding="utf-8"))
    if doc.get("baseline_rows") != rows_digest(manifest):
        raise SystemExit(f"{path}: written for another baseline (baseline_rows {doc.get('baseline_rows')} vs "
                         f"{rows_digest(manifest)}) — a new baseline starts with no accepted diffs")
    for a in doc.get("accepted", []):
        if not (a.get("reason") or "").strip():
            raise SystemExit(f"{path}: {a.get('key')} has no reason")
    return doc


def _line(d: Dict[str, Any]) -> str:
    what = {"changed": "changed " + "/".join(d["fields"]), "gone": "gone", "new": "new"}[d["kind"]]
    return f"{d['key']}  {d['image']}  {d['water'][:60]!r}: {what}  (was {d['was']}, now {d['now']})"


def check(manifest_path: Path, accepted_path: Path, pdf_path: Path, *, pages: Optional[List[int]] = None,
          workers: int = 4, report: Optional[Path] = None) -> int:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    sha = file_sha(pdf_path)
    if sha != manifest["pdf_sha256"]:
        print(f"check-against: {pdf_path} is not the baseline's book (sha {sha[:12]}…, baseline "
              f"{manifest['pdf_sha256'][:12]}…). The gate re-reads the CURRENT book; the new book is diffed by "
              f"synopsis_diff.", file=sys.stderr)
        return 2
    accepted = load_accepted(accepted_path, manifest)
    got = extract(pdf_path, pages, workers)
    res = compare(manifest, got, accepted, pages)
    scope = "the whole book" if pages is None else f"PDF page(s) {', '.join(map(str, pages))}"
    print(f"check-against {Path(manifest_path).name} ({manifest['book']}), {scope}: {res['compared']} baseline "
          f"rows, {len(got)} read now; {len(res['explained'])} accepted diff(s), {len(res['unexplained'])} "
          f"unexplained")
    for d in res["explained"]:
        print("  accepted  " + _line(d) + f" — {d['reason']}")
    for a in res["stale"]:
        print(f"  stale     {a['key']}  {a.get('image')}: accepted diff no longer seen (was {a.get('was')}, now "
              f"{a.get('now')}) — remove it from {Path(accepted_path).name}")
    for d in res["unexplained"]:
        print("  UNEXPLAINED " + _line(d))
    if report:
        write_json(res, report, one_line_key=None)
    return 1 if res["unexplained"] or res["stale"] else 0


# ---------------------------------------------------------------- the fast tier's character fixtures

def capture(pdf_path: Path, images: List[str]) -> List[Dict[str, Any]]:
    """For each row image key: its two cells' characters (as `extract_rows` hands them to `extract_text_with_bold`)
    and its visual symbols, plus the row the extractor makes of them now. Fonts are interned per row."""
    import pdfplumber
    from pipeline.regs.extraction.extract_synopsis import FishingSynopsisParser
    by_page: Dict[int, List[str]] = {}
    for im in images:
        by_page.setdefault(int(im.split("_")[1]), []).append(im)
    out = []
    with tempfile.TemporaryDirectory() as tmp, pdfplumber.open(pdf_path) as pdf:
        for n, want in sorted(by_page.items()):
            parser = FishingSynopsisParser(output_dir=tmp)
            cells, syms, got = [], [], {}
            text_of, vis = parser.extract_text_with_bold, parser.detect_visual_symbols

            def rec_text(section, _f=text_of):
                cells.append(list(section.chars))
                return _f(section)

            def rec_vis(ctx, _f=vis):
                syms.append(_f(ctx))
                return syms[-1]

            def rec_crop(page, page_num, row_index, section, x0, x1, div_x):
                got[f"page_{page_num:03d}_row_{row_index:03d}.png"] = (cells[-2], cells[-1], syms[-1])
                return f"page_{page_num:03d}_row_{row_index:03d}.png"

            parser.extract_text_with_bold, parser.detect_visual_symbols = rec_text, rec_vis
            parser.save_row_crop = rec_crop
            result = parser.extract_rows(pdf.pages[n - 1], page_num=n)
            rows = {r.image: r.to_dict() for r in result.rows}
            for im in want:
                left, right, vs = got[im]
                fonts: List[str] = []

                def pack(chars):
                    res = []
                    for c in chars:
                        if c["fontname"] not in fonts:
                            fonts.append(c["fontname"])
                        res.append([c["text"]] + [round(float(c[f]), 4) for f in CHAR_FIELDS[1:6]]
                                   + [fonts.index(c["fontname"])])
                    return res
                r = rows[im]
                out.append({"image": im, "water": r["water"], "fonts": fonts, "visual_symbols": vs,
                            "left": pack(left), "right": pack(right),
                            "expected": {f: r[f] for f in ("water", "mu", "raw_regs", "symbols")},
                            "sha": row_sha(r)})
    return out


def write_fixture(doc: Dict[str, Any], path: Path) -> None:
    """Gzipped JSON (a row is ~1,000 characters), one row per line inside; mtime 0 so a re-capture of the same
    rows is byte-identical."""
    import gzip
    head = json.dumps({k: v for k, v in doc.items() if k != "rows"}, ensure_ascii=False)[:-1]
    text = head + ', "rows": [\n' + ",\n".join(json.dumps(r, ensure_ascii=False) for r in doc["rows"]) + "\n]}\n"
    json.loads(text)
    with open(path, "wb") as fh, gzip.GzipFile(fileobj=fh, mode="wb", mtime=0, filename="") as gz:
        gz.write(text.encode("utf-8"))


def read_fixture(path: Path) -> Dict[str, Any]:
    import gzip
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def unpack(chars: List[list], fonts: List[str]) -> List[Dict[str, Any]]:
    res = []
    for c in chars:
        d = dict(zip(CHAR_FIELDS[:6], c[:6]))
        d["fontname"] = fonts[c[6]]
        d["width"] = d["x1"] - d["x0"]
        res.append(d)
    return res


# ---------------------------------------------------------------- CLI

def book_pdf(manifest: Dict[str, Any]) -> Path:
    """The baseline's own book: the repo copy (`data/source/fishing_synopsis.pdf`) while it is still that book,
    else last year's copy runbook step 1 keeps beside it (`fishing_synopsis.<book>.pdf`) — chosen by sha, never
    by name. Raises naming both when neither is the baseline's PDF."""
    from pipeline.common.synopsis_pages import default_pdf
    cur = Path(default_pdf())
    cands = [cur, cur.with_name(f"{cur.stem}.{manifest['book']}{cur.suffix}")]
    for c in cands:
        if c.exists() and file_sha(c) == manifest["pdf_sha256"]:
            return c
    raise SystemExit(f"no copy of the baseline's book ({manifest['book']}, sha {manifest['pdf_sha256'][:12]}…) "
                     f"among {', '.join(map(str, cands))} — keep last year's PDF as runbook step 1 says")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="re-extract and compare with the baseline (whole book = heavy)")
    c.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    c.add_argument("--accepted", type=Path, default=DEFAULT_ACCEPTED)
    c.add_argument("--pdf", type=Path, default=None)
    c.add_argument("--pages", type=int, nargs="+", help="PDF pages only (single-page check)")
    c.add_argument("--workers", type=int, default=4)
    c.add_argument("--report", type=Path, help="write the diffs as JSON")
    k = sub.add_parser("cut", help="the baseline from an extraction on disk + its row_boxes.json")
    k.add_argument("--raw", type=Path, default=None, help="default: the live synopsis_raw_data.json")
    k.add_argument("--boxes", type=Path, default=None, help="default: the live row_boxes.json")
    k.add_argument("--pdf", type=Path, default=None)
    k.add_argument("--book", required=True)
    k.add_argument("--commit", required=True, help="the extractor commit that made the extraction")
    k.add_argument("--note", default="")
    k.add_argument("--out", type=Path, default=DEFAULT_MANIFEST)
    k.add_argument("--accepted-out", type=Path, default=DEFAULT_ACCEPTED,
                   help="reset to an empty list for the new baseline")
    f = sub.add_parser("fixture", help="capture rows' characters for the fast tier")
    f.add_argument("--rows", nargs="+", required=True, help="row image keys (page_018_row_021.png)")
    f.add_argument("--pdf", type=Path, default=None)
    f.add_argument("--out", type=Path, required=True)
    a = ap.parse_args(argv)
    if a.cmd == "check":
        pdf = a.pdf or book_pdf(json.loads(a.manifest.read_text(encoding="utf-8")))
        return check(a.manifest, a.accepted, pdf, pages=a.pages, workers=a.workers, report=a.report)
    from pipeline.common.synopsis_pages import default_pdf
    pdf = a.pdf or Path(default_pdf())
    if a.cmd == "cut":
        from pipeline.common.curated import GENERATED
        raw = a.raw or GENERATED.regs.extraction / "synopsis_raw_data.json"
        boxes = a.boxes or GENERATED.regs.extraction / "row_boxes.json"
        doc = cut(raw, boxes, pdf, book=a.book, commit=a.commit, note=a.note)
        write_json(doc, a.out)
        write_json({"_about": ACCEPTED_ABOUT, "baseline": a.out.name, "baseline_rows": rows_digest(doc),
                    "accepted": []}, a.accepted_out, one_line_key=None)
        print(f"baseline: {doc['rows_count']} rows on {len(doc['pages'])} pages -> {a.out}; "
              f"{a.accepted_out} reset")
        return 0
    doc = {"_about": "T426 fast tier: rows' cell characters as pdfplumber reads them (after cleaning), replayed "
                     "with no PDF by test_extract_fixtures.py. Char = [text, x0, x1, top, bottom, size, font index]. "
                     "Written by `python -m pipeline.regs.extraction.baseline fixture`.",
           "pdf_sha256": file_sha(pdf), "rows": capture(pdf, a.rows)}
    write_fixture(doc, a.out)
    print(f"fixture: {len(doc['rows'])} rows -> {a.out}")
    return 0


ACCEPTED_ABOUT = ("T426: each current-book row a DELIBERATE extractor change moved, with its reason. An entry "
                  "explains exactly its move (key + was + now): the same row changing again is unexplained again. "
                  "Claude adds an entry when it commits the extractor change; `baseline cut` (runbook step 9) "
                  "empties the list, since the new baseline holds the change.")


if __name__ == "__main__":
    sys.exit(main())
