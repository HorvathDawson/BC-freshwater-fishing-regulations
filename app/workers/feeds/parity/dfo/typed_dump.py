"""T225 DFO parity, one level down: every page through the Python `type_page` (parse → untangle → verify → locations →
typed) and `header.parse_header`, as JSON lines the TS port is diffed against (test/dfo.corpus.test.ts).

    .venv/bin/python app/workers/feeds/parity/dfo/typed_dump.py <out.jsonl> <page.html>...

A page's region slug is read from its file name (region6-eng.html, region6_20240421181125.html).
"""
from __future__ import annotations

import dataclasses
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT))

from pipeline.regs.dfo_salmon import header as H  # noqa: E402
from pipeline.regs.dfo_salmon.live import type_page  # noqa: E402


def main(out: str, pages: list[str]) -> int:
    with open(out, "w", encoding="utf-8") as fh:
        for p in pages:
            slug = re.match(r"region(\d[ab]?)[-_]", Path(p).name).group(1)
            html = Path(p).read_text(encoding="utf-8")
            row = {"page": Path(p).name, "slug": slug}
            try:
                typed, locs, structure = type_page(html, slug)
                row.update(typed=typed, locations=[dataclasses.asdict(l) for l in locs], structure=structure)
            except Exception as exc:  # noqa: BLE001
                row["error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
            try:
                row["header"] = H.parse_header(html, slug)
            except Exception as exc:  # noqa: BLE001
                row["header_error"] = type(exc).__name__
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2:]))
