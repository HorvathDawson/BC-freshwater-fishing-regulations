"""Build the T184 review page (the curation review as a shareable, phone-friendly artifact).

    python curation-review/artifact/build_page.py [--export curation-review/artifact/out/export.json]
        -> curation-review/artifact/out/pilot.html   (the book pages stay beside it: out/book/*.webp)

Reads the export (`python -m pipeline.regs.review.export_artifact`) and inlines it into the template, less
the prints (the page carries each markable unit's digest `fp`; the importer looks the prints up in the
export). Marks are saved in the artifact's db: collection "marks", one doc per (target, rule) — what
`python -m pipeline.regs.review.import_artifact_marks` reads back into the repo's marks store.

Publish with capabilities {"db": {}, "user": {"scopes": ["profile"]}} and `files` mapping every
`book/<name>.webp` to `curation-review/artifact/out/book/<name>.webp` (see `publish_files`).
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"


def page_data(export: dict) -> dict:
    """What the page inlines: the export without its prints."""
    return {k: v for k, v in export.items() if k != "prints"}


def build(export_path: Path, out: Path) -> Path:
    export = json.loads(export_path.read_text(encoding="utf-8"))
    blob = json.dumps(page_data(export), ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    html = (HERE / "page_template.html").read_text(encoding="utf-8").replace("/*__DATA__*/null", blob, 1)
    out.write_text(html, encoding="utf-8")
    return out


def publish_files(export_path: Path) -> dict[str, str]:
    """The Artifact publish `files` map: published path -> source file (relative to the repo root)."""
    export = json.loads(export_path.read_text(encoding="utf-8"))
    root = HERE.parents[1]
    return {p["image"]: str((export_path.parent / p["image"]).resolve().relative_to(root))
            for p in export["pages"].values()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--export", type=Path, default=OUT / "export.json")
    ap.add_argument("--out", type=Path, default=OUT / "pilot.html")
    a = ap.parse_args()
    p = build(a.export, a.out)
    files = publish_files(a.export)
    size = sum((a.export.parent / k).stat().st_size for k in files)
    print(f"{p} {p.stat().st_size / 1e6:.2f} MB · {len(files)} book pages, {size / 1e6:.2f} MB")
    (a.out.parent / "publish_files.json").write_text(json.dumps(files, indent=1) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
