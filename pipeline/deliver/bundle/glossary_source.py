"""`glossary_source` — THE BOOK'S OWN WORDS the glossary quotes, copied into the bundle (audit F29, plan 2.14).

The answers' glossary (`pipeline/deliver/answers/glossary.py`) quotes the synopsis VERBATIM: the p.80 definitions, the
p.4 legend, and each region's title. It used to open the curated transcription (`CURATED.regulations.reference`) at
run time, which broke "the bundle is the only source" — a reader holding last edition's bundle would quote this
edition's book. The bundle copies the three now, and the answers read them from here.

    kind        term                         text
    definition  the p.80 term, lower case    its definition (definitions.md, `**term**: …`)
    legend      the p.4 term, lower case     its legend line (water-specific-tables.md, `**Term:** …`)
    region      the region code ("1", "7a")  its name (region-<code>.md's title, `# Region N — Name`)

Text is whitespace-folded with the markdown bold removed, exactly as the glossary always read it. A missing file, a
region file without its title, or a bundle without the table is refused — never an empty glossary.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

Row = Tuple[str, str, str]
KINDS = ("definition", "legend", "region")


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("**", "")).strip()


def _paragraph_terms(text: str) -> Dict[str, str]:
    """`**term**: definition` (p.80) and `**Term:** definition` (p.4) paragraphs -> {term: text}."""
    out: Dict[str, str] = {}
    for para in re.split(r"\n\s*\n", text):
        m = re.match(r"\s*\*\*(.+?)(?::\*\*|\*\*:)\s*(.+)", para, re.S)
        if m:
            out[_clean(m.group(1)).lower()] = _clean(m.group(2))
    return out


def _read(d: Path, name: str) -> str:
    p = d / name
    if not p.is_file():
        raise SystemExit(f"glossary_source: the book's text {p} is missing")
    return p.read_text(encoding="utf-8")


def rows_from_book(ref_dir: Optional[Path] = None) -> List[Row]:
    """Every row, read from the curated transcription of the synopsis (source text, never edited to fit)."""
    if ref_dir is None:
        from pipeline.common.curated import CURATED
        ref_dir = CURATED.regulations.reference
    d = Path(ref_dir)
    rows: List[Row] = [("definition", t, x) for t, x in _paragraph_terms(_read(d, "definitions.md")).items()]
    rows += [("legend", t, x) for t, x in _paragraph_terms(_read(d, "water-specific-tables.md")).items()]
    for f in sorted(d.glob("region-*.md")):
        first = _read(d, f.name).splitlines()[0]
        m = re.match(r"#\s*Region\s+(\w+)\s+—\s+(.+)", first)
        if not m:
            raise SystemExit(f"glossary_source: {f.name} has no 'Region N — Name' title")
        rows.append(("region", m.group(1).lower(), _clean(m.group(2))))
    return sorted(rows)


def write(db: sqlite3.Connection, cov, ref_dir: Optional[Path] = None) -> None:
    rows = rows_from_book(ref_dir)
    db.executemany("INSERT INTO glossary_source (kind, term, text) VALUES (?,?,?)", rows)
    cov.filled("glossary_source", len(rows))


def read(path) -> List[Row]:
    """The bundle's rows. A bundle built before the table existed is refused (rebuild it)."""
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name = 'glossary_source'").fetchone():
            raise SystemExit(f"glossary: {path} has no `glossary_source` table (plan 2.14) — rebuild the bundle: "
                             "python -m pipeline.deliver bundle")
        return [tuple(r) for r in db.execute("SELECT kind, term, text FROM glossary_source ORDER BY kind, term")]
    finally:
        db.close()
