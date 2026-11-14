"""WHAT THE CORPUS CLAIMS (T667 slice 8): the export's book-bound checks, as one explicit input.

The export, the answers step and `answers.display.decided` hold the corpus to claims about THE BOOK: the synopsis PDF
the printed-page map is read off (`guide.pdf_pages`) and the row boxes cut from it (the highlight), the prose examples
the guide declares on real waters (`examples.EXAMPLES`), every case mechanism needing a real case
(`cases.CASE_MECHANISMS`), and the duties every release carries (a recorded-retention "carry your paper licence" duty,
a rule naming a protected fish). With no `--claims` they are exactly that — `book()`, pinned whole by
`test_export_claims.py` — and nothing reads less than before.

A corpus that is not the book (the mini release's invented one) names its own in a claims file:

    {"format": "export-claims/1",
     "synopsis": "<pdf, relative to this file>", "row_boxes": "<row-boxes sidecar, relative>",
     "examples": [{Example fields}, ...], "mechanisms": ["<a CASE_MECHANISMS name>", ...],
     "duties": ["paper_licence" | "protected_fish", ...]}

and is checked as strictly: every declared example must read as declared, every declared mechanism must have a real
case, every declared duty must hold, every printed page an entry cites must be on the named PDF, and the guide prose
that names one of its waters needs a declared example (`examples.prose_gaps`, unchanged). A name that is no
mechanism or duty, an unknown example field, a missing file or a second example id is refused when the file is read.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

FORMAT = "export-claims/1"
#: the duties a release is held to (`read.paper_licence_problems`, `read.protected_problems`)
DUTIES: Tuple[str, ...] = ("paper_licence", "protected_fish")


class ClaimsError(SystemExit):
    pass


@dataclass(frozen=True)
class Claims:
    synopsis: Path
    row_boxes: Path
    examples: tuple
    mechanisms: Tuple[str, ...]
    duties: Tuple[str, ...]
    source: Optional[Path] = None          # the claims file; None = the book


def book() -> Claims:
    """THE BOOK'S CLAIMS: the repo synopsis and its row boxes, every declared example, every mechanism, every duty."""
    from pipeline.common.synopsis_pages import default_pdf
    from pipeline.deliver.export.cases import CASE_MECHANISMS
    from pipeline.deliver.export.examples import EXAMPLES
    from pipeline.regs.extraction.row_boxes import default_out
    return Claims(default_pdf(), default_out(), tuple(EXAMPLES), tuple(CASE_MECHANISMS), DUTIES)


def load(path: Path) -> Claims:
    """A claims file, validated whole (module docstring); refused naming what is wrong."""
    import dataclasses

    from pipeline.deliver.export.cases import CASE_MECHANISMS
    from pipeline.deliver.export.examples import Example
    path = Path(path)
    if not path.is_file():
        raise ClaimsError(f"claims: {path} not found")
    doc = json.loads(path.read_text(encoding="utf-8"))
    if doc.get("format") != FORMAT:
        raise ClaimsError(f"claims: {path} is format {doc.get('format')!r}, not {FORMAT!r}")
    keys = {"format", "synopsis", "row_boxes", "examples", "mechanisms", "duties", "_about"}
    if set(doc) - keys or {"synopsis", "row_boxes", "examples", "mechanisms", "duties"} - set(doc):
        raise ClaimsError(f"claims: {path} keys {sorted(doc)}; expected {sorted(keys - {'_about'})}")
    files = {}
    for k in ("synopsis", "row_boxes"):
        p = (path.parent / doc[k]).resolve()
        if not p.is_file():
            raise ClaimsError(f"claims: {path} {k} {p} not found")
        files[k] = p
    bad = [m for m in doc["mechanisms"] if m not in CASE_MECHANISMS]
    if bad:
        raise ClaimsError(f"claims: {path} names {bad}, which are no case mechanism (export/cases.json)")
    bad = [d for d in doc["duties"] if d not in DUTIES]
    if bad:
        raise ClaimsError(f"claims: {path} names duties {bad}; the duties are {list(DUTIES)}")
    fields = {f.name for f in dataclasses.fields(Example)}
    examples = []
    for x in doc["examples"]:
        extra = set(x) - fields
        if extra:
            raise ClaimsError(f"claims: {path} example {x.get('id')!r} has unknown fields {sorted(extra)}")
        examples.append(Example(**{k: tuple(v) if isinstance(v, list) else v for k, v in x.items()}))
    ids = [e.id for e in examples]
    if len(ids) != len(set(ids)):
        raise ClaimsError(f"claims: {path} declares an example id twice")
    return Claims(files["synopsis"], files["row_boxes"], tuple(examples),
                  tuple(m for m in CASE_MECHANISMS if m in doc["mechanisms"]),
                  tuple(d for d in DUTIES if d in doc["duties"]), source=path)


def duties_of(claims: Optional[Claims]) -> Tuple[str, ...]:
    """The duties the export checks require: the claims', or every one (the book's) when none are given."""
    return DUTIES if claims is None else claims.duties


def of(path: Optional[Path]) -> Claims:
    """`--claims` as given: the file's, or the book's when none is named."""
    return book() if path is None else load(path)
