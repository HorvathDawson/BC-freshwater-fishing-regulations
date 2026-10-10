"""The verbatim normalisers — `clean_verbatim` and `squash`, the ONE normaliser.

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re


#: Dash variants the extraction emits interchangeably; all mean "-" for comparison.
_DASH = dict.fromkeys(map(ord, "‐‑‒–—―−"), "-")


#: THE EXTRACTION'S MARKUP — never the book's words (FIX BATCH §6/§7, 2026-10-07). `**` is the
#: book's bold kept as markdown, `[Includes Tributaries]` the token the extraction writes for an
#: inline ✱. Both belong to `regs_verbatim` (the batch's row, stored byte for byte); a RECORD's
#: `verbatim` is the printed sentence a reader is shown, so it carries neither: 662 rules once
#: did, 40 with an unbalanced pair ("No Fishing** from …"). `clean_verbatim` is the one cleaner
#: (ingest applies it to every quote the model writes); `CatalogueEntry` refuses a quote with any.
EXTRACTION_MARKUP = re.compile(r"\*\*|\[Includes Tributaries\]")


def clean_verbatim(text: str) -> str:
    """A quote with the extraction's markup taken out: `**` dropped, `[Includes Tributaries]`
    dropped (the ✱ is a fact the rule's `includes_tributaries` carries), the spaces it leaves
    tidied — "Creek[Includes Tributaries], Dec 1" -> "Creek, Dec 1". Newlines are kept."""
    t = (text or "").replace("**", "")
    t = re.sub(r"[ \t]*\[Includes Tributaries\][ \t]*(?=[,;.:)\n]|$)", "", t)
    t = re.sub(r"[ \t]*\[Includes Tributaries\][ \t]*", " ", t)
    return "\n".join(line.strip() for line in t.split("\n")).strip()


def squash(text: str) -> str:
    """Normalise away what carries no meaning when comparing a quote to its source: EMPHASIS,
    bullets, blockquote markers, dash variants, whitespace, case.

    Markdown is OUR annotation, added by extraction — 629 of 1393 batch rows carry `**`. The
    printed regulation has no asterisks in it, so a model quoting the sentence it can see writes
    it without them, and a raw substring check then calls a perfect quote a fabrication. It called
    700 of them that. What is STORED is still the batch's own text, byte for byte; only the
    comparison is normalised."""
    t = (text or "").replace("[Includes Tributaries]", "").translate(_DASH).replace("*", "")
    t = re.sub(r"(?m)^\s*[>|]\s?", " ", t)
    t = re.sub(r"(?m)^\s*[-•]\s+", " ", t)
    return re.sub(r"\s+", " ", t).strip().lower()
