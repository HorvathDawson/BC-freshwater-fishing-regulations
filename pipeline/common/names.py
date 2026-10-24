"""THE NAME KEY (T61 2.8, audit F20): when are two spellings of a water's name the SAME name?

One function, `key`, answers that for the whole pipeline. Before it there were eight, and they
disagreed: "St. Mary's River" keyed four different ways ("st marys river" in the matcher,
"st. mary's river" in the registry passes and the steelhead list, "st mary s river" in the tiles /
gauge match and the DFO locator text, "st. marys river" in the app's search), so the same water was
one name to one stage and two to the next.

`key(text)`:
  1. accents folded: Unicode NFKD, every combining mark deleted ("Barrière" -> "barriere",
     "Tŝilhqox" -> "tsilhqox"; NFKD also folds compatibility forms: "ﬁ" -> "fi");
  2. case folded (`str.casefold`);
  3. APOSTROPHES DELETED, never spaced: ' ’ ‘ ʼ ` ("St. Mary's" -> "st marys", "Joe's" == "Joes");
  4. every other run of characters that are not letters or digits -> ONE space (". - — , * # & ( ) /"
     and whitespace: "Pend-d'Oreille" == "Pend D'Oreille", "WOLF LAKE *" == "Wolf Lake",
     "NO.2" == "NO. 2"); letters outside ASCII that do not decompose stay letters;
  5. trimmed.

What it deliberately does NOT fold (measured on the full registry, 20,632 names, 2026-10-24):
  - "St." / "Saint", "Mt." / "Mount": no registry name spells "Saint" or pairs "Mt"/"Mount", so a fold
    would join nothing today and is a new equivalence a reviewer would have to sign off; "St" stays "st".
  - "Cr." / "Creek", "R." / "River", "L." / "Lake": abbreviations live in the BOOK's and DFO's locator
    TEXT, not in names; `pipeline.regs.dfo_salmon.locations.normalize` expands them for its own
    fingerprints (a different question, below).
  - the kind word ("Lake", "Creek"): a lake and the river draining it are different waters.
  - a parenthetical: KEPT as words. Dropping one is the regulation matcher's choice ("Kwotlenemo
    (Fountain) Lake" finds "Kwotlenemo Lake"), made with `drop_notes` before `key`, never inside it.

Other questions, other functions — kept, and named for what they answer:
  - `compact_key` — `key` with the spaces removed: municipal datasets split and join words
    ("Logger's Lane" == "Loggers Lane" == "loggerslane"); the added-streams passes compare that way.
  - `drop_notes` — the "(...)" spans removed (the matcher's qualifier strip).
  - `pipeline.common.search_key.search_key` — what the APP's search compares (T114). It must be
    byte-identical to the TypeScript `searchKey`, keeps punctuation (". : —") and so is FINER than
    `key`: equal search keys always give equal keys (`test_name_key.py` pins it), not the reverse
    ("St. Mary's" and "St Marys" are one `key` but two search keys — the gap is T114's to close).
  - `pipeline.deliver.tiles.names.display` — the name SHOWN on the map (case repaired), not a key.
  - `pipeline.atlas.splits.area_catalog.area_id` — an area's id slug, stored in curated files; it
    must never change, so it is not a key.
  - `pipeline.regs.dfo_salmon.locations.normalize` / `fingerprint` — a DFO locator's TEXT
    fingerprint (abbreviations expanded); stored hashes depend on it, so it is not a name key.
"""
from __future__ import annotations

import re
import unicodedata

_APOSTROPHES = re.compile("['’‘ʼ`]")
#: Anything that is not a letter or digit (Unicode-aware), and the underscore.
_SEPARATORS = re.compile(r"[\W_]+")
_NOTES = re.compile(r"\([^)]*\)")


def fold(text: str) -> str:
    """Accents folded (NFKD, combining marks deleted); case and punctuation untouched."""
    return "".join(c for c in unicodedata.normalize("NFKD", text or "") if not unicodedata.combining(c))


def key(text: str) -> str:
    """THE name key (module docstring). Two names are the same name exactly when their keys are equal."""
    s = _APOSTROPHES.sub("", fold(text).casefold())
    return _SEPARATORS.sub(" ", s).strip()


def compact_key(text: str) -> str:
    """`key` with no spaces: "Logger's Lane" == "loggers lane" == "loggerslane"."""
    return key(text).replace(" ", "")


def drop_notes(text: str) -> str:
    """The text with every "(...)" span replaced by a space (the matcher's qualifier strip)."""
    return _NOTES.sub(" ", text or "")
