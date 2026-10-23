"""THE SEARCH KEY (T114): what the app's search compares a query with. `item.name_key` and `alias.alias_key` in the
bundle are this, of `item.name` and `alias.alias`.

The same rule as the app's `searchKey` (`app/packages/core/src/searchKey.ts`): Unicode NFD, the combining marks
U+0300-U+036F deleted, lower case, apostrophes (' and U+2019) deleted, whitespace runs collapsed to one space, trimmed.
"Barrière Lake" -> "barriere lake"; "Children's Fishing Pond" -> "childrens fishing pond". The app keys the query with
its copy and filters the stored keys with LIKE, so the two copies MUST agree: `search_key_cases.json` beside this file
is the one table both are tested against (`pipeline/tests/test_search_key.py`, `app/tools/search-key-parity.test.ts`).

The character classes are spelled as JavaScript's, not Python's: Python's `\\s` / `str.strip()` leave U+FEFF alone and
take U+001C-U+001F and U+0085, and `unicodedata.combining` is wider than the one block JS deletes.

This is NOT `pipeline.deliver.tiles.names.normalise` (that one turns every punctuation mark into a space, so
"Children's" -> "children s"); T61 2.8 folds the pipeline's name normalisers into one and reuses this rule for search.
"""
from __future__ import annotations

import re
import unicodedata

#: JavaScript's `\s` (WhiteSpace + LineTerminator) — also exactly what `String.prototype.trim` removes.
_WS = re.compile("[\t\n\v\f\r    -     　﻿]+")
_MARKS = re.compile("[̀-ͯ]")
_APOSTROPHES = re.compile("['’]")


def search_key(text: str) -> str:
    """The search key of a name or alias (module docstring; `search_key_cases.json` pins it)."""
    folded = _MARKS.sub("", unicodedata.normalize("NFD", text or ""))
    # every JS whitespace run is one " " after the sub, so stripping " " is JS's trim()
    return _WS.sub(" ", _APOSTROPHES.sub("", folded.lower())).strip(" ")
