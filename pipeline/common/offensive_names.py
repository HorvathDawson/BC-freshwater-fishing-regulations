"""NO SLUR IS EVER A NAME WE PUT ON THE MAP (owner 2026-10-28).

A short curated deny-list of racial and ethnic slurs that survive in old place names (OpenStreetMap, city layers and
historic gazetteer entries; the Province has been renaming such features). `offensive(name)` returns the matched term
or None. A mined name (`pipeline.tools.mined_names`) carrying one is refused outright, never auto-accepted; a GAZETTED
name is the Province's and is only reported (`python -m pipeline.common.offensive_names`).

Matching is case-insensitive on word boundaries ("Jap" never matches "Japan"; "Coon" never "Raccoon"), plus the
n-word anywhere inside a word ("Niggerhead"). Keep the list short and explicit: every entry is a slur in place-name use.
"""
from __future__ import annotations

import re

#: whole-word slurs (hyphen/space variants spelled out)
WORDS = (
    "negro", "negroes", "nigger", "niggers", "nigga", "nigra", "darkie", "darky", "darkey", "coon", "coons",
    "squaw", "squaws", "siwash", "klootchman", "klootch", "klooch", "kloochman", "halfbreed", "half-breed",
    "half breed", "injun", "injuns", "redskin", "redskins",
    "chinaman", "chinamans", "chinamen", "chink", "chinks", "jap", "japs", "jew", "kike", "dago", "wop", "gook",
    "wetback",
)
#: inside any word (compounds such as "Niggerhead", "Squawfish", "Siwashrock")
INFIXES = ("nigger", "nigga", r"squaw(?!k)", "siwash", "chinaman", "klootch", "halfbreed")   # "Squawk" is a bird call

_WORD_RE = re.compile(r"(?<![a-z])(" + "|".join(re.escape(w).replace(r"\ ", r"[\s-]+") for w in
                                              sorted(WORDS, key=len, reverse=True)) + r")(?![a-z])", re.IGNORECASE)
_INFIX_RE = re.compile("|".join(INFIXES), re.IGNORECASE)          # INFIXES are regex fragments


def offensive(name: str | None) -> str | None:
    """The deny-listed term `name` carries, or None."""
    if not name:
        return None
    m = _WORD_RE.search(name) or _INFIX_RE.search(name)
    return m.group(0) if m else None


def main() -> int:
    """List every GAZETTED FWA stream / lake name carrying a deny-listed term (report only)."""
    import sqlite3
    from project_config import get_config
    con = sqlite3.connect(f"file:{get_config().fwa_data_gpkg}?mode=ro", uri=True)
    rows = set()
    for sql in ("select distinct GNIS_NAME from streams",
                "select distinct GNIS_NAME_1 from lakes", "select distinct GNIS_NAME_2 from lakes",
                "select distinct GNIS_NAME_3 from lakes", "select distinct GNIS_NAME_1 from manmade",
                "select distinct GNIS_NAME_1 from wetlands"):
        try:
            rows |= {(sql.split()[-1], r[0]) for r in con.execute(sql) if r[0] and offensive(r[0])}
        except sqlite3.Error:
            pass
    for layer, nm in sorted(rows):
        print(f"{layer}\t{nm}\t{offensive(nm)}")
    print(f"{len(rows)} gazetted name(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
