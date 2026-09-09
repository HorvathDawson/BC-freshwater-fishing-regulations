"""BC fish species table — the code set a parsed `Rule.species` binds to.

Source of truth is `bc_species.csv` (the official BC species/code list, verbatim: SPECIES_TYPE,
COMMON_NAME, SCIENTIFIC_NAME, SPECIES_CODE), loaded at import. A rule with no species applies to ALL
species; a rule naming species carries the matching official code(s). `entry_models.Rule` validates
every code against `KNOWN_SPECIES_CODES`, so a code the parser can't justify surfaces as an error
rather than being stored silently wrong.

Groups ("General" rows, e.g. SLV "Char, General" = genus *Salvelinus*) are expanded to their member
species by GENUS derived from the authoritative scientific names — accurate and transparent, not
hand-guessed — with a few explicit `_GROUP_OVERRIDES` for cases where the genus is broader than the
regulatory word (the genus *Oncorhynchus* covers salmon AND rainbow/cutthroat trout, so "salmon"
can't be pure genus). Collective regulatory words with NO single official code (notably generic
"trout") are handled by `_COLLECTIVE_TERMS`, which maps to a curated list of official codes.

⚠️ EXPERT REVIEW: the group memberships and collective-term sets below are a best effort from the
scientific names and common regs usage; have a fisheries reviewer confirm them before this drives any
user-facing species filtering. The per-species codes themselves are authoritative (straight from the CSV).
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

_CSV = Path(__file__).resolve().parent / "bc_species.csv"


@dataclass(frozen=True)
class SpeciesRow:
    code: str
    common_name: str
    scientific: str
    species_type: str          # Species | Sub-Species | Hybrid | Extinct | General

    @property
    def genus(self) -> str:
        """First token of the scientific name (lowercased), '' if none. 'Salvelinus malma' -> 'salvelinus'."""
        sci = (self.scientific or "").strip()
        if not sci:
            return ""
        return sci.split()[0].strip(".,?").lower()


def _load() -> dict[str, SpeciesRow]:
    rows: dict[str, SpeciesRow] = {}
    with _CSV.open(encoding="utf-8-sig", newline="") as fh:
        for r in csv.DictReader(fh):
            code = (r.get("SPECIES_CODE") or "").strip()
            if not code:
                continue
            rows[code] = SpeciesRow(
                code=code,
                common_name=(r.get("COMMON_NAME") or "").strip(),
                scientific=(r.get("SCIENTIFIC_NAME") or "").strip(),
                species_type=(r.get("SPECIES_TYPE") or "").strip(),
            )
    return rows


SPECIES: dict[str, SpeciesRow] = _load()
#: SYNTHETIC groups — a regulatory word with no row in the official table.
#:
#: The CSV has 27 "General" rows and none of them is trout, because trout is not a taxon: the
#: six the regulations mean span three genera (*Oncorhynchus* rainbow and cutthroat, *Salmo*
#: brown, and Golden). `TR` cannot be borrowed — the table already uses it for "Unidentifiable
#: Trout - only fry <70mm in length", which is a different and much narrower claim.
#:
#: So `TRT` is ours, and it is marked as ours. Everything above this line comes from the
#: authoritative CSV; everything here is a curated regulatory grouping, and the distinction
#: has to survive because one is a fact and the other is a judgement.
#:
#: WHY STORE THE GROUP AT ALL rather than the six codes. A rule that says "Trout: 4" is one
#: claim about trout, and writing it as six codes states six claims that happen to coincide —
#: so a later correction has to find and fix all six, a reader cannot see which word the
#: synopsis used, and "trout and char" becomes seven codes that no longer resemble the
#: sentence they came from. `expand_group` turns it back into species wherever that is what a
#: caller needs.
_SYNTHETIC_GROUPS: dict[str, tuple[str, set[str]]] = {
    "TRT": ("Trout (General)", {"RB", "CT", "WCT", "CCT", "GB", "GT"}),
}


#: Every code a `Rule.species` may carry — the CSV's, plus the synthetic groups above.
KNOWN_SPECIES_CODES: frozenset[str] = frozenset(SPECIES) | frozenset(_SYNTHETIC_GROUPS)

# code -> common name (convenience for display / prompt building)
COMMON_NAME: dict[str, str] = {c: r.common_name for c, r in SPECIES.items()}
COMMON_NAME.update({c: n for c, (n, _m) in _SYNTHETIC_GROUPS.items()
                    if c not in COMMON_NAME})


# ---------------------------------------------------------------------------
# Group expansion (General rows -> member species codes)
# ---------------------------------------------------------------------------

# Only real fish species/sub-species can be group members (skip admin codes like NFC/ZZZ/SP and the
# General rows themselves).
_MEMBER_TYPES = {"Species", "Sub-Species", "Hybrid"}

# Where a group's genus is broader (or narrower) than the regulatory word, pin membership explicitly.
# All codes are from the CSV. EXPERT REVIEW these.
_GROUP_OVERRIDES: dict[str, set[str]] = {
    "AO": {"CH", "CM", "CO", "PK", "SK"},   # All Salmon: the 5 Pacific salmon (Oncorhynchus genus also = RB/CT)
    "SA": {"CH", "CM", "CO", "PK", "SK"},   # Salmon (General): same — kokanee (KO) tracked separately
    "P":  {"YP", "WP"},                     # Perch (General): Yellow Perch + Walleye (scientific "Sander (formerly Stizostedion)")
}


def _genera_of_group(row: SpeciesRow) -> set[str]:
    """Genus tokens a General row names, e.g. 'Prosopium sp; Coregonus sp; Stenodus sp' -> {prosopium, coregonus, stenodus}."""
    out: set[str] = set()
    for part in (row.scientific or "").split(";"):
        part = part.strip()
        if part:
            out.add(part.split()[0].strip(".,?").lower())
    return out


def _build_groups() -> dict[str, frozenset[str]]:
    members_by_genus: dict[str, set[str]] = {}
    for r in SPECIES.values():
        if r.species_type in _MEMBER_TYPES and r.genus:
            members_by_genus.setdefault(r.genus, set()).add(r.code)
    groups: dict[str, frozenset[str]] = {}
    for r in SPECIES.values():
        if r.species_type != "General":
            continue
        if r.code in _GROUP_OVERRIDES:
            groups[r.code] = frozenset(_GROUP_OVERRIDES[r.code])
            continue
        members: set[str] = set()
        for g in _genera_of_group(r):
            members |= members_by_genus.get(g, set())
        if members:
            groups[r.code] = frozenset(members)
    return groups




def _with_synthetic(groups: dict[str, frozenset[str]]) -> dict[str, frozenset[str]]:
    for code, (_name, members) in _SYNTHETIC_GROUPS.items():
        if code in SPECIES:                      # the CSV grew one; defer to it
            continue
        groups[code] = frozenset(members)
    return groups


# group code -> member species codes (only groups with derivable membership appear)
GROUPS: dict[str, frozenset[str]] = _with_synthetic(_build_groups())


def expand_group(code: str) -> frozenset[str]:
    """Members of a group code, or just {code} for a plain species. Use at resolve/filter time to turn
    a stored group code (e.g. SLV 'char') into the concrete species it covers."""
    return GROUPS.get(code, frozenset({code}) if code in KNOWN_SPECIES_CODES else frozenset())


# ---------------------------------------------------------------------------
# Name resolution (parser aid) — surface phrasing -> official code(s)
# ---------------------------------------------------------------------------

def _norm(text: str) -> str:
    return " ".join((text or "").split()).lower()


def _strip_paren(name: str) -> str:
    """'Black Catfish (formerly Black Bullhead)' -> 'black catfish'."""
    return _norm(name.split("(")[0])


# common-name index (auto), incl. a parenthetical-stripped form
_NAME_INDEX: dict[str, str] = {}
for _c, _r in SPECIES.items():
    if _r.common_name:
        _NAME_INDEX.setdefault(_norm(_r.common_name), _c)
        _NAME_INDEX.setdefault(_strip_paren(_r.common_name), _c)

# supplemental regs shorthand (lowercased) -> code. Common-name matches already cover the full names;
# these are the abbreviations/nicknames the synopsis uses. "char" -> the SLV group.
_ALIASES: dict[str, str] = {
    "rainbow": "RB", "rainbows": "RB", "rb": "RB",
    "cutthroat": "CT", "cutty": "CT",
    "westslope cutthroat": "WCT", "coastal cutthroat": "CCT",
    "bull trout": "BT", "bull": "BT", "bulls": "BT",
    "dolly": "DV", "dollies": "DV", "dolly varden": "DV",
    "brookie": "EB", "brookies": "EB", "brook char": "EB",
    "browns": "GB", "brown": "GB",
    "lakers": "LT", "laker": "LT",
    "kokanee": "KO", "kokes": "KO",
    "char": "SLV", "chars": "SLV",
    "grayling": "GR",
    "whitefish": "WF",
    "salmon": "SA",
    "sturgeon": "SG",
    "bass": "BS",
    "sunfish": "BS",
    "walleye": "WP",
    "pike": "NP",
    "perch": "P",
    "burbot": "BB", "ling": "BB", "ling cod": "BB",
    "steelhead": "ST",
}

# collective regs words that map to MULTIPLE official codes with no single official code of their own.
# Generic "trout" is the big one — BC has no "Trout (General)" code, so we list the resident trouts
# (char is its own group, SLV, so excluded here). EXPERT REVIEW.
#: A regulatory phrase -> the code(s) to store. Prefer a GROUP code over a list of species:
#: it keeps the stored rule the same shape as the sentence it came from.
#:
#: "trout and char" is here because it is one of the commonest lines in the synopsis
#: ("Trout and Char: 5") and every reader of this table had to compose it by hand from the
#: two halves — three independent parses did exactly that, which is three chances to differ.
_COLLECTIVE_TERMS: dict[str, list[str]] = {
    "trout": ["TRT"],
    "trouts": ["TRT"],
    "trout and char": ["TRT", "SLV"],
    "trout char": ["TRT", "SLV"],
    "trout/char": ["TRT", "SLV"],
    "char and trout": ["TRT", "SLV"],
}


def normalize_species(text: str) -> "str | None":
    """Map one surface species phrase to a single official code, or None if unrecognized. Tries the
    full common name, a parenthetical-stripped name, the shorthand aliases, then a naive singular.
    Returns None rather than guessing — the caller decides (leave species-less = all, or flag)."""
    if not text:
        return None
    key = _norm(text)
    for table in (_NAME_INDEX, _ALIASES):
        if key in table:
            return table[key]
    up = text.strip().upper()
    if up in KNOWN_SPECIES_CODES:                 # already a code
        return up
    if key.endswith("s"):                         # naive singular
        sing = key[:-1]
        for table in (_NAME_INDEX, _ALIASES):
            if sing in table:
                return table[sing]
    return None


def resolve_species_phrase(text: str) -> list[str]:
    """THE parser helper: a species phrase -> the official code(s) to store on Rule.species. A single
    species/group -> [code]; a collective word like 'trout' -> several codes; unrecognized -> []
    (parser then leaves the rule species-less and notes the phrase). Deduped, order-stable."""
    if not text:
        return []
    key = _norm(text)
    if key in _COLLECTIVE_TERMS:
        return list(_COLLECTIVE_TERMS[key])
    if key.endswith("s") and key[:-1] in _COLLECTIVE_TERMS:
        return list(_COLLECTIVE_TERMS[key[:-1]])
    code = normalize_species(text)
    return [code] if code else []


# ---------------------------------------------------------------------------
# Prompt helper — the compact, regs-relevant menu the parser chooses from
# ---------------------------------------------------------------------------

# The freshwater game species + groups a fishing-regs parser actually needs. KNOWN_SPECIES_CODES still
# accepts any code; this just keeps the prompt tight and steers the parser to the right ones.
GAME_CODES: tuple[str, ...] = (
    "RB", "ST", "CT", "WCT", "CCT", "GB", "BT", "DV", "EB", "LT", "GT",   # trout & char
    "SLV",                                                                 # char (group)
    "KO", "CH", "CO", "SK", "PK", "CM", "SA",                             # salmon & kokanee
    "MW", "LW", "PW", "RW", "WF",                                         # whitefish
    "GR", "BB", "WP", "NP", "YP", "P",                                    # grayling, burbot, walleye, pike, perch
    "SMB", "LMB", "BS",                                                   # bass/sunfish
    "WSG", "SG",                                                          # sturgeon
)


def prompt_menu() -> str:
    """A compact 'CODE\\tCommon Name[ (group: n spp)]' listing of the game species/groups for the
    parser prompt. Only the regs-relevant subset; the model leaves species empty for 'all'."""
    lines: list[str] = []
    for c in GAME_CODES:
        r = SPECIES.get(c)
        if not r:
            continue
        tag = f"  (group -> {len(GROUPS[c])} spp)" if c in GROUPS else ""
        lines.append(f"{c}\t{r.common_name}{tag}")
    return "\n".join(lines)
