"""The species vocabulary (p.80's closed list, groups, salmon, protected fish), rule families,
and the trout/char, steelhead and source-artefact scope checks.

Split out of `pipeline/regs/parsing/catalogue.py`, which re-exports every name; import from
there."""

from __future__ import annotations

import re

from typing import List, Optional

from .vocab import RuleType
from .text import squash
from .dates import _days


#: THE BOOK'S SPECIES — AND NOTHING ELSE (user ruling 2026-09-26). Page 80 ("Freshwater game fish
#: are defined as follows") prints the list, under four headings and an "OTHER":
#:
#:   TROUT      Rainbow Trout, Steelhead, Cutthroat Trout, Brown Trout
#:   CHAR       Dolly Varden, Bull Trout*, Lake Trout, Brook Trout
#:   WHITEFISH  Lake Whitefish, Mountain Whitefish
#:   BASS       Largemouth Bass, Smallmouth Bass
#:   OTHER      Kokanee, Arctic Grayling, Burbot (Ling), White Sturgeon, Black Crappie,
#:              Northern Pike, Yellow Perch, Walleye, Goldeye, Inconnu, Crayfish
#:
#:   "*Any bull trout that you catch and keep must be counted as part of your Dolly Varden quota."
#:
#: So in the regulations a bull trout IS a Dolly Varden: ONE fish, `DV`. `BT` was a second code for
#: it, and the ladder treated "Bull trout daily quota = 1" and Region 4's "1 bull trout (Dolly
#: Varden)" as statements about two fish. It is refused (`REFUSED_SPECIES`). The official table's
#: sub-species (westslope and coastal cutthroat), fish the book never lists (golden trout, arctic
#: char, splake, pygmy and round whitefish, bluegill, pumpkinseed, carp) and the CSV's "General"
#: rows are gone: no rule named them, and a code the book does not print is a code no reader can
#: check.
BOOK_FAMILIES: dict[str, tuple[str, ...]] = {
    "TROUT":     ("RB", "ST", "CT", "GB"),
    "CHAR":      ("DV", "LT", "EB"),
    "WHITEFISH": ("LW", "MW"),
    "BASS":      ("LMB", "SMB"),
    "OTHER":     ("KO", "GR", "BB", "WSG", "BCB", "NP", "YP", "WP", "GE", "IN", "CRA"),
}
#: Every fish a rule may name, in the book's order.
BOOK_SPECIES: tuple[str, ...] = tuple(c for fs in BOOK_FAMILIES.values() for c in fs)
#: THE GAME FISH (p.80's closed list minus crayfish, which are trapped, not angled; a fin-fish
#: closure leaves them): every fish a "no fishing" must hold for before a water is called closed,
#: and every fish the delivery's verdicts ask about on every rule key. Defined ONCE here (it was
#: spelled five ways: DATAFLOW M1).
GAME_FISH: tuple[str, ...] = tuple(c for c in BOOK_SPECIES if c != "CRA")

#: THE SCIENTIFIC NAME OF EVERY FISH THE BOOK LISTS (and chinook, the one salmon it names), as
#: the official B.C. species table prints it — for display only; no rule reads it. This is all
#: that survived of `pipeline/regs/parsing/species.py`, the retired code set (BT, SLV, TRT, WCT …)
#: whose codes the model refuses.
SCIENTIFIC_NAMES: dict[str, str] = {
    "RB": "Oncorhynchus mykiss", "ST": "Oncorhynchus mykiss", "CT": "Oncorhynchus clarki",
    "GB": "Salmo trutta",
    "DV": "Salvelinus malma", "LT": "Salvelinus namaycush", "EB": "Salvelinus fontinalis",
    "LW": "Coregonus clupeaformis", "MW": "Prosopium williamsoni",
    "LMB": "Micropterus salmoides", "SMB": "Micropterus dolomieui",
    "KO": "Oncorhynchus nerka", "GR": "Thymallus arcticus", "BB": "Lota lota",
    "WSG": "Acipenser transmontanus", "BCB": "Pomoxis nigromaculatus", "NP": "Esox lucius",
    "YP": "Perca flavescens", "WP": "Sander vitreus (formerly Stizostedion vitreum 10/05)",
    "GE": "Hiodon alosoides", "IN": "Stenodus leucichthys", "CRA": "Pacifastacus leniusculus",
    "CH": "Oncorhynchus tshawytscha",
}

#: THE GROUPS A RULE MAY NAME, and what each one covers. Stored as the word the book printed:
#: "Trout/char: 5" is ONE claim about trout and char, and nine codes would be nine claims that
#: merely coincide. `expand_species` turns a group back into members where a caller needs the set.
#:
#: "TROUT" IS NOT ONE OF THEM. Page 80: "trout/char: all regulations that apply to trout (as a
#: group) also apply to char unless char are specifically excluded." So the printed word "trout"
#: is `TROUT_CHAR` — and where its row or zone table MENTIONS CHAR APART (user ruling 2026-09-28,
#: `mentions_char_apart`), that is the exclusion: the row's "trout" lines are written `TROUT_CHAR`
#: with `species_except: [CHAR]` (`trout_scope_problems`). ONE representation for "trout", scoped
#: by its row; a TROUT-only code would be a second spelling of the same fish set. It is refused
#: (`REFUSED_SPECIES`); the book's TROUT heading lives on as a family (`BOOK_FAMILIES`), for
#: display.
SPECIES_GROUPS: dict[str, tuple[str, ...]] = {
    "TROUT_CHAR": BOOK_FAMILIES["TROUT"] + BOOK_FAMILIES["CHAR"],
    #: "char catch and release", Region 1's "you must release: All char (includes Dolly Varden)".
    #: The one group that NAMES its members (`NAMING_GROUPS`).
    "CHAR":       BOOK_FAMILIES["CHAR"],
    "WHITEFISH":  BOOK_FAMILIES["WHITEFISH"],
    "BASS":       BOOK_FAMILIES["BASS"],
}
#: The closed list — "Freshwater game fish are defined as follows". A rule that applies to
#: "everything" applies to THIS set, never the empty set.
SPECIES_GROUPS["ALL_GAME_FISH"] = BOOK_SPECIES

#: A GROUP THAT NAMES ITS FISH. Once "trout" swallows char (p.80), "char" is how the book names
#: char APART from trout: Region 1's "Trout: 4 … And you must release: All char (includes Dolly
#: Varden)" names the char it releases, and a lake's "Trout daily quota = 2" — a trout/char quota
#: by p.80 — must not reopen them. Read as a group, the zone's char release lost to the water's
#: group quota by place, and 56 Region 1 lakes would have let a char be kept.
NAMING_GROUPS = frozenset({"CHAR"})

#: THE SCOPE OF THE WORD "TROUT" (user ruling 2026-09-28): "trout" includes char UNLESS CHAR ARE
#: MENTIONED. p.80 says trout rules apply to char "unless char are specifically excluded", and the
#: book excludes them by naming char APART in the same row, or in the same zone table: Region 6's
#: box (p.49) prints "Trout/char: 5, but not more than … 3 Dolly Varden/bull trout and/or lake
#: trout combined, 1 trout from streams July 1-Oct 31. And you must release: … Trout under 30 cm
#: from any stream, Trout of any size from streams, Nov 1-June 30" — so "1 trout from streams",
#: "Trout under 30 cm" and "Trout of any size from streams" are about trout alone; Region 1's
#: "Trout: 4 … And you must release: … All char (includes Dolly Varden)" (p.13) likewise. A lake
#: row printing only "Trout daily quota = 2" mentions no char, and its 2 counts char too.
#:
#: A char is mentioned apart when the text names one ON ITS OWN: "char", Dolly Varden, bull trout,
#: lake trout, brook trout. The group word "trout/char" ("trout and char") is not such a mention —
#: it names char IN: Dodd Lake's "Wild trout/char daily quota = 2 (no wild trout over 40 cm)"
#: names no char apart, and its "no wild trout over 40 cm" holds for char too (user confirmation
#: 2026-09-28, "none over 40 cm like trout"). "Rainbow trout and char" names char apart (the trout
#: there is a rainbow).
_CHAR_NAMED = re.compile(r"\bchar\b|\bdolly\s+varden\b|\bbull\s+trout\b|\blake\s+trout\b"
                         r"|\bbrook\s+trout\b", re.I)
#: The group word, printed three ways.
_TROUT_GROUP = re.compile(r"\btrout\s*(?:/|\band\b|&|\bor\b)\s*char\b", re.I)
_TROUT_WORD = re.compile(r"\btrout\b", re.I)
#: A fish's own name before "trout" ("rainbow trout", "lake trout") — then "trout" is not the group.
_TROUT_KIND = re.compile(r"(?:rainbow|cutthroat|brown|lake|brook|bull|golden)[\s-]*$", re.I)


def _is_group_trout(text: str, at: int) -> bool:
    return not _TROUT_KIND.search(text[:at])


def mentions_char_apart(text: str) -> bool:
    """Does this row (or zone table) NAME A CHAR ON ITS OWN — "char", Dolly Varden, bull trout,
    lake trout, brook trout — outside the group word "trout/char"? Then its "trout" lines exclude
    char (see the note above `_CHAR_NAMED`)."""
    masked = text or ""
    for m in reversed(list(_TROUT_GROUP.finditer(masked))):
        if _is_group_trout(masked, m.start()):
            masked = masked[:m.start()] + " " * (m.end() - m.start()) + masked[m.end():]
    return bool(_CHAR_NAMED.search(masked))


def trout_word(text: str) -> Optional[str]:
    """The trout word a line prints: "trout/char" (the group word, char named in), "trout" (the
    bare word, whose scope its row decides), or None (no group trout word — "1 over 50 cm", or
    only a fish's own name, "rainbow trout")."""
    text = text or ""
    if any(_is_group_trout(text, m.start()) for m in _TROUT_GROUP.finditer(text)):
        return "trout/char"
    if any(_is_group_trout(text, m.start()) for m in _TROUT_WORD.finditer(text)):
        return "trout"
    return None


#: (a) A LINE THAT EXCLUDES CHAR IN SO MANY WORDS — "trout (not char)", "trout other than char",
#: "excluding char". The book prints none today; a parse that writes one is honoured.
_EXCLUDES_CHAR = re.compile(r"\b(?:not|other than|excluding|except)\s+(?:the\s+)?char\b", re.I)


def rule_aspects(r) -> set:
    """WHAT ASPECT OF A FISH A RULE GOVERNS (user refinement 2026-10-07 of TROUT/CHAR CLARIFIED (b)):
    ("retention", period) — how many you keep, a release (take 0) or a closure: one aspect, keep
    and release being two answers to it; ("size",) — the lengths it keeps or returns; ("gear",
    type) — a method, tackle or bait rule. A rule may govern several ("1 bull trout over 60 cm":
    retention and size)."""
    t = getattr(r.type, "value", r.type)
    out = set()
    if t == "retention_limit":
        if r.exempts and r.take is None and not r.unlimited and not r.lengths:
            out.add(("retention", r.period or "daily"))      # a lift opens the keeping of the fish
        if r.take is not None or r.unlimited:
            if not (r.lengths and r.take is None):
                out.add(("retention", r.period or "daily"))
        if r.lengths:
            out.add(("size",))
            if any(b.take is not None and b.take > 0 for b in r.lengths):
                out.add(("retention", r.period or "daily"))
    elif t in ("tackle_restriction", "bait_restriction", "method_rule"):
        out.add(("gear", t))
    return out


def _dates_meet(a, b) -> bool:
    da = set(_days(a.when.dates)) if a.when is not None and a.when.dates else None
    db = set(_days(b.when.dates)) if b.when is not None and b.when.dates else None
    return da is None or db is None or bool(da & db)


def related_rules(t, c) -> bool:
    """Do two rules govern the SAME ASPECT of a fish (`rule_aspects`), for the same kind of water
    and on days that meet? "no trout under 30 cm" (size) and "bull trout release Aug 1-Oct 31"
    (retention) do not; Region 1's "2 from streams (must be hatchery)" and its "All char" release
    (retention) do; "No wild trout over 50 cm" and "1 bull trout over 60 cm" (size) do."""
    if not (rule_aspects(t) & rule_aspects(c)):
        return False
    wa, wb = getattr(t.water, "value", t.water), getattr(c.water, "value", c.water)
    if wa and wb and wa != wb:
        return False
    return _dates_meet(t, c)


def char_rules_apart(rules) -> list:
    """(b) The rules of this row (or zone table) that SPECIFY CHAR SEPARATELY — whose `species`
    names char as a group (`CHAR`) or a char (Dolly Varden/bull trout, lake trout, brook trout),
    not the group word "trout/char" (user ruling 2026-10-07, TROUT/CHAR CLARIFIED)."""
    chars = set(BOOK_FAMILIES["CHAR"]) | {"CHAR"}
    return [r for r in rules if set(r.species) & chars and "TROUT_CHAR" not in r.species]


def trout_scope_problems(entry_id: str, regs_verbatim: str, rules) -> List[str]:
    """EVERY "TROUT" LINE IS SCOPED BY ITS ROW (user ruling 2026-10-07, TROUT/CHAR CLARIFIED; p.80
    "all regulations that apply to trout (as a group) also apply to char unless char are
    specifically excluded"). A `TROUT_CHAR` rule printing the bare word "trout" (itself, or the
    quota it is a clause `within`: Region 1's "1 over 50 cm" under "Trout: 4") carries
    `species_except: [CHAR]` exactly when
      (a) its own text excludes char explicitly (`_EXCLUDES_CHAR`), or
      (b) its row or zone table SPECIFIES CHAR SEPARATELY WITH ITS OWN RULE that is RELATED to
          this line — governs the same aspect, kind of water and days (`related_rules`): Region 1's
          "you must release: All char" beside "2 from streams (must be hatchery)", Chilliwack
          Lake's "1 bull trout over 60 cm" beside "No wild trout over 50 cm" — specifying char
          separately IS excluding it. An UNRELATED char rule excludes nothing: "no trout under 30
          cm" still covers a bull trout beside "bull trout release Aug 1-Oct 31".
    A combined "trout/char" mention names char in and never excludes them; a mention of char that is
    no rule of its own does not exclude them either ("Wild trout/char quota = 2 (no wild trout over
    40 cm)": the 40 cm cap covers char). A bare "trout" line never excludes one char alone (the
    book's exclusion is of char as a group, `CHAR`)."""
    apart = char_rules_apart(rules)
    by_id = {r.rule_id: r for r in rules}
    out: List[str] = []
    for r in rules:
        if "TROUT_CHAR" not in r.species:
            continue
        word, p, seen = trout_word(r.verbatim), r, {r.rule_id}
        while word is None and p.within and p.within in by_id and p.within not in seen:
            p = by_id[p.within]
            seen.add(p.rule_id)
            word = trout_word(p.verbatim)
        out_char = "CHAR" in r.species_except
        if word == "trout/char" and out_char:
            out.append(f"{r.rule_id}: prints 'trout/char', which names char in — it never "
                       f"carries species_except CHAR")
        if word != "trout":
            continue
        lone = sorted(c for c in r.species_except if c in BOOK_FAMILIES["CHAR"])
        if lone:
            out.append(f"{r.rule_id}: species_except {lone} — a 'trout' line excludes char as a "
                       f"group (CHAR), never one char")
        excluded = any(related_rules(r, c) for c in apart) \
            or bool(_EXCLUDES_CHAR.search(r.verbatim or ""))
        if excluded and not out_char:
            out.append(f"{r.rule_id}: prints 'trout' and its row specifies char separately (or the "
                       f"line excludes char) — its trout exclude char (p.80; user ruling 2026-10-07): "
                       f"species_except [CHAR]")
        elif not excluded and out_char:
            out.append(f"{r.rule_id}: prints 'trout', its row has no char rule of its own and the "
                       f"line excludes no char — trout includes char (p.80): drop CHAR from "
                       f"species_except")
    return out

_PRINTS_STEELHEAD = re.compile(r"\bsteelhead\b", re.I)


def _size_key(r) -> tuple:
    return tuple(sorted((b.min_cm, b.max_cm) for b in (r.lengths or [])))


def steelhead_scope_problems(entry_id: str, rules) -> List[str]:
    """STEELHEAD LEAVE A ZONE'S TROUT/CHAR SIZE LINE ONLY WHERE THE SAME TABLE PRINTS ITS OWN
    STEELHEAD QUOTA FOR THAT SIZE (user ruling 2026-10-08, answers 2.2 item D). Region 2's "1 over
    50 cm" stands beside its own "2 hatchery steelhead over 50 cm allowed" (p.21): steelhead are
    counted there, not under the 1 — `species_except: [ST]`. Region 6 prints no steelhead quota of
    its own ("1 over 50 cm (quota includes hatchery steelhead)", p.49): a hatchery steelhead IS one
    of the 1 — no exclusion. A line that prints the word steelhead names them in and never excludes
    them. Only a zone table's (`z…`) keeping size lines over the trout group are checked."""
    if not entry_id.startswith("z"):
        return []
    own_st = [q for q in rules if list(q.species) == ["ST"] and q.lengths
              and any((b.take or 0) > 0 for b in q.lengths) or (list(q.species) == ["ST"]
              and q.lengths and (q.take or 0) > 0)]
    out: List[str] = []
    for r in rules:
        if "TROUT_CHAR" not in r.species or not r.lengths or not ((r.take or 0) > 0):
            continue
        out_st = "ST" in r.species_except
        if _PRINTS_STEELHEAD.search(r.verbatim or ""):
            if out_st:
                out.append(f"{r.rule_id}: prints 'steelhead', which names them in — never "
                           f"species_except ST")
            continue
        sib = [q for q in own_st if q.rule_id != r.rule_id and q.within != r.rule_id
               and r.within != q.rule_id and _size_key(q) == _size_key(r)]
        if sib and not out_st:
            out.append(f"{r.rule_id}: its table prints its own steelhead quota for this size "
                       f"({sib[0].rule_id}) — steelhead are counted there: species_except [ST]")
        elif not sib and out_st:
            out.append(f"{r.rule_id}: its table prints no steelhead quota of its own for this size "
                       f"— steelhead count under this line: drop ST from species_except")
    return out


#: SUBJECTS THE BOOK NAMES THAT ARE NOT GAME FISH, so no fish code lies under them. Each is a word
#: the book prints in a rule, and each is OPEN — it has no member list, on purpose:
#:
#:   ALL_FIN_FISH       "any fish willfully or accidentally snagged must be released" and "release
#:                      all fin fish caught in your trap" (p.80 defines fish as "fin fish, shellfish
#:                      and crustaceans"): every fish, game or not — never crayfish, which the book
#:                      names beside fin fish ("fin fish AND crayfish").
#:   PROTECTED_SPECIES  "It is illegal to fish for … any of the fish listed below" (p.9): eleven
#:                      sculpins, sticklebacks, dace, suckers and lampreys and four white sturgeon
#:                      populations, and Region 2 adds green sturgeon (p.21). None is a game fish;
#:                      each is a NAMED MEMBER of this group (`PROTECTED_FISH`), as chinook is of
#:                      SALMON, and a protected-species rule names the members its row prints.
#:   SALMON             Pacific salmon: federal, not on the provincial list, NOT in
#:                      ALL_GAME_FISH. The book names them (the salmon stamp, "no spear fishing of
#:                      Pacific salmon"); kokanee, a land-locked sockeye, is the game fish `KO`.
#:
#: Asked about one fish (a leaf of `BOOK_SPECIES`), `ALL_FIN_FISH` speaks for every one but
#: crayfish; `PROTECTED_SPECIES` and `SALMON` speak for none (`read.speaks_for`).
OPEN_SUBJECTS: tuple[str, ...] = ("ALL_FIN_FISH", "PROTECTED_SPECIES", "SALMON")
for _s in OPEN_SUBJECTS:
    SPECIES_GROUPS[_s] = ()

#: A FISH THE BOOK NAMES INSIDE THE SALMON GROUP, NOT A GAME FISH (user ruling 2026-09-28).
#: "You must immediately record your retention of adult chinook salmon on your basic angling
#: licence" (p.7) names CHINOOK. Chinook is not on p.80's game-fish list and is in no game-fish
#: group (not `ALL_GAME_FISH`); it is a member of the SALMON group, and SALMON stays an OPEN
#: group — "no spear fishing of Pacific salmon" is every salmon, not the chinook alone, so the
#: group is never expanded to this list. Salmon regulations proper come later, with the DFO salmon
#: implementation (`pipeline.regs.dfo_salmon`, `FEDERAL_SALMON`).
SALMON_FISH: dict[str, str] = {"CH": "SALMON"}

#: THE PROTECTED FISH, BY NAME (UI consumer's report, 2026-10-03). "It is illegal to fish for, or
#: catch and retain any of the fish listed below" (p.9) — and the rule named only the group, which
#: has no members, so no reader could see WHICH fish are protected. They are not game fish (p.80's
#: list is closed, AGENTS 45), so they cannot join `BOOK_SPECIES`; like chinook in SALMON they are
#: NAMED MEMBERS of the open group `PROTECTED_SPECIES`, which still speaks for no game fish
#: (`read.speaks_for`). Each protected-species rule names the members its OWN row prints
#: (`CatalogueEntry._protected_fish_printed`): the province's twelve (p.9), Region 2's four (p.21:
#: Nooksack dace, Salish sucker, green sturgeon, Cultus Lake sculpin — green sturgeon is on no
#: other list).
#:
#: The white sturgeon member is the four protected POPULATIONS, not the game fish `WSG`: the Lower
#: Fraser's white sturgeon is a catch-and-release game fish (p.7), and naming `WSG` here would close
#: it province-wide. The value is (the name a reader sees, the words the book prints it with).
PROTECTED_FISH: dict[str, tuple[str, str]] = {
    "CULTUS_LAKE_SCULPIN": ("Cultus Lake sculpin", r"cultus lake sculpin"),
    "ENOS_LAKE_STICKLEBACK": ("Enos Lake stickleback", r"enos lake stickleback"),
    "MISTY_LAKE_STICKLEBACK": ("Misty Lake stickleback", r"misty lake stickleback"),
    "NOOKSACK_DACE": ("Nooksack dace", r"nooksack dace"),
    "PAXTON_LAKE_STICKLEBACK": ("Paxton Lake stickleback", r"paxton lake stickleback"),
    "ROCKY_MOUNTAIN_SCULPIN": ("Rocky Mountain sculpin", r"rocky mountain sculpin"),
    "SHORTHEAD_SCULPIN": ("Shorthead sculpin", r"shorthead sculpin"),
    "SALISH_SUCKER": ("Salish sucker", r"salish sucker"),
    "VANANDA_CREEK_STICKLEBACK": ("Vananda Creek stickleback", r"vananda creek stickleback"),
    "VANCOUVER_LAMPREY": ("Vancouver lamprey", r"vancouver lamprey"),
    "WESTERN_BROOK_LAMPREY_MORRISON_CREEK": (
        "Western brook lamprey (Morrison Creek population)",
        r"western brook lamprey \(morrison creek population\)"),
    "WHITE_STURGEON_PROTECTED_POPULATIONS": (
        "White sturgeon (Nechako, Upper Fraser, Kootenay and Columbia populations)",
        r"white sturgeon \(nechako, upper fraser, kootenay and columbia populations\)"),
    "GREEN_STURGEON": ("Green sturgeon", r"green sturgeon"),
}

#: Every code a synopsis rule may name: the book's fish, the salmon it names, the protected fish
#: it names, its groups, and the open subjects. A rule naming anything else is refused at
#: validation rather than printing a raw code.
KNOWN_SPECIES = frozenset(set(BOOK_SPECIES) | set(SALMON_FISH) | set(PROTECTED_FISH)
                          | set(SPECIES_GROUPS))


def protected_fish_printed(text: str) -> list[str]:
    """The protected fish (`PROTECTED_FISH` codes) a text prints, in `PROTECTED_FISH` order."""
    t = squash(text)
    return [c for c, (_, pat) in PROTECTED_FISH.items() if re.search(pat, t)]

#: CODES THAT ARE REFUSED, each with what to write instead. The two the corpus used carry the
#: book's reason; every other unknown code is "not on the book's list (p.80)".
REFUSED_SPECIES: dict[str, str] = {
    "BT": "a bull trout is a Dolly Varden in the regulations (p.80: 'Any bull trout that you catch "
          "and keep must be counted as part of your Dolly Varden quota') — write DV",
    "TROUT": "trout includes char unless char are specifically excluded (p.80) — write "
             "TROUT_CHAR, with species_except [CHAR] when the row or zone table mentions char "
             "apart (a char named on its own: 'char', Dolly Varden/bull trout, lake trout, brook "
             "trout)",
    "SA": "the salmon subject is SALMON",
}

#: FEDERAL SALMON — NOT A SYNOPSIS VOCABULARY. The DFO salmon feed (`pipeline.regs.dfo_salmon`)
#: types its own pages into `CatalogueRule`s naming chinook, coho, sockeye, pink and chum (`SA`:
#: all salmon on a DFO page). A bare rule accepts them so that feed can be typed; a synopsis ENTRY
#: refuses them (`CatalogueEntry` — the book's list is p.80's, and a salmon rule in it is `SALMON`)
#: — except chinook, which the book names (`SALMON_FISH`).
FEDERAL_SALMON = frozenset({"CH", "CO", "SK", "PK", "CM", "SA"})


def species_problems(codes, where: str = "species") -> List[str]:
    """The codes a synopsis rule may not name, each with what to write instead."""
    out = []
    for c in sorted(set(codes) - KNOWN_SPECIES):
        why = REFUSED_SPECIES.get(c) or ("not on the book's species list (p.80) — the fish are "
                                         f"{', '.join(BOOK_SPECIES)}; the groups "
                                         f"{', '.join(sorted(SPECIES_GROUPS))}")
        out.append(f"{where}: unknown species code {c!r} — {why}")
    return out


#: A SIZE THE BOOK PUTS IN THE DEFINITION, NOT IN A QUOTA. Page 80: "steelhead: a rainbow
#: trout longer than 50 cm in waters where anadromous rainbow trout are found." So a steelhead
#: under 50 cm does not exist, and a table that offers a number for one is describing a fish
#: nobody can catch — Region 2 printed "up to 50 cm: 4 / over 50 cm: 2" where only the 2 is
#: real. Held here rather than as a curated rule because no regional table states it: it is
#: what the word MEANS, everywhere in the book.
DEFINITIONAL_SIZE = {
    "ST": {"min_cm": 50,
           #: WHAT IT IS INSTEAD — recorded, but NOT applied when settling. The definition
           #: holds only "in waters where anadromous rainbow trout are found": steelhead are
           #: sea-going, so a table naming trout and not steelhead is describing a landlocked
           #: rainbow the 50 cm boundary says nothing about. Substituting globally also broke
           #: the invariant the table rests on — a verdict about steelhead came to be decided
           #: by a trout/char counter that is not on the steelhead row. Applying this needs a
           #: per-water "are there steelhead here" fact the corpus does not carry.
           "below": "RB",
           "applies_where": "anadromous rainbow trout are found",
           "says": "a steelhead is a rainbow trout longer than 50 cm, so there is no "
                   "such thing as a smaller one",
           "source": "fishing_synopsis.pdf \u00b7 2025-2027 \u00b7 page 80, Definitions"},
}


def expand_species(codes: List[str]) -> List[str]:
    """A species list with every group replaced by its members, de-duplicated, order preserved.
    Anything that is not a group passes through untouched.

    TRANSITIVELY, AND DOWN TO LEAVES. A group may hold a group. Expanding one level left `TROUT`
    unexpanded inside `TROUT_CHAR`, so a rule about trout did not register under a rule about
    trout and char; and `ALL_GAME_FISH` holds the individual fish but not the code `TROUT_CHAR`,
    so comparing sets that still held group codes said "all game fish does not cover trout and
    char" — and a river closed to every game fish reported a keep limit of 5 for trout. The
    group tables happen to be flat today, so one level would give the same answer; this does not
    depend on their staying that way.

    (There were two implementations of this. `table/subject.py` had the transitive one and this
    had the one-level one, and they disagreed on exactly one thing — see below — which is the
    kind of difference that is invisible until it is a wrong number on a page. This is the only
    one now.)

    AN EMPTY GROUP PASSES THROUGH. The open subjects (`OPEN_SUBJECTS`: `ALL_FIN_FISH`,
    `PROTECTED_SPECIES`, `SALMON`) are not memberships, and expanding them to `[]` erases the
    rule — "any fish willfully or accidentally snagged must be released immediately" would name
    nothing. The caller sees the claim that was actually made and decides what to do with it.
    """
    out: List[str] = []
    stack = list(reversed(codes))
    while stack:
        c = stack.pop()
        kids = SPECIES_GROUPS.get(c)
        if kids:
            stack.extend(reversed([k for k in kids]))
        elif c in SPECIES_GROUPS:            # a group that expands to nothing: the claim itself
            if c not in out:
                out.append(c)
        elif c not in out:
            out.append(c)
    return out


#: TIER ONE. Every type belongs to exactly one family; the reader sees these as sections.
#: THIS IS THE ONLY COPY. The bundle ships each rule's `family` (`rule.family`) so no client carries
#: the mapping; the app's `FAMILY_OF` mirror, and the test that compared the two, went with the
#: regulations integration (app/packages/core/src/regulations.ts is where it plugs back in).
_FAMILY = {
    RuleType.retention_limit: "retention",
    RuleType.stop_fishing_after_quota: "retention",
    RuleType.bait_restriction: "gear_and_method",
    RuleType.tackle_restriction: "gear_and_method",
    RuleType.method_rule: "gear_and_method",
    RuleType.vessel_rule: "vessel",
    RuleType.navigation_duty: "vessel",
    #: WHO MAY FISH HERE AT ALL. Not "retention": a closure to one kind of angler is not a limit
    #: on what anyone keeps, and filing it with the quotas is what let it displace them.
    RuleType.angler_closure: "access",
    RuleType.handling_rule: "conduct",
    RuleType.hazard: "information",
    RuleType.advisory: "information",
    RuleType.program_membership: "information",
    RuleType.facility: "information",
}

#: THE TYPES THAT COUNT FISH, and so the only ones a `period` (the clock the count runs on) means
#: anything on. "Stop fishing after your DAILY quota" is the other.
_COUNTED_TYPES = (RuleType.retention_limit, RuleType.stop_fishing_after_quota)


#: KNOWN SOURCE ARTEFACTS: text the book prints inside a row that is NOT a regulation. A rule quoting
#: one is refused (`CatalogueEntry._no_source_artefacts`), so a reparse cannot re-create it; the
#: parse prompt names each one (CATALOGUE_PARSE_PROMPT.md, "Known source artefacts") and the export
#: guide lists them (`gotchas.source_artefacts`). Keyed on the row's entry id; the text is matched
#: case-insensitively inside a rule's `verbatim`.
SOURCE_ARTEFACTS: tuple[dict, ...] = (
    {"entry_id": "r4:goat_river@4-6", "row": "GOAT RIVER 4-6",
     "text": "Leadville Creek Cameron Creek",
     "says": "GOAT RIVER 4-6 prints 'Trout/char catch and release (mainstem only) Leadville Creek "
             "Cameron Creek' — the two creek names are map labels that landed in the row, not a "
             "rule (user ruling 2026-09-30). The row is its mainstem-only release and its bait "
             "ban; Leadville and Cameron creeks are tributaries like any other."},
)


def source_artefact_problems(entry_id: str, rules) -> List[str]:
    """Rules of `entry_id` that quote a known source artefact (`SOURCE_ARTEFACTS`)."""
    out: List[str] = []
    for a in SOURCE_ARTEFACTS:
        if a["entry_id"] != entry_id:
            continue
        for r in rules:
            if a["text"].lower() in str(getattr(r, "verbatim", "") or "").lower():
                out.append(f"{r.rule_id} quotes {a['text']!r}, which is a source artefact, not a "
                           f"regulation: {a['says']}")
    return out
