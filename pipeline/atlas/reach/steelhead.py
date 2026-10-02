"""HOW SURE WE ARE THAT STEELHEAD ARE HERE — `steelhead: known | possible` per section — THE
WATERS THE BOOK MAKES STEELHEAD WATER (`Extent` op `steelhead_waters`), and WHERE A RAINBOW OVER
50 CM IS A STEELHEAD (`anadromous`).

User rulings 2026-10-01 (third) and 2026-10-02 (revised). Two different things, kept apart:

REGULATIONS COME ONLY FROM THE BOOK. A STEELHEAD ROW (`steelhead_row`) is a water row that names
steelhead: flagged `CatalogueEntry.anadromous_rainbow`, a rule whose fish names steelhead `ST`, or a
licensing record that speaks of the Steelhead Stamp (`steelhead_stamp_during`, or the waiver
`steelhead_stamp_waived` — Chilko, Horsefly, West Road, both Stellako rows: steelhead are mentioned).
A steelhead row's OWN WATER (`OWN_WATER_RULE`: its matched waters within its scope, held as its rules
are — no tributaries) and every section ANY rule of it binds are BOOK-KNOWN (`Presence.close`) —
streams, lakes and wetlands alike: Tenas Lake, reached only by the Atnarko/Bella Coola spring
closure; Khartoum and Lois lakes; the Vedder Canal; the Kingcome, whose row prints only its Class II
water and 'Steelhead Stamp mandatory'. There is NO tributary walk: a row's tributaries are
book-known only where one of its own rules binds them. The book-known set is what the `steelhead_waters` twins bind
(the provincial set beyond the steelhead-region streams, each zone's wild release on its region's
book-known lakes), and its FLOWING sections (`flows`: a stream, or a lake-typed water whose name
says it flows — Vedder Canal, Gravel Slough) are where a rainbow over 50 cm is a steelhead
(`anadromous`, the bundle's `steelhead_water`, p.86).

THE CURATED LIST IS A PRESENCE INDICATOR FOR DISPLAY, nothing more
(`data/curated/regulations/steelhead_waters.json`, `load_list` / `resolve_list`): each listed
water's own sections are KNOWN. It binds no rule, adds no twin, no stamp, no `anadromous`, and
changes no `effective_rules` answer. In the steelhead regions (1, 2, 3, 5, 6) it flips a stream from
"possible" to "known"; elsewhere (the Okanagan River in Region 8) it marks steelhead as present on a
water that carries no steelhead rule. The front end decides what to show.

  known     book-known (above) or listed; each row says which (`regulations`, `listed`).
  possible  any other stream section a provincial steelhead base rule binds (`zp:steelhead`, the
            streams of the steelhead regions): "steelhead rules apply; steelhead may not be present
            in this water".
  absent    everywhere else.

THE PROVINCIAL SET ON A BOOK-KNOWN WATER. The base rules bind streams of the steelhead regions only;
a book-known water they miss (a lake, a stream past those regions) is bound by the base's TWIN,
whose one extent is `{op: steelhead_waters, siblings: [<base>]}` — the book-known set minus the
base's sections (`build.build_reaches` resolves it after every row, `_bind_steelhead_waters`); a
zone's twin also names its own area (`area_id`, `outside_area`). A twin of its own, so the base's
competition key and extents never change (S/SHK, S/SHC).

Kept apart from the rule bindings (and their digest): this is a fact about the water, not a rule.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from pipeline.atlas.reach import extent as _resolve

#: The provincial steelhead entry: the streams its base rules bind are the streams that carry the
#: steelhead rules, and those not known are "possible".
PROVINCE_STEELHEAD = "zp:steelhead"

KNOWN, POSSIBLE = "known", "possible"

#: The source the export names for a water the curated list makes known (`steelhead_source`).
CURATED_LIST = "curated list"

#: The op of the twins' one extent (`entry_models.Op.STEELHEAD_WATERS`).
STEELHEAD_WATERS = "steelhead_waters"

#: A LAKE-TYPED WATER WHOSE NAME SAYS IT FLOWS is flowing water (user ruling 2026-10-02: sloughs,
#: canals and channels are streams — Vedder Canal, Gravel Slough, Maria Slough). The ONE definition:
#: the reach builder's `anadromous` and the known-waters generator (`pipeline.regs.steelhead.
#: known_waters`) both read `flows`.
FLOWING = re.compile(r"\b(slough|canal|channel|river|creek)\b", re.I)


def flows(kind, name) -> bool:
    """Flowing water: a stream, or a water of another kind whose name says it flows (`FLOWING`)."""
    return str(kind or "").lower() == "stream" or bool(FLOWING.search(str(name or "")))


#: A STEELHEAD ROW'S OWN WATER: its matched waters within its own scope, as its rules are held (its
#: region, its border) — the water the row is written for, with NO tributaries. Only what
#: resolution reads.
OWN_WATER_RULE = {"rule_id": "steelhead_own_water", "type": "steelhead_presence",
                  "extents": [{"op": "whole"}], "includes_tributaries": False,
                  "tributary_excludes": []}


def _kind(item) -> str:
    k = getattr(item, "kind", "")
    return str(getattr(k, "value", k) or "").lower()


def _get(x, k):
    return x.get(k) if isinstance(x, dict) else getattr(x, k, None)


def names_steelhead(e) -> bool:
    """A water row (not a zone or provincial entry) with a rule whose fish names steelhead."""
    return (not str(_get(e, "entry_id") or "").startswith("z")
            and any("ST" in (_get(r, "species") or []) for r in _get(e, "rules") or []))


def speaks_of_the_stamp(e) -> bool:
    """A water row with a licensing record that speaks of the Steelhead Stamp: it runs here
    (`steelhead_stamp_during`) or is not required here (`steelhead_stamp_waived`)."""
    return (not str(_get(e, "entry_id") or "").startswith("z")
            and any(_get(x, "steelhead_stamp_during") is not None
                    or _get(x, "steelhead_stamp_waived") is not None
                    for x in _get(e, "licensing") or []))


def steelhead_row(e) -> bool:
    """A STEELHEAD ROW: a water row flagged `anadromous_rainbow`, with a rule naming steelhead, or
    with a licensing record speaking of the Steelhead Stamp — the stamp-waiver rows ("Steelhead
    Stamp not required": Chilko, Horsefly, West Road, Stellako) count, because steelhead are
    mentioned (user ruling 2026-10-02). A dict (the corpus) or a `CatalogueEntry`."""
    return (not str(_get(e, "entry_id") or "").startswith("z")
            and (bool(_get(e, "anadromous_rainbow")) or names_steelhead(e)
                 or speaks_of_the_stamp(e)))


def steelhead_waters_extent(rule: dict) -> dict | None:
    """The rule's (or licensing record's) `steelhead_waters` extent, or None. `CatalogueEntry`
    holds it to be the only extent."""
    for x in rule.get("extents") or []:
        if isinstance(x, dict) and x.get("op") == STEELHEAD_WATERS:
            return x
    return None


# ----------------------------------------------------------------------------- the curated list
class ListWater(BaseModel):
    """One water on the user's known-steelhead list (see the file's `$comment`)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    item_id: Optional[str] = None
    name: Optional[str] = None
    region: Optional[str] = None
    mu: Optional[str] = None
    note: str = ""

    @model_validator(mode="after")
    def _one_key(self) -> "ListWater":
        if bool(self.item_id) == bool(self.name):
            raise ValueError("a listed water has exactly one of item_id or name")
        if self.region is not None and not re.fullmatch(r"[1-8][AB]?", self.region.upper()):
            raise ValueError(f"region {self.region!r}: one of 1..8 (7A/7B read as 7)")
        if self.mu is not None and not re.fullmatch(r"[1-8]-\d{1,2}", self.mu):
            raise ValueError(f"mu {self.mu!r}: a management unit like '1-4'")
        return self

    @property
    def label(self) -> str:
        where = ", ".join(x for x in (f"region {self.region}" if self.region else "",
                                      f"MU {self.mu}" if self.mu else "") if x)
        return (self.item_id or repr(self.name)) + (f" ({where})" if where else "")


class Generated(BaseModel):
    """Where a copied list came from: the generator's output (`pipeline.regs.steelhead.
    known_waters` writes `data/generated/steelhead/steelhead_waters.json`) and the FINGERPRINT of
    its `waters` (`fingerprint`). A human copies the output here; the test compares the two."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str
    generator: str
    date: str
    fingerprint: str = Field(..., pattern=r"^[0-9a-f]{16}$")


class SteelheadList(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    comment: object = Field(default=None, alias="$comment")
    #: present when the list was copied from the generator's output
    generated: Optional[Generated] = None
    waters: List[ListWater]


def fingerprint(waters) -> str:
    """The list's identity: sha256 of its `waters` (canonical JSON), 16 hex. The same for the
    generator's output and the curated copy of it."""
    import hashlib
    rows = [w.model_dump(exclude_none=True) if isinstance(w, BaseModel) else w for w in waters]
    rows = [{k: v for k, v in r.items() if v not in (None, "") or k == "note"} for r in rows]
    return hashlib.sha256(json.dumps(rows, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()[:16]


def load_document(path: Path | None = None) -> SteelheadList:
    """The curated list's whole document. A missing file RAISES (AGENTS 37)."""
    if path is None:
        from pipeline.common.curated import CURATED
        path = CURATED.regulations.steelhead_waters
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"the known-steelhead list {path} is missing")
    return SteelheadList.model_validate(json.loads(path.read_text(encoding="utf-8")))


def load_list(path: Path | None = None) -> list[ListWater]:
    """The curated list. A missing file RAISES (AGENTS 37: absent curated data is a bug)."""
    return list(load_document(path).waters)


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def _regions(item) -> set[str]:
    return {str(m).split("-")[0] for m in (getattr(item, "mus", None) or ())}


def resolve_list(waters, registry) -> list[tuple[ListWater, str]]:
    """Each listed water -> exactly one registry item, or SystemExit naming every entry that is
    unknown, ambiguous, contradicted, or listed twice. Deterministic: list order kept."""
    out: list[tuple[ListWater, str]] = []
    bad: list[str] = []
    by_name: dict[str, list[str]] = {}
    if any(w.name for w in waters):
        for k, it in registry.items():
            if str(k).startswith("area:"):
                continue
            for n in {getattr(it, "name", "") or "", *(getattr(it, "variants", ()) or ())}:
                if n:
                    by_name.setdefault(_norm(n), []).append(k)

    def fits(w: ListWater, it) -> bool:
        if w.mu and w.mu not in (getattr(it, "mus", None) or ()):
            return False
        if w.region and w.region.upper().rstrip("AB") not in _regions(it):
            return False
        return True

    for w in waters:
        if w.item_id:
            it = registry.get(w.item_id) if hasattr(registry, "get") else (
                registry[w.item_id] if w.item_id in registry else None)
            if it is None or str(w.item_id).startswith("area:"):
                bad.append(f"{w.label}: no such water in the registry")
                continue
            if not fits(w, it):
                bad.append(f"{w.label}: the registry puts {it.name!r} in MU(s) "
                           f"{list(getattr(it, 'mus', ()) or ())}")
                continue
            out.append((w, w.item_id))
            continue
        cands = sorted(set(by_name.get(_norm(w.name), ())))
        hit = [k for k in cands if fits(w, registry[k])]
        if not hit:
            bad.append(f"{w.label}: no water of that name"
                       + (f" there (the name is on {cands[:6]})" if cands else ""))
        elif len(hit) > 1:
            bad.append(f"{w.label}: ambiguous — " + ", ".join(
                f"{k} {registry[k].name!r} MU {list(registry[k].mus)}" for k in hit[:8])
                + " — add region or mu, or give the item_id")
        else:
            out.append((w, hit[0]))
    seen: dict[str, str] = {}
    for w, k in out:
        if k in seen:
            bad.append(f"{w.label}: {k} is already listed as {seen[k]}")
        seen.setdefault(k, w.label)
    if bad:
        raise SystemExit("the known-steelhead list (data/curated/regulations/steelhead_waters.json) "
                         "does not resolve:\n  " + "\n  ".join(bad))
    return out


# ----------------------------------------------------------------------------- presence
class Presence:
    """Collects the steelhead rows (`add_row`, inside `build_reaches`' loop), the curated list
    (`add_list`), then the BOOK-KNOWN sections — each steelhead row's own water and every section
    a rule of one binds
    (`close`, after every row has bound) — and the attribute (`finish`)."""

    def __init__(self, registry, graph):
        self.registry, self.graph = registry, graph
        #: the steelhead rows (`steelhead_row`)
        self.rows: set[str] = set()
        #: each steelhead row's own water (`OWN_WATER_RULE`)
        self.own: dict[str, frozenset[str]] = {}
        #: sections the curated list names (its waters' own sections — no walk)
        self.listed: set[str] = set()
        self.curated: list[dict] = []
        #: the list the run was given (`fingerprint`) — the bundle refuses a run made with another
        self.list_fingerprint: str = fingerprint([])
        #: section -> the steelhead row that binds it (the lowest entry id)
        self._book: dict[str, str] | None = None
        self.by_entry: dict[str, dict[str, int]] = {}
        self._flowing: frozenset[str] | None = None

    def add_row(self, e: dict, own=None) -> None:
        """`own()` is `build_reach` of `OWN_WATER_RULE` in this entry's own context (covered items,
        scope, region limit, border, tidal water): the row's own water, no tributaries."""
        if not steelhead_row(e):
            return
        self.rows.add(e["entry_id"])
        if own is not None:
            binding, _ = own()
            trib = frozenset(binding.via_tributary)
            self.own[e["entry_id"]] = frozenset(s for s in binding.sections if s not in trib)

    def add_list(self, resolved, outside=frozenset()) -> None:
        """The curated list: each water's own sections in B.C. (`outside` — the border, as every
        binding has it: the Taku's Alaska reach is not marked). A presence indicator only."""
        self.list_fingerprint = fingerprint([w for w, _ in resolved])
        for w, item in resolved:
            secs = tuple(s for s in self.registry[item].section_ids if s not in outside)
            self.listed.update(secs)
            self.curated.append({"item_id": item, "listed": w.label, "sections": len(secs)})

    def close(self, bindings) -> frozenset[str]:
        """THE BOOK-KNOWN SET: every steelhead row's own water and every section any rule of one
        binds. Called once, after every row has bound; what the `steelhead_waters` twins bind (less
        their siblings)."""
        if self._book is None:
            book: dict[str, str] = {}
            per_row: dict[str, set[str]] = {e: set(v) for e, v in self.own.items() if v}
            for b in bindings:
                if b.entry_id in self.rows and b.sections:
                    per_row.setdefault(b.entry_id, set()).update(b.sections)
            for eid in sorted(per_row):
                for s in per_row[eid]:
                    book.setdefault(s, eid)
            self.by_entry = {e: {"sections": len(v)} for e, v in sorted(per_row.items())}
            self._book = book
        return frozenset(self._book)

    def flowing(self, s: str) -> bool:
        """`flows` for a section: a stream of the graph, or a section of a non-stream water whose
        name says it flows (the Vedder Canal's lake section)."""
        if self._flowing is None:
            self._flowing = frozenset(
                x for k, it in self.registry.items() if not str(k).startswith("area:")
                and _kind(it) != "stream" and flows(_kind(it), getattr(it, "name", ""))
                for x in it.section_ids)
        return _resolve._kind_of(self.graph, s) == "stream" or s in self._flowing

    def finish(self, bindings, twins=frozenset()) -> tuple[list[dict], dict]:
        """Every section's row, sorted, and the report's counts. `twins` are the (entry, rule)
        keys of the `steelhead_waters` rules — they bind book-known water, so not "possible"."""
        self.close(bindings)
        book = self._book or {}
        known = set(book) | self.listed
        # the BASE provincial rules (streams of the steelhead regions)
        base = {s for b in bindings if b.entry_id == PROVINCE_STEELHEAD
                and (b.entry_id, b.rule_id) not in twins for s in b.sections}
        possible = {s for s in base if s not in known
                    and _resolve._kind_of(self.graph, s) == "stream"}
        rows = []
        for s in known:
            rows.append({"section_id": s, "steelhead": KNOWN,
                         "entry_id": book.get(s, CURATED_LIST),
                         "regulations": s in book, "listed": s in self.listed,
                         "anadromous": s in book and self.flowing(s),
                         "kind": _resolve._kind_of(self.graph, s)})
        rows += [{"section_id": s, "steelhead": POSSIBLE, "entry_id": PROVINCE_STEELHEAD,
                  "regulations": False, "listed": False, "anadromous": False, "kind": "stream"}
                 for s in possible]
        rows.sort(key=lambda r: r["section_id"])
        kinds: dict[str, int] = {}
        for r in rows:
            if r["steelhead"] == KNOWN:
                kinds[str(r["kind"])] = kinds.get(str(r["kind"]), 0) + 1
        report = {
            "known": len(known),
            #: known by the book (a steelhead row's rule binds it) — the twins' water
            "known_regulations": len(book),
            #: known by the curated list alone: the presence indicator, no rule
            "known_list_only": len(self.listed - set(book)),
            "known_streams": kinds.get("stream", 0),
            "known_lakes": kinds.get("lake", 0),
            "known_other": sum(v for k, v in kinds.items() if k not in ("stream", "lake")),
            #: where a rainbow over 50 cm is a steelhead (book-known flowing water)
            "anadromous": sum(1 for r in rows if r["anadromous"]),
            #: book-known sections the stream-only base rules do not bind — the twins' water
            "known_beyond_the_base": sum(1 for s in book if s not in base),
            "possible": len(possible),
            "steelhead_rows": len(self.rows),
            "list_fingerprint": self.list_fingerprint,
            "curated": list(self.curated),
            "by_entry": {k: self.by_entry[k] for k in sorted(self.by_entry)},
        }
        return rows, report
