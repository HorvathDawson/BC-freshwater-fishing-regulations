"""HOW SURE WE ARE THAT STEELHEAD ARE HERE — `steelhead: known | possible` per section — and THE
KNOWN STEELHEAD WATERS the provincial steelhead set reaches (`Extent` op `steelhead_waters`).

User rulings 2026-10-01 (third) and 2026-10-02. Every STREAM of Regions 1, 2, 3, 5 and 6 carries the
steelhead rules (the wild release, the annual hatchery 10, the record duty, the stamp), but the book
names steelhead on few of them. The attribute tells a reader which:

  known     anywhere in the province (2026-10-02: known waters carry the set WHEREVER they are):
            - every section ANY rule of a STEELHEAD ROW binds (`steelhead_row`: a water row flagged
              `CatalogueEntry.anadromous_rainbow`, or with a rule whose fish names steelhead `ST`) —
              streams, lakes and wetlands alike: Tenas Lake, reached only by the Atnarko/Bella
              Coola spring closure; Khartoum and Lois lakes; the Vedder Canal;
            - the TRIBUTARIES of every row flagged `anadromous_rainbow` — the same walk every
              "including tributaries" rule takes (`build.build_reach`: the row's own scope,
              `tributaries.expand`, streams only), held to no region (`regional=False`: water rows
              are region-agnostic), so the Thompson's walk into the Nicola headwaters of Region 8
              and the Skeena's into Zone 7A are known. The walk is the HYDROLOGY, not the row's
              regulation: it walks whether or not the row says "including tributaries", and no
              carve-out (`tributary_excludes`) stops it. A row that only NAMES steelhead in a rule
              and is not flagged (the Fraser's per-region rows: "No Fishing for steelhead") makes
              its own water known but is not walked — its tributaries are the whole Fraser basin;
            - every water on the user's CURATED LIST (`data/curated/regulations/steelhead_waters.json`,
              `load_list` / `resolve_list`) and its tributaries, by the same walk.
  possible  any other stream section a provincial steelhead rule binds (`zp:steelhead`, whose base
            rules bind the streams of the steelhead regions): "steelhead rules apply; steelhead may
            not be present in this water".
  absent    everywhere else.

THE PROVINCIAL SET ON A KNOWN WATER. The base rules bind streams of the steelhead regions only; a
known water they miss (a lake, a stream past those regions) is bound by the base's TWIN, whose one
extent is `{op: steelhead_waters, siblings: [<base>]}` — the known set minus the base's sections
(`build.build_reaches` resolves it after every row, `bind_steelhead_waters`). A twin of its own, so
the base's competition key and extents never change (S/SHK, S/SHC).

`anadromous_rainbow` (a rainbow over 50 cm IS a steelhead, p.86) holds on the KNOWN STREAM sections
only — the bundle's `steelhead_water` — never on a lake and never on a "possible" stream.

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

#: The rule the walk is run as — the row's whole water, clipped by the row's own scope, with its
#: tributaries. Only what resolution reads.
WALK_RULE = {"rule_id": "steelhead_presence", "type": "steelhead_presence",
             "extents": [{"op": "whole"}], "includes_tributaries": True,
             "tributary_excludes": []}


def names_steelhead(e: dict) -> bool:
    """A water row (not a zone or provincial entry) with a rule whose fish names steelhead."""
    return (not str(e.get("entry_id") or "").startswith("z")
            and any("ST" in (r.get("species") or []) for r in e.get("rules") or []))


def steelhead_row(e: dict) -> bool:
    """A STEELHEAD ROW: a water row flagged `anadromous_rainbow`, or one with a rule naming
    steelhead. A row that mentions steelhead only to waive the stamp ("Steelhead Stamp not
    required": Chilko, Horsefly, West Road, Stellako) is neither, and is not one."""
    return (not str(e.get("entry_id") or "").startswith("z")
            and (bool(e.get("anadromous_rainbow")) or names_steelhead(e)))


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


class SteelheadList(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    comment: object = Field(default=None, alias="$comment")
    waters: List[ListWater]


def load_list(path: Path | None = None) -> list[ListWater]:
    """The curated list. A missing file RAISES (AGENTS 37: absent curated data is a bug)."""
    if path is None:
        from pipeline.common.curated import CURATED
        path = CURATED.regulations.steelhead_waters
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"the known-steelhead list {path} is missing")
    return list(SteelheadList.model_validate(
        json.loads(path.read_text(encoding="utf-8"))).waters)


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
    """Collects the known sections: entry by entry (`add_row`, from inside `build_reaches`' loop, in
    each entry's own context), the curated list (`add_list`), then the rows' bound sections
    (`close`, after every row has bound) — and the possible ones from the final bindings
    (`finish`)."""

    def __init__(self, registry, graph):
        self.registry, self.graph = registry, graph
        #: section -> (rank, source order, source, scope); the lowest wins: a water's own sections
        #: (0) before a tributary (1), a row before the curated list, then the entry id
        self.known: dict[str, tuple[int, int, str, str]] = {}
        self.by_entry: dict[str, dict[str, int]] = {}
        #: the steelhead rows (`steelhead_row`) — whose bound sections `close` adds
        self.rows: set[str] = set()
        self.curated: list[dict] = []
        self._closed: frozenset[str] | None = None

    def _put(self, s: str, rank: int, eid: str, scope: str) -> None:
        key = (rank, 1 if eid == CURATED_LIST else 0, eid, scope)
        cur = self.known.get(s)
        if cur is None or key[:3] < cur[:3]:
            self.known[s] = key

    def _walk(self, binding, eid: str, own: frozenset[str], scope: str) -> dict[str, int]:
        """The walk's sections: the water's own (any kind) and its stream tributaries."""
        trib = frozenset(binding.via_tributary)
        n = {scope: 0, "trib": 0}
        for s in binding.sections:
            mine = s in own and s not in trib
            if not mine and _resolve._kind_of(self.graph, s) != "stream":
                continue
            k = scope if mine else "trib"
            n[k] += 1
            self._put(s, 0 if mine else 1, eid, k)
        return n

    def add_row(self, e: dict, covered, reach) -> None:
        """`reach(rule)` is `build_reach` bound to this entry's covered items, clip, border and
        tidal water, with `regional=False`."""
        if not steelhead_row(e):
            return
        eid = e["entry_id"]
        self.rows.add(eid)
        if e.get("anadromous_rainbow"):
            binding, _ = reach(dict(WALK_RULE))
            trib = frozenset(binding.via_tributary)
            own = frozenset(s for s in binding.sections if s not in trib
                            and _resolve._kind_of(self.graph, s) == "stream")
            self.by_entry[eid] = self._walk(binding, eid, own, "reach")

    def add_list(self, resolved, reach_item) -> None:
        """The curated list: each water's own sections and its tributaries. `reach_item(item_id)`
        is `build_reach` of `WALK_RULE` on that one water (`regional=False`)."""
        n = {"own": 0, "trib": 0}
        for w, item in resolved:
            binding, _ = reach_item(item)
            own = frozenset(self.registry[item].section_ids)
            got = self._walk(binding, CURATED_LIST, own, "own")
            self.curated.append({"item_id": item, "listed": w.label, "own": got["own"],
                                 "trib": got["trib"]})
            n["own"] += got["own"]
            n["trib"] += got["trib"]
        if resolved:
            self.by_entry[CURATED_LIST] = n

    def close(self, bindings) -> frozenset[str]:
        """Every section any rule of a steelhead row binds is known too. Called once, after every
        row has bound; the KNOWN set the `steelhead_waters` twins bind (less their siblings)."""
        if self._closed is not None:
            return self._closed
        # Only sections no walk or list has made known: a section already known keeps the source
        # it had (a tributary keeps the row it is a tributary of), so this adds water, never
        # re-attributes it.
        add: dict[str, str] = {}
        for b in bindings:
            if b.entry_id in self.rows and b.sections:
                for s in b.sections:
                    if s not in self.known and (s not in add or b.entry_id < add[s]):
                        add[s] = b.entry_id
        for s, eid in add.items():
            n = self.by_entry.setdefault(eid, {"reach": 0, "trib": 0})
            n["rules"] = n.get("rules", 0) + 1
            self._put(s, 0, eid, "rules")
        self._closed = frozenset(self.known)
        return self._closed

    def finish(self, bindings, twins=frozenset()) -> tuple[list[dict], dict]:
        """Every section's row, sorted, and the report's counts. `twins` are the (entry, rule)
        keys of the `steelhead_waters` rules — they bind known water, so not "possible" water."""
        known = self.close(bindings)
        # the BASE provincial rules (streams of the steelhead regions)
        base = {s for b in bindings if b.entry_id == PROVINCE_STEELHEAD
                and (b.entry_id, b.rule_id) not in twins for s in b.sections}
        possible = {s for s in base if s not in known
                    and _resolve._kind_of(self.graph, s) == "stream"}
        rows = [{"section_id": s, "steelhead": KNOWN, "entry_id": eid, "scope": scope,
                 "kind": _resolve._kind_of(self.graph, s)}
                for s, (_, _o, eid, scope) in self.known.items()]
        rows += [{"section_id": s, "steelhead": POSSIBLE, "entry_id": PROVINCE_STEELHEAD,
                  "scope": "rules", "kind": "stream"} for s in possible]
        rows.sort(key=lambda r: r["section_id"])
        kinds: dict[str, int] = {}
        for r in rows:
            if r["steelhead"] == KNOWN:
                kinds[str(r["kind"])] = kinds.get(str(r["kind"]), 0) + 1
        report = {
            "known": len(known),
            "known_streams": kinds.get("stream", 0),
            "known_lakes": kinds.get("lake", 0),
            "known_other": sum(v for k, v in kinds.items() if k not in ("stream", "lake")),
            #: known sections the stream-only base rules do not bind — the twins' water
            "known_beyond_the_base": sum(1 for s in known if s not in base),
            "possible": len(possible),
            "steelhead_rows": len(self.rows),
            "curated": list(self.curated),
            "by_entry": {k: self.by_entry[k] for k in sorted(self.by_entry)},
        }
        return rows, report
