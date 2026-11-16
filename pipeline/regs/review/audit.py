"""T743: THE FINAL MARKS AUDIT — one read-only command over the owner's DFO place marks, run once when he says he is done
marking (owner 2026-11-15: "we should wait until fully done marks though", "we just want to prepared for how we will
audit them"). It replaces the one-off agent audits of 2026-11-15 (handoff/PLAN/curation-review/resaved-marks-audit-*,
resaved-marks-second-look-*, fraser-channels-*, mouth-tidal-ends-audit-*) with deterministic code: the same inputs give
the same report, byte for byte.

    python -m pipeline.regs.review.audit --out <report.md>              # + <report.json> beside it
    python -m pipeline.regs.review.audit --out new.md --since old.json  # only what is new / resolved since old.json

READ-ONLY: it reads the marks store, the DFO places, the cuts, the tidal limits and the synopsis (CURATED), and the
served atlas's sidecars, registry and graph (`--build`, default GENERATED's); it writes only `--out` and its JSON.
Exit 0 whenever it ran (every finding is a CANDIDATE for the owner, never a failure); 2 when it could not run.

The checks (each a section of the report; `SECTIONS` says each in plain words):

  A  places saved more than once: the file holds exactly the last save; leftovers of superseded saves (a link, a tidal
     call, a tributary flag), cuts made from marks that no place, rule or tidal limit uses any more
  B  last save wrong / unsure while an extent is still in force — and it SHIPS (`SHIP_RULE`)
  C  tributaries: wording vs the extent (included, heading, mainstem, tributaries-only, blank)
  D  "rest of the water" / "all other places" extents and the siblings they depend on
  E  mouth / tidal ends that are not the stream's own tidal boundary; cuts near one
  F  a river's own side channels a place drops (an item restriction, or a channel outside the water record)
  G  marks written in a group (multi-target era) not since re-saved alone
  H  one-bank wordings saved as a whole-width extent (T754: no bank field exists yet)
  I  marks not landed (`apply.check`, imported), grouped by what they wait on
  J  unmarked places remaining, by region
  K  offsets: wording names a distance its cut lacks, or a cut carries an offset the wording does not name

WORDING PATTERNS live in ONE table, `WORDING`: an owner's new ask ("…make sure every X gets Y") is one row there and,
when it needs a new comparison, one small check function. Each finding carries a stable `key` (section, kind, place,
what) so two runs diff (`--since`).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Optional

FORMAT = "marks-audit/1"

#: THE WORDING TABLE — name -> (regex, what it means). Matched case-insensitively on a place's DFO wording (its
#: `source_text.specific_area`) or, for the `heading_*` rows, on its water heading (`source_text.waters`).
WORDING: dict[str, tuple[str, str]] = {
    "trib_only": (r"^\W*all\s+(?:of\s+the\s+)?tributar(?:y|ies)\b|\btributaries\s+only\b",
                  "the tributaries ONLY, not the water itself (Bella Coola 'All tributaries', Bulkley)"),
    "trib_included": (r"\b(?:including|includes|incl\.?|and|plus|with)\s+(?:all\s+)?(?:other\s+)?(?:its\s+|their\s+)?"
                      r"tributaries\b",
                      "the water AND its tributaries"),
    "trib_excluded": (r"\bmain\s?stem\b|\b(?:excluding|not\s+including|except|without)\s+(?:all\s+)?(?:its\s+)?"
                      r"tributaries\b",
                      "the water only, tributaries out ('mainstem', 'excluding tributaries')"),
    "heading_only": (r"\btributaries\s*\)?\s*$",
                     "a water heading that IS the tributaries ('Somass River tributaries')"),
    "heading_included": (r"\b(?:includ(?:es|ing)|and|plus|with)\s+(?:all\s+)?tributaries\b",
                         "a water heading that takes its tributaries in ('(includes tributaries)', '… and tributaries')"),
    "mouth": (r"\bmouth\b|\btidal\b|\bestuar|\bsalt\s?water\b|\bocean\b|\bthe\s+sea\b",
              "an end at the mouth / tidal boundary / the sea"),
    "one_bank": (r"\b(?:on|along)\s+(?:the\s+)?(?:west|east|north|south)(?:ern)?\s+bank\b(?!\s+of\b)"
                 r"|\b(?:west|east|north|south)(?:ern)?\s+half\b|\bhalf\s+of\s+(?:the\s+)?river\b"
                 r"|\bcent(?:re|er)\s?line\b",
                 "one bank / one half of the river only (T754)"),
    "distance": (r"\b\d+(?:[.,]\d+)?\s*(?:m|km|metres?|meters?|kilomet(?:re|er)s?)\b(?!\s*radius)",
                 "a stated distance ('500 m below', 'approx. 1 km downstream')"),
}
_RX = {k: re.compile(v[0], re.I) for k, v in WORDING.items()}


def says(name: str, text: Optional[str]) -> Optional[str]:
    """The words of `text` that match WORDING row `name`, or None."""
    m = _RX[name].search(text or "")
    return m.group(0) if m else None


def trib_meaning(wording: str) -> Optional[str]:
    """What a place's wording says about tributaries: only | included | excluded | None. Only wins, then included
    ("Tatshenshini mainstem and all other tributaries" takes them in), then excluded."""
    for k, v in (("trib_only", "only"), ("trib_included", "included"), ("trib_excluded", "excluded")):
        if says(k, wording):
            return v
    return None


#: a cut this close (m, along the same blue line) to a stream's tidal boundary is "near" it (the tidal audit's 500 m)
NEAR_M = 500.0
#: the note a cut written from a mark carries (`proposals.split_record`)
MARK_CUT_NOTE = "[T185 proposal"

#: THE CODE RULE BEHIND B, cited in the report header (verified 2026-11-15 in the code it names)
SHIP_RULE = ("`pipeline/atlas/reach/dfo_salmon.py` `resolve_all` binds EVERY place that has an extent (dormant places "
             "included; a variant with none rides its target's), and `pipeline/build.py` `NOT_BUILD_INPUTS` keeps "
             "`review.marks` out of every stage: a place ships the extent in its curated file whatever its last mark "
             "says. A 'wrong' or 'unsure' save with no landed fix changes nothing that ships.")

#: (T755 follow-up) THE FLAG the review app shows on a checklist row and this audit's section B writes, one predicate
#: (`old_extent_in_force`): the owner's last word is "wrong" / "unsure" with no fix, and an older extent still holds
OLD_EXTENT_FLAG = "old extent still in force"


def old_extent_in_force(m: Optional[Mapping], extents: Iterable[Mapping]) -> bool:
    """Is the place's last save (`m`, the mark in force, as a dict) "wrong" or "unsure" with NO extent fix, while the
    place still has an extent (`extents`: the curated extents it ships — its own, or the one it rides)? That extent
    is older than the owner's last word on it. A flag only: what ships does not change (`SHIP_RULE`). Pure."""
    return (m is not None and m.get("verdict") in ("wrong", "unsure") and not (m.get("extent_fix") or [])
            and bool(list(extents or [])))


SECTIONS: dict[str, tuple[str, str]] = {
    "A": ("Places saved more than once",
          "The last save is the one in force. Listed: a place whose file is not what its last save says, and anything an "
          "older save wrote that the last one no longer stands behind (a 'same place as' link, a tidal call, a "
          "tributary flag), and cuts made from marks that nothing uses any more."),
    "B": ("Last save wrong or unsure, earlier extent still in force",
          "The owner's last word on these places is 'wrong' or 'unsure', but the place still has an extent (from an "
          "older save or from before any mark), and that extent is what the app shows."),
    "C": ("Tributaries: wording vs extent",
          "C1 the wording takes tributaries in, the place does not; C2 the water's heading takes them in (or IS the "
          "tributaries), a place under it does not; C3 the wording says mainstem / excluding tributaries, the place "
          "takes them in or leaves it blank; C4 the wording means the tributaries ONLY, the place is not that form; "
          "C5 the tributary choice is blank everywhere (place and water)."),
    "D": ("'Rest of the water' and 'all the other places' extents",
          "Each such place with the places it is computed from. Flagged: a dormant sibling that still takes (or adds) "
          "water, a sibling that cannot be drawn (a rest then goes unmapped entirely), two rests on one water."),
    "E": ("Mouth / tidal ends",
          "A place's cut at or near a stream's mouth that is not the tidal boundary every stream gets (rank-1 split, "
          "coast cut, or the mouth), cuts from marks within NEAR_M of one, and pending mouth cuts not built yet."),
    "F": ("A river's own side channels dropped",
          "A place on a river leaves out a side channel of that same river (a channel FWA codes as the river's): "
          "because the mark held it to the river item, or because the channel is not in the water record at all."),
    "G": ("Marks written in a group, not re-saved alone",
          "From the multi-target era: one click saved several wordings with one extent. A wording that differs from "
          "its group's may carry the wrong stretch (Nanaimo 2442)."),
    "H": ("One-bank wordings saved whole-width",
          "The words close one bank or half of the river; the extent is the whole width (there is no bank field yet: "
          "T754)."),
    "I": ("Marks not landed",
          "What `python -m pipeline.regs.review.apply --check` reports, grouped by what each waits on."),
    "J": ("Unmarked places remaining",
          "Active places with no mark in force, by region; places that ride a marked place's extent are counted apart."),
    "K": ("Offsets: wording vs cut",
          "K1 the wording names a distance ('500 m below the power line') but no cut of the extent is offset; K2 a cut "
          "carries an offset the wording does not name, and the plain cut exists (Morice 2591/2592)."),
}


# =============================================================================================== inputs

@dataclass
class Inputs:
    """Everything the checks read, as plain data (tests build it in memory). `split_pos`, `tidal`, `registry`, `graph`
    and `landed` are None when not loaded; the section that needs one says so (`skipped`)."""
    lines: list[tuple[int, dict]]                       # (line number, mark) — every mark, store order
    places: dict[str, dict]                             # location_id -> place (as the file holds it)
    waters: dict[str, dict]                             # water_id -> water record
    cuts: dict[str, dict] = field(default_factory=dict)  # cut id -> cuts.json record
    synopsis_text: str = ""                             # the synopsis corpus, as text (cut id references)
    tidal_limits_text: str = ""                         # tidal_limits.json, as text
    tidal_rows: list[dict] = field(default_factory=list)  # tidal_limits.json `waters`
    split_pos: Optional[dict[str, tuple[str, float, str]]] = None   # atlas: split id -> (blk, measure, label)
    tidal: Optional[dict[str, dict]] = None             # atlas: blk -> {id, measure, limit, name}
    registry: Optional[dict[str, dict]] = None          # atlas: item id -> item
    graph: Any = None                                   # atlas: StreamGraph
    landed: Optional[dict] = None                       # apply.check(...)
    sources: dict = field(default_factory=dict)
    skipped: dict = field(default_factory=dict)          # section -> why it could not run


@dataclass(frozen=True)
class Finding:
    section: str
    kind: str
    place: str
    found: str
    region: str = ""
    water: str = ""
    wording: str = ""
    lines: tuple = ()
    what: str = ""                                      # disambiguates two findings of one kind on one place

    @property
    def key(self) -> str:
        return f"{self.section}|{self.kind}|{self.place}|{self.what}"

    def to_dict(self) -> dict:
        return {"key": self.key, "section": self.section, "kind": self.kind, "place": self.place,
                "region": self.region, "water": self.water, "wording": self.wording, "lines": list(self.lines),
                "found": self.found}


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def read_marks(path: Path) -> list[tuple[int, dict]]:
    """Every mark line as a dict, with its line number (blank and retired lines left out). Read as JSON, not through
    the model, so a mark field added later (the tributaries-only form) never stops the audit; `apply.check` (I)
    validates the store through the model."""
    from pipeline.regs.review.marks import RETIRED_SCOPES
    out = []
    for n, text in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not text.strip():
            continue
        m = json.loads(text)
        if m.get("scope") in RETIRED_SCOPES:
            continue
        out.append((n, m))
    return out


def read_places(d: Path) -> tuple[dict, dict]:
    places, waters = {}, {}
    files = sorted(Path(d).glob("region-*.json"))
    if not files:
        raise FileNotFoundError(f"no DFO places (region-*.json) under {d}")
    for p in files:
        doc = json.loads(p.read_text(encoding="utf-8"))
        for w in doc.get("waters") or ():
            waters[w["water_id"]] = w
        for loc in doc.get("locations") or ():
            places[loc["location_id"]] = {**loc, "_file": p.name}
    return places, waters


def tidal_canonical(build: Path, split_pos: Mapping) -> dict[str, dict]:
    """blk -> THE tidal boundary the atlas gives that stream (`tidal.resolved.json`, T326/T678 'coast/2'): a rank-1
    split / cut where one is curated; else the coast cut (`shore_cuts.json` `cut_m`) when the line was cut there; else
    the line's mouth (the `<water>__tidal_boundary` alias at its lower end). `whole` rows are tidal throughout."""
    rows = json.loads((Path(build) / "tidal.resolved.json").read_text(encoding="utf-8"))
    shore = json.loads((Path(build) / "shore_cuts.json").read_text(encoding="utf-8"))
    cut_at = {str(r["blk"]): float(r["cut_m"]) for r in shore.get("runs") or () if r.get("cut_m") is not None}
    out = {}
    for r in rows:
        blk = str(r["blk"])
        if r["limit"] in ("split", "cut"):
            pos = split_pos.get(r["split_id"])
            out[blk] = {"id": r["split_id"], "measure": pos[1] if pos else None, "limit": r["limit"],
                        "name": r.get("name") or "", "tier": "rank-1 split"}
        elif r["limit"] == "mouth":
            if blk in cut_at:
                out[blk] = {"id": f"shore:{blk}:{int(cut_at[blk])}", "measure": cut_at[blk], "limit": "coast",
                            "name": r.get("name") or "", "tier": "coast cut", "alias": r.get("alias")}
            else:
                out[blk] = {"id": r.get("alias"), "measure": float(r.get("measure") or 0.0), "limit": "mouth",
                            "name": r.get("name") or "", "tier": "the mouth"}
        else:
            out[blk] = {"id": None, "measure": None, "limit": "whole", "name": r.get("name") or "",
                        "tier": "whole", "item": r.get("item")}
    return out


def load(*, marks: Optional[Path] = None, places: Optional[Path] = None, cuts: Optional[Path] = None,
         tidal_limits: Optional[Path] = None, synopsis: Optional[Path] = None, build: Optional[Path] = None,
         with_graph: bool = True, with_landed: bool = True) -> Inputs:
    """The inputs, from CURATED (each path overridable) and the atlas `build` (default GENERATED's served one)."""
    from pipeline.common.curated import CURATED, GENERATED
    marks = Path(marks or CURATED.review.marks)
    places = Path(places or CURATED.dfo_salmon.places)
    cuts = Path(cuts or CURATED.waters.cuts)
    tidal_limits = Path(tidal_limits or CURATED.waters.tidal)
    synopsis = Path(synopsis or CURATED.regulations.synopsis)
    mtext = marks.read_text(encoding="utf-8")
    pl, wa = read_places(places)
    cdoc = json.loads(cuts.read_text(encoding="utf-8"))
    cut_by = {s["id"]: {**s, "_water": w.get("name")} for w in cdoc["waterbodies"] for s in w["splits"]}
    ttext = tidal_limits.read_text(encoding="utf-8")
    stext = "\n".join(p.read_text(encoding="utf-8") for p in sorted(synopsis.glob("region-*.json")))
    inp = Inputs(lines=read_marks(marks), places=pl, waters=wa, cuts=cut_by, synopsis_text=stext,
                 tidal_limits_text=ttext, tidal_rows=list(json.loads(ttext).get("waters") or ()))
    inp.sources = {"marks": f"{marks} ({len(mtext.splitlines())} lines, sha {_sha(mtext)})",
                   "places": f"{places} ({len(pl)} places, {len(wa)} waters)", "cuts": f"{cuts} ({len(cut_by)} cuts)",
                   "tidal_limits": f"{tidal_limits} ({len(inp.tidal_rows)} rows)", "synopsis": str(synopsis)}
    b = Path(build) if build else GENERATED.build()
    if not (b / "splits.resolved.json").exists():
        why = f"no atlas at {b} (splits.resolved.json missing): pass --build <atlas dir>"
        inp.skipped.update({"E": why, "F": why})
        inp.sources["atlas"] = why
    else:
        sp = json.loads((b / "splits.resolved.json").read_text(encoding="utf-8"))
        inp.split_pos = {r["split_id"]: (str(r["blk"]), float(r["route_measure"]), r.get("label") or "") for r in sp}
        inp.tidal = tidal_canonical(b, inp.split_pos)
        stamp = (b / "COMPLETE.json")
        inp.sources["atlas"] = f"{b} (COMPLETE.json sha {_sha(stamp.read_text())})" if stamp.exists() else str(b)
        if with_graph:
            from pipeline.common.io.serialize import read_artifact
            reg = json.loads((b / "registry.json").read_text(encoding="utf-8"))
            inp.registry = {i["id"]: i for i in reg["items"]}
            inp.graph = read_artifact(str(b / "graph.pkl"))
        else:
            inp.skipped["F"] = "--no-graph: the registry and graph were not loaded"
    if with_landed:
        from pipeline.regs.review import apply as AP
        try:
            inp.landed = AP.check(marks=marks, build=b if b.is_dir() else None)
        except Exception as ex:                          # shown in section I and on stderr, never read as "all landed"
            inp.skipped["I"] = f"apply.check could not run: {type(ex).__name__}: {ex}"
            print(f"audit: {inp.skipped['I']}", file=sys.stderr)
    else:
        inp.skipped["I"] = "--no-landed: apply.check was not run"
    return inp


# =============================================================================================== helpers

def history(inp: Inputs) -> dict[str, list[tuple[int, dict]]]:
    """DFO place -> its saves in store order (cleared lines included)."""
    out: dict[str, list] = defaultdict(list)
    for n, m in inp.lines:
        if m.get("scope") == "dfo" and not m.get("rule_id"):
            out[m["target"]].append((n, m))
    return out


def in_force(saves: list[tuple[int, dict]]) -> Optional[tuple[int, dict]]:
    """The save in force (`marks.latest`): the last one, unless it is a `cleared` line."""
    if not saves or saves[-1][1].get("verdict") == "cleared":
        return None
    return saves[-1]


def since_clear(saves: list[tuple[int, dict]]) -> list[tuple[int, dict]]:
    """The saves after the last `cleared` line (what the in-force save superseded)."""
    i = max((k for k, (_, m) in enumerate(saves) if m.get("verdict") == "cleared"), default=-1)
    return saves[i + 1:]


def fix_extents(m: Mapping) -> Optional[list[dict]]:
    """A mark's extent fix(es) as curated extents (`proposals.extent_of` for DFO: one per piece), or None when it has
    no fix or a point is a NEW cut (its id is known only once the cut is built)."""
    fixes = m.get("extent_fix") or []
    if not fixes or any(p.get("kind") != "split" for f in fixes for p in f.get("points") or ()):
        return None
    out = []
    for f in fixes:
        e: dict = {"op": f["op"]}
        sids = [p["split_id"] for p in f.get("points") or ()]
        if sids:
            e["splits"] = sids
        items = f.get("items") or []
        if len(items) == 1:
            e["item_id"] = items[0]
        elif items:
            e["item_ids"] = list(items)
        if f.get("area"):
            e["area_id"] = f["area"]
        out.append(e)
    return out


def _shape(op, splits, items, area) -> tuple:
    return (op, tuple(splits), tuple(sorted(items)), area or None)


def fix_matches(m: Mapping, extents: Iterable[Mapping]) -> Optional[bool]:
    """Does the file's extent list say what mark `m`'s fix says? None when the mark has no fix. A NEW cut's point
    matches any cut in its place (the cut's id is minted when it lands), so a landed fix with new cuts matches."""
    fixes = m.get("extent_fix") or []
    if not fixes:
        return None
    want = [_shape(f["op"], [p.get("split_id") if p.get("kind") == "split" else None for p in f.get("points") or ()],
                   f.get("items") or [], f.get("area")) for f in fixes]
    have = [_shape(e.get("op"), e.get("splits") or [], e.get("item_ids") or ([e["item_id"]] if e.get("item_id") else []),
                   e.get("area_id")) for e in extents]
    if len(want) != len(have):
        return False
    left = list(have)
    for w in want:
        hit = next((h for h in left if h[0] == w[0] and h[2] == w[2] and h[3] == w[3] and len(h[1]) == len(w[1])
                    and all(a is None or a == b for a, b in zip(w[1], h[1]))), None)
        if hit is None:
            return False
        left.remove(hit)
    return True


def pending_cuts(m: Mapping) -> bool:
    return any(p.get("kind") != "split" for f in m.get("extent_fix") or () for p in f.get("points") or ())


def words(ex: Iterable[Mapping]) -> str:
    """An extent list in a few words: `between(a, b)[item]`."""
    out = []
    for e in ex:
        items = e.get("item_ids") or ([e["item_id"]] if e.get("item_id") else [])
        s = e.get("op", "?") + (f"({', '.join(e['splits'])})" if e.get("splits") else "")
        s += f"[{', '.join(items)}]" if items else ""
        s += f" in {e['area_id']}" if e.get("area_id") else ""
        out.append(s)
    return "; ".join(out) or "no extent"


def target_of(inp: Inputs, loc: Mapping) -> Mapping:
    """The place whose extent this one ships: itself, or — with no extent of its own — the place it is a confirmed
    (or proposed, not contested) variant of (`resolve_all`)."""
    ex = (loc.get("binding") or {}).get("extents") or []
    d = loc.get("duplicate_of")
    if not ex and d and loc.get("duplicate_confirmed") is not False and d in inp.places:
        return inp.places[d]
    return loc


def tribs_in_force(inp: Inputs, loc: Mapping) -> tuple[Optional[bool], str]:
    """(tributaries, from where) — the place's own flag, else its water's (AGENTS 9; `entries.to_reach_input`), read
    on the place that ships (`target_of`)."""
    t = target_of(inp, loc)
    via = "" if t is loc else f" (via {t['location_id']})"
    v = (t.get("binding") or {}).get("tributaries")
    if v is not None:
        return v, "place" + via
    w = inp.waters.get(t.get("water_id") or "") or {}
    return w.get("tributaries"), ("water default" + via) if w.get("tributaries") is not None else "blank" + via


def wording(loc: Mapping) -> str:
    return (loc.get("source_text") or {}).get("specific_area") or ""


def heading(loc: Mapping) -> str:
    return (loc.get("source_text") or {}).get("waters") or loc.get("water") or ""


def F_(inp: Inputs, section: str, kind: str, place: str, found: str, *, lines=(), what: str = "") -> Finding:
    loc = inp.places.get(place) or {}
    return Finding(section=section, kind=kind, place=place, found=found, region=str(loc.get("region") or ""),
                   water=loc.get("water") or "", wording=wording(loc), lines=tuple(lines), what=what)


def _status(loc: Mapping) -> str:
    return "" if loc.get("status") == "active" else f" [{loc.get('status')}]"


# =============================================================================================== A

def check_A(inp: Inputs) -> list[Finding]:
    out = []
    hist = history(inp)
    for lid, saves in sorted(hist.items()):
        cur = in_force(saves)
        live = [s for s in since_clear(saves) if s[1].get("verdict") != "cleared"]
        if cur is None or len(live) < 2:
            continue
        ln, m = cur
        earlier = [s for s in live if s[0] != ln]
        loc = inp.places.get(lid)
        if loc is None:
            out.append(F_(inp, "A", "place_gone", lid, "saved more than once, but no such place in the files",
                          lines=[s[0] for s in live]))
            continue
        b = loc.get("binding") or {}
        dup = loc.get("duplicate_of")
        old_link = [n for n, e in earlier if e.get("variant_of") and e.get("variant_of") == dup]
        if dup and loc.get("duplicate_confirmed") is True and not m.get("variant_of") and old_link:
            out.append(F_(inp, "A", "leftover_link", lid,
                          f"`duplicate_of {dup}` (confirmed) was written by line {old_link[-1]}; the last save "
                          f"(line {ln}, {m.get('verdict')}) neither keeps nor removes it, so the place still rides "
                          f"{dup}'s extent ({words((inp.places.get(dup) or {}).get('binding', {}).get('extents') or [])})",
                          lines=[*old_link, ln]))
        old_tidal = [n for n, e in earlier if e.get("call") == "tidal water"]
        if loc.get("tidal_water") and m.get("call") != "tidal water" and old_tidal:
            out.append(F_(inp, "A", "leftover_tidal", lid,
                          f"`tidal_water` was written by line {old_tidal[-1]}; the last save (line {ln}) does not say "
                          "tidal water", lines=[*old_tidal, ln]))
        ok = fix_matches(m, b.get("extents") or [])
        if ok is False and not pending_cuts(m):
            out.append(F_(inp, "A", "not_last_save", lid,
                          f"the file holds {words(b.get('extents') or [])}; the last save (line {ln}) says "
                          f"{words(fix_extents(m) or [])} (see I when it waits)", lines=[ln]))
        elif ok is False:
            out.append(F_(inp, "A", "last_save_waits_cut", lid,
                          f"the last save (line {ln}) names a new cut not built yet; the file holds "
                          f"{words(b.get('extents') or [])} until it lands (see I)", lines=[ln]))
        last_t = {f.get("tributaries") for f in m.get("extent_fix") or ()} - {None}
        held = b.get("tributaries")
        wdef = (inp.waters.get(loc.get("water_id") or "") or {}).get("tributaries")
        older_t = [n for n, e in earlier for f in e.get("extent_fix") or () if f.get("tributaries") == held]
        if not last_t and held is not None and held != wdef and older_t:
            out.append(F_(inp, "A", "leftover_tributaries", lid,
                          f"tributaries {held} on the place came from line {older_t[-1]}; the last save (line {ln}) "
                          f"makes no tributary call (the water's default is {wdef})", lines=[older_t[-1], ln]))
    # cuts made from marks that nothing uses any more
    places_text = json.dumps([p.get("binding") for p in inp.places.values()])
    in_force_ids = {p.get("split_id") for s in hist.values() if in_force(s)
                    for f in in_force(s)[1].get("extent_fix") or () for p in f.get("points") or ()}
    for cid, c in sorted(inp.cuts.items()):
        if not str(c.get("_note") or "").startswith(MARK_CUT_NOTE):
            continue
        q = f'"{cid}"'
        if q in places_text or q in inp.synopsis_text or q in inp.tidal_limits_text or cid in in_force_ids:
            continue
        older = sorted({n for n, m in inp.lines for f in m.get("extent_fix") or () for p in f.get("points") or ()
                        if p.get("split_id") == cid})
        out.append(Finding(section="A", kind="orphan_cut", place=f"(cut) {cid}", water=c.get("_water") or "",
                           lines=tuple(older), what=cid,
                           found=("made from a mark; no place, rule or tidal limit names it now"
                                  + (f"; only superseded saves named it (lines {', '.join(map(str, older))})"
                                     if older else ""))))
    return out


def counts_A(inp: Inputs) -> dict:
    hist = history(inp)
    live = {k: [s for s in since_clear(v) if s[1].get("verdict") != "cleared"] for k, v in hist.items()}
    multi = {k: v for k, v in live.items() if len(v) >= 2 and in_force(hist[k])}

    def same(v):
        keep = ("verdict", "answers", "extent_fix", "new_splits", "call", "variant_of", "not_variant_of", "tributaries")
        return len({json.dumps({k: m.get(k) for k in keep}, sort_keys=True) for _, m in v}) == 1
    return {"places with a mark in force": sum(1 for v in hist.values() if in_force(v)),
            "places saved 2 or more times": len(multi),
            "... every save the same": sum(1 for v in multi.values() if same(v)),
            "cuts made from marks": sum(1 for c in inp.cuts.values()
                                        if str(c.get("_note") or "").startswith(MARK_CUT_NOTE))}


# =============================================================================================== B

def check_B(inp: Inputs) -> list[Finding]:
    out = []
    for lid, saves in sorted(history(inp).items()):
        cur = in_force(saves)
        if cur is None or cur[1].get("verdict") not in ("wrong", "unsure"):
            continue
        ln, m = cur
        loc = inp.places.get(lid)
        if loc is None:
            continue
        t = target_of(inp, loc)
        ex = (t.get("binding") or {}).get("extents") or []
        if not ex:
            continue
        if fix_matches(m, ex):
            continue                                     # the save's own fix is what is in force
        src = next((n for n, e in reversed(saves[:-1]) if fix_matches(e, ex)), None)
        how = ("with a fix that has not landed" if m.get("extent_fix")
               else f"with no fix ({OLD_EXTENT_FLAG})" if old_extent_in_force(m, ex) else "with no fix")
        via = "" if t is loc else f" (it rides {t['location_id']})"
        out.append(F_(inp, "B", m["verdict"], lid,
                      f"last save line {ln}: {m['verdict']} {how}; in force{via}: {words(ex)}, "
                      + (f"set by line {src}" if src else "from before any save of this place")
                      + f"; ships: yes{_status(loc)} — note: {(m.get('note') or '').strip()[:160]}",
                      lines=[x for x in (src, ln) if x]))
    return out


# =============================================================================================== C

def check_C(inp: Inputs) -> list[Finding]:
    out = []
    hist = history(inp)
    for lid, loc in sorted(inp.places.items()):
        if loc.get("kind") != "water":
            continue
        t = target_of(inp, loc)
        has_extent = bool((t.get("binding") or {}).get("extents"))
        if loc.get("status") != "active" and not has_extent:
            continue
        w, h = wording(loc), heading(loc)
        mean = trib_meaning(w)
        eff, src = tribs_in_force(inp, loc)
        only = bool((t.get("binding") or {}).get("tributaries_only"))
        cur = in_force(hist.get(lid, []))
        lines = [cur[0]] if cur else []
        state = f"in force: tributaries {eff} ({src}){', tributaries only' if only else ''}"
        if mean == "included" and eff is not True:
            out.append(F_(inp, "C", "C1_included", lid, f"words '{says('trib_included', w)}'; {state}", lines=lines))
        if says("heading_only", h) and not says("heading_included", h) and not only and mean != "only":
            out.append(F_(inp, "C", "C2_heading_only", lid,
                          f"heading '{h}' is the tributaries; the place is not the tributaries-only form; {state}",
                          lines=lines))
        elif says("heading_included", h) and eff is not True:
            out.append(F_(inp, "C", "C2_heading", lid,
                          f"heading '{h}' takes tributaries in; {state}", lines=lines))
        if mean == "excluded" and eff is not False:
            out.append(F_(inp, "C", "C3_mainstem", lid,
                          f"words '{says('trib_excluded', w)}'; {state}"
                          + ("" if eff else " (blank, not false)"), lines=lines))
        if mean == "only" and not only:
            mk = cur[1] if cur else None
            fixes = (mk or {}).get("extent_fix") or []
            if mk is None:
                ms = "unmarked"
            elif any(f.get("tributaries_only") is True for f in fixes):
                ms = f"the last mark (line {cur[0]}) says tributaries only: not landed (see I)"
            elif any("tributaries_only" in f for f in fixes):
                ms = f"the last mark (line {cur[0]}) says NOT tributaries only"
            else:
                ms = f"the last mark (line {cur[0]}) is not the tributaries-only form"
            out.append(F_(inp, "C", "C4_only", lid, f"words '{says('trib_only', w)}'; {state}; {ms}", lines=lines))
        if eff is None and has_extent and loc.get("status") == "active":
            out.append(F_(inp, "C", "C5_blank", lid, f"no tributary choice anywhere; {state}", lines=lines))
    return out


# =============================================================================================== D

def check_D(inp: Inputs) -> list[Finding]:
    from pipeline.regs.dfo_salmon import entries as E
    out = []
    by_region: dict[str, list[dict]] = defaultdict(list)
    for loc in inp.places.values():
        by_region[loc["_file"]].append(loc)
    drawable = set(inp.split_pos) if inp.split_pos is not None else set(inp.cuts)
    if inp.tidal:
        drawable |= {t["id"] for t in inp.tidal.values() if t.get("id")}
        drawable |= {t["alias"] for t in inp.tidal.values() if t.get("alias")}
    rests: dict[str, list[str]] = defaultdict(list)
    for f in sorted(by_region):
        objs = {x["location_id"]: E.EntryLocation.from_dict({k: v for k, v in x.items() if k != "_file"})
                for x in by_region[f]}
        for lid, loc in sorted(objs.items()):
            ops = {e.op for e in loc.binding.extents}
            kind = "rest" if "rest" in ops else "other_places" if "other_places" in ops else None
            if not kind:
                continue
            if kind == "rest":
                rests[loc.water_id or ""].append(lid)
            w = inp.waters.get(loc.water_id or "")
            wb = E.WaterBinding(**w) if w else None
            entry, _ = E.to_reach_input(loc, [{}], wb, siblings=objs)
            sibs = [r["rule_id"] for r in (entry.get("rules") or [])[1:]]
            desc = ", ".join(f"{s}{_status(inp.places[s])}" for s in sibs) or "none"
            out.append(F_(inp, "D", f"listed_{kind}", lid, f"{kind}; {len(sibs)} sibling(s): {desc}"))
            for s in sibs:
                sp = inp.places[s]
                if sp.get("status") != "active":
                    out.append(F_(inp, "D", "dormant_sibling", lid, what=s,
                                  found=f"dormant sibling {s} still "
                                        + ("takes its water out of the rest" if kind == "rest" else "adds its water")))
                bad = [x for e in (sp.get("binding") or {}).get("extents") or () for x in e.get("splits") or ()
                       if x not in drawable]
                if bad or not (wb and wb.item_ids):
                    why = (f"names cut(s) {', '.join(bad)} the atlas does not hold" if bad
                           else "its water has no registry item")
                    out.append(F_(inp, "D", "undrawable_sibling", lid, what=s,
                                  found=f"sibling {s} cannot be drawn ({why})"
                                        + ("; the whole rest goes unmapped (complement_unknown)" if kind == "rest"
                                           else "; it adds nothing to the union")))
    for wid, ids in sorted(rests.items()):
        if len(ids) > 1:
            for lid in ids:
                out.append(F_(inp, "D", "two_rests", lid, what=wid,
                              found=f"{len(ids)} rest places on {wid}: {', '.join(ids)} (each skips the other: both "
                                    "cover the same remainder)"))
    return out


# =============================================================================================== E

def check_E(inp: Inputs, near_m: float = NEAR_M) -> list[Finding]:
    if inp.split_pos is None or inp.tidal is None:
        return []
    out = []
    flagged = set()
    users: dict[str, list[str]] = defaultdict(list)
    for lid, loc in sorted(inp.places.items()):
        for e in (loc.get("binding") or {}).get("extents") or ():
            for s in e.get("splits") or ():
                users[s].append(lid)
    for lid, loc in sorted(inp.places.items()):
        w = wording(loc)
        for s in sorted({s for e in (loc.get("binding") or {}).get("extents") or () for s in e.get("splits") or ()}):
            pos = inp.split_pos.get(s)
            if not pos:
                continue
            blk, m, label = pos
            t = inp.tidal.get(blk)
            if not t or t["limit"] == "whole" or s in (t["id"], t.get("alias")):
                continue
            d = None if t["measure"] is None else abs(m - t["measure"])
            mouthy = says("mouth", s.replace("_", " ")) or says("mouth", label)
            if mouthy or (d is not None and d <= near_m):
                flagged.add(s)
                out.append(F_(inp, "E", "not_tidal_boundary", lid, what=s,
                              found=f"uses cut `{s}` ({label}, m {m:.1f} on {blk}); the stream's tidal boundary is "
                                    f"`{t['id']}` ({t['tier']}, m {t['measure'] if t['measure'] is None else round(t['measure'], 1)})"
                                    + (f", {d:.1f} m away" if d is not None else "")
                                    + (f"; words '{says('mouth', w)}'" if says("mouth", w) else "")))
    for cid, c in sorted(inp.cuts.items()):
        if cid in flagged or not str(c.get("_note") or "").startswith(MARK_CUT_NOTE):
            continue
        pos = inp.split_pos.get(cid)
        t = inp.tidal.get(pos[0]) if pos else None
        if not t or t["measure"] is None or cid in (t["id"], t.get("alias")):
            continue
        d = abs(pos[1] - t["measure"])
        if d <= near_m:
            out.append(Finding(section="E", kind="mark_cut_near_tidal", place=f"(cut) {cid}", what=cid,
                               water=c.get("_water") or "",
                               found=f"made from a mark, {d:.1f} m from the tidal boundary `{t['id']}` ({t['tier']}); "
                                     f"used by: {', '.join(users.get(cid, [])) or 'nothing'}"))
    for lid, saves in sorted(history(inp).items()):
        cur = in_force(saves)
        if not cur:
            continue
        for n in cur[1].get("new_splits") or ():
            text = " ".join(str(n.get(k) or "") for k in ("name", "anchor", "quote", "ref"))
            if says("mouth", text):
                out.append(F_(inp, "E", "pending_mouth_cut", lid, lines=[cur[0]], what=str(n.get("ref")),
                              found=f"the last save proposes a new cut by mouth words ('{says('mouth', text)}', ref "
                                    f"{n.get('ref')}); not built: check it is not a second tidal boundary"))
    built = set(inp.tidal)
    for r in inp.tidal_rows:
        if str(r.get("blk")) not in built:
            out.append(Finding(section="E", kind="atlas_behind", place=f"(tidal) {r.get('blk')}", what=str(r.get("blk")),
                               water=r.get("name") or "",
                               found=f"tidal_limits.json row ({r.get('limit')}) is not in the atlas yet: rebuild"))
    return out


# =============================================================================================== F

def channel_index(registry: Mapping[str, dict]) -> dict[str, list[str]]:
    """river gnis item -> the OTHER items FWA codes as its own (their `ref_ids` carry the river's gnis: a side channel,
    named by curation or gazetted, that inherits the river's name on its lines)."""
    out: dict[str, list[str]] = defaultdict(list)
    for iid, it in registry.items():
        if it.get("kind") not in ("stream", "lake"):
            continue
        for r in it.get("ref_ids") or ():
            if r.startswith("gnis:") and r != iid and r in registry:
                out[r].append(iid)
    return {k: sorted(v) for k, v in out.items()}


def _attach(graph, start: Iterable[str], main_blk: str, depth: int = 4) -> list[float]:
    """Where a channel's pieces meet `main_blk` (measures on it), walking at most `depth` pieces through other lines."""
    seen, frontier, got = set(start), list(start), []
    for _ in range(depth):
        nxt = []
        for n in frontier:
            for i in graph.down_adj.get(n, ()):
                e = graph.edges[i]
                to = graph.nodes.get(e.to_node)
                if to is not None and to.blk == main_blk:
                    got.append(float(e.at_measure))
                elif e.to_node not in seen:
                    seen.add(e.to_node)
                    nxt.append(e.to_node)
            for i in graph.up_adj.get(n, ()):
                e = graph.edges[i]
                fr = graph.nodes.get(e.from_node)
                if fr is not None and fr.blk == main_blk:
                    got.append(float(fr.down_m))
                elif e.from_node not in seen:
                    seen.add(e.from_node)
                    nxt.append(e.from_node)
        frontier = nxt
    return got


def _window(e: Mapping, split_pos: Mapping) -> tuple[Optional[str], Optional[tuple[float, float]], str]:
    """(main blk, (lo, hi) on it or None = the whole river, a note)."""
    sp = [split_pos.get(s) for s in e.get("splits") or ()]
    if not sp:
        return None, None, ""
    if any(p is None for p in sp):
        return None, None, "a cut is not built: checked against the whole river"
    blks = {p[0] for p in sp}
    if len(blks) > 1:
        return None, None, "its cuts are on two lines: checked against the whole river"
    ms = [p[1] for p in sp]
    op = e.get("op")
    lo, hi = ((ms[0], float("inf")) if op == "upstream_of" else (0.0, ms[0]) if op == "downstream_of"
              else (min(ms), max(ms)))
    return sp[0][0], (lo, hi), ""


def check_F(inp: Inputs) -> list[Finding]:
    if inp.registry is None or inp.graph is None or inp.split_pos is None:
        return []
    out = []
    chans = channel_index(inp.registry)
    own_water = defaultdict(list)
    for wid, w in inp.waters.items():
        for i in w.get("item_ids") or ():
            own_water[i].append(wid)
    tidal_items = {t.get("item") for t in (inp.tidal or {}).values() if t.get("limit") == "whole"}
    for lid, loc in sorted(inp.places.items()):
        w = inp.waters.get(loc.get("water_id") or "") or {}
        rec = set(w.get("item_ids") or ())
        for e in (loc.get("binding") or {}).get("extents") or ():
            if e.get("op") not in ("whole", "between", "upstream_of", "downstream_of"):
                continue
            restricted = e.get("item_ids") or ([e["item_id"]] if e.get("item_id") else [])
            held = set(restricted) if restricted else rec
            main_blk, win, note = _window(e, inp.split_pos)
            for river in sorted(held):
                if (inp.registry.get(river) or {}).get("kind") != "stream":
                    continue
                for c in chans.get(river, ()):
                    if c in held or c in tidal_items:      # a channel tidal throughout is no DFO water (T632)
                        continue
                    it = inp.registry[c]
                    nodes = [n for n in it.get("section_ids") or () if n in inp.graph.nodes]
                    if win is not None:
                        at = _attach(inp.graph, nodes, main_blk)
                        if not any(win[0] - 1 <= m <= win[1] + 1 for m in at):
                            continue
                    km = sum(inp.graph.nodes[n].length_m for n in nodes) / 1000
                    why = "not in the extent's items (item-restricted)" if c in rec else "not in the water record"
                    extra = [f"own DFO water {', '.join(x for x in own_water[c])}"] if own_water.get(c) else []
                    tok = (it.get("name") or "").split(" ")[0].lower()
                    if tok and tok in wording(loc).lower():
                        extra.append("its name is in the wording")
                    out.append(F_(inp, "F", "dropped_channel" if not note else "dropped_channel_window_unknown",
                                  lid, what=c,
                                  found=f"drops {it.get('name')} ({c}, {len(nodes)} sections, {km:.2f} km) of {river}: "
                                        f"{why}" + "".join(f"; {x}" for x in extra) + (f"; {note}" if note else "")
                                        + _status(loc)))
    return out


# =============================================================================================== G

def check_G(inp: Inputs) -> list[Finding]:
    out = []
    cur = {lid: in_force(s) for lid, s in history(inp).items()}
    groups: dict[str, list[str]] = defaultdict(list)
    for lid, c in cur.items():
        if c and c[1].get("group"):
            groups[c[1]["group"]].append(lid)
    for g, members in sorted(groups.items()):
        texts = {m: wording(inp.places.get(m) or {}) for m in members}
        dist = {m: says("distance", t) for m, t in texts.items()}
        for lid in sorted(members):
            others = [m for m in sorted(members) if m != lid]
            loc = inp.places.get(lid) or {}
            note = ""
            if others and len({t.strip().lower() for t in texts.values()}) > 1:
                note = "; the members' wordings differ"
                if dist[lid] and not all(dist[m] == dist[lid] for m in others):
                    note += f" — this one names '{dist[lid]}', which the shared extent may not hold"
            out.append(F_(inp, "G", "group_mark", lid, lines=[cur[lid][0]],
                          found=f"in force: written in {g} with {len(others)} other(s)"
                                f"{': ' + ', '.join(others) if others else ''}{note}; extent "
                                f"{words((loc.get('binding') or {}).get('extents') or [])}{_status(loc)}"))
    return out


# =============================================================================================== H

def check_H(inp: Inputs) -> list[Finding]:
    out = []
    hist = history(inp)
    for lid, loc in sorted(inp.places.items()):
        b = loc.get("binding") or {}
        ex = b.get("extents") or []
        hit = says("one_bank", wording(loc))
        if not hit or not ex:
            continue
        if any(k in b for k in ("side", "bank")) or any(k in e for e in ex for k in ("side", "bank")):
            continue
        cur = in_force(hist.get(lid, []))
        out.append(F_(inp, "H", "whole_width", lid, lines=[cur[0]] if cur else [],
                      found=f"words '{hit}'; the extent {words(ex)} is the whole width (no bank field, T754)"
                            f"{_status(loc)}"))
    return out


# =============================================================================================== I

def _short(text: str) -> str:
    """Absolute paths cut to their file name, so a report does not depend on where the tree sits."""
    return re.sub(r"(?:/[^\s/:]+)+/([^\s/:]+)", r"\1", text)


def wait_category(row: Mapping) -> str:
    """What a not-landed row waits on, in a few words (I's grouping)."""
    why = str(row.get("why") or "")
    st = row.get("status")
    if row.get("_allowed"):
        return "allowed to wait (NOT_LANDED_OK)"
    if st == "stale":
        return "a file domain is stale (run the apply)"
    if st == "would":
        return "would land now (run the apply)"
    if "a cut changes" in why:
        return "a side rebuild (the cut changes)"
    if "waits on new split" in why:
        return "a new cut not applied yet"
    if st == "refused":
        head = _short(why).split("—")[0].split(";")[0].strip()
        return "refused: " + (head[:70] or "?")
    return f"{st}: {why.splitlines()[0][:70] if why else '?'}"


def check_I(inp: Inputs) -> list[Finding]:
    if inp.landed is None:
        return []
    out = []
    rows = list(inp.landed.get("not_landed") or ()) + [{**r, "_allowed": True} for r in inp.landed.get("allowed") or ()]
    for r in rows:
        cat = wait_category(r)
        for k in r.get("marks") or [r.get("id") or r.get("domain") or "?"]:
            scope, target = (k.split("|") + ["", ""])[:2]
            place = target if scope == "dfo" else k
            out.append(F_(inp, "I", cat, place, what=str(r.get("id") or k),
                          found=f"{r.get('status')} ({r.get('domain')}): {_short(str(r.get('why') or '')).splitlines()[0][:220]}"))
    return out


# =============================================================================================== J

def check_J(inp: Inputs) -> list[Finding]:
    out = []
    hist = history(inp)
    marked = {lid for lid, s in hist.items() if in_force(s)}
    for lid, loc in sorted(inp.places.items()):
        if loc.get("status") != "active" or lid in marked:
            continue
        d = loc.get("duplicate_of")
        if d and loc.get("duplicate_confirmed") is not False and d in marked:
            out.append(F_(inp, "J", "rides_marked", lid, found=f"unmarked; rides {d}, which is marked"))
            continue
        out.append(F_(inp, "J", "unmarked", lid,
                      found=f"unmarked ({loc.get('kind')}); extent "
                            f"{words(((target_of(inp, loc)).get('binding') or {}).get('extents') or [])}"))
    return out


# =============================================================================================== K

def _base_cut(inp: Inputs, cid: str) -> Optional[str]:
    """The plain cut an offset cut is offset from (same anchor, no offset), when it exists."""
    a = (inp.cuts.get(cid) or {}).get("anchor") or {}
    if not a.get("offset_m"):
        return None
    key = {k: v for k, v in a.items() if k not in ("offset_m", "offset_dir") and not k.startswith("_")}
    for oid, o in sorted(inp.cuts.items()):
        oa = o.get("anchor") or {}
        if oid != cid and not oa.get("offset_m") and {k: v for k, v in oa.items() if not k.startswith("_")} == key:
            return oid
    return None


def check_K(inp: Inputs) -> list[Finding]:
    out = []
    hist = history(inp)
    for lid, loc in sorted(inp.places.items()):
        sids = [s for e in (loc.get("binding") or {}).get("extents") or () for s in e.get("splits") or ()]
        if not sids:
            continue
        w = wording(loc)
        dist = says("distance", w)
        offset = [s for s in sids if ((inp.cuts.get(s) or {}).get("anchor") or {}).get("offset_m")]
        cur = in_force(hist.get(lid, []))
        lines = [cur[0]] if cur else []
        if dist and not offset and not any("sign" in str((inp.cuts.get(s) or {}).get("label") or "").lower()
                                           for s in sids):
            out.append(F_(inp, "K", "K1_distance_no_offset", lid, lines=lines,
                          found=f"words '{dist}'; no cut of {', '.join(sids)} is offset{_status(loc)}"))
        if not dist:
            for s in offset:
                b = _base_cut(inp, s)
                if b:
                    out.append(F_(inp, "K", "K2_offset_not_worded", lid, lines=lines, what=s,
                                  found=f"uses offset cut `{s}` ({(inp.cuts[s].get('anchor') or {}).get('offset_m')} m "
                                        f"{(inp.cuts[s].get('anchor') or {}).get('offset_dir')}); the wording names no "
                                        f"distance and the plain cut `{b}` exists{_status(loc)}"))
    return out


CHECKS: dict[str, Callable[[Inputs], list[Finding]]] = {
    "A": check_A, "B": check_B, "C": check_C, "D": check_D, "E": check_E, "F": check_F, "G": check_G, "H": check_H,
    "I": check_I, "J": check_J, "K": check_K}


def run_checks(inp: Inputs) -> list[Finding]:
    got = [f for s in CHECKS for f in CHECKS[s](inp)]
    return sorted(got, key=lambda f: (f.section, f.kind, f.place, f.what))


# =============================================================================================== report

def _cell(s: str, n: int = 160) -> str:
    s = " ".join(str(s).split()).replace("|", "\\|")
    return s if len(s) <= n else s[:n - 1] + "…"


def _table(fs: list[dict]) -> list[str]:
    out = ["| place | region | water | DFO wording | mark line(s) | what was found |", "|---|---|---|---|---|---|"]
    for f in fs:
        out.append(f"| `{_cell(f['place'], 80)}` | {f['region']} | {_cell(f['water'], 50)} | {_cell(f['wording'])} | "
                   f"{', '.join(map(str, f['lines'])) or '–'} | {_cell(f['found'], 400)} |")
    return out


def diff(cur: list[dict], prev: list[dict]) -> dict:
    pk = {f["key"] for f in prev}
    ck = {f["key"] for f in cur}
    return {"new": [f for f in cur if f["key"] not in pk], "resolved": [f for f in prev if f["key"] not in ck]}


def document(inp: Inputs, findings: list[Finding], prev: Optional[dict] = None) -> tuple[str, dict]:
    """(markdown, json document) — deterministic: sorted, no clock."""
    cur = [f.to_dict() for f in findings]
    doc: dict = {"format": FORMAT, "sources": inp.sources, "skipped": inp.skipped, "counts_A": counts_A(inp),
                 "counts": {s: sum(1 for f in cur if f["section"] == s) for s in SECTIONS}, "findings": cur}
    d = diff(cur, prev.get("findings") or []) if prev is not None else None
    if d is not None:
        doc["since"] = {"new": [f["key"] for f in d["new"]], "resolved": [f["key"] for f in d["resolved"]]}
    md = ["# Final DFO marks audit", "",
          "Read-only. Every row is a **candidate for the owner**, not a verdict. Made by "
          "`python -m pipeline.regs.review.audit` (deterministic: the same inputs give this same report).", "",
          "## Inputs", ""] + [f"- {k}: `{v}`" for k, v in sorted(inp.sources.items())] + [""]
    if inp.skipped:
        md += ["**Not checked:**", ""] + [f"- {s}: {why}" for s, why in sorted(inp.skipped.items())] + [""]
    md += ["## What ships", "", SHIP_RULE, "", "## Counts", "", "| section | findings" + (" | new | resolved" if d else "")
           + " |", "|---|---" + ("|---|---" if d else "") + "|"]
    for s, (title, _) in SECTIONS.items():
        n = doc["counts"][s]
        row = f"| {s}. {title} | {n}"
        if d:
            row += (f" | {sum(1 for f in d['new'] if f['section'] == s)}"
                    f" | {sum(1 for f in d['resolved'] if f['section'] == s)}")
        md.append(row + " |")
    md += ["", "Section A, the marks:", ""] + [f"- {k}: {v}" for k, v in doc["counts_A"].items()] + [""]
    method = {
        "D": "Siblings are what `entries.to_reach_input` picks (the reach run's own code). 'Cannot be drawn' = names a "
             "cut the atlas does not hold, or a water with no registry item; a sibling that binds zero sections on the "
             "graph is not detected here.",
        "E": f"Method (from mouth-tidal-ends-audit-2026-11-15.md): the boundary is the atlas's `tidal.resolved.json` "
             f"(rank-1 split, else the coast cut in `shore_cuts.json`, else the mouth); a place cut that is not it is "
             f"listed when its id/label has mouth words or it is within {NEAR_M:.0f} m on the same line. An open "
             "seaward end ('downstream of X', 'whole') lands on the boundary by construction (T632) and is not listed. "
             "New cuts not built yet cannot be measured: they are listed by their words only.",
        "F": "A channel = a registry item whose `ref_ids` carry the river's gnis; 'beside the place' = it meets the "
             "river's line (graph edges, up to 4 pieces through other lines) inside the extent's cut window. A channel "
             "tidal throughout (a `whole` tidal row) is left out. `dropped_channel_window_unknown`: the window could "
             "not be placed on one line (a cut not built, or cuts on two lines), so the channel was checked against the "
             "whole river — read those with the map.",
        "I": "Rows are `apply.check` (imported, the same as `apply --check`), one per mark behind each row.",
    }
    for s, (title, plain) in SECTIONS.items():
        fs = [f for f in cur if f["section"] == s]
        md += [f"## {s}. {title}", "", plain, ""]
        if s in method:
            md += [method[s], ""]
        if s in inp.skipped:
            md += [f"**Not run:** {inp.skipped[s]}", ""]
        if s == "J":
            byr: dict[str, int] = defaultdict(int)
            for f in fs:
                if f["kind"] == "unmarked":
                    byr[f["region"]] += 1
            if byr:
                md += ["| region | unmarked (standing alone) |", "|---|---|"]
                md += [f"| {r} | {n} |" for r, n in sorted(byr.items())] + [""]
        show = [f for f in d["new"] if f["section"] == s] if d else fs
        gone = [f for f in d["resolved"] if f["section"] == s] if d else []
        if d:
            md += [f"{len(fs)} in all; {len(show)} new, {len(gone)} resolved since the previous run.", ""]
        if not show and not gone:
            md += ["Nothing found." if not d else "", ""]
            continue
        kinds = sorted({f["kind"] for f in show})
        for k in kinds:
            rows = [f for f in show if f["kind"] == k]
            md += [f"### {k} ({len(rows)}){' — new' if d else ''}", ""] + _table(rows) + [""]
        if gone:
            md += [f"### resolved ({len(gone)})", ""] + _table(gone) + [""]
    return "\n".join(md).rstrip() + "\n", doc


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.audit", description=__doc__.split("\n")[0])
    ap.add_argument("--out", type=Path, required=True, help="the report (.md); its JSON is written beside it")
    ap.add_argument("--since", type=Path, help="a previous run's JSON: show only new and resolved findings")
    ap.add_argument("--build", type=Path, help="the atlas (default: the served one, GENERATED)")
    ap.add_argument("--marks", type=Path)
    ap.add_argument("--places", type=Path)
    ap.add_argument("--cuts", type=Path)
    ap.add_argument("--tidal-limits", type=Path)
    ap.add_argument("--synopsis", type=Path)
    ap.add_argument("--no-graph", action="store_true", help="skip F (no registry / graph load: light)")
    ap.add_argument("--no-landed", action="store_true", help="skip I (apply.check loads the graph too)")
    a = ap.parse_args(argv)
    try:
        prev = json.loads(a.since.read_text(encoding="utf-8")) if a.since else None
        if prev is not None and prev.get("format") != FORMAT:
            raise ValueError(f"{a.since}: format {prev.get('format')!r}, expected {FORMAT}")
        inp = load(marks=a.marks, places=a.places, cuts=a.cuts, tidal_limits=a.tidal_limits, synopsis=a.synopsis,
                   build=a.build, with_graph=not a.no_graph, with_landed=not a.no_landed)
        md, doc = document(inp, run_checks(inp), prev)
    except Exception as ex:
        print(f"audit: cannot run — {type(ex).__name__}: {ex}", file=sys.stderr)
        return 2
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(md, encoding="utf-8")
    js = a.out.with_suffix(".json")
    js.write_text(json.dumps(doc, indent=1, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    print(f"audit: {sum(doc['counts'].values())} finding(s) -> {a.out} and {js}"
          + (f" ({len(doc['since']['new'])} new, {len(doc['since']['resolved'])} resolved)" if prev else ""))
    for s, n in doc["counts"].items():
        print(f"  {s}. {SECTIONS[s][0]}: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
