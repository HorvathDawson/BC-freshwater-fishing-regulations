"""The wire projection: rule objects -> rule ids, infinity -> null (`res_out`, `fact_out`, `cond_out`), and a
row's badge and scope (`badge_of`, `scope_of`)."""
from __future__ import annotations

from typing import Optional
from pipeline.deliver.answers.rows.rule import INF, R, _covers


def _num(v):
    if v is None or v == INF:
        return None
    if isinstance(v, float) and v.is_integer():
        return int(v)
    return v


_LINE_FIELDS = ("a", "b", "take", "o", "keepO", "status", "daily", "min", "max", "says", "only",
                "capped", "carve")


def line_out(l: dict) -> dict:
    out = {"t": l["t"], "r": l["r"].key}
    for k in _LINE_FIELDS:
        if k in l:
            out[k] = _num(l[k]) if k in ("a", "b", "take", "daily", "min", "max") else l[k]
    if "outer" in l:
        out["outer"] = l["outer"].key
    return out


def stored_role(P: "Part", key: str, role: str) -> str:
    """rows 31 (owner rule 2026-10-21; page v36 ladderHtml): a RECORD-IT RULE THAT DECIDES IS A DUTY. A rule with
    `record_retention` holding a deciding role (display `WINS`: Kootenay Lake's "Write each rainbow trout over 50 cm
    on your licence", Shuswap's two stamp records, both `floor`) is stored as `duty`, so a row's "How this was
    decided" lists it under "Other rules" — page v36 moved it there in the page; the app only reads the role. The
    computation keeps its own role (sizes, totals); only the stored answer says duty."""
    from pipeline.deliver.answers.display import WINS
    if role not in WINS or role == "duty":
        return role
    e, r = key.split("::", 1)
    return "duty" if P.B.rules[(e, r)].get("record_retention") else role


def res_out(res: Optional[dict], P: "Part") -> Optional[dict]:
    if res is None:
        return None
    return {"status": res["status"], "win": res["win"].key, "daily": _num(res["daily"]),
            "narrow": res["narrow"].key if res["narrow"] else None,
            "lines": [line_out(l) for l in res["lines"]],
            "roles": [[k, stored_role(P, k, v["role"]), v["by"]] for k, v in res["roles"].items()],
            "lift_notes": [[n["by"], n["q"]] for n in res["liftNotes"]]}


def fact_out(l: dict) -> dict:
    out = line_out(l)
    out["members"] = list(l["members"])
    out["rules"] = [r.key for r in l["rules"]]
    if l.get("general"):
        out["general"] = True
    if l.get("carveOf") is not None:
        out["carve_of"] = [_num(l["carveOf"]["a"]), _num(l["carveOf"]["b"]),
                           _num(l["carveOf"]["take"])]
    return out


def cond_out(c: dict) -> dict:
    out = {}
    for k, v in c.items():
        if k == "r":
            out["r"] = v.key if v is not None else None
        elif k in ("a", "b", "take", "sub", "daily", "min", "max"):
            out[k] = _num(v)
        elif k == "keep":
            out[k] = [{kk: _num(vv) if kk in ("a", "b") else vv for kk, vv in p.items()}
                      for p in v]
        else:
            out[k] = v
    return out


def badge_of(r: R) -> str:
    """Whose count a pool is (M2): from the rule's two typed axes (`read.source_of`: authority
    and scope), never the folded rank — a rule reached by the tributary walk is the water's; an
    area's pool is the area's; a region-wide one the region's, or B.C.'s when the province
    wrote it; a superior authority's (a national park's) is the place it names: its area, or the
    water."""
    from pipeline.deliver.bundle import read as RD
    src = RD.source_of(r.x)
    if r.via == "trib" or src.scope in (RD.Scope.water, RD.Scope.inherited):
        return "water"
    if src.scope is RD.Scope.area:
        return "area"
    return "bc" if src.authority is RD.Authority.province else "region"


def scope_of(P: "Part", row: dict) -> Optional[dict]:
    """The row badge (5.5): whose count the pool is — this water's, an area's (decision F8:
    Haida Gwaii, Bowron Lake Park, the Liard watershed), the region's or B.C.'s — whether a
    stream (or lake) share of it holds here, and (decision F7) whether the row's fish are
    counted apart from a bigger total: the pool lifts a wider pool that binds here and no
    `outer` line (a total they still count toward) is left on the row."""
    if not row.get("pool") or row["kind"] != "keep":
        return None
    p = row["pool"]
    who = badge_of(p)
    n = row["narrow"]
    share = bool(who != "water" and n is not None and n.f.get("water")
                 and n.f["water"] == P.kind and n.f.get("take") < p.f.get("take"))
    apart = False
    if who in ("water", "area"):
        lifts = [P.by_key.get(f"{e['entry_id']}::{e['rule_id']}") for e in p.f.get("exempts") or []]
        outer = any(l["t"] == "outer" for l in
                    row["everyone"] + [l for g in row["groups"] for l in g["facts"]])
        apart = any(t is not None and t.k == "pool" and t.rank >= 2 for t in lifts) and not outer
    # rows 31 (owner rule 2026-10-21; page v36 narrowAll): the narrowing clause binds EVERY fish of the row — the
    # badge then says "All Region 2 streams", not "Region 2-wide" (the app reads it, never re-derives it)
    narrow_all = n is not None and all(_covers(n, S) for S in row["members"])
    return {"of": who, "entry": p.entry_id if who != "water" else None, "share": share,
            "apart": apart, "narrow_all": narrow_all}
