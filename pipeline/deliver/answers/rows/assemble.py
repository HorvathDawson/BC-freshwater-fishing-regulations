"""`produce`: one part's card rows, assembled from the model, the conditions, the groups and the wire
projection."""
from __future__ import annotations

from typing import Dict
from pipeline.deliver.answers.rows.rule import INF
from pipeline.deliver.answers.rows.evaluate import fish_answer, row_restates
from pipeline.deliver.answers.rows.card_model import build_model
from pipeline.deliver.answers.rows.conds import eff_cap, keep_range, origin_lines, row_conds, species_items
from pipeline.deliver.answers.rows.part import Part
from pipeline.deliver.answers.rows.wire import _num, cond_out, fact_out, res_out, scope_of
from pipeline.deliver.answers.rows.groups import elsewhere_of, group_of, parent_of, total_of
from pipeline.deliver.answers.rows.presence import place_presence


#: T80 follow-up: the one-kind trout/char group is for a row DRAWN ON ITS OWN — one no
#: other pool holds (`parent_of`, rows 31; core `drawnRows` draws no article for a held row). Perry Creek's "brook trout
#: 20" row is held by the St. Mary's "trout/char 1 on all tributaries" row (which cross-references the brook trout):
#: a group there was never drawn, and the card worded it "1" through that cross-reference while the reader had
#: DISPLACED the St. Mary's 1 for brook trout (20 may be kept) — the card gate's 30 problems. The held row's fish stay
#: said inside the holding pool's article, as before T80. `model.RowsFrame._one_kind_groups` refuses the shape.
#: (T127 then removed that displaced cross-reference: Perry Creek's row is held by nothing now and is a group of 20.)
A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW = True


def produce(P: Part) -> dict:
    """Everything the card's rows show, for one part on one reading: per fish and origin the
    decided answer (`fish`), and the rows with their conditions, items and real daily limit."""
    model = build_model(P)
    fish = {S: fish_answer(res_out(v["H"], P), res_out(v["W"], P)) for S, v in model["R"].items()}
    # decision F14: every keep row's total; each total's group on ONE row — the row whose pool it
    # is, else the first row whose fish count toward it
    totals = {id(row): total_of(P, model, row) for row in model["rows"]
              if row.get("pool") is not None and row["kind"] in ("keep", "no_limit")}
    home: Dict[str, int] = {}
    for row in model["rows"]:
        T = totals.get(id(row))
        if T is not None and row["pool"] is T:
            home.setdefault(T.key, id(row))
    for row in model["rows"]:
        T = totals.get(id(row))
        if T is not None:
            home.setdefault(T.key, id(row))
    # rows 31: the pool row holding each pooled row (page v36 parentRow), decided once, before the groups — a row held
    # by another pool is drawn inside that pool's article, never as its own (`A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW`)
    stubs = [{"pool": row["pool"].key if row.get("pool") else None, "members": list(row["members"]),
              "all_members": list(row["allMembers"]),
              "groups": [{"members": g["members"], "facts": [fact_out(l) for l in g["facts"]]} for g in row["groups"]],
              # T139: the row's total, and the totals whose group it carries (`parent_of`'s second hold)
              "total": totals[id(row)].key if id(row) in totals else None,
              "draws": [k for k, h in home.items() if h == id(row)]}
             for row in model["rows"]]
    parents = {i: p for i in range(len(stubs)) if (p := parent_of(stubs, i)) is not None}
    own_row = {T.key for i, row in enumerate(model["rows"]) if (T := totals.get(id(row))) is not None
               and row["pool"] is T and not (A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW and i in parents)}
    groups = {k: group_of(P, model, P.by_key[k], own_row=k in own_row) for k in home}
    rows = []
    for row, stub in zip(model["rows"], stubs):
        o = {"kind": row["kind"], "pool": stub["pool"],
             "win": row["win"].key if row.get("win") else None,
             "members": stub["members"], "all_members": stub["all_members"],
             "daily": _num(row["daily"]),
             "narrow": row["narrow"].key if row.get("narrow") else None,
             "everyone": [fact_out(l) for l in row["everyone"]],
             "groups": stub["groups"],
             "prot": row.get("prot"), "wins": [r.key for r in row["wins"]] if row.get("wins") else None,
             "lift_notes": [[n["by"], n["q"]] for n in row["liftNotes"]],
             "scope": scope_of(P, row)}
        rs = row_restates(P, model, row)
        if rs:
            o["restates"] = rs
        if row.get("pool"):
            rc = row_conds(P, model, row)
            o["conds"] = [cond_out(c) for c in rc["conds"]]
            items = species_items(P, model, row, rc)
            o["items"] = []
            for it in items:
                q = it["q"]
                xref = it["xref"]
                io = {"members": list(it["members"]),
                      "bands": [[_num(x["a"]), _num(x["b"]), _num(x["mx"])] for x in q] if q else None,
                      "back": bool(it.get("back")), "xref": bool(xref),
                      "sub": _num(it["sub"]["sub"]) if it.get("sub") else None,
                      "conds": list(it["conds"])}
                if xref:
                    # decision F4; an unlimited outer total is the word, never null
                    io["against"] = "unlimited" if it["against"] == INF else _num(it["against"])
                if xref and len(it["members"]) == 1:
                    io["origins"] = [{k: _num(v) if k in ("n", "lo", "hi") else v
                                      for k, v in x.items()}
                                     for x in origin_lines(P, model, it["members"][0])]
                if xref and len(it["members"]) > 1:
                    # gap G4 (answers 2.3): a cross-reference of several fish carries EACH fish's
                    # keep range (`keep_range`, its own decided answer) — the page read it off the
                    # fish's own row; [fish, from_cm, to_cm | null (no top)], fish that keep none
                    # left out
                    io["ranges"] = [[S, _num(r[0]), None if r[1] == INF else _num(r[1])]
                                    for S in it["members"]
                                    for r in [keep_range(P, model, [S])] if r is not None]
                o["items"].append(io)
            ec = eff_cap(P, model, row, rc)
            o["real_daily"] = ec
            T = totals.get(id(row))
            if T is not None:                                  # decision F14
                grp = groups[T.key]
                o["group"] = grp if home[T.key] == id(row) else None
                o["elsewhere"] = elsewhere_of(P, model, row, o["scope"], T, grp) \
                    if o["scope"] is not None else None
        rows.append(o)
    from pipeline.deliver.answers.card import card_of                   # answers 2.8
    for i, o in enumerate(rows):
        if o.get("group") is not None:
            o["group"].update(card_of(P, model, o["group"], rows, i))
            # RULINGS P9: the card's one list holds the kept-elsewhere examples (`group.worked`)
            if o.get("elsewhere") is not None:
                o["elsewhere"]["ask"] = []
    for o in rows:
        if o.get("elsewhere") is not None:
            o["elsewhere"].pop("lines", None)
    # RULINGS P46: the pool row that holds a pooled row's fish (page v36 parentRow), decided here
    for i, p in parents.items():
        rows[i]["parent"] = p
    from pipeline.deliver.answers.display import steelhead_line
    has_row = not P.rules_off_in_export and any("ST" in r["allMembers"] for r in model["rows"])
    line = steelhead_line(P.presence, has_row)
    # RULINGS P23: where the whole water is closed to all fishing (every row closed: the Vedder on Jun
    # 15) there is no steelhead section to read, and no presence line is needed
    if not all(o["kind"] == "closed" for o in rows if o["all_members"]):
        place_presence(rows, line, P.steelhead_water)
    return {"spp": model["spp"], "fish": fish, "rows": rows, "steelhead_line": line}
