"""What an ANGLER is told — the effective answer, read from the live bundle.

The rest of the app shows what an entry SAYS and where each rule BINDS. This shows what comes out
the other end: on one piece of water, on one day, for one fish, which rules speak, which stand
beside, which were displaced or lifted. It is `pipeline.deliver.bundle.read.effective_rules`, the
reference reader the app and the export restate (AGENTS 53) — never a copy of it.

IT READS THE BUNDLE, SO IT REFLECTS THE LAST BUNDLE BUILD, NOT THE ENTRY FILE. An edit saved here
changes the entry and the reaches the app computes from it at once; it reaches the bundle only
when the bundle is rebuilt. So every response says how the bundle's copy of this entry compares
with the file's (`bundle_matches`), and the UI says so instead of presenting a stale answer as
the edit's consequence.

`sid` is the bundle's own section handle. It is used only between this backend and its page,
within one bundle vintage, and is never stored (the verification sidecar keys on entry_id).
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
from functools import lru_cache

from pipeline.deliver.bundle import read as R

#: licensing record kinds that bind to sections, with their table and id column
_LIC_VIEWS = (("designation", "designation_section", "designation_id"),
              ("not_classified", "not_classified_section", "not_classified_id"),
              ("requirement", "requirement_section", "req_id"),
              ("alternative", "alternative_section", "alternative_id"))


def _db() -> sqlite3.Connection:
    return sqlite3.connect(f"file:{R.BUNDLE}?mode=ro", uri=True)


@lru_cache(maxsize=1)
def bundle_meta() -> dict:
    db = _db()
    try:
        meta = dict(db.execute("SELECT k, v FROM meta WHERE k != 'schema'"))
    finally:
        db.close()
    meta["built"] = dt.datetime.fromtimestamp(os.path.getmtime(R.BUNDLE)).strftime("%Y-%m-%d %H:%M")
    return meta


def entry_in_bundle(entry_id: str, labels_now: dict[str, str]) -> dict:
    """How the bundle's copy of this entry compares with the file's: each rule's label in the
    bundle beside the label generated from the file now, how many sections each rule binds in
    the bundle, and where each licensing record binds.

    `labels_now` is rule_id -> the label the app generates from the entry file. A rule present in
    one and not the other, or worded differently, means the bundle predates the file."""
    db = _db()
    try:
        b_rules = {rid: (lab, unres) for rid, lab, unres in db.execute(
            "SELECT rule_id, label, unresolved FROM rule WHERE entry_id = ?", (entry_id,))}
        n_by_rule = dict(db.execute(
            "SELECT r.rule_id, COUNT(*) FROM ruleset r JOIN section_ruleset s "
            "ON s.set_id = r.set_id WHERE r.entry_id = ? GROUP BY r.rule_id", (entry_id,)))
        rules = []
        for rid in list(labels_now) + [r for r in b_rules if r not in labels_now]:
            lab, unres = b_rules.get(rid, (None, None))
            rules.append({"rule_id": rid, "label_bundle": lab, "label_now": labels_now.get(rid),
                          "n_sections": n_by_rule.get(rid, 0), "unresolved": unres})
        lic = []
        for kind, view, idcol in _LIC_VIEWS:
            table = view.rsplit("_section", 1)[0]
            for rec_id, label, unres in db.execute(
                    f"SELECT {idcol}, label, unresolved FROM {table} WHERE entry_id = ?",
                    (entry_id,)):
                waters = db.execute(
                    f"SELECT i.name, COUNT(*) FROM {view} v JOIN item_section x ON x.sid = v.sid "
                    f"JOIN item i ON i.ord = x.ord WHERE v.entry_id = ? AND v.{idcol} = ? "
                    f"GROUP BY i.name ORDER BY COUNT(*) DESC, i.name", (entry_id, rec_id)).fetchall()
                n = db.execute(f"SELECT COUNT(DISTINCT sid) FROM {view} WHERE entry_id = ? "
                               f"AND {idcol} = ?", (entry_id, rec_id)).fetchone()[0]
                lic.append({"kind": kind, "id": rec_id, "label": label, "unresolved": unres,
                            "n_sections": n, "n_waters": len(waters),
                            "waters": [{"name": w, "n": c} for w, c in waters[:12]]})
        in_bundle = db.execute("SELECT 1 FROM entry WHERE entry_id = ?", (entry_id,)).fetchone()
    finally:
        db.close()
    matches = bool(in_bundle) and all(r["label_bundle"] == r["label_now"] for r in rules)
    return {"bundle": bundle_meta(), "in_bundle": bool(in_bundle), "bundle_matches": matches,
            "rules": rules, "licensing": lic}


def entry_waters(entry_id: str, q: str = "", limit: int = 60) -> list[dict]:
    """The waters this entry's rules reach IN THE BUNDLE, each split into its distinct PIECES — runs
    of sections that answer to the same set of rules. A piece is what an angler can stand on and
    get one answer; `sid` is a representative section of it, `rules` the entry's own rules there.

    Sections no named registry item holds (most of a tributary walk) are one water,
    "(unnamed streams)", so the pieces still add up to every section the entry binds.
    `q` filters by water name (a zone entry reaches thousands of waters)."""
    db = _db()
    try:
        rows = db.execute(
            "WITH mine AS (SELECT DISTINCT set_id FROM ruleset WHERE entry_id = ?) "
            "SELECT COALESCE(i.item_id, ''), COALESCE(i.name, '(unnamed streams)'), s.set_id, "
            "COUNT(*), MIN(s.sid) FROM section_ruleset s "
            "JOIN mine m ON m.set_id = s.set_id LEFT JOIN item_section x ON x.sid = s.sid "
            "LEFT JOIN item i ON i.ord = x.ord "
            + ("WHERE i.name LIKE ? " if q else "")
            + "GROUP BY i.item_id, s.set_id", (entry_id, f"%{q}%") if q else (entry_id,)).fetchall()
        sets = sorted({r[2] for r in rows})
        own: dict[int, list] = {}
        for i in range(0, len(sets), 500):
            chunk = sets[i:i + 500]
            for set_id, rid, via in db.execute(
                    f"SELECT set_id, rule_id, via FROM ruleset WHERE entry_id = ? AND set_id IN "
                    f"({','.join('?' * len(chunk))})", (entry_id, *chunk)):
                own.setdefault(set_id, []).append({"rule_id": rid, "via": via})
    finally:
        db.close()
    waters: dict[str, dict] = {}
    for item_id, name, set_id, n, sid in rows:
        w = waters.setdefault(item_id, {"item_id": item_id, "name": name, "n_sections": 0,
                                        "pieces": []})
        w["n_sections"] += n
        w["pieces"].append({"sid": sid, "n_sections": n,
                            "rules": sorted(own.get(set_id, []), key=lambda x: x["rule_id"])})
    out = sorted(waters.values(), key=lambda w: (-w["n_sections"], w["name"]))[:limit]
    for w in out:
        w["pieces"].sort(key=lambda p: -p["n_sections"])
    return out


def _parse_day(day: str) -> dt.date:
    try:
        return dt.date.fromisoformat(day)
    except ValueError as ex:
        raise ValueError(f"date must be YYYY-MM-DD, not {day!r}") from ex


def effective(sid: int, day: str, fish: str) -> dict:
    """The answer at one section, on one day, for one fish: `effective_rules`, each rule with its
    `state`, plus the licensing records bound there and whether the section is tidal."""
    on = _parse_day(day)
    got = R.effective_rules(int(sid), on, fish)     # raises ValueError on a group code
    db = _db()
    try:
        waters = [n for (n,) in db.execute(
            "SELECT i.name FROM item_section x JOIN item i ON i.ord = x.ord WHERE x.sid = ? "
            "ORDER BY i.name", (sid,))]
        tidal = db.execute("SELECT entry_id FROM tidal WHERE sid = ?", (sid,)).fetchone()
        lic = []
        for kind, view, idcol in _LIC_VIEWS:
            table = view.rsplit("_section", 1)[0]
            for eid, rec_id, via, label in db.execute(
                    f"SELECT v.entry_id, v.{idcol}, v.via, t.label FROM {view} v JOIN {table} t "
                    f"ON t.entry_id = v.entry_id AND t.{idcol} = v.{idcol} WHERE v.sid = ? "
                    f"ORDER BY v.entry_id, v.{idcol}", (sid,)):
                lic.append({"kind": kind, "entry_id": eid, "id": rec_id, "via": via,
                            "label": label})
    finally:
        db.close()
    rules = [{"entry_id": x["entry"], "rule_id": x["rule"], "entry_name": x.get("entry_name", ""),
              "state": x.get("state"), "type": x.get("type"), "label": x.get("label"),
              "verbatim": x.get("verbatim"), "via": x.get("via"),
              "undrawn_part": x.get("undrawn_part"), "standing": bool(x.get("standing"))}
             for x in got]
    return {"sid": int(sid), "date": on.isoformat(), "fish": fish, "waters": waters,
            "tidal": tidal[0] if tidal else None, "rules": rules, "licensing": lic,
            "bundle": bundle_meta()}
