"""The consumer page v36's embedded data: a reproducible slice of the three live files.

REFERENCE ONLY (never shipped by the pipeline). Page v36 ("Quota Display Studies") reads every
displayed DECISION from the answers file (`ui-rules-answers.json`, format answers/2) and keeps the
export pair (`ui-rules-export.json` + `ui-rules-guide.json`) only for rule text, sources and
verbatims. This module cuts the three files down to the waters the page offers and writes the
page's three JSON blocks:

  data     the export slice: the shown waters (parts in the EXPORT's order, so a part index is
           the answers' part index), every rule and licensing record a shown part or an answers
           frame names (keyed "entry::rule" / "entry#record", with `rid` / `lid` mapping the
           answers' integer refs to those keys), entries, species, licences, tidal guide lines
  answers  the answers slice: the part keys of the shown parts (every section) and of the Checks
           cases' parts (ladder only — the other sections' `at` is null there), their segments,
           and only the frames, verdicts, decided answers, rows, holds and profile answers those
           keys reach — re-interned, every value VERBATIM
  cases    the guide's sample cases and the page's own 8 ("ours", remapped by `convert.py`),
           each with the export part it is read on, and the labels and ranks of the rules it
           lists

The pairing is refused unless the three files carry the same `about.bundle`, the answers'
`rule_ids_sha256` is the export's and the format is answers/2 (ANSWERS-SPEC §1).

Usage (no credits; nothing calls the claude CLI):
  python -m pipeline.deliver.answers.reference.page_data --out DIR          # DIR/{data,answers,cases}.json
  python -m pipeline.deliver.answers.reference.page_data --html PAGE.html   # rewrite the page's blocks
                                                                            # and its script (page_v36.js)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers.reference import convert
from pipeline.tools.export_codec import expand

HERE = Path(__file__).resolve().parent
EXPORT_DIR = convert.MAIN_TREE / "data/generated/regs"
JS = HERE / "page_v36.js"

#: The waters the page offers: v35's 28 (page order; Kootenay Lake's Lower West Arm among them), then
#: the ones added for the v36 review (Vedder River, Anderson River, Teslin Lake, Nitinat Lake — tidal).
EXTRA_WATERS = ["gnis:3062", "gnis:10030", "wbk:328961703", "wbk:329504244"]

RULE_KEEP = convert.RULE_KEEP
LIC_KEEP = convert.LIC_KEEP


class PairingError(Exception):
    pass


def load(export_dir: Path):
    raw = json.loads((export_dir / "ui-rules-export.json").read_text())
    guide = json.loads((export_dir / "ui-rules-guide.json").read_text())
    ans = json.loads((export_dir / "ui-rules-answers.json").read_text())
    return raw, guide, ans


def check_pair(raw: dict, guide: dict, ans: dict) -> None:
    """ANSWERS-SPEC §1: same bundle, same rules, same format — else refuse."""
    if ans["about"].get("format") != E.FORMAT:
        raise PairingError(f"answers format {ans['about'].get('format')!r} is not {E.FORMAT!r}")
    for name, other in (("ui-rules-export.json", raw), ("ui-rules-guide.json", guide)):
        if ans["about"]["bundle"] != other["about"]["bundle"]:
            raise PairingError(f"answers about.bundle differs from {name}'s")
    if ans["about"]["export"]["rule_ids_sha256"] != E.rule_ids_digest(raw["rule_ids"]):
        raise PairingError("answers rule_ids_sha256 is not the export's rule_ids")


class Interner:
    def __init__(self):
        self.rows, self._ix = [], {}

    def add(self, v) -> int:
        k = json.dumps(v, sort_keys=True, separators=(",", ":"))
        if k not in self._ix:
            self._ix[k] = len(self.rows)
            self.rows.append(v)
        return self._ix[k]


def _rule_refs_rows(v, out: set):
    """Every rule ref of a rows section value (decided answer or row), field by field."""
    if isinstance(v, dict):
        for k, x in v.items():
            if k in ("win", "narrow", "pool", "r", "outer") and isinstance(x, int):
                out.add(x)
            elif k in ("rules", "wins") and isinstance(x, list):
                out.update(i for i in x if isinstance(i, int))
            elif k == "roles":
                for a, _, b in x:
                    out.add(a)
                    if b is not None:
                        out.add(b)
            elif k == "lift_notes":
                for by, _ in x:
                    out.add(by)
            else:
                _rule_refs_rows(x, out)
    elif isinstance(v, list):
        for x in v:
            _rule_refs_rows(x, out)


def _rule_refs_gear(g: dict, out: set):
    if g.get("tidal"):
        return
    for c in g["counts"].values():
        out.add(c["by"][0])
        out.update(x[0] for x in c["over"])
        out.update(a["clause"][0] for a in c.get("also") or [])
    for L in g["specs"].values():
        out.update(x[0] for x in L)
    for e in g["elements"].values():
        out.add(e["by"][0])
        out.update(x[0] for x in e["over"])
    out.update(x[0] for x in g["main"])
    out.update(c["clause"][0] for c in g["circumstantial"])
    for b in g["bait"]:
        if b.get("by"):
            out.add(b["by"][0])
        out.update(a["clause"][0] for a in b.get("also_allowed") or [])
    for w in g["ways"]:
        if w.get("by"):
            out.add(w["by"][0])
        out.update(a["clause"][0] for a in w.get("while") or [])
        out.update(w.get("while_rules") or [])
        for d in (w.get("devices") or {}).values():
            out.update(x[0] for x in d["by"])
    for acts in g["conduct"].values():
        for _, rs in acts:
            out.update(rs)
    out.update(g["vessel"]["active"])
    out.update(g["vessel"]["timed"])
    for f in ("timed", "in_part", "side", "while_rules", "caught", "decides", "repeats"):
        out.update(g[f])
    for o in g["overruled"]:
        out.add(o["rule"])
        if o.get("by") is not None:
            out.add(o["by"])


def _lic_refs(h: dict, answers: list, out: set):
    if "tidal" in h and "holds" not in h:
        return
    for f in ("holds", "wrong_water", "waived", "not_yet_mapped", "designations", "considered"):
        out.update(h.get(f) or [])
    for k, v in (h.get("displaced") or {}).items():
        out.add(int(k))
        out.update(v)
    for k, v in (h.get("also_printed") or {}).items():
        out.add(int(k))
        out.update(v)
    for a in answers:
        if a.get("tidal"):
            continue
        for r in a["requirements"]:
            out.add(r["req"])
            out.update(r.get("displaced_by") or [])
            if r.get("presumes_by") is not None:
                out.add(r["presumes_by"])
            out.update(r.get("terms") or [])
            for p in r["paths"]:
                if p.get("alt") is not None:
                    out.add(p["alt"])
        for f in ("others", "guiding"):                 # G2: {req, paths} (answers 2.3)
            out.update(o["req"] for o in a.get(f) or [])
        if a.get("exempt"):
            out.update(a["exempt"]["by"])


def slice_answers(ans: dict, full_keys: list, ladder_keys: list):
    """The answers file cut to `full_keys` (every section) and `ladder_keys` (ladder only), with
    every table re-interned and every value verbatim. Returns (slice, {old key: new key},
    rule refs, licensing refs)."""
    order = list(dict.fromkeys(full_keys + ladder_keys))
    new_of = {k: i for i, k in enumerate(order)}
    full = set(full_keys)
    segs, seg_moments = Interner(), Interner()
    keys = []
    for k in order:
        row = list(ans["keys"][k])
        row[E.SEG_SLOT] = segs.add(ans["segments"][row[E.SEG_SLOT]])
        m = row[E.MOMENT_SLOT]
        row[E.MOMENT_SLOT] = None if m is None else seg_moments.add(ans["segment_moments"][m])
        keys.append(row)
    S = ans["sections"]
    rrefs, lrefs = set(), set()
    out = {}

    # ladder
    L = S["ladder"]
    verdicts, frames = Interner(), Interner()

    def vmap(i):
        v = L["verdicts"][i]
        for lst in v[:4]:
            rrefs.update(lst)
        for r, by in v[4]:
            rrefs.add(r)
            rrefs.update(by)
        for r, _, by in v[5]:
            rrefs.add(r)
            rrefs.add(by)
        return verdicts.add(v)
    at = []
    for k in order:
        row = []
        for f in L["at"][k]:
            common, fish = L["frames"][f]
            row.append(frames.add([vmap(common), [[x[0]] + [vmap(i) for i in x[1:]] for x in fish]]))
        at.append(row)
    out["ladder"] = {"version": L["version"], "reasons": L["reasons"], "reason_state": L["reason_state"],
                     "verdicts": verdicts.rows, "frames": frames.rows, "at": at}

    # rows
    R = S["rows"]
    decided, rows, frames = Interner(), Interner(), Interner()
    at = []
    for k in order:
        if k not in full:
            at.append(None)
            continue
        row = []
        for f in R["at"][k]:
            spp, fish, rs, line = R["frames"][f]
            nf = {}
            for code, (h, w) in fish.items():
                pair = []
                for i in (h, w):
                    if i is None:
                        pair.append(None)
                    else:
                        _rule_refs_rows(R["decided"][i], rrefs)
                        pair.append(decided.add(R["decided"][i]))
                nf[code] = pair
            nrs = []
            for i in rs:
                _rule_refs_rows(R["rows"][i], rrefs)
                nrs.append(rows.add(R["rows"][i]))
            row.append(frames.add([spp, nf, nrs, line]))
        at.append(row)
    out["rows"] = {"version": R["version"], "decided": decided.rows, "rows": rows.rows,
                   "frames": frames.rows, "at": at}

    # gear and display: one value per frame
    for name in ("gear", "display"):
        G = S[name]
        frames = Interner()
        at = []
        for k in order:
            if k not in full:
                at.append(None)
                continue
            row = []
            for f in G["at"][k]:
                v = G["frames"][f]
                if name == "gear":
                    _rule_refs_gear(v, rrefs)
                else:
                    rrefs.update(r for r, _ in v.get("closing") or [])
                row.append(frames.add(v))
            at.append(row)
        sec = {"version": G["version"], "frames": frames.rows, "at": at}
        if name == "gear":
            for s in ("province_methods", "parent", "methods", "moments", "conduct_means"):
                sec[s] = G[s]
        out[name] = sec

    # licence
    Lc = S["licence"]
    holds, answers, docs, frames = Interner(), Interner(), Interner(), Interner()
    at = []
    for k in order:
        if k not in full:
            at.append(None)
            continue
        row = []
        for f in Lc["at"][k]:
            h, d = Lc["frames"][f]
            hv = Lc["holds"][h]
            av = [Lc["answers"][i] for i in Lc["documents"][d]]
            _lic_refs(hv, av, lrefs)
            row.append(frames.add([holds.add(hv), docs.add([answers.add(a) for a in av])]))
        at.append(row)
    out["licence"] = {"version": Lc["version"], "holds": holds.rows, "answers": answers.rows,
                      "documents": docs.rows, "frames": frames.rows, "at": at,
                      "profiles": Lc["profiles"], "profile_dims": Lc["profile_dims"]}
    return out, keys, (segs.rows, seg_moments.rows), new_of, rrefs, lrefs


def _lic_key(l: dict) -> str:
    return f"{l['entry_id']}#{l['record_id']}"


def build(export_dir: Path, waters: list, inputs: dict):
    raw, guide, ans = load(export_dir)
    check_pair(raw, guide, ans)
    X = expand(raw, guide)
    rule_ids, lic_ids = raw["rule_ids"], raw["licensing_ids"]
    rix = {k: i for i, k in enumerate(rule_ids)}
    lix = {k: i for i, k in enumerate(lic_ids)}

    # ---- the shown waters: the export's parts, in the export's order (= the answers' order)
    full_keys = []
    W = []
    member_rules, member_lic = [], []
    for wid in waters:
        w = X["waters"][wid]
        pk = ans["parts"][wid]
        parts = []
        for i, p in enumerate(w["parts"]):
            k = pk[i]
            if p["ruleset"] is None or k is None:
                parts.append(None)
                continue
            full_keys.append(k)
            rs = X["rulesets"][p["ruleset"]]
            rules = [[rix[r], via] for via, ks in rs.items() if via != "sections" for r in ks]
            member_rules += [rule_ids[r] for r, _ in rules]
            lic = []
            if p["licensing_set"] is not None:
                ls = X["licensing_sets"][p["licensing_set"]]
                lic = [[lix[l], via] for via, ks in ls.items() if via != "sections" for l in ks]
                member_lic += [lic_ids[l] for l, _ in lic]
            parts.append({"set": p["ruleset"], "lset": p["licensing_set"], "sections": p["sections"],
                          "rules": rules, "lic": lic, "runs": p["runs"],
                          "anadromous_rainbow": p.get("anadromous_rainbow", False),
                          "steelhead": p.get("steelhead"), "steelhead_rules": p.get("steelhead_rules"),
                          "province_except": p.get("province_except") or [],
                          "home_region": p.get("home_region")})
        W.append({"id": wid, "name": w["name"], "kind": w["kind"], "sections": w["sections"],
                  "outside_bc": w.get("outside_bc", 0), "part_of": w.get("part_of"),
                  "entries": w.get("entries", []), "parts": parts,
                  "uncertain": w.get("uncertain", []), "steelhead": w.get("steelhead"),
                  "steelhead_source": w.get("steelhead_source"), "tidal": w.get("tidal")})
        member_rules += [r for r in w.get("uncertain", []) if r in X["rules"]]
        W[-1]["uncertain"] = [rix[r] for r in w.get("uncertain", []) if r in rix]

    # ---- the cases: each read on the export part whose rule set it names
    gc = guide["guide"]["cases"]
    ours = convert.remap_ours(X, inputs["ours"], inputs["old_sets"])
    case_list, ladder_keys = [], []
    for src, cs in (("guide", gc["cases"]), ("ours", ours)):
        for c in cs:
            item = c["water"]["item_id"]
            w = X["waters"].get(item)
            pi, k = None, None
            if w and c.get("ruleset"):
                cand = [i for i, p in enumerate(w["parts"]) if p["ruleset"] == c["ruleset"]]
                exact = [i for i in cand
                         if bool(w["parts"][i].get("anadromous_rainbow")) == bool(c.get("anadromous_rainbow"))
                         and (src == "ours" or w["parts"][i].get("steelhead") == c.get("steelhead"))]
                pick = (exact or cand or [None])[0]
                if pick is not None:
                    pi, k = pick, ans["parts"][item][pick]
            if k is not None:
                ladder_keys.append(k)
            members = []
            if c.get("ruleset"):
                rs = X["rulesets"][c["ruleset"]]
                members = [[rix[r], via] for via in ("reach", "trib") for r in rs.get(via, [])]
            exp = c["expect"]
            expect = ({str(rix[e["id"]]): e["state"] + (" · partly lifted" if e.get("partly_lifted") else "")
                       for e in exp} if src == "guide" else
                      {str(rix[r]): ("—" if s == "-" else s) for r, s in exp.items()})
            case_list.append({
                "src": src, "id": c.get("id") or c.get("mechanism"),
                "shows": c.get("shows") or (c.get("id") or "").replace("_", " "),
                "what": c.get("what_to_show") or c.get("what"), "ruling": c.get("ruling"),
                "water": c["water"], "ruleset": c.get("ruleset"), "date": c["date"], "fish": c["fish"],
                "at": c.get("at"),
                "part": pi, "key": k, "members": members, "expect": expect,
                "only_listed": src == "ours"})
            member_rules += [rule_ids[r] for r, _ in members] + [rule_ids[int(r)] for r in expect]

    A, keys, segments, new_of, rrefs, lrefs = slice_answers(ans, full_keys, ladder_keys)
    for c in case_list:
        c["key"] = None if c["key"] is None else new_of[c["key"]]
    parts_map = {}
    for wid in waters:
        parts_map[wid] = [None if k is None else new_of[k] for k in ans["parts"][wid]]

    # ---- rules and licensing records: every one a part or a frame names
    want_r = list(dict.fromkeys(member_rules + [rule_ids[i] for i in sorted(rrefs)]))
    rules = {}
    for k in want_r:
        r = X["rules"][k]
        y = convert.rule_out(r)
        rules[k] = y
    rid = {str(rix[k]): k for k in rules}
    own = {e for w in W for e in w["entries"]}
    want_l = list(dict.fromkeys(member_lic + [lic_ids[i] for i in sorted(lrefs)] +
                                [k for k, l in X["licensing"].items()
                                 if l.get("placement") in convert.GLOBAL_PLACEMENTS or l["entry_id"] in own]))
    licensing = {}
    for k in want_l:
        y = convert.lic_out(X["licensing"][k])
        licensing[k] = y
    lid = {str(lix[k]): k for k in licensing}
    ents = {r["entry_id"] for r in rules.values()} | {l["entry_id"] for l in licensing.values()}
    entries = {e: convert._slim(X["entries"][e], convert.ENTRY_KEEP) for e in sorted(ents) if e in X["entries"]}

    # display statics, cut to the shown rules and waters (verbatim)
    disp = ans["sections"]["display"]
    A["display"]["rules"] = {i: disp["rules"][int(i)] for i in rid}
    A["display"]["waters"] = {wid: disp["waters"][wid] for wid in waters}

    data = {"rules": rules, "rid": rid, "licensing": licensing, "lid": lid,
            "licences": X["licences"], "species": X["species"], "waters": W, "entries": entries,
            "conduct": {k: v["means"] if isinstance(v, dict) else v
                        for k, v in X["guide"]["gear"]["conduct"]["acts"].items()},
            "about": {"bundle": raw["about"]["bundle"], "export_counts": raw["about"]["counts"]["rules"]}}
    answers = {"about": {"format": ans["about"]["format"], "bundle": ans["about"]["bundle"],
                         "export": ans["about"]["export"], "sections": ans["about"]["sections"]},
               "fish": ans["fish"], "keys": keys, "segments": segments[0],
               "moments": ans["moments"], "segment_moments": segments[1], "parts": parts_map,
               "glossary": ans["glossary"], "sections": A}
    case_rules = {}
    for c in case_list:
        for i in [m[0] for m in c["members"]] + [int(r) for r in c["expect"]]:
            if str(i) not in case_rules:
                x = X["rules"][rule_ids[i]]
                case_rules[str(i)] = {"id": rule_ids[i], "label": x["label"], "family": x["family"],
                                      "rank": x["provenance"]["rank"]}
    cases = {"about": {"bundle": guide["about"]["bundle"], "reading": gc["reading"]},
             "rules": case_rules, "cases": case_list}
    return data, answers, cases


def _block(name: str, obj) -> str:
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    return f'<script type="application/json" id="{name}">{s}</script>'


def write_html(page: Path, data: dict, answers: dict, cases: dict) -> None:
    """Rewrite the page's three JSON blocks and its script (from page_v36.js), in place."""
    html = page.read_text()
    for name, obj in (("data", data), ("answers", answers), ("cases", cases)):
        pat = re.compile(r'<script type="application/json" id="%s">.*?</script>' % name, re.S)
        if not pat.search(html):
            raise SystemExit(f"{page}: no #{name} block")
        html = pat.sub(lambda _m: _block(name, obj), html, count=1)
    js = JS.read_text()
    pat = re.compile(r"(<script>\n)(.*?)(\n</script>\n\n</body>)", re.S)
    if not pat.search(html):
        raise SystemExit(f"{page}: no page script block")
    html = pat.sub(lambda m: m.group(1) + js + m.group(3), html, count=1)
    page.write_text(html)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--export-dir", type=Path, default=EXPORT_DIR)
    ap.add_argument("--out", type=Path, help="write DIR/data.json, answers.json, cases.json")
    ap.add_argument("--html", type=Path, help="rewrite this page's blocks and script in place")
    a = ap.parse_args(argv)
    inputs = json.loads(convert.INPUTS.read_text())
    waters = inputs["display_waters"] + EXTRA_WATERS
    try:
        data, answers, cases = build(a.export_dir, waters, inputs)
    except PairingError as e:
        print(f"refused: {e}", file=sys.stderr)
        return 2
    sizes = {n: len(json.dumps(o, ensure_ascii=False, separators=(",", ":")))
             for n, o in (("data", data), ("answers", answers), ("cases", cases))}
    if a.out:
        a.out.mkdir(parents=True, exist_ok=True)
        for n, o in (("data", data), ("answers", answers), ("cases", cases)):
            (a.out / f"{n}.json").write_text(json.dumps(o, ensure_ascii=False, separators=(",", ":")))
    if a.html:
        write_html(a.html, data, answers, cases)
    print(json.dumps({"waters": len(data["waters"]), "rules": len(data["rules"]),
                      "licensing": len(data["licensing"]), "keys": len(answers["keys"]),
                      "cases": len(cases["cases"]), "bytes": sizes}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
