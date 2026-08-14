"""One-shot: propose (and optionally apply) structural normalization of the bridge_road_km todo
rows in 14-locators-to-curate.json. Report-only by default; `apply` mutates + writes.

  two-boundary 'from A to B' / 'between A and B'  -> split into -a / -b rows (per-endpoint kind)
  single 'upstream/downstream of X', 'at X'       -> one point (or confluence/lake by endpoint)
  shared point (same wb: up-of-X and down-of-X)   -> annotate [shared-anchor]
Coords stay todo; this only fixes the locator STRUCTURE.
"""
import json, re, sys
from collections import defaultdict

P = "pipeline/docs/14-locators-to-curate.json"
BETW = re.compile(r'\bbetween\s+(.*?)\s+and\s+(.*)', re.I)
CONN = re.compile(r'(?i)^\s*(?:from\s+)?(.*?)\s+(?:upstream to|downstream to|up to|to)\s+(.*)$')


def clean(s):
    s = re.sub(r'^\s*(?:the|a)\s+', '', s.strip(), flags=re.I)
    return s.strip().strip(',;').strip()


def classify(lt):
    m = BETW.search(lt)
    if m:
        return ('two', clean(m.group(1)), clean(m.group(2)))
    if re.match(r'(?i)\s*from\b', lt) or ' to ' in lt.lower():
        m = CONN.match(lt)
        if m and m.group(1) and m.group(2):
            return ('two', clean(m.group(1)), clean(m.group(2)))
    return ('one', clean(lt), None)


def endpoint_kind(t):
    t = t.lower()
    if re.search(r'\b(lake|reservoir)\b', t):
        return 'lake'
    if re.search(r'\b(bridge|crossing|dam|trestle|sign|signs|falls|rapids|weir|hwy|highway|'
                 r'road|power\s*line|powerline|km\b|end of|foot of|pedestrian)\b', t):
        return 'point'
    if re.search(r'\b(river|creek|brook|slough|confluence|mouth)\b', t):
        return 'confluence'
    return 'point'


def base_feature(a):
    return re.sub(r'(?i)^(up|down)stream of (the )?|^above |^below |^at ', '', a).strip().lower()


def main():
    doc = json.load(open(P))
    rows = doc['locators']
    br = [r for r in rows if r.get('resolver_hint') == 'bridge_road_km' and r['status'] == 'todo']
    by_wb = defaultdict(list)
    for r in br:
        by_wb[(r['name_verbatim'], tuple(r.get('mus') or []))].append(r)

    ops = []          # list of {row_id, kind: split|reclassify|shared, ...}
    for (w, mu), lst in by_wb.items():
        singles = defaultdict(list)
        for r in lst:
            c, a, b = classify(r['locator_text'])
            if c == 'two':
                ka, kb = endpoint_kind(a), endpoint_kind(b)
                ops.append({"id": r['id'], "op": "split", "a": a, "b": b, "ka": ka, "kb": kb,
                            "wb": w, "lt": r['locator_text']})
            else:
                k = endpoint_kind(a)
                singles[base_feature(a)].append(r['id'])
                if k != 'point':
                    ops.append({"id": r['id'], "op": "reclass", "to": k, "wb": w,
                                "lt": r['locator_text']})
        for base, ids in singles.items():
            if len(ids) > 1:
                ops.append({"op": "shared", "ids": ids, "feature": base, "wb": w})

    splits = [o for o in ops if o['op'] == 'split']
    recl = [o for o in ops if o['op'] == 'reclass']
    shared = [o for o in ops if o['op'] == 'shared']
    print(f"SPLIT {len(splits)} rows -> {2*len(splits)} | RECLASS {len(recl)} | SHARED groups {len(shared)}")
    L = ["# Bridge structural-normalization proposal (scratch; review before apply)\n"]
    for (w, mu), lst in by_wb.items():
        mine = [o for o in ops if o.get('wb') == w]
        if not mine:
            continue
        L.append(f"## {w} · MU {list(mu)}")
        for o in mine:
            if o['op'] == 'split':
                L.append(f"- SPLIT  “{o['lt']}”\n    → -a **{o['a']}** [{o['ka']}]\n    → -b **{o['b']}** [{o['kb']}]")
            elif o['op'] == 'reclass':
                L.append(f"- RECLASS → {o['to']}  “{o['lt']}”")
            else:
                L.append(f"- SHARED point ‘{o['feature']}’ across {o['ids']}")
        L.append("")
    open("scratchpad_bridge_proposal.md", "w").write("\n".join(L) + "\n")
    json.dump(ops, open("scratchpad_bridge_ops.json", "w"), indent=1, ensure_ascii=False)

    if sys.argv[1:2] == ['apply']:
        by_id = {r['id']: r for r in rows}
        out = []
        applied_split = applied_recl = 0
        shared_note = {}
        for o in shared:
            for i in o['ids']:
                shared_note[i] = o['feature']
        for r in rows:
            oid = r['id']
            sp = next((o for o in splits if o['id'] == oid), None)
            rc = next((o for o in recl if o['id'] == oid), None)
            if sp:
                for suf, txt, k in (('-a', sp['a'], sp['ka']), ('-b', sp['b'], sp['kb'])):
                    nr = dict(r)
                    nr['id'] = oid + suf
                    nr['locator_text'] = txt
                    nr['anchor_kind'] = k
                    nr['resolver_hint'] = ('tributary' if k == 'confluence' else
                                           'lake_reach' if k == 'lake' else 'bridge_road_km')
                    nr['notes'] = ((r.get('notes') or '').strip() +
                                   f" [structural-split 2026-08-05 from “{sp['lt']}”] boundary={txt}").strip()
                    out.append(nr)
                applied_split += 1
                continue
            if rc:
                r['anchor_kind'] = rc['to']
                r['resolver_hint'] = 'tributary' if rc['to'] == 'confluence' else 'lake_reach' if rc['to'] == 'lake' else r['resolver_hint']
                r['notes'] = ((r.get('notes') or '').strip() + f" [structural-reclass 2026-08-05 → {rc['to']}]").strip()
                applied_recl += 1
            if oid in shared_note:
                r['notes'] = ((r.get('notes') or '').strip() + f" [shared-anchor ‘{shared_note[oid]}’ — author ONE split]").strip()
            out.append(r)
        doc['locators'] = out
        json.dump(doc, open(P, 'w'), indent=2, ensure_ascii=False)
        open(P, 'a').write('\n')
        print(f"APPLIED: {applied_split} splits (+{applied_split} rows), {applied_recl} reclass; total rows {len(out)}")


if __name__ == "__main__":
    main()
