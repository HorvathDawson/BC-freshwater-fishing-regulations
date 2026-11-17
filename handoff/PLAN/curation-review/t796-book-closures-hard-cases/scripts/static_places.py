"""Every static (book) place in the candidate: its rule, its days (as windows), the sections whose chains carry it, the
named waters among them, and how many of those sections DFO's own chain lets fish. Writes out/static_places.json.
Usage: python3 -I static_places.py [substring-of-rule ...] (prints names for the matching places)."""
import json, sqlite3, sys
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from salmonfile import load, at
T = Path('/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/t796')
pc, cc, _ = load(T / 'candidate-release/r-685b700b2802')
M = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]; DBM = [0,31,60,91,121,152,182,213,244,274,305,335]
def dw(d):
    m = max(i for i in range(12) if DBM[i] <= d); return f"{M[m]} {d-DBM[m]+1}"
def windows(days):
    runs = []
    for d in sorted(days):
        if runs and runs[-1][1] == d - 1: runs[-1][1] = d
        else: runs.append([d, d])
    return [f"{dw(a)}–{dw(b)}" for a, b in runs]
by_chain = defaultdict(list)
for s, c in cc.items(): by_chain[c].append(s)
db = sqlite3.connect(f"file:{T/'cand-inputs/bundle.sqlite'}?mode=ro", uri=True)
want = sys.argv[1:]
out = {}
for lid, st in pc['static'].items():
    days = {d for d in range(366) if any(at(r, d) >= 0 for r in st['sp'])}
    chains = [i for i, ch in enumerate(pc['chains']) if lid in ch['l']]
    sids = [s for c in chains for s in by_chain[c]]
    out[lid] = {"rule": pc['p'][lid]['book']['rule'], "pages": pc['p'][lid]['book']['pages'], "water": pc['p'][lid].get('water'),
                "r": pc['p'][lid].get('r'), "days": len(days), "windows": windows(days), "chains": len(chains), "sections": len(sids)}
    if want and any(w in lid for w in want):
        db.execute("create temp table if not exists s(sid integer primary key)"); db.execute("delete from s")
        db.executemany("insert into s values (?)", [(x,) for x in sids])
        names = Counter(n for (n,) in db.execute("select i.name from s join item_section x using(sid) join item i on i.ord=x.ord"))
        kinds = Counter(k for (k,) in db.execute("select k.kind from s join section_ruleset sr using(sid) join rule_key k on k.key_ix=sr.key_ix"))
        out[lid]["names"] = names.most_common(60); out[lid]["kinds"] = dict(kinds)
        print(lid, out[lid]['windows'], 'sections', len(sids), dict(kinds)); print('   ', names.most_common(60))
json.dump(out, open(Path(__file__).parent / 'out/static_places.json', 'w'), indent=0)
print(len(out), 'static places')
