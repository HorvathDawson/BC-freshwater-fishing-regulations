"""The 71 rows with their sections as handles (sids), the bundle's rule (verbatim, when, pages, name), each section's
candidate chain and the book places in it. Writes out/rows.json. Re-run: python3 -I rows_prep.py"""
import json, sqlite3, sys
from collections import Counter, defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from salmonfile import load, at

T = Path('/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/t796')
C = T / 'candidate-release/r-685b700b2802'
H = Path('/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/generated/atlas/full/section_handles.txt')
L = Path('/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/release/r-46e4355a7277')
handle = {}
with open(H) as f:
    for i, line in enumerate(f, 1):
        handle[line.rstrip('\n')] = i
pc, cc, _ = load(C); pl, cl, _ = load(L)
db = sqlite3.connect(f"file:{T/'cand-inputs/bundle.sqlite'}?mode=ro", uri=True)
rows = json.load(open(T / 'book-closures-raw.json'))
out = []
for r in rows:
    secs = json.load(open(T / r['sections_file']))
    sids = [handle[s] for s in secs]
    e, rid = r['rule_id'].split('::', 1)
    rr = db.execute("select r.verbatim, r.when_, r.closure_grade, r.type, r.species, r.exempts, r.undrawn_part, e.pages, e.name, e.verbatim "
                    "from rule r join entry e using(entry_id) where r.entry_id=? and r.rule_id=?", (e, rid)).fetchone()
    by_chain = defaultdict(list)
    for s in sids:
        by_chain[cc.get(s, pc['default'])].append(s)
    chains = []
    for ch, ss in sorted(by_chain.items()):
        C_ = pc['chains'][ch]
        chains.append({"chain": ch, "n": len(ss), "sid": ss[0], "sids": ss[:5], "live_chain": Counter(cl.get(s, 0) for s in ss).most_common(3),
                       "l": C_['l'], "books": [l for l in C_['l'] if pc['p'][l].get('static')]})
    out.append({**{k: r[k] for k in ('id', 'rule_id', 'region', 'water', 'dfo_place_id', 'fish_closed', 'counts')},
                "book": r['book'], "after_words": r['after_words'], "secs": secs, "sids": sids,
                "bundle": {"verbatim": rr[0], "when": json.loads(rr[1]) if rr[1] else None, "grade": rr[2], "type": rr[3],
                           "species": rr[4], "exempts": rr[5], "undrawn_part": rr[6], "pages": rr[7], "entry_name": rr[8],
                           "entry_verbatim": rr[9]},
                "chains": chains})
json.dump(out, open(Path(__file__).parent / 'out/rows.json', 'w'), indent=0)
for o in out:
    print(o['id'], len(o['sids']), 'chains', len(o['chains']), 'books', sorted({b for c in o['chains'] for b in c['books']})[:4], o['bundle']['grade'])
