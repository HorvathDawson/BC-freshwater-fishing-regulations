"""Every section whose salmon answer differs between the live release and the candidate: per (live chain, candidate
chain) pair, the species-days that change (status, words), and whether a book place in the candidate chain answers
each changed day. Writes out/diff_pairs.json. Re-run: python3 -I diff_live_cand.py"""
import json, sys
from collections import defaultdict
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from salmonfile import load, at

C = Path('/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/t796/candidate-release/r-685b700b2802')
L = Path('/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/release/r-46e4355a7277')
pc, cc, _ = load(C); pl, cl, _ = load(L)
SP = ["CH", "CO", "SK", "PK", "CM"]
pairs = defaultdict(list)
for sid in set(cc) | set(cl):
    pairs[(cl.get(sid, pl['default']), cc.get(sid, pc['default']))].append(sid)
out = []
for (a, b), sids in pairs.items():
    A, B = pl['chains'][a], pc['chains'][b]
    changes = []; wordonly = 0; unexplained = []
    for k in range(5):
        for d in range(366):
            va, vb = pl['verdicts'][at(A['sp'][k], d)], pc['verdicts'][at(B['sp'][k], d)]
            who = at(B['a'][k], d) if B.get('a') else -1
            book = who >= 0 and pc['p'][B['l'][who]].get('static') == 1
            if va['s'] != vb['s']:
                changes.append((SP[k], d, va['s'], vb['s'], B['l'][who] if who >= 0 else None, book))
                if not book: unexplained.append((SP[k], d, va['s'], vb['s'], va['t'][:60], vb['t'][:60]))
            elif va['t'] != vb['t'] or va.get('f') != vb.get('f'):
                wordonly += 1
                if not book: unexplained.append(('words', SP[k], d, va['t'][:80], vb['t'][:80], va.get('f'), vb.get('f')))
    mA = [at(A['m'], d) for d in range(366)]; mB = [at(B['m'], d) for d in range(366)]
    mchg = sum(1 for d in range(366) if mA[d] != mB[d])
    if changes or wordonly or mchg:
        books = [l for l in B['l'] if pc['p'][l].get('static')]
        out.append({"live": a, "cand": b, "n": len(sids), "sids": sorted(sids)[:50], "all_sids": sorted(sids),
                    "status_changes": len(changes), "word_changes": wordonly, "map_day_changes": mchg,
                    "books": books, "dfo_live": A['l'], "dfo_cand": [l for l in B['l'] if not pc['p'][l].get('static')],
                    "changes": changes, "unexplained": unexplained[:40], "n_unexplained": len(unexplained)})
json.dump(out, open(Path(__file__).parent / 'out/diff_pairs.json', 'w'))
st = [o for o in out if o['status_changes']]
print("pairs with any change", len(out), "| with status changes", len(st), "| sections with status change", sum(o['n'] for o in st))
print("pairs with UNEXPLAINED changes (not answered by a book place)", sum(1 for o in out if o['n_unexplained']))
for o in out:
    if o['n_unexplained']:
        print(o['live'], o['cand'], o['n'], o['dfo_live'][:3], o['n_unexplained'], o['unexplained'][:3])
