"""Follow each section of a row downstream (bundle section_down) and list the named waters it reaches, in order, until
the mouth: shows which big river a row's sections really drain to. Usage: python3 -I downstream.py row-29 [max_sections]"""
import json, sqlite3, sys
from collections import Counter
from pathlib import Path
T = Path('/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/t796')
db = sqlite3.connect(f"file:{T/'cand-inputs/bundle.sqlite'}?mode=ro", uri=True)
rows = {r['id']: r for r in json.load(open(Path(__file__).parent / 'out/rows.json'))}
r = rows[sys.argv[1]]; lim = int(sys.argv[2]) if len(sys.argv) > 2 else 60
paths = Counter()
for sid in r['sids'][:lim]:
    seen, names, s = set(), [], sid
    while s and s not in seen and len(seen) < 20000:
        seen.add(s)
        n = db.execute("select i.name from item_section x join item i on i.ord=x.ord where x.sid=? limit 1", (s,)).fetchone()
        if n and (not names or names[-1] != n[0]): names.append(n[0])
        d = db.execute("select down_sid from section_down where sid=?", (s,)).fetchone()
        s = d[0] if d else None
    paths[' > '.join(names[:8])] += 1
for p, n in paths.most_common(20): print(n, p)
