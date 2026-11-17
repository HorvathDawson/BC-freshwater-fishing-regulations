"""Geography of each row's closed sections from the candidate's bundle: how the book rule binds them (reach / trib),
lake or stream, the MU codes, the region home, tidal, and the named waters (item names) with counts. Writes
out/geo.json and prints a summary. Re-run: python3 -I geo.py [row-id ...]"""
import json, sqlite3, sys
from collections import Counter
from pathlib import Path
here = Path(__file__).parent
T = Path('/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/t796')
db = sqlite3.connect(f"file:{T/'cand-inputs/bundle.sqlite'}?mode=ro", uri=True)
rows = json.load(open(here / 'out/rows.json'))
want = set(sys.argv[1:])
db.execute("create temp table s(sid integer primary key)")
out = {}
for r in rows:
    if want and r['id'] not in want: continue
    e, rid = r['rule_id'].split('::', 1)
    db.execute("delete from s"); db.executemany("insert into s values (?)", [(x,) for x in r['sids']])
    via = Counter(v for (v,) in db.execute("select rs.via from s join section_ruleset sr using(sid) join rule_key k on k.key_ix=sr.key_ix "
                                            "join ruleset rs on rs.set_id=k.set_id where rs.entry_id=? and rs.rule_id=?", (e, rid)))
    kind = Counter(k for (k,) in db.execute("select k.kind from s join section_ruleset sr using(sid) join rule_key k on k.key_ix=sr.key_ix"))
    mus = Counter(c for (c,) in db.execute("select m.codes from s join section_ruleset sr using(sid) left join mu_set m on m.mus=sr.mus"))
    home = Counter(c for (c,) in db.execute("select h.region from s left join section_home h using(sid)"))
    tidal = db.execute("select count(*) from s join tidal using(sid)").fetchone()[0]
    names = Counter(n for (n,) in db.execute("select i.name from s join item_section x using(sid) join item i on i.ord=x.ord"))
    unnamed = db.execute("select count(*) from s where sid not in (select sid from item_section)").fetchone()[0]
    out[r['id']] = {"via": dict(via), "kind": dict(kind), "mus": dict(mus.most_common(8)), "home": dict(home), "tidal": tidal,
                    "names": names.most_common(40), "n_names": len(names), "unnamed_sections": unnamed}
    o = out[r['id']]
    print(r['id'], r['water'][:30], '| n', len(r['sids']), '| via', o['via'], '| kind', o['kind'], '| mus', o['mus'], '| home', o['home'], '| tidal', tidal, '| unnamed', unnamed)
    print('    names:', o['names'][:14])
json.dump(out, open(here / ('out/geo.json' if not want else 'out/geo_partial.json'), 'w'), indent=0)
