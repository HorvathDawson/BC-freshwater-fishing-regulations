"""Build output/review_queue.json — every todo row, with any candidate(s) merged from the resolver
outputs, ordered so candidate-bearing rows come first (grouped by kind). Then it's served in batches."""
import json, os, re, sys
sys.path.insert(0, os.getcwd())
from pipeline.oneoff.waterbody_splits import load_curation

CAND_FILES = ['output/osm_candidates.json', 'output/osm_all_candidates.json',
              'output/falls_candidates.json', 'output/confluence_candidates.json',
              'output/lake_candidates.json', 'output/fwa_candidates.json']


def norm(v):
    if isinstance(v, dict):
        cs = v.get('cands') or ([v['coord']] if v.get('coord') else [])
        return cs, v.get('target')
    if v and isinstance(v[0], (int, float)):
        return [v], None
    return (v or []), None


def evkind(lt):
    t = lt.lower()
    if re.search(r'\bdam\b', t): return 'dam'
    if re.search(r'power\s*line|powerline|transmission', t): return 'powerline'
    if re.search(r'hwy|highway|bridge|\broad\b|crossing|trestle|street|railway|cnr|\brail\b|avenue', t): return 'bridge'
    if re.search(r'\bfalls?\b|canyon', t): return 'falls'
    if re.search(r'weir|fence|hatchery|fishway', t): return 'weir/fishway'
    if re.search(r'\bpark\b', t): return 'park'
    if re.search(r'radius|within \d', t): return 'buffer'
    if re.search(r'confluence', t): return 'confluence'
    if re.search(r'\bkm\b|\d+\s*m (up|down)|approximately \d+\s*m', t): return 'offset'
    if re.search(r'\blake\b', t): return 'lake'
    if re.search(r'signs?\b', t): return 'signs'
    return 'other'


def main():
    todo = {r['id']: r for r in load_curation() if r['status'] == 'todo'}
    cand, tgt = {}, {}
    for f in CAND_FILES:
        if not os.path.exists(f):
            continue
        for rid, v in json.load(open(f)).items():
            if rid in todo and rid not in cand:
                cs, t = norm(v)
                if cs:
                    cand[rid] = [[round(c[0], 5), round(c[1], 5)] for c in cs]
                if t:
                    tgt[rid] = t
    from collections import Counter
    q = []
    for rid, r in todo.items():
        q.append({'id': rid, 'water': r['name_verbatim'], 'mu': r.get('mus'),
                  'locator': r['locator_text'], 'kind': evkind(r['locator_text']),
                  'cands': cand.get(rid, []), 'target': tgt.get(rid), 'reviewed': False})
    # GROUP BY WATERBODY: all locators of a reg entry are contiguous, so we curate a whole
    # waterbody at a time (never half). wb_count = how many todo locators that waterbody has.
    wbc = Counter((x['water'], tuple(x['mu'] or [])) for x in q)
    for x in q:
        x['wb_count'] = wbc[(x['water'], tuple(x['mu'] or []))]
    # order: multi-locator waterbodies first (bigger units), then by water; within a wb, cand-first
    q.sort(key=lambda x: (-x['wb_count'], x['water'], tuple(x['mu'] or []), not x['cands'], x['kind']))
    json.dump(q, open('output/review_queue.json', 'w'), indent=1)
    wc = sum(1 for x in q if x['cands'])
    print('queue:', len(q), '| with candidate:', wc, '| needs-coord:', len(q) - wc)
    print('multi-locator waterbodies:', sum(1 for k, v in wbc.items() if v > 1),
          '| single:', sum(1 for k, v in wbc.items() if v == 1))
    print('by kind (needs-coord):', dict(Counter(x['kind'] for x in q if not x['cands'])))


if __name__ == '__main__':
    main()
