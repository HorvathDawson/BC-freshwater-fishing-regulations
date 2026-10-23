"""For every census polygon in the T55 v2 pools with a named main flow through it: which water entries bind the river's
sections AT the polygon (the section(s) the main flow inside occupies, plus the next section up- and downstream).
The census's river_regulated is blue-line-wide (any section anywhere on the line), which over-claims for a piece far
from the regulated reach or in another region. Writes OUT/local_entries.json {wbk: [entry_id, ...]}. Read-only."""
import collections, json, sqlite3, bisect
import pandas as pd

REPO = '/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/'
OUT = '/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/t55v2/'
cen = pd.read_parquet(OUT + 'census.parquet', columns=['wbk', 'main_flow_blk', 'river_through'])
cen = cen[cen.main_flow_blk.notna()]
want = {int(w): int(b) for w, b in zip(cen.wbk, cen.main_flow_blk)}
blks = set(want.values())
gp = sqlite3.connect(f'file:{REPO}data/source/bc_fisheries_data.gpkg?mode=ro', uri=True)
meas = collections.defaultdict(list)
for wk, blk, m, ln in gp.execute('select WATERBODY_KEY, BLUE_LINE_KEY, DOWNSTREAM_ROUTE_MEASURE, LENGTH_METRE from streams '
                                 'where WATERBODY_KEY is not null and EDGE_TYPE = 1200'):
    if wk is None:
        continue
    wk = int(wk)
    if want.get(wk) == blk:
        meas[wk].append((m, m + ln))
print('polygons with measures', len(meas), 'of', len(want))
starts = collections.defaultdict(list)
with open(REPO + 'data/generated/atlas/full/section_handles.txt') as f:
    for n, line in enumerate(f, 1):
        h = line.rstrip('\n')
        if h.startswith('lake:') or ':' not in h:
            continue
        b, m = h.rsplit(':', 1)
        b = int(b)
        if b in blks:
            starts[b].append((float(m), n))
for b in starts:
    starts[b].sort()
bd = sqlite3.connect(f'file:{REPO}data/generated/bundle/bundle.sqlite?mode=ro', uri=True)
sec_set = dict(bd.execute('select sid,set_id from section_ruleset'))
set_entries = collections.defaultdict(set)
for s, e in bd.execute('select set_id, entry_id from ruleset'):
    set_entries[s].add(e)
res = {}
for wk, spans in meas.items():
    b = want[wk]
    st = starts.get(b)
    if not st:
        continue
    lo, hi = min(s[0] for s in spans), max(s[1] for s in spans)
    ms = [x[0] for x in st]
    i0 = max(bisect.bisect_right(ms, lo) - 1, 0)
    i1 = max(bisect.bisect_left(ms, hi) - 1, 0)
    sids = [st[i][1] for i in range(max(i0 - 1, 0), min(i1 + 2, len(st)))]
    ents = set()
    for s in sids:
        ents |= set_entries.get(sec_set.get(s), set())
    res[wk] = sorted(e for e in ents if not e.startswith('z'))
json.dump(res, open(OUT + 'local_entries.json', 'w'))
print('wrote', len(res))
