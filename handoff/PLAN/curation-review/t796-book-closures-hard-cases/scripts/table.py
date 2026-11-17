"""The HARD-CASES.md per-row table, from out/rows.json + out/render.json + the verdicts set by hand below.
Re-run: python3 -I table.py > out/table.md"""
import json
rows = json.load(open('out/rows.json')); ren = {r['id']: r for r in json.load(open('out/render.json'))}
V = {  # row -> (verdict, finding id)
 'row-45': ('WRONG', 'W1'), 'row-56': ('WRONG', 'W1'),
 'row-15': ('WRONG', 'W2'), 'row-22': ('WRONG', 'W2'), 'row-37': ('WRONG', 'W2'), 'row-46': ('WRONG', 'W2'),
 'row-52': ('WRONG', 'W2'), 'row-58': ('WRONG', 'W2'),
 'row-34': ('SUSPECT', 'S1'), 'row-70': ('SUSPECT', 'S1'), 'row-33': ('SUSPECT', 'S1'),
 'row-04': ('SUSPECT', 'S2'), 'row-26': ('SUSPECT', 'S2'), 'row-27': ('SUSPECT', 'S2'),
 'row-41': ('SUSPECT', 'S3'), 'row-13': ('SUSPECT', 'S3'),
 'row-44': ('SUSPECT', 'S4'), 'row-62': ('SUSPECT', 'S4'), 'row-69': ('SUSPECT', 'S4'),
}
T = {'row-01': 'T1', 'row-02': 'T2', 'row-03': 'T2', 'row-07': 'T2', 'row-08': 'T2', 'row-09': 'T2', 'row-10': 'T2', 'row-11': 'T2',
     'row-12': 'T2,T5', 'row-14': 'T2', 'row-16': 'T2', 'row-18': 'T2', 'row-20': 'T2', 'row-21': 'T2', 'row-23': 'T2', 'row-24': 'T2',
     'row-25': 'T2', 'row-35': 'T2', 'row-36': 'T2,T5', 'row-05': 'T3', 'row-06': 'T3', 'row-13': 'T3', 'row-19': 'T3',
     'row-30': 'T3', 'row-57': 'T3', 'row-71': 'T3', 'row-17': 'T3', 'row-29': 'T4', 'row-48': 'T6', 'row-49': 'T6', 'row-63': 'T6',
     'row-65': 'T7', 'row-67': 'T7', 'row-68': 'T7', 'row-50': 'T5', 'row-42': 'T5', 'row-47': 'T5', 'row-53': 'T5', 'row-54': 'T5',
     'row-64': 'S1', 'row-32': '', 'row-60': 'T2', 'row-61': 'T2'}
print('| row | water (book row) | p. | book dates | DFO place | sections | fish closed | app words (tail) | verdict | notes |')
print('|---|---|---|---|---|---|---|---|---|---|')
for r in rows:
    words = ''
    for c in ren[r['id']]['chains']:
        for d in c['days']:
            if any(d['rowAnswers']):
                for x in d['off']['rows']:
                    for n in x['notes']:
                        if n.startswith('Closed to all fishing by the rule book'): words = n
                if words: break
        if words: break
    tail = words.split('): ', 1)[-1].replace('\n', '⏎').replace('|', '/')
    tail = (tail[:70] + '…' + tail[-22:]) if len(tail) > 95 else tail
    v, f = V.get(r['id'], ('PASS', ''))
    t = T.get(r['id'], '')
    print(f"| {r['id']} | {r['water'][:38]} | {','.join(map(str, r['book']['pages']))} | {r['book']['dates']} | `{r['dfo_place_id'][:40]}` | {len(r['sids'])} | "
          f"{'; '.join(f'{k} {v2}' for k, v2 in r['fish_closed'].items())} | {tail} | **{v}** | {', '.join(x for x in [f, t] if x)} |")
