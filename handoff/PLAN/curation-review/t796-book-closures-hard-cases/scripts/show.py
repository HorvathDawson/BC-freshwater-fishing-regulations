"""Print what the app said (from out/render.json, edge days) for a row: show.py row-45 [day-substring]"""
import json, sys
out = {r['id']: r for r in json.load(open('out/render.json'))}
r = out[sys.argv[1]]
for c in r['chains']:
    for d in c['days']:
        if len(sys.argv) > 2 and sys.argv[2] not in d['day']: continue
        o = d['off']
        print(c['chain'], d['day'], '| head', o['head'][1:3], '| map', o['map'], '| bar', o['bar'], '| rows', [(x['label'], x['status'], x['notes'][:2], x['from']) for x in o['rows']], '| live-feed same:', d['live']['rows'] == o['rows'])
