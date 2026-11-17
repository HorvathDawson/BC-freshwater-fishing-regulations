"""Item 5: the 71 rows' candidate chains whose map runs `m` are ONE run (the same status all year) while the live
chain's were not — the Skeena reserve's pattern. Re-run: python3 -I onerun.py"""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from salmonfile import load
pc, cc, _ = load(Path('/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/64288e7d-44dc-46e9-9576-a31a32c1991a/scratchpad/t796/candidate-release/r-685b700b2802'))
pl, cl, _ = load(Path('/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/data/release/r-46e4355a7277'))
for r in json.load(open(Path(__file__).parent / 'out/rows.json')):
    for c in r['chains']:
        m = pc['chains'][c['chain']]['m']; lm = pl['chains'][c['live_chain'][0][0]]['m']
        if len(m) == 2: print(r['id'], r['water'][:40], 'chain', c['chain'], 'm', m, 'live chain', c['live_chain'][0][0], 'live m', lm[:10], 'sections', c['n'])
