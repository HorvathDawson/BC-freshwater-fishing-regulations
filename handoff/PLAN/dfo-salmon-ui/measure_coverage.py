import json, sys, re, os
WT='/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/.claude/worktrees/agent-a846a65591941d5ae'
sys.path.insert(0,WT); os.chdir(WT)
from pathlib import Path
from collections import defaultdict, Counter
from pipeline.regs.dfo_salmon.parse import parse_region
from pipeline.regs.dfo_salmon.untangle import untangle
from pipeline.regs.dfo_salmon.locations import extract
from pipeline.regs.dfo_salmon import entries as E
MAIN=Path('/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations')
srcs=[]
for f in sorted((MAIN/'cache/dfo_salmon/history').glob('region*.html')):
    m=re.match(r'region(\w+?)_(\d+)',f.stem); srcs.append((m[1],m[2],f))
for f in sorted((MAIN/'cache/dfo_salmon/raw').glob('region*-eng.html')):
    s=f.stem.split('-')[0][6:]
    if s!='5': srcs.append((s,'live',f))
for f in sorted(Path('pipeline/tests/fixtures/dfo_salmon').glob('region6_*.html')):
    srcs.append(('6',f.stem.split('_')[1]+'fx',f))
seen={}
vers=Counter(); fails=[]
for slug,ts,f in srcs:
    try:
        locs,_,_=extract(untangle(parse_region(f.read_text(encoding='utf-8',errors='replace'),slug)))
    except Exception as e:
        fails.append((f.name,str(e)[:80])); continue
    vers[slug]+=1
    for l in locs:
        k=(slug,l.fingerprint)
        if k not in seen: seen[k]=[l,[]]
        seen[k][1].append(ts)
out=defaultdict(Counter); gaps=[]
for slug in sorted({s for s,_ in seen}):
    ef=E.load(slug)
    idx=ef.by_fingerprint(); byid={x.location_id:x for x in ef.locations}
    for (s,fp),(l,ts) in seen.items():
        if s!=slug: continue
        hit=idx.get(fp)
        if hit is None: cat='no_entry'
        else:
            tgt=(byid.get(hit.duplicate_of) if hit.is_variant else hit) or hit
            w=ef.water(tgt.water_id) if tgt.water_id else None
            ext=tgt.binding.resolved or hit.binding.resolved
            if tgt.kind!='water': cat='matched' if ext else 'scope_unbound'
            elif w is None: cat='no_water'
            elif not w.bound: cat='water_'+w.resolution
            elif ext: cat='matched'
            else: cat='water_bound_no_extent'
        out[slug][cat]+=1
        live='live' in ts
        if cat!='matched':
            gaps.append(dict(slug=slug,cat=cat,live=live,last='live' if live else max(ts),n_versions=len(ts),water=l.water,area=l.specific_area[:100],kind=l.kind,fp=fp,lid=hit.location_id if hit else None,status=hit.status if hit else None))
tot=Counter()
for s,c in out.items(): tot.update(c); print(s, vers[s], sum(c.values()), dict(c))
print('TOTAL',sum(tot.values()),dict(tot)); print('fails',fails)
print('entries locations total', sum(len(E.load(s).locations) for s in out))
json.dump(gaps,open(sys.argv[1],'w'),indent=1)
