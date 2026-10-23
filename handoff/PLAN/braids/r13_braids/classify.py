import json, collections
D=json.load(open('census_r13.json'))
rows=D['rows']
def cls(r):
  w=r['w100']; st = r['comp_lo'] and r['comp_hi']
  hit = w and w['spans'] and w['k']>=1
  bulge = w and (not w['spans']) and w['k']>=2
  if st and hit: return 'S_cut'
  if st and bulge: return 'S_bulge'
  if st: return 'S_nohit'
  if hit: return 'N_hit'
  if bulge: return 'N_bulge'
  return 'N'
c=collections.Counter((r['src'], cls(r)) for r in rows); print(sorted(c.items()))
for r in rows:
  k=cls(r)
  if k!='N': print(k, r['src'][:3], int(r['bound']), r['split'][-36:], r['ch'], round(r['ch_len']), 'comp', r['comp'], r['comp_n'], 'dm', r['w100']['hit_host_dm'], 'toend', r['w100']['to_end'] and round(r['w100']['to_end']), 'already', int(r['already']))
