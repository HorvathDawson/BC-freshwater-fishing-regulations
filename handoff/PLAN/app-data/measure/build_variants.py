"""Measure candidate app packages (SQLite form vs struct-of-arrays binary form).
Read-only on copies: b.sqlite (bundle), r.sqlite (regs store), status_index.bin. Writes into out/."""
import os, sqlite3, struct, subprocess, sys, json
import numpy as np

D = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(D, "out")
B = os.path.join(D, "b.sqlite"); R = os.path.join(D, "r.sqlite")
os.makedirs(OUT, exist_ok=True)

def comp(path):
    raw = os.path.getsize(path)
    br = subprocess.run(["brotli", "-q", "11", "-c", path], capture_output=True).stdout
    zs = subprocess.run(["zstd", "-19", "-q", "-c", path], capture_output=True).stdout
    gz = subprocess.run(["gzip", "-9", "-c", path], capture_output=True).stdout
    return raw, len(gz), len(zs), len(br)

def sqlite_pkg(name, stmts, src=B, attach_regs=False):
    p = os.path.join(OUT, name + ".sqlite"); t = p + ".tmp"
    for f in (p, t):
        if os.path.exists(f): os.remove(f)
    c = sqlite3.connect(t)
    c.execute(f"attach '{src}' as s")
    if attach_regs: c.execute(f"attach '{R}' as r")
    for s in stmts: c.execute(s)
    c.commit(); c.execute(f"vacuum into '{p}'"); c.close(); os.remove(t)
    return p

def uvarint(a):
    a = np.asarray(a, dtype=np.uint64); out = bytearray()
    for v in a.tolist():
        while v >= 0x80: out.append((v & 0x7F) | 0x80); v >>= 7
        out.append(v)
    return bytes(out)

def zz(a):
    a = np.asarray(a, dtype=np.int64); return ((a << 1) ^ (a >> 63)).astype(np.uint64)

def write(name, parts):
    p = os.path.join(OUT, name + ".bin")
    with open(p, "wb") as f:
        for x in parts: f.write(x)
    return p

res = {}
def rec(k, p): res[k] = comp(p); print(k, res[k], flush=True)

b = sqlite3.connect(B); r = sqlite3.connect(R)

# ---------- A: SQLite packages ----------
rec("A.core.sqlite (item,alias,item_section,place)", sqlite_pkg("core", [
  "create table item(ord integer primary key, item_id text, name text, kind text, part_of text, area_ha integer)",
  "insert into item select ord,item_id,name,kind,part_of,area_ha from s.item",
  "create table alias as select * from s.alias",
  "create table item_section(sid integer primary key, ord integer) without rowid",
  "insert into item_section select sid, ord from s.item_section order by sid",
  "create index section_by_item on item_section(ord)",
  "create table place as select * from s.place"]))
rec("A.status_index.bin", os.path.join(D, "status_index.bin"))
rec("A.near.sqlite (place_water)", sqlite_pkg("near", [
  "create table place_water(place_id integer, ckm integer, ord integer, primary key(place_id,ckm,ord)) without rowid",
  "insert into place_water select * from s.place_water"]))
rec("A.cond.sqlite (gauges, panels, down)", sqlite_pkg("cond", [
  f"create table {t} as select * from s.{t}" for t in
  ("gauge","section_gauge","lake_gauge","section_panel","panel_member","basin_member","section_down")]))
rec("A.clim.sqlite (gauge_clim f32)", sqlite_pkg("clim", ["create table gauge_clim as select * from s.gauge_clim"]))
regs_tables = [x for (x,) in r.execute("select name from sqlite_master where type='table'")]
core_regs = [t for t in regs_tables if t not in ("section_akey","section_place")]
rec("A.regs.sqlite (all regs tables)", sqlite_pkg("regs", [f"create table {t} as select * from r.{t}" for t in regs_tables], attach_regs=True))
rec("A.regs-frames.sqlite (no sid maps)", sqlite_pkg("regsf", [f"create table {t} as select * from r.{t}" for t in core_regs], attach_regs=True))
rec("A.regs-sid.sqlite (section_akey+place blocks)", sqlite_pkg("regss", [f"create table {t} as select * from r.{t}" for t in ("section_akey","section_place")], attach_regs=True))
rec("A.topo.sqlite (part_section)", sqlite_pkg("topo", [
  "create table part_section(ord integer, sid integer, part_ix integer, primary key(ord,sid)) without rowid",
  "insert into part_section select * from s.part_section"]))

# ---------- B: struct-of-arrays binary ----------
# item_section sorted by sid: sid deltas + ord u16
rows = np.array(b.execute("select sid, ord from item_section order by sid").fetchall(), dtype=np.int64)
sid, ordc = rows[:,0], rows[:,1]
rec("B.item_section soa", write("item_section", [uvarint(np.diff(sid, prepend=0)), ordc.astype("<u2").tobytes()]))
# core strings: item names/ids/kinds as newline columns (sorted by ord)
it = b.execute("select ord,item_id,name,coalesce(kind,''),coalesce(part_of,''),coalesce(area_ha,0) from item order by ord").fetchall()
cols = list(zip(*it))
rec("B.item soa", write("item", ["\n".join(cols[1]).encode(), b"\0", "\n".join(cols[2]).encode(), b"\0",
     bytes(["stream lake wetland".split().index(k) if k in ("stream","lake","wetland") else 3 for k in cols[3]]),
     "\n".join(cols[4]).encode(), uvarint(cols[5])]))
al = b.execute("select item_id, alias from alias order by item_id").fetchall()
pl = b.execute("select place_id,osm,coalesce(name,''),coalesce(kind,''),coalesce(pop,0),lat,lon from place order by place_id").fetchall()
pc = list(zip(*pl))
rec("B.alias+place soa", write("aliasplace", ["\n".join(a+"\t"+x for a,x in al).encode(), b"\0",
     uvarint(np.diff(np.array(pc[0]),prepend=0)), "\n".join(pc[1]).encode(), "\n".join(pc[2]).encode(), "\n".join(pc[3]).encode(),
     uvarint(pc[4]), (np.array(pc[5])*1e5).round().astype("<i4").tobytes(), (np.array(pc[6])*1e5).round().astype("<i4").tobytes()]))
# place_water grouped by place: count per place, ckm deltas, ord u16
pw = np.array(b.execute("select place_id, ckm, ord from place_water order by place_id, ckm, ord").fetchall(), dtype=np.int64)
pid, ckm, pord = pw[:,0], pw[:,1], pw[:,2]
_, counts = np.unique(pid, return_counts=True)
newp = np.r_[True, pid[1:] != pid[:-1]]
dck = np.where(newp, ckm, ckm - np.r_[0, ckm[:-1]])
rec("B.place_water soa", write("place_water", [uvarint(np.unique(pid)), uvarint(counts), uvarint(dck), pord.astype("<u2").tobytes()]))
# conditions
st = [s for (s,) in b.execute("select station from gauge order by station")]; six = {s:i for i,s in enumerate(st)}
sg = b.execute("select sid, station, trust, coalesce(mag,0) from section_gauge order by sid").fetchall()
trusts = sorted({t for _,_,t,_ in sg})
sp = np.array(b.execute("select sid, panel_id, coalesce(area_km2,-1) from section_panel order by sid").fetchall())
sd = np.array(b.execute("select sid, down_sid from section_down order by sid").fetchall(), dtype=np.int64)
g = b.execute("select station,name,coalesce(item_id,''),coalesce(sid,0),lon,lat,coalesce(area_km2,0),coalesce(mag,0),coalesce(matched_by,''),coalesce(match_m,0) from gauge order by station").fetchall()
gc = list(zip(*g))
others = json.dumps([b.execute(f"select * from {t}").fetchall() for t in ("lake_gauge","panel_member","basin_member")]).encode()
parts = [uvarint(np.diff(np.array([x[0] for x in sg]),prepend=0)), np.array([six[x[1]] for x in sg],dtype="<u2").tobytes(),
         bytes([trusts.index(x[2]) for x in sg]), uvarint([x[3] for x in sg]),
         uvarint(np.diff(sp[:,0].astype(np.int64),prepend=0)), uvarint(sp[:,1].astype(np.int64)),
         np.where(sp[:,2] < 0, np.float16(np.nan), sp[:,2]).astype("<f2").tobytes(),
         uvarint(np.diff(sd[:,0],prepend=0)), uvarint(zz(sd[:,1]-sd[:,0])),
         "\n".join("\t".join(map(str,x)) for x in g).encode(), others]
rec("B.cond soa (+gauge/members as text)", write("cond", parts))
# gauge_clim: blob = u8 + 5*f32 records? quantise
cl = b.execute("select station, parameter, bands from gauge_clim order by station, parameter").fetchall()
raw32 = b"".join(x[2] for x in cl)
rec("B.clim f32 concat", write("clim32", [raw32]))
recs = []
for s,p_,blob in cl:
    n = len(blob)//21
    a = np.frombuffer(blob[:n*21], dtype=np.uint8).reshape(n,21)
    recs.append(a)
A = np.concatenate(recs)
days = A[:,0]; vals = A[:,1:].copy().view("<f4").reshape(-1,5)
rec("B.clim f16 soa", write("clim16", [days.tobytes(), vals.astype("<f2").T.copy().tobytes()]))
# log-quantised u8 relative to each series' max? use u16 log scale
lv = np.log1p(np.clip(vals, 0, None)); q = np.round(lv / np.log1p(12000) * 65535).astype("<u2")
rec("B.clim log-u16 soa", write("climq", [days.tobytes(), q.T.copy().tobytes()]))
# regs sid maps: dense u16/u8 concat vs RLE
ak = b"".join(bb for (bb,) in r.execute("select b from section_akey order by block"))
pl8 = b"".join(bb for (bb,) in r.execute("select b from section_place order by block"))
rec("B.section_akey dense u16", write("akey_dense", [ak]))
akv = np.frombuffer(ak, dtype="<u2").astype(np.int64)
ch = np.r_[True, akv[1:] != akv[:-1]]; starts = np.flatnonzero(ch); lens = np.diff(np.r_[starts, len(akv)])
rec("B.section_akey RLE varint", write("akey_rle", [uvarint(lens), uvarint(akv[starts])]))
plv = np.frombuffer(pl8, dtype=np.uint8).astype(np.int64)
ch = np.r_[True, plv[1:] != plv[:-1]]; s2 = np.flatnonzero(ch); l2 = np.diff(np.r_[s2, len(plv)])
rec("B.section_place RLE varint", write("place_rle", [uvarint(l2), bytes(plv[s2].astype(np.uint8))]))
ps = np.array(b.execute("select sid, ord, part_ix from part_section order by sid").fetchall(), dtype=np.int64)
rec("B.part_section soa", write("part_section", [uvarint(np.diff(ps[:,0],prepend=0)), ps[:,1].astype("<u2").tobytes(), bytes(ps[:,2].clip(0,255).astype(np.uint8))]))
json.dump(res, open(os.path.join(OUT, "results.json"), "w"), indent=1)
