import sqlite3, os, subprocess, numpy as np, zlib, time
def br(p): return len(subprocess.run(["brotli","-q","11","-c",p],capture_output=True).stdout)
b=sqlite3.connect("b.sqlite")
st={s:i+1 for i,(s,) in enumerate(b.execute("select station from gauge order by station"))}
tr={w:i+1 for i,(w,) in enumerate(b.execute("select distinct trust from section_gauge order by 1"))}
N=1<<21
def blocks(sids, vals, dtype, deflate):
    arr=np.zeros(N, dtype=dtype); arr[np.asarray(sids)]=vals
    out=[]
    for blk in np.unique(np.asarray(sids)>>12):
        raw=arr[blk<<12:(blk+1)<<12].tobytes()
        out.append((int(blk), (b'\x01'+zlib.compress(raw,9)[2:-4]) if deflate else raw))
    return out
sg=b.execute("select sid, station, trust, coalesce(mag,0) from section_gauge").fetchall()
sp=b.execute("select sid, panel_id, coalesce(area_km2,0) from section_panel").fetchall()
sd=b.execute("select sid, down_sid from section_down").fetchall()
print("max panel", max(x[1] for x in sp), "mag range", min(x[3] for x in sg), max(x[3] for x in sg))
for deflate in (False, True):
    p=f"pkg/cond_blocks{'_d' if deflate else ''}.sqlite"
    if os.path.exists(p): os.remove(p)
    c=sqlite3.connect(p)
    dt=np.dtype([('st','<u2'),('tr','u1'),('mag','<u4')])
    v=np.array([(st[x],tr[t],m) for _,x,t,m in sg], dtype=dt)
    c.execute("create table section_gauge_b(block integer primary key, b blob not null)")
    c.executemany("insert into section_gauge_b values(?,?)", blocks([x[0] for x in sg], v, dt, deflate))
    dt2=np.dtype([('p','<u2'),('a','<f4')])
    v2=np.array([(p_,a) for _,p_,a in sp], dtype=dt2)
    c.execute("create table section_panel_b(block integer primary key, b blob not null)")
    c.executemany("insert into section_panel_b values(?,?)", blocks([x[0] for x in sp], v2, dt2, deflate))
    c.execute("create table section_down_b(block integer primary key, b blob not null)")
    c.executemany("insert into section_down_b values(?,?)", blocks([x[0] for x in sd], np.array([x[1] for x in sd],dtype='<u4'), np.dtype('<u4'), deflate))
    c.commit(); c.execute("vacuum"); c.close()
    print(p, os.path.getsize(p), br(p))
c=sqlite3.connect("pkg/cond_rows.sqlite") if False else None
