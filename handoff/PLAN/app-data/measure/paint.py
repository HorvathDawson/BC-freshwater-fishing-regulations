"""paint.bin = status index v2 (regs paint) + a flow-paint section in the same run-column form:
sid runs -> panel_id, plus panel_member/basin_member as small columns. Measure raw/gzip/brotli."""
import sqlite3, numpy as np, subprocess, os, json
def uv(a):
    out=bytearray()
    for v in np.asarray(a,dtype=np.int64).tolist():
        while v>=0x80: out.append((v&0x7f)|0x80); v>>=7
        out.append(v)
    return bytes(out)
def sizes(p):
    return os.path.getsize(p), len(subprocess.run(["gzip","-9","-c",p],capture_output=True).stdout), len(subprocess.run(["brotli","-q","11","-c",p],capture_output=True).stdout)
b=sqlite3.connect("b.sqlite")
sp=np.array(b.execute("select sid, panel_id from section_panel order by sid").fetchall(),dtype=np.int64)
sid,pan=sp[:,0],sp[:,1]
brk=np.r_[True,(sid[1:]!=sid[:-1]+1)|(pan[1:]!=pan[:-1])]
st=np.flatnonzero(brk); cnt=np.diff(np.r_[st,len(sid)]); starts=sid[st]; ends=starts+cnt
gap=starts-np.r_[0,ends[:-1]]
st_ix={s:i for i,(s,) in enumerate(b.execute("select station from gauge order by station"))}
pm=b.execute("select panel_id, ord, station, role, area_km2, years, regulated from panel_member order by panel_id, ord").fetchall()
bm=b.execute("select basin_id, station, coalesce(area_km2,0), years from basin_member order by basin_id, station").fetchall()
flow=[uv([len(st)]),uv(gap),uv(cnt),uv(pan[st]),
      uv([len(pm)]),uv(np.diff([r[0] for r in pm],prepend=0)),uv([r[1] for r in pm]),uv([st_ix[r[2]] for r in pm]),bytes([r[3]=='down' for r in pm]),
      np.array([r[4] for r in pm],dtype='<f4').tobytes(),uv([r[5] for r in pm]),bytes([r[6] for r in pm]),
      json.dumps([[r[0],st_ix[r[1]],r[2],r[3]] for r in bm]).encode()]
open("out/flow.bin","wb").write(b"".join(flow))
open("out/paint.bin","wb").write(open("status_index.bin","rb").read()+b"".join(flow))
print("flow runs", len(st), "flow.bin", sizes("out/flow.bin"), "paint.bin", sizes("out/paint.bin"), "status", sizes("status_index.bin"))
