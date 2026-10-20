"""Build the proposed split packages (SQLite, original DDL + indexes kept) from copies, and measure size.
Packages: core, cond, near, clim, regs-common, regs-frames, regs-sid, topo (+ per-region regs-frames estimate)."""
import os, sqlite3, subprocess, json, numpy as np, collections
D = os.path.dirname(os.path.abspath(__file__)); OUT = os.path.join(D, "pkg"); os.makedirs(OUT, exist_ok=True)
B = os.path.join(D, "b.sqlite"); R = os.path.join(D, "r.sqlite")

def comp(p):
    raw = os.path.getsize(p)
    br = len(subprocess.run(["brotli", "-q", "11", "-c", p], capture_output=True).stdout)
    gz = len(subprocess.run(["gzip", "-9", "-c", p], capture_output=True).stdout)
    return raw, gz, br

def ddl(src, tables):
    c = sqlite3.connect(src); out = []
    for t in tables:
        out.append(c.execute("select sql from sqlite_master where type='table' and name=?", (t,)).fetchone()[0])
        out += [s for (s,) in c.execute("select sql from sqlite_master where type='index' and tbl_name=? and sql is not null", (t,))]
    return out

def pkg(name, src, tables, extra=(), where=None):
    p = os.path.join(OUT, name + ".sqlite"); t = p + ".tmp"
    for f in (p, t):
        if os.path.exists(f): os.remove(f)
    c = sqlite3.connect(t); c.execute(f"attach '{src}' as s")
    for s in ddl(src, tables): c.execute(s)
    for tb in tables:
        w = (where or {}).get(tb, "")
        c.execute(f"insert into main.{tb} select * from s.{tb} {w}")
    for s in extra: c.execute(s)
    c.commit(); c.execute("detach s"); c.execute(f"vacuum into '{p}'"); c.close(); os.remove(t)
    return p

res = {}
def rec(k, p): res[k] = comp(p); print(k, res[k], flush=True)

rec("core", pkg("core", B, ["item", "alias", "item_section", "place", "meta"],
    extra=["create index item_by_name_nc on item(name collate nocase)", "create index alias_nc on alias(alias collate nocase)"]))
rec("cond", pkg("cond", B, ["gauge", "section_gauge", "lake_gauge", "section_panel", "panel_member", "basin_member", "section_down"]))
rec("near", pkg("near", B, ["place_water"]))
rec("clim", pkg("clim", B, ["gauge_clim"]))
rec("topo", pkg("topo", B, ["part_section"], extra=[]))
r = sqlite3.connect(R)
alltabs = [x for (x,) in r.execute("select name from sqlite_master where type='table'")]
FR = ["cell", "ladder_frame", "ladder_verdict", "rows_frame", "rows_row", "rows_decided", "gear_frame",
      "licence_frame", "licence_hold", "licence_answer", "licence_doc", "licence_u16", "display_frame", "display_rule"]
SID = ["section_akey", "section_place"]
common = [t for t in alltabs if t not in FR + SID]
rec("regs-common", pkg("regs_common", R, common))
rec("regs-frames", pkg("regs_frames", R, [t for t in FR if t in alltabs]))
rec("regs-sid", pkg("regs_sid", R, SID))
rec("regs-all", pkg("regs_all", R, alltabs))
json.dump({"tables_common": common, "res": res}, open(os.path.join(OUT, "sizes.json"), "w"), indent=1)
