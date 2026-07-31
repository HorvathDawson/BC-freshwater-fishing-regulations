"""Generate a SELF-CONTAINED offline labelling page for 14-locators-to-curate.json.

The output is a single .html file with all locator data embedded. Open it locally (file://) —
no server, no internet needed to LABEL (map links open when you have wifi). Your decisions persist
in the browser (localStorage) and can be exported to a decisions.json, which
`apply_review.py` writes back into the doc when you return.

    .venv/bin/python -m stream_sections.oneoff.build_review_html            # -> output/locator_review.html
    .venv/bin/python -m stream_sections.oneoff.build_review_html --out /path/to/review.html
    .venv/bin/python -m stream_sections.oneoff.build_review_html --status todo deferred   # subset
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

DOC = Path("stream_sections/docs/14-locators-to-curate.json")
_CAND = re.compile(r"Candidate coord\s*\[\s*(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)\s*\]")
_CONF = re.compile(r"\[auto-proposal\s*([HML])\b")


def _record(x: dict) -> dict:
    notes = x.get("notes") or ""
    cand = None
    m = _CAND.search(notes)
    if m:
        cand = [float(m.group(1)), float(m.group(2))]
    elif isinstance(x.get("coord"), list):
        cand = x["coord"]
    conf = (_CONF.search(notes) or [None, ""])[1] if _CONF.search(notes) else ""
    return {
        "id": x["id"], "name": x["name_verbatim"], "region": x.get("region", ""),
        "mus": x.get("mus") or [], "src": x.get("src", ""),
        "loc": x.get("locator_text") or "", "reg": x.get("full_regulation") or "",
        "kind": x["anchor_kind"], "hint": x.get("resolver_hint", ""),
        "status": x["status"], "coord": x.get("coord"), "cand": cand, "conf": conf,
        "notes": notes,
    }


def build(rows: list[dict]) -> str:
    data = json.dumps([_record(x) for x in rows], ensure_ascii=False)
    return _TEMPLATE.replace("/*__DATA__*/", data)


_TEMPLATE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>BC fishing locator review</title>
<style>
:root{--bg:#0f1115;--card:#171a21;--fg:#e6e8ec;--mut:#9aa3af;--line:#2a2f3a;--accent:#4f8cff;
 --ok:#2ecc71;--bad:#ff6b6b;--skip:#f0b429;--na:#a06bff;--def:#6b7280}
@media(prefers-color-scheme:light){:root{--bg:#f6f7f9;--card:#fff;--fg:#111;--mut:#5b6472;--line:#e3e6ea}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 system-ui,sans-serif}
header{position:sticky;top:0;z-index:5;background:var(--bg);border-bottom:1px solid var(--line);padding:10px 14px;display:flex;flex-wrap:wrap;gap:8px;align-items:center}
header h1{font-size:15px;margin:0 8px 0 0}
select,input,button,textarea{background:var(--card);color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:5px 8px;font:inherit}
button{cursor:pointer}button:hover{border-color:var(--accent)}
#prog{color:var(--mut);margin-left:auto}
main{max-width:900px;margin:0 auto;padding:14px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px;margin:0 0 12px}
.card.done{opacity:.62}
.row1{display:flex;flex-wrap:wrap;gap:8px;align-items:baseline}
.id{font:12px ui-monospace,monospace;color:var(--mut)}
.name{font-weight:600}
.badge{font-size:11px;padding:1px 7px;border-radius:20px;border:1px solid var(--line);color:var(--mut)}
.conf-H{color:var(--ok);border-color:var(--ok)}.conf-M{color:var(--skip);border-color:var(--skip)}.conf-L{color:var(--bad);border-color:var(--bad)}
.loc{margin:6px 0 2px;font-weight:600}
.reg{color:var(--mut);white-space:pre-wrap;font-size:13px;max-height:5.5em;overflow:auto;border-left:2px solid var(--line);padding-left:8px}
.note{color:var(--mut);font-size:12px;margin-top:6px}
.links a{margin-right:10px;color:var(--accent);text-decoration:none}.links a:hover{text-decoration:underline}
.ctl{display:flex;flex-wrap:wrap;gap:6px;align-items:center;margin-top:10px;border-top:1px dashed var(--line);padding-top:10px}
.v{border:1px solid var(--line);border-radius:6px;padding:5px 10px;background:transparent;color:var(--fg)}
.v.sel{color:#fff}
.v[data-v=correct].sel{background:var(--ok);border-color:var(--ok)}
.v[data-v=wrong].sel{background:var(--bad);border-color:var(--bad)}
.v[data-v=skip].sel{background:var(--skip);border-color:var(--skip);color:#111}
.v[data-v=not_a_split].sel{background:var(--na);border-color:var(--na)}
.v[data-v=defer].sel{background:var(--def);border-color:var(--def)}
.coordin{width:210px;font:12px ui-monospace,monospace}
.notein{flex:1;min-width:180px}
small.hint{color:var(--mut)}
</style></head><body>
<header>
 <h1>Locator review</h1>
 <select id="fStatus"></select>
 <select id="fKind"></select>
 <select id="fHint"></select>
 <select id="fShow"><option value="all">all</option><option value="un">unlabelled</option><option value="lab">labelled</option><option value="cand">has candidate</option></select>
 <input id="q" placeholder="search name / id / reg…" size="20">
 <button id="exp">⬇ Export decisions</button>
 <label class="v" style="cursor:pointer">⬆ Import<input id="imp" type="file" accept=".json" hidden></label>
 <span id="prog"></span>
</header>
<main id="list"></main>
<script>
const DATA = /*__DATA__*/;
const KEY = "bcfish_review_decisions_v1";
let dec = {};
try{ dec = JSON.parse(localStorage.getItem(KEY)||"{}"); }catch(e){}
const save = ()=>{ try{localStorage.setItem(KEY,JSON.stringify(dec));}catch(e){} };
const $=s=>document.querySelector(s); const el=(t,c)=>{const e=document.createElement(t);if(c)e.className=c;return e;};

// filter option lists
function opts(sel, vals, label){ sel.innerHTML=""; const a=el("option");a.value="";a.textContent=label;sel.appendChild(a);
  [...new Set(vals)].filter(Boolean).sort().forEach(v=>{const o=el("option");o.value=v;o.textContent=v;sel.appendChild(o);}); }
opts($("#fStatus"), DATA.map(d=>d.status), "status: any");
opts($("#fKind"), DATA.map(d=>d.kind), "anchor_kind: any");
opts($("#fHint"), DATA.map(d=>d.hint), "hint: any");

function links(c){ if(!c) return ""; const [lon,lat]=c;
  const osm=`https://www.openstreetmap.org/?mlat=${lat}&mlon=${lon}#map=15/${lat}/${lon}`;
  const g=`https://www.google.com/maps/search/?api=1&query=${lat},${lon}`;
  const sat=`https://www.google.com/maps/@${lat},${lon},15z/data=!3m1!1e3`;
  return `<div class="links"><b>[${lon}, ${lat}]</b> &nbsp; <a href="${osm}" target="_blank">OSM</a><a href="${g}" target="_blank">Google</a><a href="${sat}" target="_blank">Satellite</a></div>`; }

function highlightCand(notes){ return (notes||"").replace(/(Candidate coord\s*\[[^\]]+\])/g,"<b>$1</b>"); }

function card(d){
  const c=el("div","card"); const dv=dec[d.id];
  if(dv&&dv.verdict) c.classList.add("done");
  const mus=d.mus.join(",");
  c.innerHTML=`<div class="row1"><span class="name">${d.name}</span>
    <span class="badge">${d.kind}${d.hint?" / "+d.hint:""}</span>
    <span class="badge">${d.status}</span>${d.conf?`<span class="badge conf-${d.conf}">${d.conf}</span>`:""}
    <span class="badge">${d.region} ${mus}</span></div>
    <div class="id">${d.id}</div>
    <div class="loc">“${d.loc||d.name}”</div>
    <div class="reg">${d.reg}</div>
    ${links(d.cand)}
    <div class="note">${highlightCand(d.notes)}</div>`;
  const ctl=el("div","ctl");
  const verdicts=[["correct","✓ correct"],["wrong","✗ wrong"],["skip","skip"],["not_a_split","not a split"],["defer","defer"]];
  verdicts.forEach(([v,lbl])=>{ const b=el("button","v");b.dataset.v=v;b.textContent=lbl;
    if(dv&&dv.verdict===v)b.classList.add("sel");
    b.onclick=()=>{ dec[d.id]=Object.assign(dec[d.id]||{},{verdict:v,ts:Date.now()});
      ctl.querySelectorAll(".v[data-v]").forEach(x=>x.classList.remove("sel")); b.classList.add("sel");
      c.classList.add("done"); save(); progress(); }; ctl.appendChild(b); });
  const ci=el("input","coordin"); ci.placeholder="lon,lat (paste if 'wrong')";
  ci.value=(dv&&dv.coord)?dv.coord.join(","):(d.cand?d.cand.join(","):"");
  ci.onchange=()=>{ const m=ci.value.trim().match(/(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)/);
    dec[d.id]=Object.assign(dec[d.id]||{},{coord:m?[parseFloat(m[1]),parseFloat(m[2])]:null}); save(); };
  const ni=el("input","notein"); ni.placeholder="note (optional)"; ni.value=(dv&&dv.note)||"";
  ni.onchange=()=>{ dec[d.id]=Object.assign(dec[d.id]||{},{note:ni.value}); save(); };
  ctl.appendChild(ci); ctl.appendChild(ni); c.appendChild(ctl);
  return c;
}

function render(){
  const st=$("#fStatus").value,kd=$("#fKind").value,ht=$("#fHint").value,sh=$("#fShow").value,q=$("#q").value.toLowerCase();
  const list=$("#list"); list.innerHTML="";
  const rows=DATA.filter(d=>{
    if(st&&d.status!==st)return false; if(kd&&d.kind!==kd)return false; if(ht&&d.hint!==ht)return false;
    const lab=!!(dec[d.id]&&dec[d.id].verdict);
    if(sh==="un"&&lab)return false; if(sh==="lab"&&!lab)return false; if(sh==="cand"&&!d.cand)return false;
    if(q&&!(d.id+" "+d.name+" "+d.reg+" "+d.loc).toLowerCase().includes(q))return false;
    return true; });
  rows.slice(0,600).forEach(d=>list.appendChild(card(d)));
  const shown=rows.length; const lab=rows.filter(d=>dec[d.id]&&dec[d.id].verdict).length;
  $("#prog").textContent=`${lab}/${shown} labelled in view · ${Object.values(dec).filter(x=>x.verdict).length} total`;
  if(shown>600){ const w=el("div","note");w.textContent=`(showing first 600 of ${shown} — narrow the filter)`;list.appendChild(w);}
}
function progress(){ const total=Object.values(dec).filter(x=>x.verdict).length;
  $("#prog").textContent=$("#prog").textContent.replace(/\d+ total/,total+" total"); }

["#fStatus","#fKind","#fHint","#fShow","#q"].forEach(s=>$(s).addEventListener("input",render));
$("#exp").onclick=()=>{ const blob=new Blob([JSON.stringify(dec,null,2)],{type:"application/json"});
  const a=el("a");a.href=URL.createObjectURL(blob);a.download="decisions.json";a.click(); };
$("#imp").onchange=e=>{ const f=e.target.files[0]; if(!f)return; const r=new FileReader();
  r.onload=()=>{ try{ dec=Object.assign(dec,JSON.parse(r.result)); save(); render(); alert("Imported "+Object.keys(dec).length+" decisions"); }catch(err){alert("bad json");} }; r.readAsText(f); };
// default: show the review queue
$("#fShow").value="cand"; render();
</script></body></html>"""


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="output/locator_review.html")
    p.add_argument("--status", nargs="*", help="only these statuses (default: all)")
    args = p.parse_args()
    doc = json.loads(DOC.read_text())
    rows = doc["locators"]
    if args.status:
        rows = [x for x in rows if x["status"] in args.status]
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build(rows), encoding="utf-8")
    print(f"wrote {out}  ({len(rows)} rows, {out.stat().st_size//1024} KB)")
    print("Open it in a browser (file://). Label offline; ⬇ Export decisions.json when done.")


if __name__ == "__main__":
    main()
