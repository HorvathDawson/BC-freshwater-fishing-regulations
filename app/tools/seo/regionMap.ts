/**
 * (T1010, owner 2026-11-22: option B) A REGION PAGE'S FIRST SCREEN: a picture of the region made of its waters (one dot each,
 * from the release), the "Open the map of Region N" button under it (the app framed on the region, `mapLink`), and a search
 * for a water in the region beside / under it. Drawn at build time as inline SVG: no third-party host, no image to fetch.
 * The crawlable lists (topics, A-Z, every water) stay below it, as before.
 */
import { esc } from "./html";
import type { Box, WaterModel } from "./model";

/** the picture's width in drawing units; its height follows the region's shape (capped, so a tall region stays on a phone) */
const W = 400, MAX_H = 460;

/**
 * One dot per water in `region`: the middle of its first part held by that region (`WaterModel.at`), laid out equirectangular
 * with longitude scaled by cos(latitude), so the region has its true shape. Dots on the same drawing unit are one dot (the
 * picture could not show them apart); a dot is `M x y h0` under a round line cap. Null: nothing to draw.
 */
export function dotPicture(models: readonly WaterModel[], region: number, box: Box): { svg: string; dots: number; waters: number } | null {
  const [w, s, e, n] = box;
  const k = Math.cos(((s + n) / 2) * Math.PI / 180);
  const dx = Math.max((e - w) * k, 1e-6), dy = Math.max(n - s, 1e-6);
  const scale = Math.min(W / dx, MAX_H / dy);
  const cw = Math.max(Math.round(dx * scale), 1), ch = Math.max(Math.round(dy * scale), 1);
  const cells = new Set<string>(); let waters = 0;
  for (const m of models) {
    const a = m.at.find((p) => p.regions.includes(region));
    if (!a) continue;
    waters++;
    cells.add(`${Math.round((a.mid[0] - w) * k * scale)} ${Math.round((n - a.mid[1]) * scale)}`);
  }
  if (!cells.size) return null;
  // sorted: the same picture every build, and a smaller file once compressed
  const path = [...cells].map((c) => c.split(" ").map(Number) as [number, number]).sort((p, q) => p[0] - q[0] || p[1] - q[1])
    .map(([x, y]) => `M${x} ${y}h0`).join("");
  const pad = 6;
  return { dots: cells.size, waters,
    svg: `<svg class="dots" viewBox="${-pad} ${-pad} ${cw + 2 * pad} ${ch + 2 * pad}" role="img" aria-labelledby="rm-t"><title id="rm-t">Map of B.C. fishing Region ${region}: one dot for each of its ${waters} waters</title><path d="${path}"/></svg>` };
}

/** the search's names file for a region, by name: [name, path] and, for a renamed water, [name, path, [former names]] */
export function namesFile(ws: readonly WaterModel[], pathOf: (m: WaterModel) => string, former: ReadonlyMap<string, readonly string[]> = new Map()): string {
  return JSON.stringify([...ws].sort((a, b) => a.name.localeCompare(b.name, "en")).map((m) => {
    const f = former.get(m.id);
    return f && f.length ? [m.name, pathOf(m), [...f]] : [m.name, pathOf(m)];
  }));
}

/** the region's first screen (picture, button, search); `button` is the "Open the map" link, `names` where its names file is */
export function regionLead(label: string, pic: { svg: string; waters: number }, button: string, names: string, scriptHref: string,
                            cfg: { crash: string | null; env: string; release: string }): string {
  return `<div class="lead"><div class="mapcard"><span class="tag2">Each dot: a water in ${esc(label)}</span>${pic.svg}${button}</div>` +
    `<div class="find"><form data-names="${esc(names)}" data-cfg="${esc(JSON.stringify(cfg))}" role="search" hidden>` +
    `<label for="rf-q">Find a water in ${esc(label)}</label><div class="row"><input id="rf-q" type="search" autocomplete="off" placeholder="Lake, river or creek name"></div>` +
    `<ul class="sugg"></ul><p class="said small"></p><p class="sr" role="status" aria-live="polite"></p></form>` +
    `<p class="small">The map colours every water open or closed for the day, with flows and every rule. This page lists the rules by name.</p></div></div>` +
    `<script src="${esc(scriptHref)}" defer></script>`;
}

/**
 * The search's script BODY (gauges.ts `findScript` wraps it with the page reporter, written in from gaugeClient.ts, as it does
 * for the Join card): reveals the form, loads the region's names on first use, lists up to 8 matches as links. Ranked: names that
 * START with what was typed, then names with a WORD starting with it, then any other match; alphabetical within each (the names
 * file is in that order). A failed load is said on the page and reported (an environment failure, no connection, only said).
 */
export const FIND_BODY = `
var f=document.querySelector("form[data-names]");if(!f)return;
var cfg=JSON.parse(f.getAttribute("data-cfg"));cfg.app="region-page";cfg.route="other";
var report=pageReporter(cfg,window.fetch.bind(window),Date.now);
var q=f.querySelector("input"),ul=f.querySelector(".sugg"),said=f.querySelector(".said"),sr=f.querySelector(".sr"),names=null,tried=false;
f.hidden=false;
function show(t){said.textContent=t;sr.textContent=t;}
function fold(s){return s.normalize("NFD").replace(/[\u0300-\u036f]/g,"").toLowerCase();}
function rank(n,v){var l=fold(n);if(l.indexOf(v)===0)return 0;if((" "+l.replace(/[^a-z0-9]+/g," ")).indexOf(" "+v)>=0)return 1;return l.indexOf(v)>=0?2:3;}
function draw(){
  var v=fold(q.value.trim());ul.textContent="";show("");
  if(!v||!names)return;
  var g=[[],[],[]];
  for(var i=0;i<names.length;i++){
    var e=names[i],best=rank(e[0],v),label=e[0],fm=e[2]||[];
    for(var j=0;j<fm.length;j++){var r=rank(fm[j],v);if(r<best){best=r;label=e[0]+" (formerly "+fm[j]+")";}}
    if(best<3)g[best].push([label,e[1]]);
  }
  var all=g[0].concat(g[1],g[2]),hits=all.slice(0,8);
  if(!hits.length){show("No water found.");return;}
  hits.forEach(function(h){var li=document.createElement("li"),a=document.createElement("a");a.href=h[1];a.textContent=h[0];li.appendChild(a);ul.appendChild(li);});
  sr.textContent=all.length+(all.length===1?" water found":" waters found")+(all.length>hits.length?", the first "+hits.length+" shown":"")+".";
}
function stale(){
  said.textContent="";sr.textContent="This page is out of date: reload it.";
  said.appendChild(document.createTextNode("This page is out of date: "));
  var a=document.createElement("a");a.href=location.href;a.textContent="reload it";a.addEventListener("click",function(e){e.preventDefault();location.reload();});said.appendChild(a);said.appendChild(document.createTextNode("."));
}
function load(){
  if(tried)return;tried=true;show("Loading the names...");
  fetch(f.getAttribute("data-names")).then(function(r){if(r.status===404){var x=new Error("stale");x.stale=true;throw x;}if(!r.ok)throw new Error("names file "+r.status);return r.json();})
    .then(function(j){names=j;draw();},function(e){
      if(e&&e.stale){stale();return;}      // a release was cut since this page loaded: expected, said with a reload link, not a defect
      show("The names could not be loaded ("+e.message+"). Use the lists below.");report(e);});
}
f.addEventListener("submit",function(e){e.preventDefault();var a=ul.querySelector("a");if(a)location.href=a.href;});
q.addEventListener("focus",load);q.addEventListener("input",function(){if(!names){load();return;}draw();});
`;
