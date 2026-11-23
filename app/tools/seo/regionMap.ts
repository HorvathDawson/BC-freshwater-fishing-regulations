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

/** the search's names file for a region: [name, path] for every water in it, by name */
export function namesFile(ws: readonly WaterModel[], pathOf: (m: WaterModel) => string): string {
  return JSON.stringify([...ws].sort((a, b) => a.name.localeCompare(b.name, "en")).map((m) => [m.name, pathOf(m)]));
}

/** the region's first screen (picture, button, search); `button` is the "Open the map" link, `names` where its names file is */
export function regionLead(label: string, pic: { svg: string; waters: number }, button: string, names: string, scriptHref: string,
                            cfg: { crash: string | null; env: string; release: string }): string {
  return `<div class="lead"><div class="mapcard"><span class="tag2">Each dot: a water in ${esc(label)}</span>${pic.svg}${button}</div>` +
    `<div class="find"><form data-names="${esc(names)}" data-cfg="${esc(JSON.stringify(cfg))}" role="search" hidden>` +
    `<label for="rf-q">Find a water in ${esc(label)}</label><div class="row"><input id="rf-q" type="search" autocomplete="off" placeholder="Lake, river or creek name"></div>` +
    `<ul class="sugg" aria-live="polite"></ul><p class="said small" role="status"></p></form>` +
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
var report=pageReporter(JSON.parse(f.getAttribute("data-cfg")),window.fetch.bind(window),Date.now);
var q=f.querySelector("input"),ul=f.querySelector(".sugg"),said=f.querySelector(".said"),names=null,loading=false;
f.hidden=false;
function show(t){said.textContent=t;}
function rank(n,v){var l=n.toLowerCase();if(l.indexOf(v)===0)return 0;if((" "+l.replace(/[^a-z0-9\u00c0-\uffff]+/g," ")).indexOf(" "+v)>=0)return 1;return l.indexOf(v)>=0?2:3;}
function draw(){
  var v=q.value.trim().toLowerCase();ul.textContent="";show("");
  if(!v||!names)return;
  var g=[[],[],[]];
  for(var i=0;i<names.length;i++){var r=rank(names[i][0],v);if(r<3)g[r].push(names[i]);}
  var hits=g[0].concat(g[1],g[2]).slice(0,8);
  if(!hits.length){show("No water in this region has that in its name.");return;}
  hits.forEach(function(h){var li=document.createElement("li"),a=document.createElement("a");a.href=h[1];a.textContent=h[0];li.appendChild(a);ul.appendChild(li);});
}
function load(){
  if(names||loading)return;loading=true;show("Loading the names...");
  fetch(f.getAttribute("data-names")).then(function(r){if(!r.ok)throw new Error("names file "+r.status);return r.json();})
    .then(function(j){names=j;loading=false;draw();},function(e){loading=false;show("The names could not be loaded ("+e.message+"). Use the lists below.");report(e);});
}
f.addEventListener("submit",function(e){e.preventDefault();var a=ul.querySelector("a");if(a)location.href=a.href;});
q.addEventListener("focus",load);q.addEventListener("input",function(){load();draw();});
`;
