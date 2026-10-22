"""T47: build compare.html — today's palette and every candidate, side by side, on the same map views, with the ΔE numbers.

    .venv/bin/python -I handoff/PLAN/app-data/status-colours/scripts/build-compare.py

Reads candidates.json, measurements.json (scripts/measure.mjs) and candidates/<id>/<id>--<view>--<ground>.jpg
(scripts/shots.mjs). Writes ONE self-contained file: every screenshot is embedded (downscaled to 1x, 390 px wide) so the
page opens anywhere with no other file; the full-size shots stay in candidates/ and the page links them by relative path.
Colour-vision simulation is done in the page by SVG feColorMatrix filters in linear RGB, with the Machado, Oliveira &
Fernandes (2009) severity-1 matrices — the same matrices as app/tools/cvd.ts.
"""
import base64
import io
import json
from html import escape
from pathlib import Path

from PIL import Image

DIR = Path(__file__).resolve().parent.parent
cands = json.loads((DIR / "candidates.json").read_text())
meas = json.loads((DIR / "measurements.json").read_text())
ids = [k for k in cands if not k.startswith("$")]
VIEWS = [("vedder-z11", "Vedder / Chilliwack z11"), ("kootenay-z9", "Kootenay Lake z9"), ("province-z6", "Province z6"),
         ("boundary-z10", "Boundaries · Coquihalla z10"), ("permit-z12", "Permit area (restricted) z12"),
         ("temperature-z8", "Temperature dots (open) z8"), ("card-keep-top", "Card · answer + year strip"),
         ("card-keep-release", "Card · release words")]
GROUNDS = [("light", "Light"), ("dark", "Dark"), ("cvd", "Colour-blind palette"), ("satellite", "Satellite")]

shots = {}
for i in ids:
    for v, _ in VIEWS:
        for g, _ in GROUNDS:
            p = DIR / "candidates" / i / f"{i}--{v}--{g}.jpg"
            if not p.exists():
                continue
            im = Image.open(p).convert("RGB")
            im = im.resize((im.width // 2, im.height // 2), Image.LANCZOS)
            buf = io.BytesIO()
            im.save(buf, "JPEG", quality=74, optimize=True)
            shots[f"{i}|{v}|{g}"] = {"src": "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode(),
                                     "full": f"candidates/{i}/{p.name}"}

data = {"ids": ids, "views": VIEWS, "grounds": GROUNDS, "shots": shots, "meas": meas}

MACHADO = {
    "protan": [[0.152286, 1.052583, -0.204868], [0.114503, 0.786281, 0.099216], [-0.003882, -0.048116, 1.051998]],
    "deutan": [[0.367322, 0.860646, -0.227968], [0.280085, 0.672501, 0.047413], [-0.011820, 0.042940, 0.968881]],
    "tritan": [[1.255528, -0.076749, -0.178779], [-0.078411, 0.930809, 0.147602], [0.004733, 0.691367, 0.303900]],
}
filters = "".join(
    f'<filter id="sim-{k}" color-interpolation-filters="linearRGB"><feColorMatrix type="matrix" values="'
    + " ".join(" ".join(f"{x:.6f}" for x in row) + " 0 0" for row in m) + ' 0 0 0 1 0"/></filter>'
    for k, m in MACHADO.items())

intro = ("The palette before T47, part 1's three candidates (A, B, C), D0 (the first D, superseded after review; numbers only) and D, the palette part 2 applied, shot on the same map views "
         "(the app's Map tab, today's statuses, Expo web at a 390 × 844 phone viewport). The permit-area and temperature views and the two card views show the two "
         "statuses the map views never do: restricted (the permit hatch, its label, the release words on the card) and open (the cool temperature dot, the Open answer); "
         "they are rendered for B and D. Pick a view, a ground and a colour-vision simulation; the numbers under "
         "each map are the worst ΔE2000 for that theme across normal vision and protan, deutan and tritan simulation "
         "(Viénot 1999 and Machado 2009). Twenty is a different category at a glance on a thin line; under twelve, two lines "
         "read as one. D puts the owner's pair (closed / own) first, then pushes all five statuses apart, and keeps every status "
         "off body ink. Tap a map for the full-size shot.")

page = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>T47 Status Colours</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Archivo:wght@400;600;800&family=JetBrains+Mono:wght@500&display=swap">
<style>
/* Layout: a control strip, then one column per palette (wrapping to one on a phone), each a map + its numbers. */
:root {{
  --paper: #EFEFEC; --card: #FFFFFF; --ink: #15181C; --sub: #555C62; --line: #D3D5CF; --accent: #5F26E0;
  --warn: #940018; --ok: #1D7741;
  --body: "Archivo", "Helvetica Neue", Arial, sans-serif;
  --mono: "JetBrains Mono", ui-monospace, Menlo, monospace;
  color-scheme: light;
}}
@media (prefers-color-scheme: dark) {{ :root:not([data-theme="light"]) {{
  --paper: #0A0B0D; --card: #15181B; --ink: #E9ECEF; --sub: #B0B8BF; --line: #2C3136; --accent: #A98BFF;
  --warn: #FC4164; --ok: #48b07a; color-scheme: dark; }} }}
:root[data-theme="dark"] {{ --paper: #0A0B0D; --card: #15181B; --ink: #E9ECEF; --sub: #B0B8BF; --line: #2C3136;
  --accent: #A98BFF; --warn: #FC4164; --ok: #48b07a; color-scheme: dark; }}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--paper); color: var(--ink); font: 15px/1.5 var(--body);
  padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px));
  padding-block: 20px 48px; }}
h1 {{ font-weight: 800; font-size: 1.6rem; margin: 0 0 6px; text-wrap: balance; letter-spacing: -0.01em; }}
p.lede {{ max-width: 70ch; color: var(--sub); margin: 0 0 18px; }}
.controls {{ display: flex; flex-wrap: wrap; gap: 12px 24px; margin-bottom: 18px; position: sticky;
  top: env(safe-area-inset-top, 0px); background: var(--paper); padding-block: 8px; z-index: 5; }}
@media (max-width: 700px) {{ .controls {{ position: static; }} }}
.col h2 {{ min-height: 1.5em; }}
fieldset {{ border: 0; padding: 0; margin: 0; display: flex; flex-wrap: wrap; gap: 4px; align-items: center; }}
legend {{ font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--sub); margin-bottom: 4px; }}
fieldset label {{ border: 1.5px solid var(--ink); padding: 4px 10px; cursor: pointer; font-weight: 600; font-size: 0.85rem;
  background: var(--card); }}
fieldset input {{ position: absolute; opacity: 0; }}
fieldset input:checked + span {{ text-decoration: underline 2px; text-underline-offset: 3px; }}
fieldset label:has(input:checked) {{ background: var(--ink); color: var(--paper); }}
fieldset label:has(input:focus-visible) {{ outline: 2px solid var(--accent); outline-offset: 2px; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 16px; }}
.col {{ background: var(--card); border: 1px solid var(--line); padding: 12px; display: flex; flex-direction: column; gap: 10px; min-width: 0; }}
.col h2 {{ font-size: 1rem; font-weight: 800; margin: 0; }}
.swatches {{ display: flex; flex-wrap: wrap; gap: 6px 12px; font: 500 0.72rem var(--mono); color: var(--sub); }}
.sw {{ display: inline-flex; align-items: center; gap: 5px; }}
.sw i {{ width: 22px; height: 4px; display: inline-block; }}
.sw i.dash {{ height: 0; border-top: 2px dashed; }}
.map {{ display: block; aspect-ratio: 390 / 844; max-width: 100%; background: var(--line); }}
.map.empty {{ display: grid; place-items: center; color: var(--sub); font-size: 0.85rem; padding: 12px; text-align: center; }}
.map img {{ width: 100%; height: 100%; object-fit: cover; display: block; }}
table {{ border-collapse: collapse; width: 100%; font: 500 0.78rem var(--mono); font-variant-numeric: tabular-nums; }}
td, th {{ padding: 3px 4px; border-bottom: 1px solid var(--line); text-align: left; }}
th {{ font: 600 0.7rem var(--body); color: var(--sub); text-transform: uppercase; letter-spacing: 0.06em; }}
td.n {{ text-align: right; }}
.bad {{ color: var(--warn); font-weight: 800; }}
.good {{ color: var(--ok); }}
.summary {{ font-size: 0.85rem; color: var(--sub); max-width: 65ch; }}
.note {{ font-size: 0.8rem; color: var(--sub); max-width: 80ch; margin-top: 24px; }}
@media (prefers-reduced-motion: reduce) {{ * {{ transition: none !important; }} }}
</style>
</head>
<body>
<svg width="0" height="0" style="position:absolute" aria-hidden="true">{filters}</svg>
<h1>Status colours, side by side</h1>
<p class="lede">{escape(intro)}</p>
<div class="controls">
  <fieldset id="f-view"><legend>View</legend></fieldset>
  <fieldset id="f-ground"><legend>Ground</legend></fieldset>
  <fieldset id="f-sim"><legend>Simulate</legend></fieldset>
</div>
<div class="grid" id="grid"></div>
<p class="note">Numbers: <em>line</em> = both colours as thin lines over the same ground (coverage 0.6, boundaries 0.8), worst
ground; <em>lake</em> = both as lake fills over the basemap's water; <em>sat</em> = thin lines over the satellite grounds. <em>boundary</em> = the
worst of the region and unit boundary against all five statuses (closed, own, base, restricted, open). The colour-blind palette and satellite rows use their
own theme's colours (satellite draws the light theme). Source: T47-colours.md, scripts/measure.mjs.
In A to D the unit boundary wears the region's colour on purpose and is told apart by width and dash. Restricted pairs are the
permit-area hatch over water; open pairs are the temperature dots (cool / warm / critical), 5 px filled circles in a paper stroke,
so they are measured as tokens only; neither is ever a lake fill. <em>status vs ink</em> = the closest status to body ink, normal vision (floor 20)
and the worst of every vision (floor 15): a release word or a restricted pill must never read as plain text. The card views read the same tokens
(app/packages/ui-native/src/theme.ts, <code>color.status.*</code>), so the colours there are the theme's own.</p>
<script>
const D = {json.dumps(data)};
const state = {{ view: "boundary-z10", ground: "light", sim: "none" }};
function radios(id, name, opts) {{
  const fs = document.getElementById(id);
  for (const [v, label] of opts) {{
    const l = document.createElement("label");
    l.innerHTML = `<input type="radio" id="${{name}}-${{v}}" name="${{name}}" value="${{v}}"><span>${{label}}</span>`;
    const input = l.querySelector("input");
    input.checked = state[name] === v;
    input.addEventListener("change", () => {{ state[name] = v; render(); }});
    fs.appendChild(l);
  }}
}}
radios("f-view", "view", D.views);
radios("f-ground", "ground", D.grounds);
radios("f-sim", "sim", [["none", "Normal"], ["protan", "Protan"], ["deutan", "Deutan"], ["tritan", "Tritan"]]);
const f1 = (x) => (x == null ? "–" : x.toFixed(1));
const cls = (x, floor = 20) => (x < 12 ? "bad" : x < floor ? "bad" : "good");
function numbers(id) {{
  const th = state.ground === "dark" ? "dark" : state.ground === "cvd" ? "cvd" : "light";
  const T = D.meas[id].themes[th], P = T.pairs;
  const sim = state.sim === "none" ? null : state.sim;
  // only closed / own / base are ever lake fills: no lake number for a restricted or open pair
  const lakeless = (k) => /restricted|open/.test(k);
  const val = (cond, k) => {{ if (cond === "lake" && lakeless(k)) return null;
    const r = P[cond][k]; return r ? (sim ? Math.min(r[sim], r.normal) : r.worst) : null; }};
  // the gate's model: the temperature pairs are gauge DOTS (tokens only); open is never a line, so no boundary line for it
  const dot = (k) => /open/.test(k);
  const cell = (c, k) => {{ const v = (dot(k) && c !== "pure") ? null : val(c, k); return `<td class="n ${{v == null ? "" : cls(v)}}">${{f1(v)}}</td>`; }};
  const row = (label, k) => `<tr><td>${{label}}</td>` + ["pure", "line", "lake", "satellite"].map((c) => cell(c, k)).join("") + "</tr>";
  const bnd = (c) => Math.min(...["region", "mu"].flatMap((b) => ["closed", "own", "base", "restricted", "open"]
    .filter((s) => c === "pure" || s !== "open").map((s) => val(c, `${{s}}/${{b}}`) ?? 99)));
  const b = ["pure", "line", "lake", "satellite"].map((c) => {{ const v = c === "lake" ? null : bnd(c); return `<td class="n ${{v == null ? "" : cls(v)}}">${{f1(v)}}</td>`; }}).join("");
  const ink = T.ink ? `<tr><td>status vs ink</td><td class="n ${{cls(T.ink.normal)}}">${{f1(T.ink.normal)}}</td><td colspan="3" class="n ${{cls(T.ink.worst, 15)}}">worst ${{f1(T.ink.worst)}} (${{T.ink.status}})</td></tr>` : "";
  return `<table><thead><tr><th>${{th}}${{sim ? " · " + sim : " · worst"}}</th><th>pure</th><th>line</th><th>lake</th><th>sat</th></tr></thead><tbody>`
    + row("closed / own", "closed/own") + row("closed / base", "closed/base") + row("own / base", "own/base")
    + row("restricted / closed", "closed/restricted") + row("restricted / own", "own/restricted")
    + row("restricted / base", "base/restricted") + row("open / restricted (dots)", "restricted/open") + row("open / closed (dots)", "closed/open")
    + `<tr><td>boundary</td>${{b}}</tr>` + ink + `</tbody></table>`;
}}
function swatches(id) {{
  const th = state.ground === "dark" ? "dark" : state.ground === "cvd" ? "cvd" : "light";
  const c = D.meas[id].themes[th].colours;
  return ["closed", "own", "base", "restricted", "open"].map((s) => `<span class="sw"><i style="background:${{c[s]}}"></i>${{s}} ${{c[s]}}</span>`).join("")
    + `<span class="sw"><i style="background:${{c.region}}"></i>region ${{c.region}}</span>`
    + `<span class="sw"><i class="dash" style="border-color:${{c.mu}}"></i>unit ${{c.mu}}</span>`;
}}
function render() {{
  const grid = document.getElementById("grid");
  grid.innerHTML = "";
  for (const id of D.ids) {{
    const s = D.shots[`${{id}}|${{state.view}}|${{state.ground}}`];
    const col = document.createElement("section");
    col.className = "col";
    const filt = state.sim === "none" ? "" : `filter:url(#sim-${{state.sim}})`;
    col.innerHTML = `<h2>${{D.meas[id].label}}</h2><div class="swatches">${{swatches(id)}}</div>`
      + (s ? `<a class="map" href="${{s.full}}" target="_blank" rel="noopener"><img alt="${{D.meas[id].label}}, ${{state.view}}, ${{state.ground}}" src="${{s.src}}" style="${{filt}}"></a>`
           : `<div class="map empty">${{id === "D0" ? "superseded: not rendered" : state.view.startsWith("card") && state.ground === "satellite" ? "the card has no satellite ground" : "not rendered for this palette"}}</div>`)
      + numbers(id) + `<p class="summary">${{D.meas[id].summary}}</p>`;
    grid.appendChild(col);
  }}
}}
render();
</script>
</body>
</html>
"""
out = DIR / "compare.html"
out.write_text(page)
print(f"wrote {out} ({out.stat().st_size / 1e6:.1f} MB, {len(shots)} shots)")
