// T47: measure today's palette and every candidate; write measurements.json and the tables in T47-colours.md.
//
//   node handoff/PLAN/app-data/status-colours/scripts/measure.mjs
//
// Reads candidates.json (each candidate laid on the baseline, colour-math.mjs `layered`) and the theme files for every
// token no candidate names. Rewrites only the text between the GENERATED markers in T47-colours.md.
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { BOUNDARIES, GROUNDS, isMapPair, LAKE_GROUND, layered, LINE_ALPHA, measure, REPO, STATUSES, VISIONS, themeTokens } from "./colour-math.mjs";

const DIR = join(REPO, "handoff/PLAN/app-data/status-colours");
const cands = JSON.parse(readFileSync(join(DIR, "candidates.json"), "utf8"));
const ids = Object.keys(cands).filter((k) => !k.startsWith("$"));
const THEMES = ["light", "dark", "cvd"];

const results = {};
for (const id of ids) results[id] = measure(layered(cands, id));

const f1 = (x) => (x === undefined ? "–" : x.toFixed(1));
const V = { normal: "n", protan: "p", deutan: "d", tritan: "t" };
const cell = (r, withGround = true) => (r ? `${f1(r.worst.dE)} ${V[r.worst.under]}${withGround && r.worst.ground && r.worst.ground !== "-" ? `·${r.worst.ground}` : ""}` : "–");
const bold = (s, x, floor) => (x < floor ? `**${s}**` : s);
const pairs = (th) => Object.keys(results.baseline.pairs[th].pure);

// ---- 1. today, every pair, every theme
const out = [];
out.push("### 1 · Today's palette, every pair (ΔE2000)\n");
out.push("Pure = the two tokens. Line = both drawn as thin lines (coverage 0.6; boundaries 0.8) over the same ground, worst ground. " +
  "Lake = both as lake fills at `opacity.lake` over the basemap's water (a status lake is drawn on the basemap's own water polygon). Satellite = thin lines over the Esri imagery grounds. " +
  "Every column after `pure n` is the WORST of normal vision and protan / deutan / tritan under Viénot 1999 and Machado 2009; " +
  "the letter says which (n p d t), then the ground. **Bold** = under 20 for a status pair a reader sees side by side on the map " +
  "(closed / own / base as lines and lakes; restricted, the permit-area colour, against each of them; the temperature trio open / restricted / closed; and any boundary against any status).\n");
for (const th of THEMES) {
  const c = results.baseline[th];
  out.push(`\n#### ${th} — closed ${c.closed} · own ${c.own} · base ${c.base} · restricted ${c.restricted} · open ${c.open} · region ${c.region} · unit ${c.mu}\n`);
  out.push("| pair | pure n | pure p | pure d | pure t | pure worst | line | lake | satellite |");
  out.push("|---|---|---|---|---|---|---|---|---|");
  for (const k of pairs(th)) {
    const P = results.baseline.pairs[th];
    const [a, b] = k.split("/");
    const onMap = isMapPair(a, b);
    const flag = (r, cond) => (r && isMapPair(a, b, cond) ? bold(cell(r), r.worst.dE, 20) : cell(r));
    out.push(`| ${k}${onMap ? "" : " ᶜ"} | ${VISIONS.map((v) => f1(P.pure[k][v])).join(" | ")} | ${flag(P.pure[k])} | ${flag(P.line[k])} | ${flag(P.lake[k], "lake")} | ${flag(P.satellite[k])} |`);
  }
}
out.push("\nᶜ = a pair that is never adjacent on the map (own or base against open: open is only a temperature dot, and that view draws no status water; and region against unit, one neutral by design in the candidates); reported, not flagged. Only closed / own / base are ever lake fills, so the lake column is flagged for those pairs only.\n");

// ---- 2. card contrast
out.push("### 2 · Status words on the card (WCAG contrast)\n");
out.push("`on card` = the colour as a word or swatch on `color.ui.card` (the ticket's floor: 3:1). `pill` = the word on a 13.3% tint of " +
  "itself (tools/cvd.test.ts holds closed / restricted / open to 4.5:1 there; own and base are never pill text).\n");
out.push("| palette | theme | closed | own | base | restricted | open |");
out.push("|---|---|---|---|---|---|---|");
for (const id of ids)
  for (const th of THEMES) {
    const C = results[id].card[th];
    out.push(`| ${id} | ${th} | ${STATUSES.map((s) => `${C[s].hex} ${C[s].onCard.toFixed(2)} / ${C[s].onTint.toFixed(2)}`).join(" | ")} |`);
  }

// ---- 3. candidates side by side on the pairs that matter
out.push("\n### 3 · Candidates against today, the pairs that matter (worst ΔE2000 over all visions and both models)\n");
out.push("`bnd` = the WORST of region and unit boundary against ALL five statuses (closed / own / base / restricted / open). In A–C the unit boundary is the region's " +
  "colour on purpose (an alias), told apart by width and dash, so region/unit is not a colour pair there.\n");
for (const th of THEMES) {
  out.push(`\n#### ${th}\n`);
  out.push("| palette | closed | own | base | region | closed/own pure · line · lake · sat | closed/base line | own/base line | bnd pure | bnd line | bnd sat |");
  out.push("|---|---|---|---|---|---|---|---|---|---|---|");
  for (const id of ids) {
    const R = results[id], P = R.pairs[th], c = R[th];
    const bnd = (cond) => {
      let w = { dE: Infinity };
      for (const b of BOUNDARIES) for (const s of STATUSES) { const r = P[cond][`${s}/${b}`]; if (r && r.worst.dE < w.dE) w = { ...r.worst, pair: `${s}/${b}` }; }
      return `${bold(f1(w.dE), w.dE, 20)} ${V[w.under]} (${w.pair})`;
    };
    const co = ["pure", "line", "lake", "satellite"].map((k) => bold(f1(P[k]["closed/own"].worst.dE), P[k]["closed/own"].worst.dE, 20)).join(" · ");
    out.push(`| ${id} | ${c.closed} | ${c.own} | ${c.base} | ${c.region}${c.mu === c.region ? " (= unit)" : ` / unit ${c.mu}`} | ${co} | ` +
      `${bold(f1(P.line["closed/base"].worst.dE), P.line["closed/base"].worst.dE, 20)} | ${bold(f1(P.line["own/base"].worst.dE), P.line["own/base"].worst.dE, 20)} | ${bnd("pure")} | ${bnd("line")} | ${bnd("satellite")} |`);
  }
}

// ---- 3b. all five statuses (part 2): the minima search-d.mjs maximised, per palette
out.push("\n### 3b · All five statuses, the minima palette D maximised (worst ΔE2000 over all visions and both models)\n");
out.push("Each cell is the WORST pair of its group, with the pair and the reader. `water` = closed / own / base against each other; " +
  "`permit · temperature` = restricted against closed, own and base (the permit-area hatch over water) and open against restricted and closed " +
  "(the temperature dots); `boundary` = the region and unit boundaries against all five. Satellite = thin lines over the Esri imagery grounds.\n");
const WATER_P = ["closed/own", "closed/base", "own/base"];
const OTHER_P = ["closed/restricted", "own/restricted", "base/restricted", "restricted/open", "closed/open"];
const BND_P = STATUSES.flatMap((s) => BOUNDARIES.map((b) => `${s}/${b}`));
for (const th of THEMES) {
  out.push(`\n#### ${th}\n`);
  out.push("| palette | water token | water line | water lake | water satellite | permit · temperature token | … line | … satellite | boundary token | boundary line |");
  out.push("|---|---|---|---|---|---|---|---|---|---|");
  for (const id of ids) {
    const P = results[id].pairs[th];
    const min = (cond, keys) => {
      let w = null;
      for (const k of keys) { const r = P[cond][k]; if (r && (!w || r.worst.dE < w.dE)) w = { ...r.worst, k }; }
      return w ? `${bold(f1(w.dE), w.dE, 20)} ${V[w.under]} ${w.k}` : "–";
    };
    out.push(`| ${id} | ${min("pure", WATER_P)} | ${min("line", WATER_P)} | ${min("lake", WATER_P)} | ${min("satellite", WATER_P)} | ` +
      `${min("pure", OTHER_P)} | ${min("line", OTHER_P)} | ${min("satellite", OTHER_P)} | ${min("pure", BND_P)} | ${min("line", BND_P)} |`);
  }
}

// ---- 4. the grounds used
out.push("\n### 4 · The grounds (sampled from the baseline screenshots)\n");
for (const [k, g] of Object.entries(GROUNDS)) out.push(`- ${k}: ${Object.entries(g).map(([n, h]) => `${n} \`${h}\``).join(", ")}`);
out.push(`- lake: a status lake fill lies on the basemap's water: ${Object.entries(LAKE_GROUND).map(([th, h]) => `${th} \`${h}\``).join(", ")}`);
out.push(`- line coverage ${LINE_ALPHA}; boundary coverage 0.8; lake fill at the theme's \`opacity.lake\` (light ${themeTokens("light")["opacity.lake"]}, dark ${themeTokens("dark")["opacity.lake"]}, cvd ${themeTokens("cvd")["opacity.lake"]}; B and C raise light to 0.6).`);

const md = join(DIR, "T47-colours.md");
const doc = readFileSync(md, "utf8");
const [B, E] = ["<!-- BEGIN GENERATED (scripts/measure.mjs) -->", "<!-- END GENERATED -->"];
const i = doc.indexOf(B), j = doc.indexOf(E);
if (i < 0 || j < 0) throw new Error("T47-colours.md has no GENERATED markers");
writeFileSync(md, `${doc.slice(0, i + B.length)}\n${out.join("\n")}\n${doc.slice(j)}`);

// measurements.json: what compare.html reads
const slim = {};
for (const id of ids) {
  slim[id] = { label: cands[id].label, summary: cands[id].summary, themes: {} };
  for (const th of THEMES) {
    const P = results[id].pairs[th];
    slim[id].themes[th] = {
      colours: results[id][th],
      card: results[id].card[th],
      pairs: Object.fromEntries(Object.entries(P).map(([cond, rows]) => [cond, Object.fromEntries(Object.entries(rows).map(([k, r]) =>
        [k, { ...Object.fromEntries(VISIONS.map((v) => [v, +r[v].toFixed(1)])), worst: +r.worst.dE.toFixed(1), under: r.worst.under, ground: r.worst.ground }]))])),
    };
  }
}
writeFileSync(join(DIR, "measurements.json"), `${JSON.stringify(slim, null, 1)}\n`);
console.log(`wrote ${md} (generated section) and measurements.json for ${ids.join(", ")}`);
