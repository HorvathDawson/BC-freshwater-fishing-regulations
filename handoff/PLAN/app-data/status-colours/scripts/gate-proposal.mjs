// T47: the PROPOSED gate (T47-colours.md "Proposed gate"), run in Node against today's palette and every candidate.
//
//   node handoff/PLAN/app-data/status-colours/scripts/gate-proposal.mjs [--json]
//
// The same assertions as the vitest file the doc proposes, over colour-math.mjs (the app's cvd.ts maths), so the
// results table in the doc is reproducible without writing a test into app/tools/. Exports gate() for the tests.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { BOUNDARIES, colours, GROUNDS, LAKE_GROUND, layered, MAP_STATUSES, over, REPO, STATUSES, themeTokens, worst } from "./colour-math.mjs";

export const FLOORS = { status: 20, line: 18, lake: 15, boundary: 20, boundaryLine: 10, neutralSpread: 16 };
const LINE = 0.6, EDGE = 0.8;
const THEMES = ["light", "dark", "cvd"];
const groundList = (th) => Object.values(th === "dark" ? GROUNDS.dark : GROUNDS.light);
const drawn = (a, b, gs, ca, cb) =>
  gs.map((g) => ({ g, ...worst(over(a, g, ca), over(b, g, cb)) })).sort((x, y) => x.dE - y.dE)[0];

/** Every failed assertion for one palette's per-theme overrides: [{ theme, check, what, dE }]. */
export function gate(overrides = {}, floors = FLOORS) {
  const fails = [];
  let checks = 0;
  const hold = (theme, check, what, dE, floor) => { checks++; if (dE < floor) fails.push({ theme, check, what, dE: +dE.toFixed(1), floor }); };
  for (const th of THEMES) {
    const t = themeTokens(th, overrides[th] ?? {});
    const c = colours(t);
    const lakeA = Number(t["opacity.lake"]);
    for (let i = 0; i < MAP_STATUSES.length; i++)
      for (let j = i + 1; j < MAP_STATUSES.length; j++) {
        const [a, b] = [MAP_STATUSES[i], MAP_STATUSES[j]];
        const p = worst(c[a], c[b]);
        hold(th, "status token", `${a}/${b} (${p.under})`, p.dE, floors.status);
        const l = drawn(c[a], c[b], groundList(th), LINE, LINE);
        hold(th, "status line", `${a}/${b} on ${l.g} (${l.under})`, l.dE, floors.line);
        const k = drawn(c[a], c[b], [LAKE_GROUND[th]], lakeA, lakeA);   // a status lake lies on the basemap's water
        hold(th, "status lake", `${a}/${b} (${k.under})`, k.dE, floors.lake);
      }
    for (const b of BOUNDARIES)
      for (const s of STATUSES) {          // all five: a boundary crosses permit areas and temperature dots too
        const p = worst(c[b], c[s]);
        hold(th, "boundary token", `${b}/${s} (${p.under})`, p.dE, floors.boundary);
        const l = drawn(c[b], c[s], groundList(th), EDGE, LINE);
        hold(th, "boundary line", `${b}/${s} on ${l.g} (${l.under})`, l.dE, floors.boundaryLine);
      }
    for (const b of BOUNDARIES) {
      const v = c[b].slice(1).match(/../g).map((x) => parseInt(x, 16));
      hold(th, "boundary neutral", `${b} ${c[b]} spread`, floors.neutralSpread - (Math.max(...v) - Math.min(...v)), 0);
    }
  }
  return { checks, fails };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const cands = JSON.parse(readFileSync(join(REPO, "handoff/PLAN/app-data/status-colours/candidates.json"), "utf8"));
  const out = {};
  for (const id of Object.keys(cands).filter((k) => !k.startsWith("$"))) out[id] = gate(layered(cands, id));
  if (process.argv.includes("--json")) console.log(JSON.stringify(out, null, 1));
  else for (const [id, r] of Object.entries(out)) {
    console.log(`== ${id}: ${r.checks - r.fails.length} / ${r.checks} assertions pass`);
    for (const f of r.fails) console.log(`   ${f.theme} ${f.check}: ${f.what} ${f.dE} < ${f.floor}`);
  }
}
