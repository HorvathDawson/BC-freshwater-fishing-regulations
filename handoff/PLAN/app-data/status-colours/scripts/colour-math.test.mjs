// T47: the measurement model, pinned against the screenshots it claims to describe.
//
//   node --test handoff/PLAN/app-data/status-colours/scripts/colour-math.test.mjs
//
// Review 2026-10-21 found three ways the model disagreed with the map it measures: lakes were laid over LAND (the map
// lays a status lake over the basemap's WATER), `open` was left out of the boundary checks, and `restricted` was treated
// as card-only although the map draws it (the permit-area hatch, temperature mode's "warm"). Each test below fails on
// the old model.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "node:test";
import { GROUNDS, isMapPair, LAKE_GROUND, measure, over, parseHex, REPO, STATUSES } from "./colour-math.mjs";

const dist = (a, b) => Math.max(...parseHex(a).map((c, i) => Math.abs(c - parseHex(b)[i])));

test("a status lake is its colour over the basemap WATER, as the screenshots show", () => {
  // Kootenay Lake (own) at (400,500) in the z9 light shots, 9x9 mean: today #94aa86 (own #B37B00 at 0.4), B #aea061
  // (own #D68100 at 0.6). Over land #d9cecc the model would give #c39b7a and #d7a052 — 47 and 41 levels off. Over water
  // they are 3 and 10 off (JPEG q≈85 on a 9x9 mean).
  assert.ok(dist(over("#B37B00", LAKE_GROUND.light, 0.4), "#94aa86") <= 6, over("#B37B00", LAKE_GROUND.light, 0.4));
  assert.ok(dist(over("#D68100", LAKE_GROUND.light, 0.6), "#aea061") <= 12, over("#D68100", LAKE_GROUND.light, 0.6));
  // the dark theme fills at 0.85: today's own #F0E442 over Kootenay renders #d5c051
  assert.ok(dist(over("#F0E442", LAKE_GROUND.dark, 0.85), "#d5c051") <= 16, over("#F0E442", LAKE_GROUND.dark, 0.85));
});

test("no ground is a status-coloured pixel", () => {
  // `forest-dark` #94aa86 was sampled from Cultus Lake: it IS today's own lake fill, and vanishes from the A renders.
  for (const [theme, g] of Object.entries(GROUNDS))
    for (const [name, hex] of Object.entries(g))
      assert.ok(dist(hex, over("#B37B00", LAKE_GROUND.light, 0.4)) > 8, `${theme}.${name} ${hex} is today's own lake fill`);
});

test("the lake column is measured over water", () => {
  const m = measure({});
  for (const th of ["light", "dark", "cvd"]) assert.equal(m.pairs[th].lake["closed/own"].worst.ground, "water", th);
  // today's light closed/own as lake fills over water (17.0), not over land (12.8)
  assert.ok(Math.abs(m.pairs.light.lake["closed/own"].worst.dE - 17.0) < 0.1, String(m.pairs.light.lake["closed/own"].worst.dE));
});

test("every status a boundary can meet on the map is a map pair, open and restricted included", () => {
  for (const s of STATUSES) for (const b of ["region", "mu"]) assert.ok(isMapPair(s, b), `${s}/${b}`);
});

test("restricted is drawn on the map: next to the water statuses (permit areas) and in the temperature trio", () => {
  for (const s of ["closed", "own", "base"]) assert.ok(isMapPair("restricted", s), `restricted/${s}`);
  assert.ok(isMapPair("open", "restricted") && isMapPair("open", "closed"), "temperature: cool / warm / critical");
  // never adjacent: open is drawn only as a temperature dot, and the water statuses are not drawn in that view
  assert.ok(!isMapPair("own", "open") && !isMapPair("base", "open"));
  // a lake fill is only ever a water status
  assert.ok(isMapPair("closed", "own", "lake") && !isMapPair("restricted", "own", "lake"));
});

test("the doc's tables come from this model", () => {
  const md = readFileSync(join(REPO, "handoff/PLAN/app-data/status-colours/T47-colours.md"), "utf8");
  assert.match(md, /lake fills at `opacity\.lake` over the basemap's water/);
  assert.doesNotMatch(md, /forest-dark/);
});

// ---- the doc's proposed gate (review 2026-10-22: it laid lakes on land, lowered the ticket's boundary floor of 20 to
// 15 in every theme, and left `open` out of the boundary loop)
const MD = () => readFileSync(join(REPO, "handoff/PLAN/app-data/status-colours/T47-colours.md"), "utf8");
const proposedTs = () => {
  const md = MD();
  const s = md.slice(md.indexOf("## Proposed gate"));
  return s.slice(s.indexOf("```ts"), s.indexOf("```", s.indexOf("```ts") + 5));
};

test("the proposed gate lays a status lake on the basemap's water, not the land", () => {
  const ts = proposedTs();
  assert.doesNotMatch(ts, /a lake sits on the land/);
  assert.doesNotMatch(ts, /GROUND\[th\]!\[0\]/);
  assert.match(ts, /LAKE_GROUND/);
  assert.doesNotMatch(ts, /light: \[[^\]]*#94aa86/, "the status-coloured 'forest-dark' sample is not a ground");
});

test("the proposed gate holds every boundary off EVERY status at the ticket's 20", () => {
  const ts = proposedTs();
  assert.match(ts, /const BOUNDARY_FLOOR = 20;/);
  assert.doesNotMatch(ts, /\[\.\.\.WATER_STATUSES, "restricted"\]/, "open left out of the boundary loop");
  assert.match(ts, /"closed", "own", "base", "restricted", "open"/);
});

test("the doc's gate results are what the proposed gate computes", async () => {
  const { gate } = await import("./gate-proposal.mjs");
  const cands = JSON.parse(readFileSync(join(REPO, "handoff/PLAN/app-data/status-colours/candidates.json"), "utf8"));
  const md = MD();
  for (const id of ["baseline", "A", "B", "C"]) {
    const r = gate(cands[id].themes);
    const label = id === "baseline" ? "today" : id;
    const pass = r.checks - r.fails.length;
    assert.match(md, new RegExp(`\\| ${label} \\| ${pass} / ${r.checks} pass`), `${id}: ${pass} / ${r.checks}`);
  }
  // the corrected model: A's closed/own lake passes (15.2 over water); every candidate fails boundary vs open (14.9, deutan)
  const a = gate(cands.A.themes);
  assert.ok(!a.fails.some((f) => f.check === "status lake" && f.what.startsWith("closed/own")));
  for (const id of ["A", "B", "C"])
    assert.ok(gate(cands[id].themes).fails.some((f) => f.theme === "light" && f.what.startsWith("region/open") && f.dE === 14.9), id);
  assert.doesNotMatch(md, /lakes still 12\.6 apart|lake fill pairs 12\.6|only one that fixes lakes|\*\*8 fail\*\*|9 pass \/ \*\*1 fail|12\.8 \(tritan\)/);
});

test("table 1 flags a boundary against open and restricted (no ᶜ)", () => {
  const md = MD();
  for (const k of ["open/region", "open/mu", "restricted/region", "restricted/mu"])
    assert.doesNotMatch(md, new RegExp(`\\| ${k} ᶜ \\|`), k);
});
