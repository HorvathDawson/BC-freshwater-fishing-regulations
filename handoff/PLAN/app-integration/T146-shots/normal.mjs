// node normal.mjs <theme> <hex...>: NORMAL-vision ΔE of a no-access candidate against each status as token / thin line
// (worst ground) / lake fill — what the owner (normal vision) sees.
const W = new URL("../../../..", import.meta.url).pathname.replace(/\/$/, "");
const cm = await import(W + "/handoff/PLAN/app-data/status-colours/scripts/colour-math.mjs");
const cvd = await import(W + "/app/tools/cvd.ts");
const [th, ...xs] = process.argv.slice(2);
const t = cm.themeTokens(th);
const G = th === "dark" ? Object.values(cm.GROUNDS.dark) : Object.values(cm.GROUNDS.light);
const lake = th === "dark" ? cm.GROUNDS.dark.river : cm.GROUNDS.light.river;
const la = Number(t["opacity.lake"]);
for (const x of xs) {
  const out = [];
  for (const s of ["closed", "own", "base", "restricted", "open"]) {
    const c = t[`color.status.${s}`];
    const line = Math.min(...G.map((g) => cvd.deltaE(cm.over(x, g, 0.6), cm.over(c, g, 0.6))));
    const lk = cvd.deltaE(cm.over(x, lake, la), cm.over(c, lake, la));
    out.push(`${s} ${cvd.deltaE(x, c).toFixed(1)}/${line.toFixed(1)}/${lk.toFixed(1)}`);
  }
  console.log(th, x, out.join(" | "));
}
