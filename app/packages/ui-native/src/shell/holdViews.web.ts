/**
 * (T508) The web holds the Regulations view only: a web map is built once it is seen (`@app/map` `firstSeen`), so the
 * Conditions and Map views' maps could never draw in a held pane — `holdViews.d.ts`. The phone twin is
 * holdViews.native.ts.
 */
export function holdsView(view: "regs" | "conditions" | "map"): boolean {
  return view === "regs";
}
