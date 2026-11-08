/**
 * (T508) Which of a water's views a pane opened over the map is held for (`paneReveal.ts`). A phone
 * (`holdViews.native.ts`): every view — Regulations, Conditions and the water's Map, each held until its content and its
 * live maps are drawn. The web (`holdViews.web.ts`): Regulations only — a web map is built only once it is seen
 * (`@app/map` `firstSeen`), so a map in a held (unseen) pane could never draw.
 */
export declare function holdsView(view: "regs" | "conditions" | "map"): boolean;
