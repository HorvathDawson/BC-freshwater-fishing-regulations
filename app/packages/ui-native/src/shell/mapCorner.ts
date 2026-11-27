/**
 * (T1212, owner 2026-11-26: "do tiday with the attribution moved to botton corner") WHERE THE MAP'S © STANDS ON A PHONE.
 *
 * The foot block over the map (`map-foot`) is one of two shapes. On the Map tab, while the regulations are drawn, it is the
 * floating key chip at the LEFT (T1211, `MapKey`): the right corner is free, so the © takes the map's own bottom-right
 * corner, level with the chip. Anywhere else it is a bar across the whole foot (Conditions' ramp, Runs, a water layer's
 * key), and the © stands just above it. The desktop draws every key as a card off the foot (`mapFootInset` is 0 there).
 *
 * `keyFloats` must say exactly when `MapLegend` draws `MapKey` (T1212.corner.test.tsx holds the two together).
 */
export function keyFloats(o: { tab: string; tabsRail: boolean; statusOk: boolean; waterDrawn: string }): boolean {
  return o.tab === "map" && !o.tabsRail && o.statusOk && o.waterDrawn === "regulations";
}

/** how far the © sits above the map's foot (MapProps.insetCredits): 0 beside a floating key, else the foot's height */
export function creditsInset(floats: boolean, footInset: number): number {
  return floats ? 0 : footInset;
}
