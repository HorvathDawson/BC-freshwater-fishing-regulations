/**
 * (T620 G25) A whole number with its thousands grouped by commas, the same on every device: "60,648". Not
 * `toLocaleString()`, which follows the phone's language (a French-Canadian phone printed "60 648" where the web in
 * English printed "60,648") and is not the same engine on Hermes and in a browser.
 */
export function grouped(n: number): string {
  const [whole, frac] = String(Math.abs(n)).split(".");
  const g = whole!.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return `${n < 0 ? "−" : ""}${g}${frac ? "." + frac : ""}`;
}

/**
 * (T620 G26) A point as the reader sees it: "49.0974, -121.9675", four decimals (about 11 m) wherever a point is
 * printed — a spot's page, its parking line, its list row. (A link's address keeps more; it is not read.)
 */
export function latLon(p: { lat: number; lon: number }): string {
  return `${p.lat.toFixed(4)}, ${p.lon.toFixed(4)}`;
}
