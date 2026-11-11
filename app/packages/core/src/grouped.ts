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
