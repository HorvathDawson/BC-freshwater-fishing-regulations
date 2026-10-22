/**
 * T43 — the credits of the fish pictures ONE page can show, for its foot ("Fish pictures"): not the app's whole list
 * (`FISH_CREDITS`, the menu's Data sources page), so a water with two fish never lists 39 lines.
 *
 * A page shows a fish's picture where its Keep tab pictures it (every `FishPicVM` with a picture), where an Other fish row
 * thumbs it, and inside the sheet that row opens; each such picture opens the fish's `sp:` sheet, which shows EVERY picture
 * of that fish — so every picture of each reached fish is credited. The lines are the manifest's (`FISH_CREDIT_OF`, emitted
 * by app/tools/emit-fish.mjs); nothing here words a credit. Not a regulation: no answer is decided here.
 */
import { FISH, FISH_CREDIT_OF, type FishManifest } from "./fish.generated";
import type { RegsPage } from "./model";

/** Every species code a value pictures: a `FishPicVM` with a picture (`sil: false`), or a sheet's `image` block. */
function pictured(v: unknown, out: Set<string>): void {
  if (Array.isArray(v)) { for (const x of v) pictured(x, out); return; }
  if (!v || typeof v !== "object") return;
  const o = v as Record<string, unknown>;
  if (typeof o.code === "string" && (o.sil === false || o.k === "image")) out.add(o.code);
  for (const x of Object.values(o)) pictured(x, out);
}

/** The credit lines of the pictures `page` can show, sorted, unique. A frozen page without `sheet` reads its Keep alone. */
export function creditsFor(page: RegsPage, fish: FishManifest = FISH,
                           creditOf: Readonly<Record<string, string>> = FISH_CREDIT_OF): readonly string[] {
  const codes = new Set<string>();
  pictured(page.keep, codes);
  for (const row of page.keep.otherFish ?? []) {
    if (row.thumb !== null) codes.add(row.thumb);
    if (typeof page.sheet === "function") pictured(page.sheet(row.open)?.blocks, codes);
  }
  const lines = new Set<string>();
  for (const c of codes) for (const img of fish[c] ?? []) { const l = creditOf[img.file]; if (l !== undefined) lines.add(l); }
  return [...lines].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}
