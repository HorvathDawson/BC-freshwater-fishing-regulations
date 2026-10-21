/**
 * T36 part 2 — the fish sheet (`sp:CODE`): every picture of the species with its credit, then "What to look for".
 *
 * Not a regulation: the pictures and credits are the fish manifest's (T20 `FISH`), the tips page v36's FISHART table
 * (`FISH_TIPS`, emitted by app/tools/emit-fish.mjs), the name the export's species book's (prototype fishName).
 *
 * Ports (regs-v11.html @ 7fb6809f): sheetHtml sp: branch (`.pic` + `.cred`, `.tips`), fishName.
 */
import { FISH, FISH_TIPS, type FishManifest } from "../fish.generated";
import type { RegsInputs } from "../inputs";
import type { SheetBlock, SheetVM } from "../model";
import type { SheetKey } from "../sheetKey";
import { fishNames } from "../tree";
import { WORDS } from "../words";
import { text } from "../line";

/** sheetHtml sp: — null when the species is neither in the export's book nor pictured (the prototype's `!f && !pics`). */
export function fishSheet(inp: RegsInputs, key: SheetKey, code: string, fish: FishManifest = FISH): SheetVM | null {
  const pics = fish[code] ?? [];
  if (!inp.statics.exportSpecies.fish[code] && !pics.length) return null;
  const blocks: SheetBlock[] = pics.map((p) => ({ k: "image", code, file: p.file, alt: p.caption, width: p.width, height: p.height,
    credit: `${p.caption}${p.phase ? WORDS.DOT + p.phase : ""}${WORDS.DOT}${p.credit} (${p.source})` }));
  const tips = FISH_TIPS[code] ?? [];
  if (tips.length) blocks.push({ k: "head", text: WORDS.SHEET_LOOK_FOR }, { k: "list", items: tips.map((t) => [text(t)]) });
  return { key, title: fishNames(inp, fish)(code), sub: null, blocks };
}
