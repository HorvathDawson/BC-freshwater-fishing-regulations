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
import { SALMON_TIPS } from "./salmonTips";

/** sheetHtml sp: — null when the species is neither in the export's book nor pictured (the prototype's `!f && !pics`).
 *  (T344, owner 2026-10-31) A GROUP the book names ("Whitefish": lake, mountain) is one page per member that has a
 *  sheet, swiped between; a group with one such member is that member's sheet. The members are the book's. */
export function fishSheet(inp: RegsInputs, key: SheetKey, code: string, fish: FishManifest = FISH): SheetVM | null {
  const group = fish[code] ? undefined : inp.statics.exportSpecies.groups?.[code];
  if (group) {
    const pages = group.members.flatMap((m) => {
      const s = fishSheet(inp, `sp:${m}`, m, fish);
      return s ? [s] : [];
    });
    if (pages.length < 2) return pages[0] ? { ...pages[0], key } : null;
    return { key, title: group.name, sub: null, blocks: [{ k: "swipe", pages }] };
  }
  const pics = fish[code] ?? [];
  if (!inp.statics.exportSpecies.fish[code] && !pics.length) return null;
  const blocks: SheetBlock[] = pics.map((p) => ({ k: "image", code, file: p.file, alt: p.caption, width: p.width, height: p.height,
    credit: `${p.caption}${p.phase ? WORDS.DOT + p.phase : ""}${WORDS.DOT}${p.credit} (${p.source})` }));
  const tips = FISH_TIPS[code] ?? SALMON_TIPS[code] ?? [];
  if (tips.length) blocks.push({ k: "head", text: WORDS.SHEET_LOOK_FOR }, { k: "list", items: tips.map((t) => ({ line: [text(t)], strong: null })) });
  return { key, title: fishNames(inp, fish)(code), sub: null, blocks };
}
