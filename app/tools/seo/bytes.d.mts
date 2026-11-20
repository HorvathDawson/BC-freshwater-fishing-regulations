/** T917 — the types of bytes.mjs (plain JavaScript so a job thread loads it without the module runner). */
export interface Card { readonly fontFiles: readonly string[]; readonly font: string; readonly width: number; readonly height: number }
export function compressPage(raw: Buffer): { gz: Buffer; br: Buffer };
export function drawCard(svg: string, card: Card): Uint8Array;
