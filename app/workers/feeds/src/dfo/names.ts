// T225 port of pipeline/common/names.py: THE name key (`key`) and `drop_notes`.
import { strip } from "./py";

/** NFKD, combining marks deleted (unicodedata.combining != 0 ≈ the Mn/Mc/Me marks NFKD splits off). */
export const fold = (text: string): string => (text || "").normalize("NFKD").replace(/\p{M}/gu, "");

/** str.casefold() — lower(), plus the full folds lower() misses that a water name could carry. */
const casefold = (s: string): string => s.toLowerCase().replace(/ß/g, "ss").replace(/ς/g, "σ");

const APOSTROPHES = /['’‘ʼ`]/g;
const SEPARATORS = /[^\p{L}\p{N}]+/gu; // Python [\W_]+: not a letter or digit, and the underscore

export function key(text: string): string {
  const s = casefold(fold(text)).replace(APOSTROPHES, "");
  return strip(s.replace(SEPARATORS, " "));
}

export const dropNotes = (text: string): string => (text || "").replace(/\([^)]*\)/g, " ");
