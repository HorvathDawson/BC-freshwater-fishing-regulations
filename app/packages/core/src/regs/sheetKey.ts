/**
 * The keys of the sheets a Regulations page can open (contracts.md §1). The app's keys carry NO state id — a
 * RegsPage is one state; the golden's keys embed it, and T36's `appKeyOf`/`goldenKeyOf` bridge the two.
 *
 * T00 declares the types only. T36 adds `parseSheetKey`, `appKeyOf`, `goldenKeyOf` and `profileKeyOf` here.
 */
import type { RuleKey } from "./line";

export type SheetKey =
  | `rule:${RuleKey}` | `gl:${string}`            // gl: one id, or ids joined by "," (multi-term sheet)
  | `sp:${string}` | "hatch" | "parts"
  | `srcs:${"bait" | "ways" | "always" | "boats"}` | `srcs:gl|${"mode" | "lines" | "hook" | "weight"}`
  | `licsrc:${ProfileKey}` | `other:o${number}` | "pos:pos" | `src:r${number}`;

export type ProfileKey = "adult" | "adult_non" | "adult_alien" | "u16_res" | "u16_non" | "u16_alien";
