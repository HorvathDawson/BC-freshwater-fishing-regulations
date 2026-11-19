/**
 * (T286/T287, T910) THE RULES, DECODED ONCE PER STORE: every `rule` + `display_rule` row inflated and parsed, a few hundred
 * at a time with the screen's turn between — never one long hold of the JS thread (all at once it was a ~0.3 s freeze on
 * the emulator). `decodeRulesHere` is the phone's way (`rulesDecode.native.ts`) and the web's when it has no worker
 * (`rulesDecode.web.ts`, said and reported); the row checks are the same for both.
 */
import type { ExportRule, RuleFacts } from "@app/core";
import type { Cell, Row } from "../sql/db";
import { RegsStoreError } from "./errors";
import type { StoredRule } from "./reads";

/** Rules decoded per turn of the JS thread, and the turn (a macrotask: the screen draws between). */
export const RULES_SLICE = 250;
export const turnOf = () => new Promise<void>((r) => setTimeout(r, 0));

/** What decoding reads off the store: a table's `val` (inflate + parse) and its preset dictionary. */
export interface RulesBase {
  val<T = unknown>(table: string, blob: Cell): T;
  dict?(table: string): Uint8Array | null;
}

/** Row `i` is rule `i` and has its display row, or the store is damaged. */
export function checkRuleRow(r: Row, i: number): void {
  if (Number(r.rix) !== i) throw new RegsStoreError("damaged", `regs store: the rules are out of sequence at ${i}`);
  if (r.f === null || r.f === undefined) throw new RegsStoreError("damaged", `regs store: rule ${i} has no display_rule`);
}

/** Inflate and parse every rule on this thread, a slice per turn. */
export async function decodeRulesHere(rs: readonly Row[], base: RulesBase): Promise<ReadonlyMap<number, StoredRule>> {
  const out = new Map<number, StoredRule>();
  for (let i = 0; i < rs.length; i++) {
    if (i && i % RULES_SLICE === 0) await turnOf();
    const r = rs[i]!;
    checkRuleRow(r, i);
    out.set(i, { rix: i, export: base.val<ExportRule>("rule", r.e), facts: base.val<RuleFacts>("display_rule", r.f) });
  }
  return out;
}
