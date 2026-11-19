/** (T910) The phone decodes its rules on the JS thread, a slice per turn, as it always has (T287). Web twin: rulesDecode.web.ts. */
import type { Row } from "../sql/db";
import type { StoredRule } from "./reads";
import { decodeRulesHere, type RulesBase } from "./rulesShared";

export function decodeRules(rs: readonly Row[], base: RulesBase): Promise<ReadonlyMap<number, StoredRule>> {
  return decodeRulesHere(rs, base);
}
