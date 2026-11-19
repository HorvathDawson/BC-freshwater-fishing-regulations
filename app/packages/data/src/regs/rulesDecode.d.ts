/**
 * The shared declaration for `./rulesDecode` (T910): the store's rules (`reads.rules()`), every row inflated and parsed.
 * `rulesDecode.native.ts`: on the JS thread, a slice per turn (T287, unchanged). `rulesDecode.web.ts`: the inflating in a
 * worker (fflate there too: the tables' preset dictionaries rule out `DecompressionStream`), the parsing here, a slice
 * per turn. Same rows, same refusals, same order. Bundlers pick by platform extension, TypeScript does not.
 */
import type { Row } from "../sql/db";
import type { StoredRule } from "./reads";
import type { RulesBase } from "./rulesShared";

export declare function decodeRules(rs: readonly Row[], base: RulesBase): Promise<ReadonlyMap<number, StoredRule>>;
