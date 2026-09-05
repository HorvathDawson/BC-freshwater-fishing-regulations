/**
 * Legacy MapLibre filters, converted to expressions.
 *
 * WHY THIS EXISTS. The basemap's labels are confined to British Columbia with a `within`
 * test, which is expression-only. Protomaps writes most of its label filters in the LEGACY
 * syntax (`["in", "kind", "river", "stream"]`), and `all` is a valid operator in BOTH
 * syntaxes — so `["all", <expression>, <legacy>]` is ambiguous and MapLibre resolves the
 * whole thing as legacy, where `!` is not an operator:
 *
 *   layers.roads_labels_major.filter[1][0]: expected one of
 *   [==, !=, >, >=, <, <=, in, !in, all, any, none, has, !has], "!" found
 *
 * The style-spec ships `convertFilter`, but `deps.md` has that package as TEST-ONLY —
 * validating the generated style against the renderer's own schema — and importing it into
 * shipped code would change what the app carries without saying so. This is the small part
 * of it we actually need.
 *
 * `legacy-filter.test.ts` runs every filter Protomaps really emits through `isLegacy` and
 * asserts this handles it, so an upgrade that introduces an operator we do not know about
 * fails a test instead of failing the map at runtime.
 */

type Filter = unknown;

const COMPARE = new Set(["==", "!=", ">", ">=", "<", "<="]);

/**
 * Is this the legacy syntax? The style-spec's own rule, which turns on the SHAPE of the
 * arguments rather than the operator alone — `["==", "kind", "x"]` is legacy and
 * `["==", ["get", "kind"], "x"]` is an expression, and they share an operator.
 */
export function isLegacy(filter: Filter): boolean {
  if (typeof filter === "boolean" || filter == null) return false;
  if (!Array.isArray(filter) || filter.length === 0) return false;
  const op = filter[0];
  if (op === "!in" || op === "!has" || op === "none") return true;
  if (op === "in")
    // Legacy is `["in", key, ...values]`; the expression form takes a lookup as its second
    // argument, or a literal collection as its third.
    return filter.length >= 3 && typeof filter[1] === "string" && !Array.isArray(filter[2]);
  if (op === "has")
    return filter.length === 2 && typeof filter[1] === "string";
  if (COMPARE.has(op as string))
    return filter.length === 3 && !Array.isArray(filter[1]) && !Array.isArray(filter[2]);
  if (op === "all" || op === "any") return filter.slice(1).some(isLegacy);
  return false;
}

/** The property a legacy filter names, as an expression. `$type`/`$id` have their own. */
const get = (key: unknown) =>
  key === "$type" ? ["geometry-type"] : key === "$id" ? ["id"] : ["get", key];

/**
 * Convert if it is legacy; return it untouched if it is already an expression.
 *
 * Unknown operators are returned as-is rather than guessed at: a filter this does not
 * understand is a filter it must not rewrite, and the test above is what makes sure we
 * learn about one rather than shipping a silent mangling.
 */
export function toExpression(filter: Filter): Filter {
  if (!Array.isArray(filter) || filter.length === 0) return filter;
  if (!isLegacy(filter)) {
    // An `all`/`any` can be an expression overall and still hold a legacy child.
    const op = filter[0];
    if (op === "all" || op === "any")
      return [op, ...filter.slice(1).map(toExpression)];
    return filter;
  }
  const [op, ...rest] = filter as [string, ...unknown[]];
  switch (op) {
    case "==": case "!=": case ">": case ">=": case "<": case "<=":
      return [op, get(rest[0]), rest[1]];
    case "has":  return ["has", rest[0]];
    case "!has": return ["!", ["has", rest[0]]];
    case "in":   return ["match", get(rest[0]), rest.slice(1), true, false];
    case "!in":  return ["!", ["match", get(rest[0]), rest.slice(1), true, false]];
    case "all":  return ["all", ...rest.map(toExpression)];
    case "any":  return ["any", ...rest.map(toExpression)];
    case "none": return ["!", ["any", ...rest.map(toExpression)]];
    default:     return filter;
  }
}
