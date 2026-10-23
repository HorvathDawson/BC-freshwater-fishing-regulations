/**
 * (T114) THE SEARCH KEY — the one answer to "does this query match this water's name or alias?".
 *
 * Lower-case, accents off, apostrophes gone, whitespace collapsed: "Barrière Lake" → "barriere lake",
 * "Children's Fishing Pond" → "childrens fishing pond". It is what the ranking compares (`rankWaters`), and it is what
 * the bundle STORES: the pipeline writes `item.name_key` / `alias.alias_key` by the same rule
 * (`pipeline/common/search_key.py`), and both SEARCH queries filter on those columns with `searchKey(query)`. Before,
 * the SQL filtered the raw name with LIKE (case-folding ASCII only) while the ranking folded accents, so "barriere"
 * never reached the ranking and "childrens" never found the pond.
 *
 * ONE RULE, TWO LANGUAGES: `pipeline/common/search_key_cases.json` is the fixture table both sides are tested against
 * (`app/tools/search-key-parity.test.ts`, `pipeline/tests/test_search_key.py`). Change the rule here and there
 * together, with a case.
 *
 * No imports, on purpose: `tools/build-fixture.mjs` imports this file directly to key the development bundle.
 */
export function searchKey(s: string): string {
  return s.normalize("NFD").replace(/[̀-ͯ]/g, "")
    .toLowerCase().replace(/[’']/g, "").replace(/\s+/g, " ").trim();
}
