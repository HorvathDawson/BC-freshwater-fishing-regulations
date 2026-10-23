/**
 * (T114) THE SEARCH KEY (`@app/core` `searchKey`), held to the ONE case table the pipeline's `search_key` is also held to
 * (`pipeline/common/search_key_cases.json`, `pipeline/tests/test_search_key.py`): the key the bundle stores and the key
 * the app makes of a query are one rule in two languages, and this table is where they must agree. It lives in tools/
 * because core may import nothing, not even node:fs to read the shared table.
 *
 * Mutations, each failing a case: drop the accent fold ("barrière lake" for "Barrière Lake"); drop the apostrophe
 * delete ("children's fishing pond"); collapse only spaces, not `\s` ("mcarthur\tcreek"); lower-case before NFD and
 * strip marks after (same result — the order is not pinned, the output is).
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { searchKey } from "../packages/core/src/searchKey";
import { searchKey as fromIndex } from "@app/core";

const TABLE = JSON.parse(readFileSync(new URL("../../pipeline/common/search_key_cases.json", import.meta.url),
                                      "utf8")) as { cases: [string, string][] };

describe("searchKey: the shared case table", () => {
  it("is a real table, holding the ticket's two cases", () => {
    expect(TABLE.cases.length).toBeGreaterThan(10);
    expect(TABLE.cases).toContainEqual(["Barrière Lake", "barriere lake"]);
    expect(TABLE.cases).toContainEqual(["Children's Fishing Pond", "childrens fishing pond"]);
  });

  for (const [text, key] of TABLE.cases) {
    it(`${JSON.stringify(text)} → ${JSON.stringify(key)}`, () => {
      expect(searchKey(text)).toBe(key);
    });
  }

  it("is the one @app/core exports (no second spelling of the rule)", () => {
    expect(fromIndex).toBe(searchKey);
  });
});
