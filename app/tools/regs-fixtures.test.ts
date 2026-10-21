/**
 * Drift check for core's store-free regs fixtures (packages/core/src/regs/testing/): they say "copied from the data,
 * not written", and this holds them to it. Both are SUBSETS of their source — rows and entries trimmed of keys the
 * tests do not read — so the check is: every key a fixture keeps has exactly the source's value (arrays whole,
 * objects key by key), and nothing in a fixture is absent from the source. Source: page_v36.html's own #data rule /
 * #answers display rule per rix and #data species (the oracle's inputs, always committed). The one exception is the
 * group speciesBook.ts says it hand-builds (`SA`), which must still be absent from the data. Core cannot read files
 * (layers.json bans node:fs there), so the check lives here, beside emit-fish.test.ts.
 */
import { describe, expect, it } from "vitest";
import { RULE_ROWS } from "../packages/core/src/regs/testing/ruleRows";
import { BOOK } from "../packages/core/src/regs/testing/speciesBook";
import { pageSource } from "../conformance/src/regs/ruleSources";

const page = pageSource();
type Bag = Record<string, Record<string, unknown> | undefined>;

describe("core regs fixtures match their source (page_v36.html #data / #answers)", () => {
  it("every RULE_ROWS row is a key-subset of the page's rule and display rule for its rix", () => {
    const rixes = Object.keys(RULE_ROWS).map(Number);
    expect(rixes.length).toBeGreaterThan(0);
    for (const rix of rixes) {
      const src = page.stored.get(rix);
      expect(src, `rix ${rix} in page_v36.html`).toBeDefined();
      expect(src!.facts, `rix ${rix} display rule`).not.toBeNull();
      const [exp, facts] = RULE_ROWS[rix]!;
      expect(src!.export, `rix ${rix} export`).toMatchObject(exp);
      expect(src!.facts, `rix ${rix} facts`).toMatchObject(facts);
    }
  });

  it("every BOOK entry is a key-subset of the page's export.species entry; SA (hand-built) is not in the data", () => {
    const src = page.book as unknown as Bag, mine = BOOK as unknown as Bag;
    let n = 0;
    for (const [sec, entries] of Object.entries(mine)) {
      for (const [code, v] of Object.entries(entries ?? {})) {
        if (sec === "groups" && code === "SA") { expect(src.groups?.SA, "SA is still absent from export.species").toBeUndefined(); continue; }
        const have = src[sec]?.[code];
        expect(have, `${sec}.${code} in export.species`).toBeDefined();
        expect(have, `${sec}.${code}`).toMatchObject(v as object);
        n++;
      }
    }
    expect(n).toBeGreaterThan(0);
  });
});
