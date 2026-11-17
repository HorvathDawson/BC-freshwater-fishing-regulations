// T787 (owner 2026-11-16: "we should keep reading them for provenance but mark them hidden somehow so they are
// ignoired"), the worker half of pipeline/tests/test_t787_hidden_rows.py: a DFO table that STARTS inside an HTML comment
// (balanced or unterminated) is hidden — every row read, each `hidden`, and a hidden row answers nothing. The saved
// pages (regions 4, 5A, 7) are pinned against the Python by dfo.vectors.test.ts; these are the edges on hand pages.
import { describe, expect, it } from "vitest";
import { build } from "../src/dfo/feed";
import { commentSpans, parseRegion, tableHidden } from "../src/dfo/parse";
import { Interner, inForce, locationCalendar } from "../src/dfo/salmon";
import { REG } from "./wording.fixture";

const TABLE = `<table><tbody>
<tr><td>Nechako River</td><td>All</td><td>Sockeye</td><td>Aug 22 to Sept 21</td><td>2 per day</td></tr>
</tbody></table>`;
const page = (body: string) => `<html><body><h1 id="wb-cont">Region 7</h1><p>There is no fishing for salmon.</p>${body}</body></html>`;

describe("(T787) the comment spans of a page and the hidden table", () => {
  it("a balanced comment, then an unterminated one running to the end", () => {
    const html = "a<!-- one -->b<!-- two";
    expect(commentSpans(html)).toEqual([[1, 13], [14, html.length]]);
    expect(commentSpans("no comment")).toEqual([]);
  });
  it("a table starting inside a comment is hidden; one after a closed comment is not", () => {
    const hid = page(`<!-- ${TABLE} -->`);
    const shown = page(`<!-- a note --> ${TABLE}`);
    expect([tableHidden(hid, hid.indexOf("<table")), tableHidden(shown, shown.indexOf("<table"))]).toEqual([true, false]);
  });
});

describe("(T787) every row of a hidden table is kept, marked, and answers nothing", () => {
  for (const [name, body] of [["balanced", `<!-- ${TABLE} -->`], ["unterminated", `<!-- ${TABLE}`]] as const)
    it(`${name}: the rows read, each hidden; the typed rules hidden; the locator names them; no verdict`, () => {
      const p = parseRegion(page(body), "7");
      expect([p.rows.length, p.rows.every((r) => r.hidden), p.table_hidden]).toEqual([1, true, true]);
      const recs = [{ fingerprint: "f".repeat(16), species: "Sockeye", dates: "Aug 22 to Sept 21", limits_gear: "2 per day",
                      fishery_notices: [], source: "table", order: 0, hidden: true as const }];
      const [loc] = build({ region: "7", date_modified: null, preamble: [], rules: recs }, true, REG).locators;
      expect([loc!.hidden, loc!.rules.length > 0, loc!.rules.every((r) => r.hidden === true), loc!.words]).toEqual([[0], true, true, undefined]);
      expect(inForce(loc!.rules)).toEqual([]);
      const v = new Interner<unknown>();
      expect([locationCalendar(loc!.rules as never[], v, new Interner<unknown>()).sp.every((r) => r.length === 2 && r[1] === -1), v.items]).toEqual([true, []]);
    });
  it("a visible table: no row hidden, no `table_hidden` key", () => {
    const p = parseRegion(page(`<!-- a note --> ${TABLE}`), "7");
    expect([p.rows.map((r) => r.hidden), "table_hidden" in p]).toEqual([[false], false]);
  });
});
