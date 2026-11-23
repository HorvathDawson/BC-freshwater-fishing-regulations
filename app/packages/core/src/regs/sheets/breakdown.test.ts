/**
 * (T1031 item 4) A fish's source breakdown over hand-built rules: grouped by the authority that makes each rule (the
 * synopsis, DFO, a local authority — T981's citation), in that order, the caller's order kept inside a group, each rule
 * once, its role in words, its pages its own (a rule's highlight boxes ride on its book runs, T215).
 */
import { describe, expect, it } from "vitest";
import type { RuleView } from "../inputs";
import { ALL_SOURCES, WORDS } from "../words";
import { SOURCE_AUTHORITIES } from "../../tabSources";
import { authorityOf, breakdown } from "./breakdown";

const rule = (key: string, o: { dfo?: boolean; cited?: boolean; gate?: "closed" | "release"; box?: boolean } = {}): RuleView => ({
  key, missing: false, kind: o.gate ? "gate" : "quota", f: o.dfo ? { source: "dfo" } : {},
  export: { verbatim: `words of ${key}`, label: key, fields: {} },
  facts: { title: `Title ${key}`, ...(o.gate === "closed" ? { closure: true } : {}), book: [[21, 23]], ...(o.box ? { book_box: [{ pdf: 23, box: [1, 2, 3, 4] }] } : {}),
           ...(o.cited ? { cited: { link: "StanleyParkVan.com", url: "https://example.org/x", quote: "q" } } : {}) },
} as unknown as RuleView);

/** the block as drawn: each group's head, then each row "title · role · pages" */
function drawn(items: Parameters<typeof breakdown>[0]): string[] {
  const b = breakdown(items);
  if (b.k !== "sources") throw new Error("not a sources block");
  return b.groups.flatMap((g) => [g.where, ...g.rows.map((r) => [r.title, r.role, ...r.book.map((x) =>
    (x.t === "book" ? `p.${x.printed}${x.box ? "[hl]" : ""}` : x.t === "web" ? x.s : ""))].filter(Boolean).join(" · "))]);
}

describe("breakdown", () => {
  it("groups by authority in one order, keeps the caller's order inside a group, says each role, each rule once", () => {
    const a = rule("a"), d = rule("d", { dfo: true }), l = rule("l", { cited: true }), b = rule("b", { box: true });
    expect(drawn([{ r: l, role: "governs" }, { r: d, role: "limit" }, { r: b, role: "floor" }, { r: a, role: "" }, { r: b, role: "season" }]))
      .toEqual([SOURCE_AUTHORITIES.synopsis.name, "Title b · Size limit · p.21[hl]", "Title a · p.21",
        SOURCE_AUTHORITIES.dfo.name, "Title d · A limit inside the total · p.21",
        ALL_SOURCES.LOCAL, "Title l · Sets the number · StanleyParkVan.com"]);
  });
  it("a governing gate says whether it closes the fish or releases it", () => {
    expect(drawn([{ r: rule("c", { gate: "closed" }), role: "governs" }])[1]).toBe(`Title c · ${WORDS.SHEET_CLOSES_IT} · p.21`);
    expect(drawn([{ r: rule("r", { gate: "release" }), role: "governs" }])[1]).toBe(`Title r · ${WORDS.SHEET_RELEASE_ALL} · p.21`);
  });
  it("(W46) a role with no words is refused, never drawn blank; no role at all is no role words", () => {
    expect(() => breakdown([{ r: rule("x"), role: "invented" }])).toThrow(/no words for the role "invented"/);
    expect(drawn([{ r: rule("x"), role: "" }])).toEqual([SOURCE_AUTHORITIES.synopsis.name, "Title x · p.21"]);
  });
  it("an authority with no rule draws no group; nothing at all is no group", () => {
    expect(drawn([{ r: rule("d", { dfo: true }), role: "" }])).toEqual([SOURCE_AUTHORITIES.dfo.name, "Title d · p.21"]);
    expect(drawn([])).toEqual([]);
    expect([authorityOf(rule("x")), authorityOf(rule("x", { dfo: true })), authorityOf(rule("x", { dfo: true, cited: true }))])
      .toEqual(["synopsis", "dfo", "local"]);
  });
});
