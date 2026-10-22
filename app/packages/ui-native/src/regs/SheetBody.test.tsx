/**
 * T37 render: one case per `SheetBlock` kind — every word the block holds is drawn exactly once, every pressable is a
 * button / link / radio with a name, a press says where it goes. (No colour literal: tools/check-colours.mjs holds the
 * sources to the palette; regs-tokens.test.ts bans weights, folded colours and radii in regs components.)
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render } from "@testing-library/react";
import type { GroupVM, SheetBlock, SheetKey, Tab } from "@app/core";
import { SheetBlockView } from "./SheetBody";
import { LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

const t = (s: string) => ({ t: "text" as const, s });
const book = (printed: number, pdf: number | null) => ({ t: "book" as const, printed, pdf });
const rgb = (h: string) => `rgb(${[1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16)).join(", ")})`;
const LIGHT_RGB = { sub: rgb(LIGHT.sub), ink: rgb(LIGHT.ink) };
const GROUP = (vedder1010 as unknown as { keep: { groups: GroupVM[] } }).keep.groups[0]!;

function mount(b: SheetBlock) {
  const opened: SheetKey[] = [], parts: number[] = [], tabs: Tab[] = [], books: number[] = [];
  const r = render(<SheetBlockView b={b} palette={LIGHT} onOpen={(k) => opened.push(k)} onPart={(i) => parts.push(i)}
                                   onTab={(x) => tabs.push(x)} onBook={(p) => books.push(p)} />);
  const text = r.container.textContent ?? "";
  const once = (s: string) => text.split(s).length - 1;
  const press = (el: Element) => fireEvent.click(el);
  // every focusable thing names itself and says what it is
  const pressables = [...r.container.querySelectorAll("[tabindex='0'], [role='button'], [role='link'], [role='radio']")];
  const unnamed = pressables.filter((e) => !["button", "link", "radio"].includes(e.getAttribute("role") ?? "")
    || !(e.getAttribute("aria-label") || (e.textContent ?? "").trim()));
  return { r, text, once, press, opened, parts, tabs, books, unnamed };
}

const CASES: [string, SheetBlock, string[]][] = [
  ["para", { k: "para", text: [t("You may fish for it."), { t: "term", ids: ["catch_and_release"] }] }, ["You may fish for it.", "ⓘ"]],
  ["para (small)", { k: "para", tone: "small", text: [t("No fishing, Jun 1–30.")] }, ["No fishing, Jun 1–30."]],
  ["example", { k: "example", text: [t("For example:"), t(" "), t("With 4 trout a day you may have 8.")] }, ["For example: With 4 trout a day you may have 8."]],
  ["head", { k: "head", text: "How this was decided" }, ["How this was decided"]],
  ["quote", { k: "quote", text: "Waste the fish you catch." }, ["Waste the fish you catch."]],
  ["list", { k: "list", items: [{ line: [t("Pink or red band")], strong: null }, { line: [t("Small black spots")], strong: null }] }, ["Pink or red band", "Small black spots"]],
  ["term", { k: "term", id: "hatchery_wild", title: "Hatchery or wild (adipose fin)", body: [{ k: "para", text: [t("A fish with its fin is wild.")] }] },
    ["Hatchery or wild (adipose fin)", "A fish with its fin is wild."]],
  ["rule", { k: "rule", title: "No netting", level: "This water’s", applies: ["A B.C.-wide rule — everywhere in B.C."], book: [book(8, 10)], open: "rule:zp:p::p.r1" },
    ["No netting", "This water’s", "A B.C.-wide rule — everywhere in B.C.", "Rule book p.8 ↗"]],
  ["value", { k: "value", text: [{ t: "num", s: "20", size: "big" }, t("a day")] }, ["20a day"]],
  ["weekBox", { k: "weekBox", day: "Today · Sat", scope: "This water", status: "Weekends: keep 5 · Weekdays: release", says: "Keep up to 5 on Saturdays." },
    ["Today · Sat", "This water", "Weekends: keep 5 · Weekdays: release", "Keep up to 5 on Saturdays."]],
  ["ladder", { k: "ladder", q: "What you can keep", result: "Release every one here.", steps: [{ level: "Region 2", lines: [
    { text: [t("Sat–Sun Keep up to 5")], tag: "Today", today: true, days: "Sat–Sun", rule: "r4:k::k.r3", won: true, applies: ["This water’s own row."], book: [book(37, 39)], notes: ["Kokanee from both arms: 5."] }] }] },
    ["What you can keep", "✓ Release every one here.", "Region 2", "Sat–Sun Keep up to 5", "Today", "This water’s own row.", "Rule book p.37 ↗", "Kokanee from both arms: 5."]],
  ["sources", { k: "sources", groups: [{ where: "Region 2 · region-wide", rows: [{ key: "z2:t::t.r1", title: "Keep up to 4", quote: "Trout/char: 4", role: "Sets the number", book: [book(21, 23)] }] }] },
    ["Region 2 · region-wide", "Keep up to 4", "Trout/char: 4", "Sets the number", "Rule book p.21 ↗"]],
  ["sources (overruled)", { k: "sources", overruled: true, groups: [{ where: "Provincial · province-wide", rows: [{ key: "zp:q::q.r1", title: null, quote: "Possession quota", role: "Beaten by “2 daily quotas”", book: [], notes: ["Curator note"], trib: true }] }] },
    ["“Possession quota”", "Provincial · province-wide", "Beaten by “2 daily quotas”", "⚑ Curator note", "ⓘ"]],
  ["pictures", { k: "pictures", pics: [{ code: "KO", name: "Kokanee", caption: "Kokanee ›", open: "sp:KO", sil: false }] }, ["Kokanee ›"]],
  ["image", { k: "image", code: "RB", file: "rainbow-trout.webp", alt: "Rainbow trout", credit: "Rainbow trout · Freshwater Fisheries Society of BC (GoFishBC)", width: 800, height: 400 },
    ["Rainbow trout · Freshwater Fisheries Society of BC (GoFishBC)"]],
  ["adipose", { k: "adipose", figures: [{ variant: "wild", caption: [t("Wild"), t(" · adipose fin there")] }, { variant: "clipped", caption: [t("Hatchery"), t(" · clipped")] }] },
    ["Wild · adipose fin there", "Hatchery · clipped"]],
  ["pages", { k: "pages", line: [t("Rule book"), t(" "), { t: "book", printed: 4, pdf: 6, bare: true }] }, ["Rule book", "p.4 ↗"]],
  ["source", { k: "source", line: [{ t: "link", s: "Source ›", open: "rule:z2:p::p.r1" }, t(" "), book(21, 23)] }, ["Source ›", "Rule book p.21 ↗"]],
  ["link", { k: "link", line: [{ t: "link", s: "See every regulation line ›", open: "src:r0" }] }, ["See every regulation line ›"]],
  ["choice", { k: "choice", items: [{ text: "Mouth → Canyon", sub: null, status: "open", long: "From the mouth up to the canyon", checked: true, part: 0 },
    { text: "Canyon → Lake", sub: null, status: "closed", long: null, checked: false, part: 1 }] },
    ["Mouth → Canyon", "open", "From the mouth up to the canyon", "Canyon → Lake", "closed"]],
  ["rowTitle", { k: "rowTitle", title: "Black crappie", scope: "Region 2-wide" }, ["Black crappie", "Region 2-wide"]],
  ["group", { k: "group", group: GROUP, titled: true }, [GROUP.title, "See every regulation line ›"]],
  ["list (small, stressed)", { k: "list", tone: "small", items: [{ line: [t("With 4 a day you may have 8.")], strong: "Possession doubles the 4." }] }, ["With 4 a day you may have 8. Possession doubles the 4."]],
];

describe("<SheetBlockView>: one case per block kind", () => {
  it.each(CASES)("%s: every word once, every pressable named", (_k, b, words) => {
    const m = mount(b);
    expect(words.map((w) => [w, m.once(w)])).toEqual(words.map((w) => [w, 1]));
    expect(m.unnamed.map((e) => e.outerHTML.slice(0, 80))).toEqual([]);
  });

  it("(T41 walk) a small list is the prototype's ul.posx: no markers, the words small in sub, the stressed words bold in ink", () => {
    const m = mount({ k: "list", tone: "small", items: [{ line: [t("With 4 a day you may have 8.")], strong: "Possession doubles the 4." }, { line: [t("With 15 a day: 30.")], strong: null }] });
    const leaf = (s: string) => [...m.r.container.querySelectorAll<HTMLElement>("*")].filter((e) => e.textContent === s && ![...e.children].some((c) => c.textContent === s)).pop()!;
    const look = (e: HTMLElement) => { const c = getComputedStyle(e); return [c.fontSize, c.fontFamily, c.color]; };
    const markers = [...m.r.container.querySelectorAll<HTMLElement>("div")].filter((e) => !e.textContent && getComputedStyle(e).backgroundColor === LIGHT_RGB.ink);   // the plain list's square marker
    expect({ plain: look(leaf("With 4 a day you may have 8.")), stressed: look(leaf("Possession doubles the 4.")), markers: markers.length })
      .toEqual({ plain: ["12.48px", "Archivo_400Regular", LIGHT_RGB.sub], stressed: ["12.48px", "Archivo_700Bold", LIGHT_RGB.ink], markers: 0 });
  });

  it("(T41 walk) a source row as the prototype's .srow: the title bold on its line, the book's words under it — quoted, small, in sub", () => {
    const m = mount({ k: "sources", groups: [{ where: "Region 2 · region-wide", rows: [{ key: "z2:t::t.r1", title: "Keep up to 4", quote: "Trout/char: 4", role: "Sets the number", book: [] }] }] });
    const leaf = (s: string) => [...m.r.container.querySelectorAll<HTMLElement>("*")].filter((e) => e.textContent === s && ![...e.children].some((c) => c.textContent === s)).pop() ?? null;
    const t = leaf("Keep up to 4"), q = leaf("“Trout/char: 4”");
    const look = (e: HTMLElement | null) => { if (!e) return null; const c = getComputedStyle(e); return [c.fontSize, c.color]; };
    expect({ title: look(t), quote: look(q), ownLine: !!t && !!q && t.tagName === "DIV" && q.tagName === "DIV" && !t.contains(q) && !q.contains(t) })   // a Text inside a Text is an inline span
      .toEqual({ title: ["15px", LIGHT_RGB.ink], quote: ["12.48px", LIGHT_RGB.sub], ownLine: true });
  });

  it("fold: closed by default (its body is not drawn), a named button; pressed, its body is", () => {
    const m = mount({ k: "fold", summary: "Technical detail", open: false, body: [{ k: "para", tone: "mono", text: [t("zp:conduct::conduct.r1 { }")] }] });
    const b = m.r.getByRole("button", { name: "Technical detail" });
    expect([b.getAttribute("aria-expanded"), m.once("zp:conduct::conduct.r1")]).toEqual(["false", 0]);
    fireEvent.click(b);
    expect([b.getAttribute("aria-expanded"), (m.r.container.textContent ?? "").includes("zp:conduct::conduct.r1 { }")]).toEqual(["true", true]);
  });

  it("presses say where they go: a rule row, a ladder line, a source row, a picture, a link, a part, a book page", () => {
    const go = (b: SheetBlock, find: (m: ReturnType<typeof mount>) => Element) => { const m = mount(b); m.press(find(m)); const out = [m.opened, m.parts, m.books]; cleanup(); return out; };
    const [rule, ladder, src, pic, link, part, pages] = [CASES[7]![1], CASES[10]![1], CASES[11]![1], CASES[13]![1], CASES[18]![1], CASES[19]![1], CASES[16]![1]];
    expect([
      go(rule, (m) => m.r.getByRole("button", { name: "No netting · This water’s" })),
      go(ladder, (m) => m.r.getByRole("button", { name: "Sat–Sun Keep up to 5 · Today" })),
      go(src, (m) => m.r.getByRole("button", { name: "Keep up to 4" })),
      go(pic, (m) => m.r.getByRole("button", { name: "Kokanee — what it looks like" })),
      go(link, (m) => m.r.getByText("See every regulation line ›")),
      go(part, (m) => m.r.getByRole("radio", { name: "Canyon → Lake · closed" })),
      go(pages, (m) => m.r.getByText("p.4 ↗")),
    ]).toEqual([[["rule:zp:p::p.r1"], [], []], [["rule:r4:k::k.r3"], [], []], [["rule:z2:t::t.r1"], [], []], [["sp:KO"], [], []],
      [["src:r0"], [], []], [[], [1], []], [[], [], [6]]]);
  });

  it("an untitled group draws no title line; the Today mark is a visible word, not colour alone", () => {
    expect(mount({ k: "group", group: GROUP, titled: false }).r.queryByRole("heading", { name: GROUP.title })).toBeNull();
    cleanup();
    expect(mount(CASES[10]![1]).once("Today")).toBe(1);
  });
});
