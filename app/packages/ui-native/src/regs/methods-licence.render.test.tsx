/**
 * T35 render: the Methods and Licence tabs drawn from the FROZEN golden pages (fixtures/*.json and, for all six anglers of
 * vedder_1010, fixtures/licence_vedder_1010.json — written by `UPDATE_REGS_FIXTURES=1` from buildRegsPage over the parity
 * store, never hand-edited) and compared line for line to the golden's own drawn lines (fixtures/golden_lines.json, copied
 * from golden.json by the same writer). Components draw the VM: every word, option and key is core's.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import type { LicenceVM, MethodsVM, RegsPage, SheetKey, Who } from "@app/core";
import { LIGHT } from "../theme";
import type { KeepActions } from "./LineText";
import { LicenceTab } from "./LicenceTab";
import { MethodsTab } from "./MethodsTab";
import { domLines } from "./testing/domLines";
import vedder1010 from "./fixtures/vedder_1010.json";
import vedder615 from "./fixtures/vedder_615.json";
import vedder515 from "./fixtures/vedder_515.json";
import bc1501 from "./fixtures/bc1_501.json";
import licences from "./fixtures/licence_vedder_1010.json";
import goldenLines from "./fixtures/golden_lines.json";

afterEach(cleanup);

const GL = goldenLines as Record<string, { gear: string[]; methods: string[]; licence: Record<string, string[]> }>;
const LIC = licences as unknown as Record<string, LicenceVM>;
const methodsOf = (f: unknown) => (f as RegsPage).methods as MethodsVM;
const ADULT: Who = { age: "adult", residency: "resident" };

function acts() {
  const sheets: SheetKey[] = [], books: number[] = [];
  const actions: KeepActions = { onSheet: (k) => sheets.push(k), onDay() {}, onTab() {}, onBook: (p) => books.push(p) };
  return { sheets, books, actions };
}
function mountMethods(vm: MethodsVM, open = false) {
  const a = acts();
  render(<MethodsTab vm={vm} palette={LIGHT} actions={a.actions} referenceOpen={open} />);
  return { ...a, lines: () => domLines(screen.getByTestId("methods-tab")) };
}
function mountLicence(vm: LicenceVM, who: Who = ADULT) {
  const a = acts(), whos: Who[] = [];
  render(<LicenceTab vm={vm} who={who} palette={LIGHT} actions={a.actions} onWho={(w) => whos.push(w)} />);
  return { ...a, whos, lines: () => domLines(screen.getByTestId("licence-tab")) };
}

describe("<MethodsTab>", () => {
  it("bc1_501: ✓ ways with their conditions, ✕ ways, the boat rule and its source", () => {
    const { sheets } = mountMethods(methodsOf(bc1501));
    expect(domLines(screen.getByTestId("methods-ways"))).toEqual(["Ways to fish", "Allowed",
      "✓ Angling (rod and line) with a downrigger: attached to your line by a quick-release · with a light: submerged, attached to the line, within 1 m of the hook",
      "✓ Spear or bow — not for trout or other game fish not for game fish, salmon or protected species · burbot allowed",
      "✓ Crayfish traps release anything in the trap that isn’t a crayfish", "Not allowed", "✕ Set lines", "✕ Nets", "✕ Snagging", "✕ Chumming",
      "Province › Rule book p.8 ↗"]);
    expect(domLines(screen.getByTestId("methods-boats"))).toEqual(["Boats",
      "No powered boats — between the Goat Creek confluence and the Talchako River confluence", "This water’s › Rule book p.43 ↗"]);
    fireEvent.click(within(screen.getByTestId("methods-boats")).getByRole("button", { name: "Sources: This water’s" }));
    expect(sheets).toEqual(["srcs:boats"]);
  });

  it("the Reference fold: closed by default (its body not drawn); a tap opens it — the five heads and Province ›", () => {
    mountMethods(methodsOf(bc1501));
    const summary = screen.getByTestId("methods-reference-summary");
    expect([domLines(summary), summary.getAttribute("aria-expanded")]).toEqual([["Reference Handling fish 21 rules that apply everywhere"], "false"]);
    expect(screen.queryByText("Gently, back in the water")).toBeNull();
    fireEvent.click(summary);
    expect(summary.getAttribute("aria-expanded")).toBe("true");
    const body = domLines(screen.getByTestId("methods-reference-body"));
    expect(["When you catch a fish", "When you release a fish", "When you keep a fish", "Carrying fish", "Never"].map((h) => body.includes(h))).toEqual([true, true, true, true, true]);
    expect(body[body.length - 1]).toBe("Province › Rule book p.8 ↗ Rule book p.10 ↗");
    expect(screen.getByText("Gently, back in the water")).toBeTruthy();
  });

  it("golden mounts (fold opened: the golden reads the folded text): vedder_1010, vedder_515, bc1_501", () => {
    for (const [id, f] of [["vedder_1010", vedder1010], ["vedder_515", vedder515], ["bc1_501", bc1501]] as const) {
      const { lines } = mountMethods(methodsOf(f), true);
      expect([id, lines()]).toEqual([id, GL[id]!.methods]);
      cleanup();
    }
  });

  it("vedder_615 (closed): only the sentence", () => {
    const { lines } = mountMethods(methodsOf(vedder615));
    expect([lines(), GL.vedder_615!.methods]).toEqual([["No fishing here until Jul 1."], ["No fishing here until Jul 1."]]);
    expect(screen.queryByRole("heading")).toBeNull();
  });

  it("(owner wave A) an allowed Set lines way draws its set_line ⓘ, which opens gl:set_line", () => {
    const vm = methodsOf(bc1501), ways = vm.ways!;
    const allowed: MethodsVM = { ...vm, ways: { ...ways, list: [{ ...ways.list[0]!, items: [{ mark: "✓", name: "Set lines", note: null, terms: ["set_line"] }] }] } };
    const { sheets } = mountMethods(allowed);
    expect(domLines(screen.getByTestId("methods-ways")).slice(0, 3)).toEqual(["Ways to fish", "Allowed", "✓ Set lines ⓘ"]);
    fireEvent.click(within(screen.getByTestId("methods-ways")).getByText("ⓘ"));
    expect(sheets).toEqual(["gl:set_line"]);
  });

  it("mutation pins: a lost note, a dropped boat or a reordered group fails the golden mount", () => {
    const vm = methodsOf(bc1501), ways = vm.ways!, ref = vm.reference!, golden = GL.bc1_501!.methods;
    const muts: Record<string, MethodsVM> = {
      note: { ...vm, ways: { ...ways, list: ways.list.map((c) => ({ ...c, items: c.items.map((i) => ({ ...i, note: null })) })) } },
      boats: { ...vm, boats: null },
      groups: { ...vm, reference: { ...ref, groups: [...ref.groups].reverse() } },
    };
    const same = Object.entries(muts).filter(([, m]) => { const { lines } = mountMethods(m, true), eq = JSON.stringify(lines()) === JSON.stringify(golden); cleanup(); return eq; });
    expect(same.map(([k]) => k)).toEqual([]);
  });
});

describe("<LicenceTab>", () => {
  it("adult|resident: the question with one ⓘ (under_16), two radiogroups named by the VM, the checked options, the label", () => {
    const { sheets } = mountLicence(LIC["adult|resident"]!);
    const who = within(screen.getByTestId("licence-who"));
    expect(domLines(screen.getByTestId("licence-who"))[0]).toBe("Who is fishing? ⓘ");
    fireEvent.click(who.getByText("ⓘ"));
    expect(sheets).toEqual(["gl:under_16"]);
    expect(who.getAllByRole("radiogroup").map((g) => g.getAttribute("aria-label"))).toEqual(["Who is fishing?", "Who is fishing?"]);
    expect(who.getAllByRole("radio").map((r) => `${r.textContent}=${r.getAttribute("aria-checked")}`)).toEqual([
      "16 or older=true", "Under 16=false", "B.C. resident=true", "Canadian, other province=false", "From outside Canada=false"]);
    expect(screen.getByText("B.C. resident, 16 or older")).toBeTruthy();
  });

  it("adult|resident: 'Also, only if…' before the steelhead stamp; the basic licence's price; the source opens licsrc:adult, no pages", () => {
    const { sheets } = mountLicence(LIC["adult|resident"]!);
    const needs = domLines(screen.getByTestId("licence-needs"));
    expect(needs.indexOf("Also, only if…")).toBeLessThan(needs.findIndex((l) => l.startsWith("Steelhead Conservation Surcharge Stamp")));
    expect(needs[1]).toBe("Basic angling licence Needed to fish at all $41.15 a year · $11.43 a day · $22.86 for 8 days");
    const src = screen.getByRole("button", { name: "Sources: Province, This water" });
    expect(src.textContent).toBe("Province · This water ›");
    expect(within(screen.getByTestId("licence-tab")).queryAllByRole("link")).toEqual([]);
    fireEvent.click(src);
    expect(sheets).toEqual(["licsrc:adult"]);
  });

  it("a pick hands up the whole angler: Under 16 keeps the residency; From outside Canada keeps the age", () => {
    const { whos } = mountLicence(LIC["adult|resident"]!);
    fireEvent.click(screen.getByRole("radio", { name: "Under 16" }));
    fireEvent.click(screen.getByRole("radio", { name: "From outside Canada" }));
    expect(whos).toEqual([{ age: "u16", residency: "resident" }, { age: "adult", residency: "non_resident_alien" }]);
  });

  it("u16|non_resident: the youth box first, then nothing-if-with-an-adult and the basic licence; no 'Also, only if…'", () => {
    mountLicence(LIC["u16|non_resident"]!, { age: "u16", residency: "non_resident" });
    expect(screen.getByText("Canadian, other province, under 16")).toBeTruthy();
    expect(domLines(screen.getByTestId("licence-classbox"))[0]!.startsWith("Under 16: no licence needed if you fish with a licensed adult")).toBe(true);
    const needs = domLines(screen.getByTestId("licence-needs"));
    expect([needs[1]!.startsWith("Nothing, if you fish with a licensed adult (16 or older)"), needs[2]!.startsWith("Basic angling licence"),
      needs.includes("Also, only if…")]).toEqual([true, true, false]);
  });

  it("golden mounts: all six anglers of vedder_1010 draw the golden's lines", () => {
    for (const [k, vm] of Object.entries(LIC)) {
      const [age, residency] = k.split("|") as [Who["age"], Who["residency"]];
      const { lines } = mountLicence(vm, { age, residency });
      expect([k, lines()]).toEqual([k, GL.vedder_1010!.licence[k]]);
      cleanup();
    }
    expect(Object.keys(LIC).sort()).toEqual(Object.keys(GL.vedder_1010!.licence).sort());
  });

  it("vedder_615 (closed, D16 a): only the heading — no radiogroup, no answer, no source", () => {
    const { lines } = mountLicence((vedder615 as unknown as RegsPage).licence!);
    expect([lines(), GL.vedder_615!.licence["adult|resident"]]).toEqual([["Licence"], ["Licence"]]);
    expect(screen.queryByRole("radiogroup")).toBeNull();
  });

  it("mutation pins: a lost price, a dropped 'only if' head or an unchecked option fails the golden mount", () => {
    const vm = LIC["adult|resident"]!, golden = GL.vedder_1010!.licence["adult|resident"];
    const needs = vm.blocks[0] as Extract<LicenceVM["blocks"][number], { cls: "needs" }>;
    const muts: Record<string, LicenceVM> = {
      price: { ...vm, blocks: [{ ...needs, items: needs.items.map((i) => ({ ...i, price: null })) }, ...vm.blocks.slice(1)] },
      onlyIf: { ...vm, blocks: [{ ...needs, onlyIf: needs.onlyIf && { ...needs.onlyIf, label: "" } }, ...vm.blocks.slice(1)] },
      label: { ...vm, label: "" },
    };
    const same = Object.entries(muts).filter(([, m]) => { const { lines } = mountLicence(m), eq = JSON.stringify(lines()) === JSON.stringify(golden); cleanup(); return eq; });
    expect(same.map(([k]) => k)).toEqual([]);
  });
});
