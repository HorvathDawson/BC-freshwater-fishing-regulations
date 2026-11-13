/**
 * T35 render: the Methods and Licence tabs drawn from the FROZEN pages (fixtures/*.json and, for all six anglers of
 * vedder_1010, x5p1_1010 (a classified water: the class box, D17) and klmb_1010 (the not-yet-mapped box),
 * fixtures/licence_<id>.json — written by `UPDATE_REGS_FIXTURES=1` from buildRegsPage over the parity
 * store, never hand-edited). Components draw the VM: every word, option and key is core's. (T439: the line-for-line
 * "golden mounts" against golden.json's drawn lines are gone.)
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { LIC as LICW, METHODS, type LicenceVM, type MethodsVM, type RegsPage, type SheetKey, type Who } from "@app/core";
import { LIGHT } from "../theme";
import type { KeepActions } from "./LineText";
import { LicenceTab } from "./LicenceTab";
import { ReferenceFold } from "./ReferenceFold";
import { MethodsTab } from "./MethodsTab";
import { domLines } from "./testing/domLines";
import vedder615 from "./fixtures/vedder_615.json";
import bc1501 from "./fixtures/bc1_501.json";
import licences from "./fixtures/licence_vedder_1010.json";
import licencesX5p1 from "./fixtures/licence_x5p1_1010.json";
import licencesKlmb from "./fixtures/licence_klmb_1010.json";

afterEach(cleanup);

const LIC = licences as unknown as Record<string, LicenceVM>;
const LIC_X5P1 = licencesX5p1 as unknown as Record<string, LicenceVM>;
const LIC_KLMB = licencesKlmb as unknown as Record<string, LicenceVM>;
const whoOf = (k: string): Who => { const [age, residency] = k.split("|") as [Who["age"], Who["residency"]]; return { age, residency }; };
type Block = LicenceVM["blocks"][number];
const methodsOf = (f: unknown) => (f as RegsPage).methods as MethodsVM;
const ADULT: Who = { age: "adult", residency: "resident" };

function acts() {
  const sheets: SheetKey[] = [], books: number[] = [];
  const actions: KeepActions = { onSheet: (k) => sheets.push(k), onDay() {}, onTab() {}, onBook: (p) => books.push(p) };
  return { sheets, books, actions };
}
function mountMethods(vm: MethodsVM, open = false) {
  const a = acts();
  render(<><MethodsTab vm={vm} palette={LIGHT} actions={a.actions} />
    {vm.reference && <ReferenceFold vm={vm.reference} palette={LIGHT} actions={a.actions} initiallyOpen={open} />}</>);
  return { ...a, lines: () => domLines(screen.getByTestId("methods-tab")) };
}
function mountLicence(vm: LicenceVM, who: Who = ADULT) {
  const a = acts(), whos: Who[] = [];
  render(<LicenceTab vm={vm} who={who} palette={LIGHT} actions={a.actions} onWho={(w) => whos.push(w)} />);
  return { ...a, whos, lines: () => domLines(screen.getByTestId("licence-tab")) };
}

/** (owner 2026-11-04) the guiding block is folded, closed by default: its lines are drawn once it is opened */
function openGuiding(): void {
  const fold = screen.queryByTestId("licence-guiding-fold");
  if (fold) fireEvent.click(within(fold).getByRole("button"));
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

  it("(T395) the vessel restrictions: their own section under Methods — one row per kind opening its rule, then Boats", () => {
    const vm = methodsOf(bc1501);
    const a = acts();
    const boats = [{ kind: "unpowered", text: "No powered boats (in part)", rule: "r1" },
                   { kind: "speed", text: "Speed limit 8 km/h", rule: "r2" }] as unknown as RegsPage["boats"];
    render(<MethodsTab vm={vm} palette={LIGHT} actions={a.actions} boats={boats} />);
    const section = screen.getByTestId("methods-vessels");
    expect(domLines(section)).toEqual([METHODS.VESSELS, "No powered boats (in part)", "Speed limit 8 km/h", "Boats",
      "No powered boats — between the Goat Creek confluence and the Talchako River confluence", "This water’s › Rule book p.43 ↗"]);
    // under Methods: after its Ways to fish
    const ways = screen.getByTestId("methods-ways");
    expect(ways.compareDocumentPosition(section) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    fireEvent.click(screen.getByTestId("boat:speed"));
    expect(a.sheets).toEqual(["rule:r2"]);
  });

  it("(T395) a closed day keeps the vessel restrictions under its sentence; none at all draws no section", () => {
    const a = acts();
    const boats = [{ kind: "towing", text: "No towing", rule: "r9" }] as unknown as RegsPage["boats"];
    render(<MethodsTab vm={methodsOf(vedder615)} palette={LIGHT} actions={a.actions} boats={boats} />);
    expect(domLines(screen.getByTestId("methods-tab"))).toEqual(["No fishing here until Jul 1.", METHODS.VESSELS, "No towing"]);
    cleanup();
    render(<MethodsTab vm={{ ...methodsOf(bc1501), boats: null }} palette={LIGHT} actions={a.actions} boats={[]} />);
    expect(screen.queryByTestId("methods-vessels")).toBeNull();
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

  it("the Reference fold's mark is the set's chevron: expand closed, collapse open", () => {
    mountMethods(methodsOf(bc1501));
    const summary = screen.getByTestId("methods-reference-summary");
    const marks = () => [within(summary).queryByTestId("icon-expand") !== null, within(summary).queryByTestId("icon-collapse") !== null];
    expect(marks()).toEqual([true, false]);
    fireEvent.click(summary);
    expect(marks()).toEqual([false, true]);
  });

  it("vedder_615 (closed): only the sentence", () => {
    const { lines } = mountMethods(methodsOf(vedder615));
    expect(lines()).toEqual(["No fishing here until Jul 1."]);
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

  it("adult|resident: 'Also, only if…' before the steelhead stamp; the basic licence's price; no pages", () => {
    mountLicence(LIC["adult|resident"]!);
    const needs = domLines(screen.getByTestId("licence-needs"));
    expect(needs.indexOf("Also, only if…")).toBeLessThan(needs.findIndex((l) => l.startsWith("Steelhead Conservation Surcharge Stamp")));
    expect(needs[1]).toBe("Basic angling licence Needed to fish at all $41.15 a year · $11.43 a day · $22.86 for 8 days");
    expect(within(screen.getByTestId("licence-tab")).queryAllByRole("link")).toEqual([]);
  });

  it("(owner 2026-11-04: licence sources sheet removed) no angler of any licence fixture draws a sources line or a \"Source\" button", () => {
    for (const [id, all] of [["vedder_1010", LIC], ["x5p1_1010", LIC_X5P1], ["klmb_1010", LIC_KLMB]] as const) {
      for (const [k, vm] of Object.entries(all)) {
        const { lines, sheets } = mountLicence(vm, whoOf(k));
        openGuiding();
        const tab = within(screen.getByTestId("licence-tab"));
        const buttons = tab.queryAllByRole("button").map((b) => b.textContent ?? "");
        expect([id, k, screen.queryByTestId("regs-src"), buttons.filter((t) => /^Source|›$/.test(t.trim())),
          lines().filter((l) => /(^| )Sources?( |$)|›$/.test(l))]).toEqual([id, k, null, [], []]);
        for (const b of tab.queryAllByRole("button")) fireEvent.click(b);
        expect([id, k, sheets.filter((s) => s.startsWith("licsrc:"))]).toEqual([id, k, []]);
        cleanup();
      }
    }
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

  it("(owner 2026-11-04) guiding is folded, closed: its summary shows, its cards only once opened", () => {
    const vm = Object.values(LIC).find((v) => JSON.stringify(v).includes("lostlist lopen"))!;
    const { lines } = mountLicence(vm);
    const fold = screen.getByTestId("licence-guiding-fold");
    expect(within(fold).getByRole("button").getAttribute("aria-expanded")).toBe("false");
    expect(lines().some((l) => /angling guide licence/.test(l))).toBe(false);
    openGuiding();
    expect(lines().some((l) => /angling guide licence/.test(l))).toBe(true);
  });

  it("(D17) x5p1_1010: the class box draws its head, ⓘ and unit — no \"Source\" (owner 2026-11-04)", () => {
    const { lines } = mountLicence(LIC_X5P1["adult|resident"]!);
    expect(lines()).toContain("Classified (Class II) whenever this water is open ⓘ Licence unit: Elk River.");
    expect(within(screen.getByTestId("licence-answer")).queryByRole("button", { name: "Source" })).toBeNull();
  });

  it("(T670 G7, RULINGS Li13) x5p1_1010 (the Elk, Class II): the licence item draws its p.7 term in the book's words, per angler", () => {
    const termsOf = (k: string) => {
      mountLicence(LIC_X5P1[k]!, whoOf(k));
      const got = within(screen.getByTestId("licence-needs")).queryAllByTestId("need-book").map((b) => domLines(b));
      cleanup();
      return got;
    };
    expect(termsOf("adult|resident")).toEqual([["From the rule book:",
      "If you are a B.C. resident: Your Classified Waters Licence is valid for the licencing year. This means you can fish on any Class I or Class II water in the province during the year."]]);
    expect(termsOf("adult|non_resident")).toEqual([["From the rule book:",
      "If you are a NON-GUIDED “Non-Resident” or NON-GUIDED “Non-Resident Alien”: Your Classified Waters Licence for Class I or Class II waters is sold on a per day basis and is date and water-specific. Although anglers may purchase as many Classified Waters Licences as they wish, each licence may not exceed 8 consecutive days."]]);
    // a water that is not classified (the Vedder) carries no term
    expect(Object.entries(LIC).flatMap(([k, vm]) => vm.blocks.flatMap((b) => b.cls === "needs" ? b.items.filter((i) => i.book.length).map(() => k) : []))).toEqual([]);
  });

  it("klmb_1010: the not-yet-mapped box — its label, what the part says, the note", () => {
    mountLicence(LIC_KLMB["adult|resident"]!);
    const lines = domLines(screen.getByTestId("licence-answer"));
    const at = lines.indexOf("In one part of this water");
    expect(lines.slice(at, at + 3)).toEqual(["In one part of this water",
      "Only the south end, within the Creston Valley Wildlife Management Area: you need a Creston Valley Wildlife Management Area permit to fish.",
      "That part isn’t drawn on the map yet. It doesn’t apply to the rest of the water."]);
  });

  it("flag, scope note (no Source: owner 2026-11-04), muted: no golden state draws them — core's own words, placed as VM blocks", () => {
    const vm = LIC_X5P1["adult|resident"]!;
    const extra: Block[] = [{ cls: "flag", text: LICW.CONTESTED }, { cls: "scopenote", text: LICW.EXEMPT_FROM("Basic angling licence") },
      { cls: "muted", text: LICW.NOTHING }];
    mountLicence({ ...vm, blocks: [...extra, ...vm.blocks.slice(1)] });
    expect(domLines(screen.getByTestId("licence-answer")).slice(0, 3)).toEqual([
      "? Check: part of this water is also marked “not a Classified Water”.", "You’re exempt from: Basic angling licence.",
      "Nothing is required of this angler here."]);
    expect(within(screen.getByTestId("licence-answer")).queryByRole("button", { name: "Source" })).toBeNull();
  });

  it("vedder_615 (closed): no Licence section at all (owner 2026-11-04; the prototype drew only its heading, D16 a)", () => {
    expect((vedder615 as unknown as RegsPage).licence).toBeNull();
  });

});
