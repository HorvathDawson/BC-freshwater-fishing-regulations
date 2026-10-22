/**
 * T43 over the store (vedder_1010, regs v38 goldens): (1) every book run in every sheet the state opens reads as one
 * spelling ("Rule book p.N ↗" / bare "p.N ↗") and opens the PDF page the RELEASE stores for that printed page (the
 * synopsis is not byte-identical to the repo copy: the page is never computed — check.mjs `rule-book-label`,
 * `sheet-book-words`); the glossary pages equal the golden's in order. (2) the foot's "Fish pictures" lines for the page.
 */
import { describe, expect, it } from "vitest";
import { appKeyOf, bookText, buildRegsPage, creditsFor, sheetLines, type Run, type SheetVM } from "@app/core";
import type { GoldenSheet } from "@app/core/regs/testing";
import { ADULT_RESIDENT, golden, inputsOf, parityGate, requestOf } from "./golden";
import { groupLines } from "./keepPage";

const G = golden();
const gate = await parityGate();
const ID = "vedder_1010";

it(`T43 book/credits parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

/** every book run anywhere in a sheet (blocks, folds, nested groups) */
function bookRuns(v: unknown, out: Extract<Run, { t: "book" }>[] = []): Extract<Run, { t: "book" }>[] {
  if (Array.isArray(v)) { for (const x of v) bookRuns(x, out); return out; }
  if (!v || typeof v !== "object") return out;
  const o = v as Record<string, unknown>;
  if (o.t === "book" && typeof o.printed === "number") out.push(o as Extract<Run, { t: "book" }>);
  else for (const x of Object.values(o)) bookRuns(x, out);
  return out;
}

describe.skipIf(gate.skip)(`T43 ${ID}`, () => {
  const pageOf = async () => {
    const inp = (await inputsOf(ID))!;
    return { inp, page: buildRegsPage(inp, { today: requestOf(ID).today, who: ADULT_RESIDENT, pid: ID }) };
  };

  it("every book run in every sheet: one spelling, linked to the stored PDF page; glossary pages = the golden's, in order", async () => {
    const { inp, page } = await pageOf();
    const sheets = Object.entries(G.states[ID]!.sheets as Record<string, GoldenSheet | "@">)
      .map(([k, v]) => [k, v === "@" ? G.sheets[k] as GoldenSheet : v] as const);
    expect(sheets.length).toBe(65);   // the ticket's 63 was v33; the v38 golden holds 65
    let runs = 0;
    for (const [gk, want] of sheets) {
      const vm = page.sheet(appKeyOf(gk, ID)) as SheetVM;
      for (const r of bookRuns(vm.blocks)) {
        runs++;
        expect(bookText(r), `${gk} p.${r.printed}`).toMatch(/^(Rule book )?p\.\d+ ↗$/);
        expect(r.pdf, `${gk} p.${r.printed}: the release's own printed → PDF page`).toBe(inp.statics.pdfPages[r.printed]);
      }
      expect(sheetLines(vm, { groupLines }).pages.map((p) => p.pdf), gk).toEqual(want.pages.map((p) => p.pdf));
    }
    expect(runs).toBeGreaterThan(65);
  });

  it("gl:bait_ban: the bare pages p.4 ↗ → PDF 6, p.8 ↗ → PDF 10", async () => {
    const { page } = await pageOf();
    expect(sheetLines(page.sheet("gl:bait_ban")!).pages).toEqual([{ text: "p.4 ↗", pdf: 6 }, { text: "p.8 ↗", pdf: 10 }]);
  });

  it("the foot's Fish pictures: the page's fish, every picture of each, sorted — and no fish it does not show", async () => {
    const { page } = await pageOf();
    const got = creditsFor(page);
    expect(got).toContain("Rainbow trout — Freshwater Fisheries Society of BC (GoFishBC)");
    expect(got).toContain("Steelhead · ocean — Freshwater Fisheries Society of BC (GoFishBC)");
    expect(got).toContain("Steelhead · spawning — WDFW (eRegulations WA)");
    expect(got.some((l) => l.startsWith("Walleye"))).toBe(false);
    expect([...got]).toEqual([...new Set(got)].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)));
    expect(got).toEqual(EXPECTED_VEDDER);
  });
});

/** vedder_1010's foot, whole: its Keep fish, its Other fish thumbs and the pictures their sheets open (18 of 39) */
const EXPECTED_VEDDER: readonly string[] = [
  "Black crappie — Duane Raver / USFWS (fws.gov)",
  "Brook trout — Freshwater Fisheries Society of BC (GoFishBC)",
  "Brown trout — Duane Raver / USFWS (fws.gov)",
  "Bull trout — Joel Sartore / National Geographic (Commons)",
  "Coastal cutthroat trout — Freshwater Fisheries Society of BC (GoFishBC)",
  "Dolly Varden — WDFW (eRegulations WA)",
  "Kokanee · spawning — WDFW (sockeye male) (eRegulations WA)",
  "Kokanee — Freshwater Fisheries Society of BC (GoFishBC)",
  "Lake trout — Sherman F. Denton (Commons)",
  "Lake whitefish — Sherman F. Denton (Commons)",
  "Largemouth bass — Duane Raver / USFWS (Commons)",
  "Mountain whitefish — What To Fish (whattofish.ca)",
  "Rainbow trout — Freshwater Fisheries Society of BC (GoFishBC)",
  "Smallmouth bass — Freshwater Fisheries Society of BC (GoFishBC)",
  "Steelhead · ocean — Freshwater Fisheries Society of BC (GoFishBC)",
  "Steelhead · spawning — WDFW (eRegulations WA)",
  "Westslope cutthroat trout — Freshwater Fisheries Society of BC (GoFishBC)",
  "White sturgeon — Joseph Huët (Paris Museum) (Commons)",
];
