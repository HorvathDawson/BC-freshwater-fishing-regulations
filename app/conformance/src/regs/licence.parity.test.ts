/**
 * T33 parity: the Licence tab (core buildLicence via buildRegsPage) for all six anglers (age × residency) of every golden
 * state, against app/fixtures/golden/golden.json `states[id].licence["<age>|<res>"]` (regs v38 @ 7fb6809f):
 * the question and its ⓘ, the choices (text, checked, value), the profile's label, every block as {cls, lines}
 * (`licenceBlockLines`). The golden's licence sources (its source line, a class box's "Source", the licsrc: sheets) are
 * dropped (exclusions.ts `goldenLicenceUnsourced`, LICSRC_REASON: owner 2026-11-04, licence sources sheet removed).
 * Exclusions (exclusions.ts LICENCE_*) are listed with their reasons in the afterAll summary.
 */
import { afterAll, describe, expect, it } from "vitest";
import { appKeyOf, licenceBlockLines, settle, makePlace, type LicenceVM, type Who } from "@app/core";
import { caseIds, golden, inputsOf, parityGate, requestOf, summary } from "./golden";
import { GOLDEN_LICSRC_KEY, goldenLicenceUnsourced, licenceDropped, LICENCE_PROFILES, STEELHEAD_DROP_IDS, TIDAL_LICENCE_IDS,
  YOUTH_ALT_IDS } from "./exclusions";
import type { RegsInputs } from "@app/core";
import { buildRegsPage } from "@app/core";

const gate = await parityGate();
const ids = caseIds("licence");

type GOpt = { text: string; checked: boolean; age: string | null; res: string | null };
type GLic = { question: string | null; questionTerms: string[]; axes: { axis: string; aria: string; options: GOpt[] }[]; label: string;
  blocks: { cls: string; lines: string[] }[]; src?: { who: string; open: string } | null };
const gl = (id: string, k: string) => (golden().states[id]!.licence as Record<string, GLic>)[k]!;

const ADULT: Who = { age: "adult", residency: "resident" };
const whoOf = (k: string): Who => { const [age, residency] = k.split("|") as [Who["age"], Who["residency"]]; return { age, residency }; };

/** the VM in the golden's shape */
export const projVM = (vm: LicenceVM) => ({
  question: vm.question === null ? null : [vm.question, ...vm.questionTerms.map(() => "ⓘ")].join(" "),
  questionTerms: vm.questionTerms,
  options: vm.axes.flatMap((a) => a.options.map((o) => ({ text: o.text, checked: o.checked, value: o.value }))),
  aria: vm.axes.map((a) => a.aria),
  label: vm.label,
  blocks: vm.blocks.map((b) => ({ cls: b.cls, lines: [...licenceBlockLines(b)] })),
});
/** the golden without its licence sources (LICSRC_REASON: owner 2026-11-04, licence sources sheet removed) */
const projG = (g0: GLic) => {
  const g = goldenLicenceUnsourced(g0);
  return {
    question: g.question, questionTerms: g.questionTerms,
    options: g.axes.flatMap((a) => a.options.map((o) => ({ text: o.text, checked: o.checked, value: o.age ?? o.res }))),
    aria: g.axes.filter((a) => a.options.length).map((a) => a.aria),
    label: g.label, blocks: g.blocks,
  };
};
/** a keyed exclusion's lines removed from BOTH sides of a block (the golden lacks a line the stored answer carries) */
const drop = <T extends { blocks: readonly { cls: string; lines: readonly string[] }[] }>(p: T, re: readonly RegExp[]): T => re.length
  ? { ...p, blocks: p.blocks.map((b) => ({ ...b, lines: b.lines.filter((l) => !re.some((r) => r.test(l))) })) } : p;

const licOf = async (id: string, who: Who): Promise<LicenceVM | null> => {
  const r = requestOf(id);
  return buildRegsPage((await inputsOf(id))!, { today: r.today, who, pid: id }).licence;
};

it(`T33 licence parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

describe.skipIf(gate.skip)("T33 Licence", () => {
  let equal = 0, answers = 0, unsourced = 0;
  afterAll(() => console.info(`${summary("licence", equal, ids.length)} · ${answers} profile answers equal (${ids.length * 6} compared) · ${unsourced} golden licsrc: sheets dropped (owner 2026-11-04: licence sources sheet removed)`));

  it.each(ids)("1. %s: all six anglers equal the golden licence block", async (id) => {
    // (owner 2026-11-04) a closed day has NO Licence section: the golden's empty one (D16 (a)) is not drawn
    const inp = (await inputsOf(id))!;
    if (inp.key && settle(inp, makePlace(inp)).status?.status === "closed") {
      for (const k of LICENCE_PROFILES) expect(await licOf(id, whoOf(k)), `${id} ${k}`).toBeNull();
      equal++;
      return;
    }
    for (const k of LICENCE_PROFILES) {
      const vm = (await licOf(id, whoOf(k)))!;
      const re = licenceDropped(id, k);
      expect(drop(projVM(vm), re), `${id} ${k}`).toEqual(drop(projG(gl(id, k)), re));
      answers++;
    }
    equal++;
  });

  it.each(ids)("1b. %s: the golden's licsrc: sheets (one per angler it answers) are dropped — owner 2026-11-04: licence sources sheet removed", (id) => {
    const keys = Object.keys(golden().states[id]!.sheets).filter((k) => GOLDEN_LICSRC_KEY.test(k));
    const opened = LICENCE_PROFILES.map((k) => gl(id, k).src?.open).filter((o): o is string => !!o);
    expect(keys.sort(), id).toEqual([...new Set(opened)].sort());       // each golden sheet is one a source line opened
    for (const k of keys) expect(() => appKeyOf(k, id), `${id} ${k}`).toThrow(/not a sheet key/);
    unsourced += keys.length;
  });

  const L = async (id: string, k: string) => projVM((await licOf(id, whoOf(k)))!);

  it("2. vedder_1010: the six labels; no source line (owner 2026-11-04), where the golden drew one for each", async () => {
    const got = await Promise.all(LICENCE_PROFILES.map(async (k) => (await licOf("vedder_1010", whoOf(k)))!));
    expect(got.map((x) => x.label)).toEqual(["B.C. resident, 16 or older", "Canadian, other province, 16 or older", "From outside Canada, 16 or older",
      "B.C. resident, under 16", "Canadian, other province, under 16", "From outside Canada, under 16"]);
    expect(got.filter((x) => "src" in x)).toEqual([]);
    expect(LICENCE_PROFILES.map((k) => gl("vedder_1010", k).src?.who)).toEqual(LICENCE_PROFILES.map(() => "Province · This water ›"));
  });

  it("3. vedder_1010 adult|resident: Basic, then 'Also, only if…' the three stamps; paper line; guiding fold", async () => {
    expect((await L("vedder_1010", "adult|resident")).blocks).toEqual([
      { cls: "needs", lines: ["You need", "Basic angling licence Needed to fish at all $41.15 a year · $11.43 a day · $22.86 for 8 days", "Also, only if…",
        "Steelhead Conservation Surcharge Stamp ⓘ Only if you fish for steelhead, even to release them $28.57 a year",
        "White Sturgeon Conservation Licence Only if you fish for white sturgeon, even to release them $28.57 a year · $9.14 a day · $17.14 for 8 days",
        "Conservation Surcharge Stamp for salmon ⓘ Only if you keep salmon (not kokanee) $17.14 a year",
        "Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence"] },
      { cls: "paper", lines: ["Keep an adult chinook or hatchery steelhead? Carry your paper licence (16 and over) and record it right away."] },
      { cls: "lostlist lopen", lines: ["If you are guiding other anglers", "For every angler:", "You need an angling guide licence or an assistant angling guide licence to guide anglers.",
        "angling guide licence", "or", "assistant angling guide licence"] },
    ]);
  });

  it("4. vedder_1010 u16|non_resident: the youth box, then Nothing / Basic for your own limit (no 'Also, only if…')", async () => {
    const b = (await L("vedder_1010", "u16|non_resident")).blocks;
    expect(b.slice(0, 2)).toEqual([
      { cls: "classbox youth", lines: ["Under 16: no licence needed if you fish with a licensed adult ⓘ Fish with someone 16 or older who holds what this fishing needs (Basic angling licence). What you keep counts toward their limit. Or buy the Basic angling licence yourself to have your own limit."] },
      { cls: "needs", lines: ["You need", "Nothing, if you fish with a licensed adult (16 or older) What you keep counts toward their limit",
        "Basic angling licence Or, for your own limit: what a 16+ angler buys $62.87 a year · $22.86 a day · $41.15 for 8 days",
        "Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence"] }]);
  });

  it("5. vedder_1010 u16|resident: the no-licence box becomes the one 'You need' item (asNeeds)", async () => {
    expect((await L("vedder_1010", "u16|resident")).blocks[0]).toEqual({ cls: "needs", lines: ["You need",
      "No licence needed to fish here Anglers under 16 who live in B.C. don’t need a basic angling licence. The fishing rules and limits still apply."] });
  });

  it("7. x5p1_1010 adult|resident: the classified box (no \"Source\": owner 2026-11-04), the Classified Waters Licence while classified", async () => {
    const x = await L("x5p1_1010", "adult|resident");
    expect(x.blocks[0]).toEqual({ cls: "classbox", lines: ["Classified (Class II) whenever this water is open ⓘ Licence unit: Elk River."] });
    expect(gl("x5p1_1010", "adult|resident").blocks[0]!.lines[0]!.endsWith(". Source")).toBe(true);   // the golden's, dropped
    expect(x.blocks[1]!.lines[2]).toBe("Classified Waters Licence ⓘ Needed while it’s classified: class II whenever this water is open $17.15 a year");
  });

  it("8. klmb_1010 adult|resident: the not-yet-mapped box", async () => {
    const x = await L("klmb_1010", "adult|resident");
    expect(x.blocks.find((b) => b.cls === "nymbox")).toEqual({ cls: "nymbox", lines: ["In one part of this water",
      "Only the south end, within the Creston Valley Wildlife Management Area: you need a Creston Valley Wildlife Management Area permit to fish.",
      "That part isn’t drawn on the map yet. It doesn’t apply to the rest of the water."] });
  });

  it("9. x13p0_1010 u16|non_resident: the stamp-waiver box and the youth documents", async () => {
    const x = await L("x13p0_1010", "u16|non_resident");
    expect(x.blocks[0]!.lines[0]).toMatch(/^Classified \(Class II\) Jun 16-Oct 31 ⓘ Licence unit: Dean River Class II, Anahim Lake to Iltasyuko River\. Dean River .*no Steelhead Stamp is required.* The Classified Waters Licence and the steelhead rules still apply\.$/);
    expect(x.blocks[1]!.lines[0]).toContain("(Basic angling licence, Classified Waters Licence)");
  });

  it("10. vedder_615 (closed), all six: no section (owner 2026-11-04; was the empty section, D16 (a))", async () => {
    for (const k of LICENCE_PROFILES) expect(await licOf("vedder_615", whoOf(k))).toBeNull();
  });

  it("the youth box and its two items are WORDED from the stored under_16 entry, never from the 16+ answer (mutation)", async () => {
    const inp = (await inputsOf("vedder_1010"))!, F = inp.frames!.licence, r = requestOf("vedder_1010");
    const u16 = F.under_16!.map((u) => u.residency !== "non_resident" ? u : { ...u,
      with_adult: { who: "MUT-WHO", counts: false },
      buy: [{ doc: "basic_licence", name: "MUT-DOC", prices: { year: 1 } }, { doc: "x", name: "MUT-TWO", prices: { year: 2 } }] });
    // the 16+ answers emptied: a derivation from them would lose the documents
    const profiles = F.profiles.map((p) => "documents" in p && p.documents.length ? { ...p, documents: [] } : p);
    const mut: RegsInputs = { ...inp, frames: { ...inp.frames!, licence: { ...F, under_16: u16, profiles } } };
    const b = buildRegsPage(mut, { today: r.today, who: { age: "u16", residency: "non_resident" }, pid: "vedder_1010" }).licence!.blocks
      .map((x) => ({ cls: x.cls, lines: [...licenceBlockLines(x)] }));
    expect(b.slice(0, 2)).toEqual([
      { cls: "classbox youth", lines: ["Under 16: no licence needed if you fish with a licensed adult ⓘ Fish with someone MUT-WHO who holds what this fishing needs (MUT-DOC, MUT-TWO). Or buy the MUT-DOC + MUT-TWO yourself to have your own limit."] },
      { cls: "needs", lines: ["You need", "Nothing, if you fish with a licensed adult (16 or older) They must hold what this fishing needs",
        "MUT-DOC + MUT-TWO Or, for your own limit: what a 16+ angler buys $1.00 a year",
        "Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence"] }]);
  });

  it("(licence 7) the youth alternatives are the STORED under_16 `or`: exactly the 3 Teslin states carry one, compared whole above", async () => {
    const hit: string[] = [];
    for (const id of caseIds("licence")) {
      const inp = (await inputsOf(id))!, F = inp.frames?.licence;
      if (!F?.under_16 || settle(inp, makePlace(inp)).status?.status === "closed") continue;
      if (F.under_16.some((u) => u.buy.some((b) => (b.or ?? []).length))) hit.push(id);
    }
    expect(hit).toEqual(YOUTH_ALT_IDS);
    expect(licenceDropped("x12p_1010", "u16|non_resident")).toEqual([]);              // no line is dropped any more
    const F = (await inputsOf("x12p_1010"))!.frames!.licence;
    expect(F.under_16!.find((u) => u.residency === "non_resident")!.buy).toEqual([{ doc: "basic_licence", name: "Basic angling licence",
      prices: { year: 62.87, day: [22.86], eight_days: [41.15] }, or: [{ need: ["yukon_angling_licence"], names: ["Yukon angling licence"] }] }]);
  });

  it("(licence 7, mutation) the youth box names exactly the stored alternatives — never the 16+ answer's", async () => {
    const inp = (await inputsOf("x12p_1010"))!, F = inp.frames!.licence, r = requestOf("x12p_1010");
    const u16 = F.under_16!.map((u) => ({ ...u, buy: u.buy.map((b) => ({ ...b, or: [{ need: ["m"], names: ["MUT-ALT"] }] })) }));
    const mut: RegsInputs = { ...inp, frames: { ...inp.frames!, licence: { ...F, under_16: u16 } } };
    const b = buildRegsPage(mut, { today: r.today, who: { age: "u16", residency: "non_resident" }, pid: "x12p_1010" }).licence!.blocks
      .map((x) => ({ cls: x.cls, lines: [...licenceBlockLines(x)] }));
    expect(b.slice(0, 2).map((x) => x.lines)).toEqual([
      ["Under 16: no licence needed if you fish with a licensed adult ⓘ Fish with someone 16 or older who holds what this fishing needs (Basic angling licence or MUT-ALT). What you keep counts toward their limit. Or buy the Basic angling licence or MUT-ALT yourself to have your own limit."],
      ["You need", "Nothing, if you fish with a licensed adult (16 or older) What you keep counts toward their limit",
        "Basic angling licence or MUT-ALT Or, for your own limit: what a 16+ angler buys $62.87 a year · $22.86 a day · $41.15 for 8 days",
        "Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence"]]);
  });

  it("tidal x31p_1010 (excluded, D33 (b)), all six: page v36's tidal box with the stored tidal licence words", async () => {
    expect(TIDAL_LICENCE_IDS).toEqual(["x31p_1010"]);
    for (const k of LICENCE_PROFILES) {
      const x = await L("x31p_1010", k);
      expect([x.blocks, x.label === gl("vedder_1010", k).label]).toEqual([[{ cls: "classbox", lines: ["Tidal water federal Tidal Waters Sport Fishing Licence"] }], true]);
    }
  });

  it("(licence 7) the steelhead stamp is left out by the STORE exactly where the card closes steelhead (the prototype's 7 states)", async () => {
    const closed: string[] = [], listed: string[] = [];
    for (const id of caseIds("licence")) {
      const inp = (await inputsOf(id))!;
      if (!inp.frames || settle(inp, makePlace(inp)).status?.status === "closed") continue;
      if (inp.frames.rows.rows.some((r) => (r.group?.not_kept ?? []).some((x) => x.status === "closed" && (x.species ?? []).includes("ST")))) closed.push(id);
      if (inp.frames.licence.profiles.some((p) => "documents" in p && p.documents.some((d) => d.doc === "steelhead_stamp"))) listed.push(id);
    }
    expect(closed).toEqual(STEELHEAD_DROP_IDS);
    expect(closed.filter((id) => listed.includes(id))).toEqual([]);                    // the stored documents leave it out there
    expect(STEELHEAD_DROP_IDS.flatMap((id) => LICENCE_PROFILES.flatMap((k) => licenceDropped(id, k)))).toEqual([]);
  });

  it("(licence 7, mutation) the app draws whatever the stored documents say — a stamp put back is drawn, never dropped by the app", async () => {
    const inp = (await inputsOf("bc1_501"))!, F = inp.frames!.licence, r = requestOf("bc1_501");
    const st = (await inputsOf("vedder_1010"))!.frames!.licence.profiles
      .map((p) => ("documents" in p ? p.documents.find((d) => d.doc === "steelhead_stamp") : undefined)).find(Boolean)!;
    const profiles = F.profiles.map((p) => ("documents" in p && p.documents.length ? { ...p, documents: [...p.documents, st] } : p));
    const mut: RegsInputs = { ...inp, frames: { ...inp.frames!, licence: { ...F, profiles } } };
    const lines = buildRegsPage(mut, { today: r.today, who: ADULT, pid: "bc1_501" }).licence!.blocks
      .flatMap((x) => [...licenceBlockLines(x)]);
    expect(lines.filter((l) => l.startsWith("Steelhead"))).toEqual(["Steelhead Conservation Surcharge Stamp ⓘ Only if you fish for steelhead, even to release them $28.57 a year"]);
  });

  it("closed is the stored display status (settle), never the licence frame", async () => {
    const inp = (await inputsOf("vedder_615"))!;
    expect(settle(inp, makePlace(inp)).status?.status).toBe("closed");
    expect(inp.frames?.licence).toBeTruthy();   // the frame exists on a closed day: hiding it is D16's choice
  });
});
