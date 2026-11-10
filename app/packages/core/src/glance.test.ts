import { describe, expect, it } from "vitest";
import { groupGlance, salmonGlance, speciesBlocks, type GlanceRowVM } from "./glance";
import type { GroupVM } from "./regs/model";
import { SALMON_WORDS, type SalmonCardVM } from "./salmon";

const group = (over: Partial<GroupVM["head"]>, tree: GroupVM["tree"] = []): GroupVM => ({
  row: 0, title: "Trout and char", scope: null, pinLine: null, origin: null, year: null, tree, notKept: [], worked: null,
  quiet: null, more: { text: "", open: "src:r0" as never },
  head: { form: null, rootTier: null, lead: null, leadN: null, ofTotal: false, whyHere: null, shareOf: null, terms: [],
          nolimit: false, exception: null, words: [{ t: "num", s: "5", size: "big" }, { t: "text", s: " a day per angler across Region 6" }], ...over },
});
const pic = (code: string) => ({ code, name: code, caption: code, open: `sp:${code}` as never, sil: false as const });

describe("(T221) the at-a-glance rows say no count", () => {
  it("a group: its title and a rainbow's picture where it has one — never a number", () => {
    const kinds = [{ t: "kind", pictures: [pic("EB")] }, { t: "kind", pictures: [pic("RB")] }] as unknown as GroupVM["tree"];
    expect(groupGlance(group({}, kinds))).toEqual({ source: "bc", thumb: "RB", name: "Trout and char", value: null, tone: "keep" });
  });
  it("salmon: a status word only when every row has it ((T372) an undrawn place is the caveat, not the answer)", () => {
    const row = (status: string) => ({ status }) as never;
    const card = (st: string[]) => ({ title: SALMON_WORDS.title, rows: st.map(row), caveat: null }) as unknown as SalmonCardVM;
    expect([salmonGlance(card(["check", "check"])).value, salmonGlance(card(["closed"])).tone,
            salmonGlance(card(["open", "closed"])).value])
      .toEqual(["check DFO", "closed", null]);
  });
});

describe("(T531) a species sits only under its own source's calendar", () => {
  const row = (source: GlanceRowVM["source"], name: string) => ({ key: name, row: { source, thumb: null, name, value: null, tone: "keep" as const } });
  const rows = [row("bc", "Trout and char"), row("dfo", "Salmon"), row("bc", "Kokanee")];
  it("the rows' own source decides the block; no hand list of species", () => {
    const b = speciesBlocks(rows, true);
    expect([b.dfo.map((x) => x.key), b.rest.map((x) => x.key)]).toEqual([["Salmon"], ["Trout and char", "Kokanee"]]);
    expect(b.dfo.every((x) => x.row.source === "dfo") && b.rest.every((x) => x.row.source === "bc")).toBe(true);
  });
  it("no DFO calendar drawn: a DFO row has no block of its own and stays with the rest", () => {
    const b = speciesBlocks(rows, false);
    expect([b.dfo.length, b.rest.map((x) => x.key)]).toEqual([0, ["Trout and char", "Salmon", "Kokanee"]]);
  });
  it("the real constructors tag their rows: a group is B.C., the salmon card is DFO", () => {
    const g = group({});
    const card = { title: SALMON_WORDS.title, rows: [], caveat: null, closedOnly: true } as unknown as SalmonCardVM;
    expect([groupGlance(g).source, salmonGlance(card).source]).toEqual(["bc", "dfo"]);
  });
});
