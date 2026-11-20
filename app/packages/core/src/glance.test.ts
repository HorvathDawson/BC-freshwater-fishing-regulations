import { describe, expect, it } from "vitest";
import { SOURCE_WORDS, groupGlance, salmonGlance } from "./glance";
import type { GroupVM } from "./regs/model";
import { SALMON_WORDS, type SalmonCardVM } from "./salmon";

const group = (over: Partial<GroupVM["head"]>, tree: GroupVM["tree"] = []): GroupVM => ({
  row: 0, title: "Trout and char", scope: null, origin: null, year: null, tree, notKept: [], worked: null,
  more: { text: "", open: "src:r0" as never },
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
    const card = (st: string[]) => ({ title: SALMON_WORDS.title, rows: st.map(row), caveat: null, issues: [] }) as unknown as SalmonCardVM;
    expect([salmonGlance(card(["check", "check"])).value, salmonGlance(card(["closed"])).tone,
            salmonGlance(card(["open", "closed"])).value])
      .toEqual(["check DFO", "closed", null]);
  });
  it("(T759) a card with an issue banner marks its row, and a closed-all-year one still folds open to it", () => {
    const issue = [{ key: "k" }];
    const card = (over: object) => ({ title: SALMON_WORDS.title, rows: [{ status: "closed" }], caveat: null, issues: issue, book: [], ...over }) as unknown as SalmonCardVM;
    expect(salmonGlance(card({})).issue).toBe(true);
    expect(salmonGlance(card({ closedOnly: true }))).toMatchObject({ issue: true, fold: true });
    expect("issue" in salmonGlance(card({ issues: [], closedOnly: true }))).toBe(false);
  });
});

describe("(T531, T879) each row says whose rule it is", () => {
  it("the real constructors tag their rows: a group is B.C., the salmon card is DFO", () => {
    const g = group({});
    const card = { title: SALMON_WORDS.title, rows: [], caveat: null, issues: [], closedOnly: true, book: [] } as unknown as SalmonCardVM;
    expect([groupGlance(g).source, salmonGlance(card).source]).toEqual(["bc", "dfo"]);
  });
});

describe("(T879) the whose-rule tags", () => {
  it("a row's tag is its source's word; a salmon bar's tag names the rule book where its days are the book's", () => {
    expect([SOURCE_WORDS.tag.dfo, SOURCE_WORDS.tag.bc]).toEqual(["DFO", "B.C."]);
    expect([SOURCE_WORDS.salmonTag.dfo, SOURCE_WORDS.salmonTag.both, SOURCE_WORDS.salmonTag.book]).toEqual(["DFO", "DFO + rule book", "Rule book"]);
  });
});
