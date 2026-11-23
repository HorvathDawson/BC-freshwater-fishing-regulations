/**
 * (T754, owner 2026-11-22 23:45: "Other bank: the water's other rules") A DFO PLACE FOR ONE BANK of its river stands
 * BESIDE the page's answer (AGENTS rule 49, the trout card's `side`): the pipeline puts it in its sections' chains' `u`,
 * never `l`, and marks it `p[lid].side` (`pipeline/deliver/salmon.py` `build`). The card keeps the water's other rules'
 * answer and lists the place in the one-side caveat ("West bank only · …"), never in "not drawn yet".
 * Hand-built payload in `build`'s shape (the Kitimat's west bank between the hatchery outfall signs).
 */
import { describe, expect, it } from "vitest";
import { decodeSalmonHead, salmonCard, SalmonError, type SalmonFacts } from "./salmon";
import { WORDS } from "./regs/words";

const WHOLE = "6:kitimat-river:whole", BANK = "6:kitimat-river:on-west-bank-between-signs-at-kiti";
const LABEL = "Kitimat River — on west bank between signs at Kitimat hatchery outfall.";
const HANDLES = "147b20dce7d8576c";
const OPEN_COHO = [[0, 0], [0, 1], [0, 0], [0, 0], [0, 0]];
const CLOSED_YEAR = [[0, 2], [0, 2], [0, 2], [0, 2], [0, 2]];

const payload = (side: object) => ({
  format: "salmon/4", credit: "DFO", default: 0,
  regions: { "6": { name: "Skeena", asof: "2026-11-01", url: "https://example/region6", shared: [], sizes: [] } },
  verdicts: [
    { s: "closed", t: "Not opened by DFO: closed to salmon fishing", d: 1 },   // 0
    { s: "open", t: "2 coho per day (Jan 1–Dec 31)" },                         // 1
    { s: "closed", t: "No fishing (Jan 1–Dec 31)" },                           // 2
  ],
  gears: [],
  chains: [
    { r: [], w: "", l: [], sp: CLOSED_YEAR.map(() => [0, 0]), g: [0, -1], m: [0, 3] },
    // 1: the stretch between the signs — the water's other place answers, the bank place beside it
    { r: ["6"], w: "Kitimat River", l: [WHOLE], u: [BANK], sp: OPEN_COHO, g: [0, -1], m: [0, 1] },
    // 2: the bank place's own chain (its rules, the days DFO speaks for it)
    { r: ["6"], w: LABEL, l: [BANK], sp: CLOSED_YEAR, g: [0, -1], m: [0, 3], n: [0, 1] },
  ],
  unmapped: [{ items: ["gnis:3225"], r: "6", w: LABEL, chain: 2 }],
  p: { [WHOLE]: { r: "6", water: "Kitimat River", sec: "E" }, [BANK]: { r: "6", water: "Kitimat River", sec: "E", ...side } },
});
const facts = (side: object = { side: "west" }): SalmonFacts => {
  const P = payload(side) as unknown as SalmonFacts["payload"];
  return { handles: HANDLES, chains: P.chains.length, payload: P, loaded: ["6"], chainOf: (sid) => (sid === 10 ? 1 : 0) };
};
const hx = (h: string) => Array.from({ length: 8 }, (_, i) => parseInt(h.slice(2 * i, 2 * i + 2), 16));
const head = (p: object) => {
  const body = new TextEncoder().encode(JSON.stringify(p));
  return new Uint8Array([0x42, 0x43, 0x53, 0x41, 4, ...hx(HANDLES), body.length & 255, (body.length >> 8) & 255,
    (body.length >> 16) & 255, (body.length >>> 24) & 255, ...body]);
};
const JUL20 = new Date(2026, 6, 20, 12);

describe("a DFO place for one bank of the river (T754)", () => {
  it("stands beside the page's answer: the water's other rules answer, the bank is the one-side caveat", () => {
    const c = salmonCard(facts(), { sids: [10], item: "gnis:3225" }, JUL20);
    expect(c.headlineStatus).toBe("open");                         // the other bank: the water's other rules
    expect(c.caveat).toBeNull();                                    // never "not drawn yet"
    expect(c.side?.kind).toBe("side");
    expect(c.side?.title).toBe(WORDS.SIDE_TITLE);
    expect(c.side?.calm).toBe("On the other bank, this page’s salmon answer applies.");
    expect(c.side?.hot).toBe(true);                                 // DFO speaks for it today
    expect(c.side?.items.map((i) => i.where)).toEqual([`West bank only · ${LABEL}`]);
    expect(c.side?.items[0]!.detail).toContain("No fishing (Jan 1–Dec 31)");
  });

  it("MUTATION: the same place without `side` (a release before T754) is the in-part caveat, as before", () => {
    const c = salmonCard(facts({}), { sids: [10], item: "gnis:3225" }, JUL20);
    expect(c.side).toBeNull();
    expect(c.caveat?.kind).toBe("inpart");
    expect(c.caveat?.items.map((i) => i.where)).toEqual([WORDS.WHERE(LABEL)]);
  });

  it("a stretch the place is not on says nothing of it", () => {
    expect(salmonCard(facts(), { sids: [11], item: "gnis:3225" }, JUL20).side).toBeNull();
  });

  it("the file is checked on decode: a bank is one of the four words", () => {
    expect(decodeSalmonHead(head(payload({ side: "west" })), HANDLES).p?.[BANK]).toMatchObject({ side: "west" });
    expect(() => decodeSalmonHead(head(payload({ side: "left" })), HANDLES)).toThrow(SalmonError);
  });
});
