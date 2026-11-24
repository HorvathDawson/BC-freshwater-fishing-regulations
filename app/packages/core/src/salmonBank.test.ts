/**
 * (T754, owner 2026-11-22 23:45: "Other bank: the water's other rules") A DFO PLACE FOR ONE BANK of its river stands
 * BESIDE the page's answer (AGENTS rule 49, the trout card's `side`): the pipeline puts it in its sections' chains' `u`,
 * never `l`, and marks it `p[lid].side` (`pipeline/deliver/salmon.py` `build`). The card keeps the water's other rules'
 * answer and lists the place in the one-side caveat ("West bank only · …"), never in "not drawn yet".
 * Hand-built payload in `build`'s shape (the Kitimat's west bank between the hatchery outfall signs).
 */
import { describe, expect, it } from "vitest";
import { decodeSalmonHead, salmonBankLabels, salmonCard, SalmonError, type SalmonFacts } from "./salmon";
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

/**
 * (T1029, look B) THE ONE-BANK CUE'S LABELS: per section the bank place stands beside (`bk`, the pipeline's measured side
 * of the drawn line), "L"/"R" + the bank place's OWN chain label (`s2` here: the `unmapped` chain whose places are exactly
 * the bank's). Checked on decode: sides 1 / -1 on section ids, only beside a `side`, one own chain, one bank per section.
 */
describe("the one-bank cue's labels (T1029)", () => {
  const withBk = (bk: unknown, more: object = {}) => ({ ...payload({ side: "west", bk }), ...more });
  const factsOf = (p: object): SalmonFacts => {
    const P = decodeSalmonHead(head(p), HANDLES);
    return { handles: HANDLES, chains: P.chains.length, payload: P, loaded: ["6"], chainOf: () => 0 };
  };

  it("labels each section beside the bank: its side of the drawn line, then the bank's own chain", () => {
    const labels = salmonBankLabels(factsOf(withBk({ "10": -1, "11": 1 })));
    expect([...labels]).toEqual([[10, "Ls2"], [11, "Rs2"]]);
  });

  it("a release without `bk` (or a place without `side`) labels nothing: no cue, the map as before", () => {
    expect(salmonBankLabels(factsOf(payload({ side: "west" }))).size).toBe(0);
    expect(salmonBankLabels(factsOf(payload({}))).size).toBe(0);
  });

  it("the own chain is the unmapped one whose places are exactly the bank's, never the chain it stands beside", () => {
    // a second unmapped entry on the chain the bank stands beside (l = [WHOLE]) changes nothing
    const p = withBk({ "10": 1 });
    const more = { unmapped: [{ items: ["gnis:1"], r: "6", w: "x", chain: 1 }, ...p.unmapped] };
    expect([...salmonBankLabels(factsOf({ ...p, ...more }))]).toEqual([[10, "Rs2"]]);
  });

  it("a composed payload (a live feed) that no longer lists the bank's own chain labels nothing for it", () => {
    const f = factsOf(withBk({ "10": 1 }));
    const composed = { ...f, payload: { ...f.payload, unmapped: [] } } as SalmonFacts;
    expect(salmonBankLabels(composed).size).toBe(0);
  });

  it("the file is checked on decode: `bk` is section id -> 1 | -1, beside a `side`, one own chain, one bank a section", () => {
    expect(decodeSalmonHead(head(withBk({ "10": -1 })), HANDLES).p?.[BANK]).toMatchObject({ bk: { "10": -1 } });
    for (const bk of [{ "10": 2 }, { "10": 0 }, { "10": "1" }, { "x": 1 }, { "-3": 1 }, { "01": 1 }, {}, [1], null, 1])
      expect(() => decodeSalmonHead(head(withBk(bk)), HANDLES), JSON.stringify(bk)).toThrow(SalmonError);
    // only on a place for one bank
    expect(() => decodeSalmonHead(head(payload({ bk: { "10": 1 } })), HANDLES)).toThrow(/for no one bank/);
    // its own chain: exactly one
    expect(() => decodeSalmonHead(head(withBk({ "10": 1 }, { unmapped: [] })), HANDLES)).toThrow(/0 own chains/);
    // one bank per section: a second one-bank place beside section 10
    const p = withBk({ "10": 1 });
    const two = { ...p, p: { ...p.p, "6:kitimat-river:east": { r: "6", side: "east", bk: { "10": -1 } } },
                  chains: [...p.chains, { ...p.chains[2]!, l: ["6:kitimat-river:east"] }],
                  unmapped: [...p.unmapped, { items: ["gnis:3225"], r: "6", w: "east", chain: 3 }] };
    expect(() => decodeSalmonHead(head(two), HANDLES)).toThrow(/beside two one-bank places/);
  });
});
