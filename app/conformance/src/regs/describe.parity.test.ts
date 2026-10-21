/**
 * T06 parity: core's rule views and describe() against page v36's OWN output (app/design/regs-v11/port/golden/
 * hydrate.json, written by port/hydrate.mjs running page_v36.js lines 1–242 in node). Nothing here is a hand-copied string:
 * every expected value is read from the oracle.
 *
 * Run over two inputs (ruleSources.ts): page_v36.html's own #data/#answers (always — the oracle's inputs), and the live
 * release when it is on this machine, read through `openRegsStore(loader)` as the app reads it (skipped otherwise).
 *   7. every rix the oracle describes: describe(ruleIndex(stored, new Map(), book).get(rix), book) === oracle
 *   8. every rix: {key, kind, baseRank, species, speciesExcept, wins, tcond, part, notes} deep-equal the oracle's
 *   9. every state: via = viaOf(the part's ruleset); every oracle `via` entry is "trib" there, every `ranks[rix]` equals
 *      ruleIndex(…, via, …).get(rix).rank — and (the pin) NOT what the view says without the via.
 */
import { describe as group, expect, it } from "vitest";
import { describe, ruleIndex, viaOf, type RegsPage, type RuleView } from "@app/core";
import { hydrate, taps } from "./oracles";
import { pageDecode, pageSource, storeSource, type RuleSource } from "./ruleSources";

const H = hydrate();
const T = taps();
const store = await storeSource();

/** The port target core's contract names (model.ts `RegsPage.version`): typed, so a contract bump fails tsc here. */
const TARGET: RegsPage["version"] = "regs-v38";

const factsOf = (r: RuleView) => ({ key: r.key, kind: r.kind, baseRank: r.baseRank, species: r.species, speciesExcept: r.speciesExcept,
  wins: r.wins, tcond: r.tcond, part: r.part, notes: r.notes });

function parity(src: () => RuleSource) {
  it("the oracle is page v38's 726 rules over 134 states, written after this sync's taps.json", () => {
    // provenance of the CONTENT, not of the run: the prototype version is the port target, and the golden/version are
    // the ones taps.json carries — hydrate.mjs copies them from the taps.json it read, so a hydrate.json written before
    // taps.py (the old sync.mjs order: golden d4a0ccd9 / v37 beside taps' bdda3586 / v38) fails here.
    expect(H.meta.generator).toBe("app/design/regs-v11/port/hydrate.mjs");
    expect(`regs-${H.meta.version}`).toBe(TARGET);
    expect({ golden: H.meta.golden, version: H.meta.version }).toEqual({ golden: T.meta.golden, version: T.meta.version });
    expect(Object.keys(H.describe).length).toBe(726);
    expect(Object.keys(H.states).length).toBe(134);
  });

  it("7. describe() says what page v36 says, for every rule the oracle describes", () => {
    const s = src();
    const ix = ruleIndex(s.stored, new Map(), s.book);
    const bad: string[] = [];
    for (const [rix, want] of Object.entries(H.describe)) {
      expect(ix.has(+rix), `rix ${rix} stored`).toBe(true);
      const got = describe(ix.get(+rix), s.book);
      if (got !== want) bad.push(`${rix}: ${JSON.stringify(got)} ≠ ${JSON.stringify(want)}`);
    }
    expect(bad).toEqual([]);
  });

  it("8. every rule view decodes as page v36 does (key, kind, ranks, species, windows, time, part, notes)", () => {
    const s = src();
    const ix = ruleIndex(s.stored, new Map(), s.book);
    for (const [rix, want] of Object.entries(H.rules)) expect(factsOf(ix.get(+rix)), `rix ${rix}`).toEqual(want);
  });

  it("9. per state, the part's ruleset gives every oracle trib via and every per-part rank", () => {
    const s = src();
    let tribs = 0, ranks = 0;
    for (const [id, st] of Object.entries(H.states)) {
      const via = viaOf(s.ruleset(T.states[id]!));
      const ix = ruleIndex(s.stored, via, s.book), flat = ruleIndex(s.stored, new Map(), s.book);
      for (const rix of Object.keys(st.via)) { expect(via.get(+rix), `${id} via ${rix}`).toBe("trib"); tribs++; }
      for (const [rix, rank] of Object.entries(st.ranks)) {
        expect(ix.get(+rix).rank, `${id} rank ${rix}`).toBe(rank);
        expect(flat.get(+rix).rank, `${id} rank ${rix} needs the via`).not.toBe(rank);
        ranks++;
      }
    }
    expect(tribs).toBeGreaterThan(0);
    expect(ranks).toBeGreaterThan(0);
  });
}

group("T06 parity: page_v36.html's own inputs", () => parity(pageSource));

group.skipIf(!store)(`T06 parity: the regs store (${store?.name ?? "absent"})`, () => {
  it("holds every rule of the store, a superset of the page's slice", () => {
    expect(store!.stored.size).toBeGreaterThanOrEqual(Object.keys(H.describe).length);
  });
  parity(() => store!);

  // hydrate.json's 726 rules all have empty notes; the store's rules with prov.uncertain (or a review_reason) are the only
  // ones that exercise the UNCERTAIN / CURATOR wording. The oracle is page v36's own decode run over the same store rows.
  it("10. every store rule with notes has page v36's notes (and its facts and describe())", () => {
    const page = pageDecode(store!);
    const ix = ruleIndex(store!.stored, new Map(), store!.book);
    const noted = [...page.rules.values()].filter((r) => r.notes.length > 0);
    expect(noted.length, "store rules with notes").toBeGreaterThan(0);
    for (const r of noted) {
      const v = ix.get(r.ix);
      expect(v.notes, `rix ${r.ix} notes`).toEqual(r.notes);
      expect(factsOf(v), `rix ${r.ix}`).toEqual({ key: r.key, kind: r.k, baseRank: r.baseRank, species: r.species,
        speciesExcept: r.speciesExcept, wins: r.wins, tcond: r.tcond, part: r.part, notes: r.notes });
      expect(describe(v, store!.book), `rix ${r.ix} describe`).toBe(page.describe(r.ix));
    }
    console.log(`T06 parity 10: ${noted.length} store rules with notes match page v36's decode`);
  });
});
