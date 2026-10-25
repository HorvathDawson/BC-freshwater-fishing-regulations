/**
 * T15 parity: the inherited line, "Where you can’t fish" and the Possession row — core `inheritedOf` / `whereNotOf` /
 * `possessionOf` over each golden state's inputs (T12 `cases("keep")`, the live release paired with the oracles) — against
 * golden.json `keep.inherited` / `keep.whereNot` / `keep.possession` (regs v38 @ 7fb6809f), on every state. The expected
 * values are the golden's; the ticket's literals are asserted beside it as whole strings.
 */
import { afterAll, describe, expect, it } from "vitest";
import {
  buildModel, inheritedOf, makePlace, plain, possessionOf, settle, whereNotOf, type Line, type RegsInputs,
} from "@app/core";
import { eqPlain } from "./eq";
import { cases } from "./golden";

interface GPage { text: string; pdf: number | null }
interface GKeep {
  inherited: { label: string; lines: { text: string; pages: GPage[] }[]; cautions: string[] } | null;
  whereNot: { heading: string; scope: string | null; items: { text: string; pages: GPage[] }[] } | null;
  possession: { label: string; value: string; open: string | null } | null;
}

const pagesOf = (l: Line): GPage[] => l.filter((r) => r.t === "book").map((r) => ({ text: plain([r]), pdf: r.pdf }));
const blocks = (inp: RegsInputs) => {
  const place = makePlace(inp), settled = settle(inp, place), card = buildModel(inp, place, settled);
  return { inherited: inheritedOf(inp), whereNot: whereNotOf(settled), possession: possessionOf(card, inp, settled) };
};
/** The app's form of each block, projected to the golden's fields (sheet keys carry no state id: `${id}|` is the golden's). */
const project = (id: string, b: ReturnType<typeof blocks>): GKeep => ({
  inherited: b.inherited && { label: b.inherited.label, lines: b.inherited.lines.map((x) => ({ text: plain(x.says), pages: pagesOf(x.says) })),
    cautions: b.inherited.lines.flatMap((x) => (x.caution ? [x.caution] : [])) },
  whereNot: b.whereNot && { heading: b.whereNot.heading, scope: b.whereNot.scope,
    items: b.whereNot.items.map((x) => ({ text: plain(x.text), pages: pagesOf(x.text) })) },
  possession: b.possession && { label: b.possession.label, value: b.possession.value,
    open: b.possession.open.replace(/^pos:/, `pos:${id}|`) },
});
const goldenOf = (g: GKeep): GKeep => ({
  inherited: g.inherited, possession: g.possession,
  whereNot: g.whereNot && { ...g.whereNot, scope: g.whereNot.scope || null },
});

let passed = 0, total = 0;
afterAll(() => { if (total) console.info(`keep T15 (inherited · whereNot · possession) parity: ${passed}/${total} states equal`); });

describe("T15 standing blocks vs golden keep (every state)", () => {
  // (T115) the 4 states whose part the release merged away (x15p14/15/17/18, taps.json `gone`) are not cases; (T183) and the West Road's x27p1 (its Region 6 stretch shares part 0's rules now, taps.json `gone`) neither
  it("127/127 states: inherited, whereNot and possession equal the golden", async () => {
    const bad: string[] = [];
    for await (const c of cases("keep")) {
      total++;
      const got = project(c.id, blocks(await c.inputs())), want = goldenOf(c.g.keep as GKeep);
      const lines = (k: GKeep) => [JSON.stringify(k.inherited), JSON.stringify(k.whereNot), JSON.stringify(k.possession)];
      const eq = eqPlain(lines(got), lines(want));
      if (eq.ok) passed++; else bad.push(`${c.id}\n${eq.diff}`);
    }
    expect(bad).toEqual([]);
    expect(total).toBe(127);   // 134 golden − 7 gone (T115 ×4, T67 ×2, T183 ×1)
  });

  it("2. glenn_501: the Duncan River's exemption, its Rule book page and the nearer row's caution", async () => {
    for await (const c of cases("keep")) {
      if (c.id !== "glenn_501") continue;
      const b = blocks(await c.inputs());
      expect(b.inherited && { label: b.inherited.label, lines: b.inherited.lines.map((x) => ({ says: plain(x.says), caution: x.caution })) }).toEqual({
        label: "By another water’s rule",
        lines: [{ says: "Open Apr 1–Jun 14: the Duncan River’s exemption from the spring stream closure covers its tributaries. Rule book p.36 ↗",
          caution: "Trout Lake’s row, nearer this stream, prints no such exemption; the Duncan River’s is applied here because its row covers all its tributaries." }],
      });
      expect(pagesOf(b.inherited!.lines[0]!.says)).toEqual([{ text: "Rule book p.36 ↗", pdf: 38 }]);
    }
  });

  it("3. iron_720: one line, the Quinsam River's exemption, no caution", async () => {
    for await (const c of cases("keep")) {
      if (c.id !== "iron_720") continue;
      const b = blocks(await c.inputs());
      expect(b.inherited!.lines.map((x) => [plain(x.says), x.caution])).toEqual([[
        "Open Jul 15–Aug 31: the Quinsam River’s exemption from the summer stream closure (MUs 1-1 to 1-6) covers its tributaries. Rule book p.18 ↗", null]]);
    }
  });

  it("4–6. vedder_1010 where-not and possession; x31p_1010 (tidal) draws none of the three", async () => {
    for await (const c of cases("keep")) {
      if (c.id === "vedder_1010") {
        const b = blocks(await c.inputs());
        expect([b.whereNot!.heading, b.whereNot!.scope, ...b.whereNot!.items.map((x) => plain(x.text))]).toEqual([
          "Where you can’t fish", null,
          "No fishing: within 23 m (75 ft) downstream of the lower entrance to any fishway, canal, obstacle or leap Rule book p.9 ↗",
          "No fishing: within a 100 m (330 ft) radius of any government facility operated for counting, passing or rearing fish (e.g., fishway, fish hatchery) unless otherwise designated Rule book p.9 ↗",
        ]);
        expect(b.possession && [b.possession.label, b.possession.value, b.possession.term, b.possession.open])
          .toEqual(["Possession", "2 days’ quota", "possession_quota", "pos:pos"]);
      }
      if (c.id === "x31p_1010") expect(blocks(await c.inputs())).toEqual({ inherited: null, whereNot: null, possession: null });
    }
  });

  // (T115) the 4 states whose part the release merged away (x15p14/15/17/18, taps.json `gone`) are not cases: all 4 carried the possession row;
  // (T183) and the West Road's x27p1 (its Region 6 stretch shares part 0's rules now, taps.json `gone`) carried it too
  it("5. census: the possession row is on 102 states, always \"2 days’ quota\"; null on the other 25", async () => {
    const values: string[] = [];
    let none = 0;
    for await (const c of cases("keep")) {
      const p = blocks(await c.inputs()).possession;
      if (p) values.push(p.value); else none++;
    }
    if (!values.length && !none) return;   // no paired store: cases() yielded nothing (a failure under REGS_STORE_REQUIRED=1)
    expect([values.length, none, [...new Set(values)]]).toEqual([102, 25, ["2 days’ quota"]]);
  });
});
