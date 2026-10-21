/**
 * T28 parity: the pin bar over a scroll of every golden state's Keep tab (prototype regs v38 @ 7fb6809f pinUpdate, round
 * UR6), replayed through core `pinChain`.
 *
 * Oracles: golden.json `pins` (the v33-era anchors: each article, its group line, each tier's short line, and every bar the
 * prototype drew at 8 px steps) and pins.json (port/pins.mjs, the same prototype: each tier's `end` = secEnd and each drawn
 * line's measured height — what v38's pinUpdate reads and golden.json does not record). No store is needed for A–C, so
 * they never skip; D holds the app's own strings (GroupVM.pinLine, TierVM.short over the paired store) to the golden's.
 */
import { afterAll, describe, expect, it } from "vitest";
import { boldFirst, pinChain, pinLineEstimate, PIN_ROW, tierLine, type PinAnchor, type PinLine, type TreeNodeVM } from "@app/core";
import { caseIds, golden, parityGate, requestOf, ADULT_RESIDENT, inputsOf, summary } from "./golden";
import { buildRegsPage } from "@app/core";
import { excluded } from "./exclusions";
import { pinsOracle } from "./oracles";

type GBox = { top: number; height: number };
type GTier = { tid: string; tier: string; n: number | null; top: number; height: number; head: GBox | null; nextHead: number | null; short: string | null };
type GAnchor = { row: string; group: boolean; tree: boolean; formA: boolean; headlineN: number | null; top: number; height: number;
  groupLine: PinLine | null; pin: GBox | null; tiers: GTier[] };
type GSample = { y: number; height: number; lines: { cls: PinLine["cls"]; text: string; sub: string | null; big: string }[] };
type GPins = { error: unknown; viewport: { header: number; LH: number }; anchors: GAnchor[]; samples: GSample[] };

const G = golden(), P = pinsOracle();
const ids = Object.keys(P.states);
const gp = (id: string) => G.states[id]!.pins as GPins;

const lineKey = (l: PinLine) => `${l.cls}|${l.text}|${l.sub ?? ""}`;
/** every drawn line's measured height (the prototype's PIN_HC, keyed by its words as pinUpdate keys it) */
const MEASURED = new Map<string, number>();
for (const s of Object.values(P.states)) for (const x of s.samples) for (const l of x.lines) MEASURED.set(lineKey(l), l.h);
const measured = (l: PinLine) => MEASURED.get(lineKey(l)) ?? pinLineEstimate(l);

/** pins.json's geometry with the strings `str` gives (golden's, or the app's). `short` is WITHOUT "↳ " (TierVM.short). */
function anchorsOf(id: string, str: (a: GAnchor) => { groupLine: PinLine | null; short: (t: GTier) => string | null }): PinAnchor[] {
  const g = gp(id);
  return P.states[id]!.anchors.map((a, i) => {
    const ga = g.anchors[i]!, s = str(ga);
    return { row: a.row, group: a.group, tree: a.tree, formA: a.formA, headlineN: a.headlineN, top: a.top, height: a.height,
      groupLine: s.groupLine, pin: a.pin,
      tiers: a.tiers.map((t, j) => ({ tid: t.tid, tier: t.tier, n: t.n, head: t.head, end: t.end, short: s.short(ga.tiers[j]!) })) };
  });
}
const ARROW = "↳ ";
const goldenStrings = (a: GAnchor) => ({ groupLine: a.groupLine, short: (t: GTier) => (t.short == null ? null : t.short.slice(ARROW.length)) });

type Step = { y: number; k: string };
const k3 = (ls: readonly PinLine[]) => JSON.stringify(ls.map((l) => [l.cls, l.text, l.sub]));
/** every change of the bar at 8 px steps until the last article has scrolled past the bar (the golden's samples run to the
 *  end of the Keep tab and record only changes, so nothing may change after the last one) */
function replay(id: string, anchors: readonly PinAnchor[], heightOf = measured, ROW = PIN_ROW): Step[] {
  const vp = { header: P.states[id]!.viewport.header, LH: P.states[id]!.viewport.LH, ROW };
  const s = gp(id).samples, out: Step[] = [];
  const lim = Math.max(s.length ? s[s.length - 1]!.y : 0, ...anchors.map((a) => a.top + a.height));
  let last: string | null = null;
  for (let y = 0; y <= lim; y += 8) { const k = k3(pinChain(anchors, y, vp, heightOf)); if (k !== last) { out.push({ y, k }); last = k; } }
  return out;
}
const want = (id: string): Step[] => gp(id).samples.map((x) => ({ y: x.y, k: k3(x.lines) }));

describe("A. pins.json and golden.json are one prototype run", () => {
  it("same prototype version and sha, every state, v38's PIN_ROW", () => {
    expect([P.meta.version, P.meta.sha, P.meta.states]).toEqual([G.meta.version, G.meta.sha, G.meta.states]);
    expect(ids.sort()).toEqual(Object.keys(G.states).sort());
    expect(new Set(ids.map((id) => P.states[id]!.viewport.ROW))).toEqual(new Set([PIN_ROW]));
  });
  it.each(ids)("%s: the same bars at the same offsets, the same articles and tier heads", (id) => {
    const p = P.states[id]!, g = gp(id);
    expect(p.samples.map((x) => ({ y: x.y, k: k3(x.lines) }))).toEqual(want(id));
    expect(p.anchors.map((a) => [a.row, Math.round(a.top), Math.round(a.height), a.tiers.map((t) => [t.tid, t.head && Math.round(t.head.top)])]))
      .toEqual(g.anchors.map((a) => [a.row, a.top, a.height, a.tiers.map((t) => [t.tid, t.head && t.head.top])]));
  });
});

describe("B. pinChain replays every golden state", () => {
  let equal = 0;
  afterAll(() => console.info(`pins replay (golden strings): ${equal}/${ids.length} states reproduce every bar`));
  it.each(ids)("%s", (id) => {
    expect(replay(id, anchorsOf(id, goldenStrings))).toEqual(want(id));
    equal++;
  });
  it("big numbers: each line's bold number is boldFirst of its text, else of its sub", () => {
    let n = 0;
    for (const id of ids) for (const x of gp(id).samples) for (const l of x.lines) {
      expect(boldFirst(l.text)?.n ?? boldFirst(l.sub ?? "")?.n ?? "", `${id} ${l.text}`).toBe(l.big); n++;
    }
    expect(n).toBeGreaterThan(200);
  });
  it("mutation pins: each part of the rule is load-bearing on the goldens", () => {
    const failing = (f: (id: string) => Step[]) => ids.filter((id) => JSON.stringify(f(id)) !== JSON.stringify(want(id))).length;
    const gs = (id: string) => anchorsOf(id, goldenStrings);
    expect({
      estimatedHeights: failing((id) => replay(id, gs(id), pinLineEstimate)) > 0,
      noRowMargin: failing((id) => replay(id, gs(id), measured, 0)) > 0,
      noFormASkip: failing((id) => replay(id, gs(id).map((a) => ({ ...a, formA: false })))) > 0,
      noSectionEnd: failing((id) => replay(id, gs(id).map((a) => ({ ...a, tiers: a.tiers.map((t) => ({ ...t, end: 1e9 })) })))) > 0,
      groupLessPins: failing((id) => replay(id, gs(id).map((a) => ({ ...a, group: true, tree: true, groupLine: a.groupLine ?? { cls: "g", text: "x", sub: null }, pin: a.pin ?? { top: a.top, height: 10 } })))) > 0,
    }).toEqual({ estimatedHeights: true, noRowMargin: true, noFormASkip: true, noSectionEnd: true, groupLessPins: true });
  });
});

// ── D. the app's strings over the paired store ──────────────────────────────────────────────────────────────────────────
const gate = await parityGate();
const storeIds = caseIds("pins");

it(`T28 pins parity runs against a paired regs store${gate.skip ? ` — SKIPPED: ${"reason" in gate ? gate.reason : gate.fail}` : ""}`, () => {
  if (gate.fail) expect.fail(gate.fail);
});

const shorts = (ns: readonly TreeNodeVM[] | null, out = new Map<string, string>()): Map<string, string> => {
  for (const x of ns ?? []) if (x.t === "tier") { out.set(x.tid, x.short); shorts(x.children, out); }
  return out;
};

describe.skipIf(gate.skip)("D. GroupVM.pinLine and TierVM.short (the store's words) draw the golden bars", () => {
  let equal = 0;
  afterAll(() => console.info(summary("pins", equal, storeIds.length)));
  it.each(storeIds)("%s", async (id) => {
    const page = buildRegsPage((await inputsOf(id))!, { today: requestOf(id).today, who: ADULT_RESIDENT, pid: id });
    const byRow = new Map(page.keep?.groups.map((g) => [`r${g.row}`, g]) ?? []);
    const vmStrings = (a: GAnchor) => {
      const g = byRow.get(a.row), sh = shorts(g?.tree ?? null);
      return { groupLine: g?.pinLine ?? null, short: (t: GTier) => sh.get(t.tid) ?? null };
    };
    const anchors = gp(id).anchors, ruled = anchors.some((_, i) => excluded("pins", id, `anchors.${i}.tiers`));
    anchors.forEach((a, i) => {
      if (!(a.group && a.tree && a.pin)) return;
      const s = vmStrings(a);
      expect(s.groupLine, `${id} ${a.row} group line`).toEqual(a.groupLine);
      if (excluded("pins", id, `anchors.${i}.tiers`)) return;
      for (const t of a.tiers) if (t.short != null)
        expect(tierLine(s.short(t) ?? "(no TierVM)").text, `${id} ${a.row} ${t.tid}`).toBe(t.short);
    });
    if (!ruled) expect(replay(id, anchorsOf(id, vmStrings))).toEqual(want(id));
    equal++;
  });
});
