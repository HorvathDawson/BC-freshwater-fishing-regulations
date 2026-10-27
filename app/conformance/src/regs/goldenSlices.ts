/**
 * T23 / T29: the slices of the golden oracle the RN render tests read (jsdom; a ui-native file may not read files, and
 * golden.json lies outside its rootDir). Copies, never edits: every value is golden.json's or pins.json's.
 *
 *   golden-headers.json  every state's golden `header` (T23: RegsHeader drawn over each)
 *   golden-pins.json     every state's pin geometry in the APP's coordinates (T29): each anchor's boxes minus the
 *                        prototype header's height (`viewport.header`, 119) so the bar's top line is the scroll view's top
 *                        (pinChain `header: 0`), the viewport height minus the same; the golden strings (group line, tier
 *                        short line without "↳ "); every sample bar; and each drawn line's measured height by its words.
 *   golden-sheets-vedder_1010.json  (T206) every sheet vedder_1010 can open — title and lines, a global "@" resolved —
 *                        as golden() reads them (its load-time rulings applied: the retired r3, oracles.ts retiredRuled)
 *
 * Written by `UPDATE_REGS_FIXTURES=1`, checked otherwise (goldenSlices.test.ts) — no store needed, so the check never skips.
 */
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import type { PinAnchor, PinLine } from "@app/core";
import { FIXTURE_DIR, stableJson } from "./fixtures";
import { golden, pinsOracle } from "./oracles";

export const SLICE_FILES = { headers: `${FIXTURE_DIR}/golden-headers.json`, pins: `${FIXTURE_DIR}/golden-pins.json`,
  sheets: `${FIXTURE_DIR}/golden-sheets-vedder_1010.json` } as const;

type GBox = { top: number; height: number };
type GTier = { tid: string; short: string | null };
type GAnchor = { row: string; groupLine: PinLine | null; tiers: GTier[] };
type GPins = { viewport: { header: number; height: number }; anchors: GAnchor[];
  samples: { y: number; lines: { cls: PinLine["cls"]; text: string; sub: string | null }[] }[] };

export interface PinsSlice {
  readonly meta: { readonly version: string; readonly sha: string };
  /** each drawn line's measured height, by `cls|text|sub` (pinStore's pinLineKey) */
  readonly heights: Readonly<Record<string, number>>;
  readonly states: Readonly<Record<string, {
    readonly viewport: { readonly height: number; readonly header: number };
    readonly anchors: readonly PinAnchor[];
    readonly samples: readonly { readonly y: number; readonly lines: readonly PinLine[] }[];
  }>>;
}

const ARROW = "↳ ";
const shift = (b: GBox | null, d: number): GBox | null => (b ? { top: b.top - d, height: b.height } : null);

export function headersSlice(): Record<string, unknown> {
  const G = golden();
  return Object.fromEntries(Object.keys(G.states).sort().map((id) => [id, G.states[id]!.header]));
}

/** (T206) vedder_1010's sheets as golden() holds them: key → { title, lines } (SheetHost.test's golden mount). */
export function sheetsSlice(): Record<string, { title: string; lines: string[] }> {
  const G = golden() as unknown as { states: Record<string, { sheets: Record<string, unknown> }>; sheets: Record<string, unknown> };
  const one = (v: unknown) => { const x = v as { title: string; lines: string[] }; return { title: x.title, lines: x.lines }; };
  return Object.fromEntries(Object.entries(G.states.vedder_1010!.sheets).map(([k, v]) => [k, one(v === "@" ? G.sheets[k] : v)]));
}

export function pinsSlice(): PinsSlice {
  const G = golden(), P = pinsOracle();
  const heights: Record<string, number> = {};
  const states: Record<string, PinsSlice["states"][string]> = {};
  for (const id of Object.keys(P.states).sort()) {
    const p = P.states[id]!, g = G.states[id]!.pins as GPins, d = p.viewport.header;
    for (const x of p.samples) for (const l of x.lines) heights[`${l.cls}|${l.text}|${l.sub ?? ""}`] = l.h;
    states[id] = {
      viewport: { height: p.viewport.height - d, header: d },
      anchors: p.anchors.map((a, i) => {
        const ga = g.anchors[i]!;
        return { row: a.row, group: a.group, tree: a.tree, formA: a.formA, headlineN: a.headlineN,
          top: a.top - d, height: a.height, groupLine: ga.groupLine, pin: shift(a.pin, d),
          tiers: a.tiers.map((t, j) => {
            const s = ga.tiers[j]!.short;
            return { tid: t.tid, tier: t.tier, n: t.n, head: shift(t.head, d), end: t.end == null ? null : t.end - d,
              short: s == null ? null : s.slice(ARROW.length) };
          }) };
      }),
      samples: g.samples.map((x) => ({ y: x.y, lines: x.lines.map((l) => ({ cls: l.cls, text: l.text, sub: l.sub })) })),
    };
  }
  return { meta: { version: P.meta.version, sha: P.meta.sha }, heights, states };
}

/** UPDATE_REGS_FIXTURES=1: write the slices; otherwise return the names of the stale ones. */
export function syncSlices(env: Readonly<Record<string, string | undefined>> = process.env): string[] {
  const want = { headers: stableJson(headersSlice()), pins: stableJson(pinsSlice()), sheets: stableJson(sheetsSlice()) };
  const stale: string[] = [];
  for (const k of ["headers", "pins", "sheets"] as const) {
    const f = SLICE_FILES[k];
    if (env.UPDATE_REGS_FIXTURES === "1") writeFileSync(f, want[k]);
    else if (!existsSync(f) || readFileSync(f, "utf8") !== want[k]) stale.push(k);
  }
  return stale;
}
