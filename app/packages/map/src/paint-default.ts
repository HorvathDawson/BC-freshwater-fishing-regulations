/**
 * (T903, owner 2026-11-19: "maybe it should default stream color to red then only paint things different then that?")
 * PAINT ONLY WHAT DIFFERS FROM A DEFAULT — the map-lab switch `paintDefault` (`lab.ts`), ON for every build since the
 * interleaved emulator A/B of 2026-11-19 (mode switches faster, pans the same, the same look: `tools/paint-default.test.ts`).
 * `labelMatch` is ON for every build too since the stressed A/B of 2026-11-19 ~13:00 on top of it (quick pinch and
 * zoom-out draw +61 / +78 % frames, map workers −18 / −20 %, pans and mode switches not worse; T903).
 *
 * Where the time goes on the Salmon map is not the feature-state WRITES (one per section whatever the mode: the status,
 * salmon and method labels ride ONE state call per section, so a mode with a default would save ~10 % of them) but the
 * paint's LABEL TESTS: `["in", label, ["literal", [...]]]` is a linear scan, and the Salmon day's `closed` set is 920 of
 * its 1,095 chain labels, tested by ~50 such lists across the stream, lake and mark layers (54,699 labels at worst per
 * feature, Trout 4,318: T903 counts). MapLibre evaluates them per feature on every tile, and again on every
 * feature-state change.
 *
 * The owner's idea, made exact: every label a channel writes is in its day's sets (the UNIVERSE: @app/core
 * `dayProfiles`, `salmonDayProfiles`, `methodDayProfiles` place every label), so a test of a big set is the NEGATION of
 * a test of the small rest — the default — plus `""` (no label yet), which the big set never held:
 *   in(closed)                  ⇒  !in(open ∪ check ∪ release ∪ none ∪ {""})
 * and a colour `match` keeps the most common answer as its FALLBACK (the default colour) and lists only the others,
 * with what fell through before (`""`, any label no set places) listed explicitly against the old fallback. Same
 * colour, width and opacity for every label and for none; only the length of what is scanned changes.
 *
 * `labelMatch` goes one step further: each test becomes a `match` (a hashed lookup in both MapLibres) on the shorter
 * of the set and its complement.
 */
import type { ChannelValues } from "./style";

export interface PaintDefaultOptions {
  /** each label test a `match` (hashed) instead of an `in` (a scan) */
  readonly match: boolean;
}

/** The labels each feature-state key can hold, from the channels' day sets (null: that channel is not installed). */
export function labelUniverse(values: ChannelValues | null): Map<string, ReadonlySet<string>> {
  const out = new Map<string, ReadonlySet<string>>();
  if (!values) return out;
  const add = (key: string, sets: Readonly<Record<string, readonly string[] | undefined>> | null, skip: readonly string[] = []) => {
    if (!sets) return;
    const u = new Set<string>();
    for (const [k, v] of Object.entries(sets)) if (!skip.includes(k) && v) for (const l of v) u.add(l);
    if (u.size) out.set(key, u);
  };
  // the status day carries the finfish closures' SALMON labels beside its own: they belong to `sp`
  // (T1029 Q6) and `alone`, the isolated class's name: no label
  add("p", values.status as never, ["finfish", "finfishNone", "alone"]);
  add("sp", values.salmon as never);
  add("gm", values.method as never);
  return out;
}

const isArr = (x: unknown): x is unknown[] => Array.isArray(x);

/** `["coalesce", ["feature-state", K], ""]` → K, else null. */
function stateKey(x: unknown): string | null {
  if (!isArr(x) || x[0] !== "coalesce" || x.length !== 3 || x[2] !== "") return null;
  const fs = x[1];
  return isArr(fs) && fs[0] === "feature-state" && typeof fs[1] === "string" ? fs[1] : null;
}

/** A label test written as `labels` (true there) or its negation, as `in` or `match`. */
function test(needle: unknown, labels: readonly string[], negate: boolean, o: PaintDefaultOptions): unknown {
  if (!labels.length) return negate;
  // a match refuses a repeated label
  if (o.match) return ["match", needle, [...new Set(labels)], !negate, negate];
  const t = ["in", needle, ["literal", [...labels]]];
  return negate ? ["!", t] : t;
}

function rewrite(x: unknown, u: Map<string, ReadonlySet<string>>, o: PaintDefaultOptions): unknown {
  if (!isArr(x)) return x;
  if (x[0] === "literal") return x;
  // a label test: in(L) over a key whose universe holds every label of L
  if (x[0] === "in" && x.length === 3) {
    const key = stateKey(x[1]), lit = x[2];
    const universe = key === null ? undefined : u.get(key);
    if (universe && isArr(lit) && lit[0] === "literal" && isArr(lit[1]) && lit[1].every((l) => typeof l === "string" && universe.has(l))) {
      const L = lit[1] as string[], inL = new Set(L);
      const rest = [...universe].filter((l) => !inL.has(l));
      rest.push("");                                   // no label yet: never in L, so in the rest
      return rest.length < L.length ? test(x[1], rest, true, o) : test(x[1], L, false, o);
    }
  }
  // a colour match over a key's labels: the largest branch becomes the fallback
  if (x[0] === "match" && x.length >= 5 && x.length % 2 === 1) {
    const key = stateKey(x[1]);
    const universe = key === null ? undefined : u.get(key);
    const branches: { labels: string[]; out: unknown }[] = [];
    let ok = !!universe;
    for (let i = 2; ok && i < x.length - 1; i += 2) {
      const ls = isArr(x[i]) ? x[i] as unknown[] : [x[i]];
      if (!ls.every((l) => typeof l === "string" && (l === "" || universe!.has(l)))) ok = false;
      else branches.push({ labels: ls as string[], out: rewrite(x[i + 1], u, o) });
    }
    if (ok && branches.length) {
      const fallback = rewrite(x[x.length - 1], u, o);
      const big = branches.reduce((a, b) => (b.labels.length > a.labels.length ? b : a));
      const placed = new Set(branches.flatMap((b) => b.labels));
      const fell = [...universe!].filter((l) => !placed.has(l));
      if (!placed.has("")) fell.push("");
      if (fell.length < big.labels.length) {
        const out: unknown[] = ["match", x[1]];
        for (const b of branches) if (b !== big) out.push(b.labels.length === 1 ? b.labels[0] : b.labels, b.out);
        if (fell.length) out.push(fell.length === 1 ? fell[0] : fell, fallback);
        out.push(big.out);
        return out.length > 3 ? out : big.out;
      }
    }
  }
  return x.map((y) => rewrite(y, u, o));
}

type Layer = { paint?: Record<string, unknown>; filter?: unknown } & Record<string, unknown>;

/** Every layer's paint (and filter) with its label tests written against the default; the same style when nothing is installed. */
export function paintDefaultStyle<S>(style: S, values: ChannelValues | null, o: PaintDefaultOptions): S {
  const u = labelUniverse(values);
  if (!u.size) return style;
  const s = style as unknown as { layers: Layer[] };
  const layers = s.layers.map((l) => {
    if (!l.paint && l.filter === undefined) return l;
    const paint = l.paint ? Object.fromEntries(Object.entries(l.paint).map(([k, v]) => [k, rewrite(v, u, o)])) : undefined;
    return { ...l, ...(paint ? { paint } : {}), ...(l.filter !== undefined ? { filter: rewrite(l.filter, u, o) } : {}) };
  });
  return { ...(style as object), layers } as unknown as S;
}
