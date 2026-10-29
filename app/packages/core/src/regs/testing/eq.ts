/**
 * The golden comparison (contracts.md §1/§5, tests.md §1.2; golden ≥ b999cfd7). The golden carries ONE space wherever two
 * elements abut; the app side is `plain()`/`plainAll()` (line.ts). Both sides are `norm`ed, then compared line by line.
 * Ports app/design/regs-v11/port/golden.mjs (deleted in T87; in git) `gap()`/`lines()` normalisation. A comparison, not a regulation: it decides
 * nothing about the water.
 */

/** Collapse every whitespace run (U+00A0 included) to one space, trim. */
export const norm = (s: string): string => s.replace(/\s+/g, " ").trim();

/** Line-by-line equality after `norm`; the failure carries `diffLines(actual, expected)`. */
export function eqPlain(actual: readonly string[], expected: readonly string[]): { ok: true } | { ok: false; diff: string } {
  const a = actual.map(norm), e = expected.map(norm);
  return a.length === e.length && a.every((s, i) => s === e[i]) ? { ok: true } : { ok: false, diff: diffLines(actual, expected) };
}

type Op = { k: " " | "-" | "+"; s: string; line: number };

/** The edit script expected → actual over the longest common subsequence: within a change, every "-" before its "+". */
function script(a: readonly string[], e: readonly string[]): Op[] {
  const n = e.length, m = a.length;
  const L: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--)
    for (let j = m - 1; j >= 0; j--) L[i]![j] = e[i] === a[j] ? L[i + 1]![j + 1]! + 1 : Math.max(L[i + 1]![j]!, L[i]![j + 1]!);
  const ops: Op[] = [];
  let i = 0, j = 0, minus: Op[] = [], plus: Op[] = [];
  const flush = () => { ops.push(...minus, ...plus); minus = []; plus = []; };
  while (i < n || j < m) {
    if (i < n && j < m && e[i] === a[j]) { flush(); ops.push({ k: " ", s: e[i]!, line: i + 1 }); i++; j++; }
    else if (j >= m || (i < n && L[i + 1]![j]! >= L[i]![j + 1]!)) { minus.push({ k: "-", s: e[i]!, line: i + 1 }); i++; }
    else { plus.push({ k: "+", s: a[j]!, line: j + 1 }); j++; }
  }
  flush();
  return ops;
}

const MAX = 40;
const q = (s: string) => JSON.stringify(s);
const clip = (s: string) => q(s.length > 40 ? `${s.slice(0, 40)}…` : s);

/**
 * A unified, line-numbered diff of the normalised lines (`-` expected, `+` actual, `ctx` equal lines either side of the
 * changes), at most 40 lines, ending with "first difference at line L char N: expected <x>, got <y>" (N 1-based).
 */
export function diffLines(a: readonly string[], e: readonly string[], ctx = 2): string {
  const A = a.map(norm), E = e.map(norm);
  const at = Array.from({ length: Math.max(A.length, E.length) }, (_, i) => i).find((i) => A[i] !== E[i]);
  if (at === undefined) return "no difference";
  const ops = script(A, E);
  const first = ops.findIndex((o) => o.k !== " "), last = ops.length - 1 - [...ops].reverse().findIndex((o) => o.k !== " ");
  const body = ops.slice(Math.max(0, first - ctx), last + ctx + 1)
    .map((o) => o.k === " " ? `    ${q(o.s)}` : `  ${o.k} ${q(o.s)}   (${o.k === "-" ? "expected" : "actual"})`);

  const x = E[at], y = A[at];
  let tail: string;
  if (x === undefined) tail = `expected end-of-lines, got ${clip(y!)}`;
  else if (y === undefined) tail = `expected ${clip(x)}, got end-of-lines`;
  else {
    let c = 0;
    while (c < x.length && c < y.length && x[c] === y[c]) c++;
    const rest = (s: string) => c >= s.length ? "end-of-line" : clip(s.slice(c));
    tail = `char ${c + 1}: expected ${rest(x)}, got ${rest(y)}`;
  }
  const end = `first difference at line ${at + 1} ${tail.startsWith("char") ? tail : `char 1: ${tail}`}`;

  const head = `@@ line ${ops[first]!.line} @@`;
  const room = MAX - 2;
  const shown = body.length <= room ? body : [...body.slice(0, room - 1), `  … ${body.length - (room - 1)} more lines`];
  return [head, ...shown, end].join("\n");
}
