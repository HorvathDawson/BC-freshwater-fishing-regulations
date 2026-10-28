// T225 DFO port: the Python semantics the feed's output depends on, spelled once.
//
// The Python (pipeline/regs/dfo_salmon/*) is the spec and the feed files are diffed against it byte for byte, so the
// places where JS and Python disagree are pinned here rather than patched at each use:
//   * regex: Python's `\s \w \d \b` are Unicode-aware, JS's are ASCII; Python's `$` also matches before a final "\n";
//     Python's `.` excludes only "\n". `pyRe` translates a Python pattern so the same source reads the same way.
//   * str.strip() (Python whitespace, not JS's), repr() of a str, float repr / round() / "{:.2f}" (round-half-even
//     on the exact binary value, not JS's toFixed), int() (raises on "2px", JS parseInt would not).
//   * exceptions: a Python exception's class name reaches the feed ("the page did not parse (ValueError: ...)"), so a
//     thrown `PyError` carries the Python class name.

/** An exception carrying the Python class name the reference would have raised (it reaches `held`). */
export class PyError extends Error {
  constructor(
    public pyType: string,
    message: string,
  ) {
    super(message);
  }
}

export const pyTypeName = (e: unknown): string =>
  e instanceof PyError ? e.pyType : e instanceof Error ? e.name || "Exception" : "Exception";

export const pyStrOf = (e: unknown): string => (e instanceof Error ? e.message : String(e));

// ── whitespace ───────────────────────────────────────────────────────────────────────────────────────────────────────

/** Python's str.isspace() set (what `\s`, strip() and split() use). JS's `\s` adds U+FEFF and lacks U+001C-1F, U+0085. */
export const PY_WS_CLASS = "\\t\\n\\x0B\\f\\r\\x1C-\\x1F \\x85\\xA0\\u1680\\u2000-\\u200A\\u2028\\u2029\\u202F\\u205F\\u3000";
const WS_SET = new Set<number>([
  0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x85, 0xa0, 0x1680, 0x2000, 0x2001, 0x2002, 0x2003,
  0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000,
]);
export const isPySpace = (ch: string): boolean => WS_SET.has(ch.codePointAt(0)!);

/** str.strip([chars]) */
export function strip(s: string, chars?: string): string {
  return rstrip(lstrip(s, chars), chars);
}
export function lstrip(s: string, chars?: string): string {
  let i = 0;
  const test = chars === undefined ? (c: string) => isPySpace(c) : (c: string) => chars.includes(c);
  while (i < s.length && test(s[i]!)) i++;
  return s.slice(i);
}
export function rstrip(s: string, chars?: string): string {
  let j = s.length;
  const test = chars === undefined ? (c: string) => isPySpace(c) : (c: string) => chars.includes(c);
  while (j > 0 && test(s[j - 1]!)) j--;
  return s.slice(0, j);
}

/** str.split() with no argument: runs of whitespace, no empty strings. */
export function splitWs(s: string): string[] {
  const out: string[] = [];
  let cur = "";
  for (const c of s) {
    if (isPySpace(c)) {
      if (cur) out.push(cur);
      cur = "";
    } else cur += c;
  }
  if (cur) out.push(cur);
  return out;
}

// ── regex ────────────────────────────────────────────────────────────────────────────────────────────────────────────

const W = "\\p{L}\\p{N}_";
const SYNTAX = "^$\\.*+?()[]{}|/";

/** Translate a Python `re` pattern (the subset this feed uses) to a JS `u`-mode source with Python's meaning. */
export function translate(src: string, dotAll: boolean): string {
  let out = "";
  let inClass = false;
  for (let i = 0; i < src.length; i++) {
    const c = src[i]!;
    if (c === "\\") {
      const n = src[++i]!;
      switch (n) {
        case "s":
          out += inClass ? PY_WS_CLASS : `[${PY_WS_CLASS}]`;
          break;
        case "S":
          if (inClass) throw new Error(`pyRe: \\S inside a class: ${src}`);
          out += `[^${PY_WS_CLASS}]`;
          break;
        case "w":
          out += inClass ? W : `[${W}]`;
          break;
        case "W":
          if (inClass) throw new Error(`pyRe: \\W inside a class: ${src}`);
          out += `[^${W}]`;
          break;
        case "d":
          out += "\\p{Nd}";
          break;
        case "D":
          out += "\\P{Nd}";
          break;
        case "b":
          out += inClass ? "\\x08" : `(?:(?<=[${W}])(?![${W}])|(?<![${W}])(?=[${W}]))`;
          break;
        case "B":
          out += `(?:(?<=[${W}])(?=[${W}])|(?<![${W}])(?![${W}]))`;
          break;
        case "Z":
          out += "(?![\\s\\S])";
          break;
        case "A":
          out += "(?<![\\s\\S])";
          break;
        default:
          if (/[A-Za-z0-9]/.test(n)) out += "\\" + n;
          else if (n === "-") out += inClass ? "\\-" : "-";
          else if (SYNTAX.includes(n)) out += "\\" + n;
          else out += n;
      }
      continue;
    }
    if (inClass) {
      if (c === "]") inClass = false;
      out += c;
      continue;
    }
    if (c === "[") {
      inClass = true;
      out += c;
      if (src[i + 1] === "^") out += src[++i];
      if (src[i + 1] === "]") {
        out += "\\]";
        i++;
      }
      continue;
    }
    if (c === "." && !dotAll) out += "[^\\n]";
    else if (c === "$") out += "(?=\\n?(?![\\s\\S]))";
    else if (c === "(" && src.startsWith("(?P<", i)) {
      out += "(?<";
      i += 3;
    } else if (c === "{" && !/^\{\d*(,\d*)?\}/.test(src.slice(i))) out += "\\{";
    else if (c === "}" && !/\{\d*(,\d*)?$/.test(src.slice(0, i))) out += "\\}";
    else out += c;
  }
  return out;
}

export interface PyRe {
  src: string;
  /** global (for search-from / finditer / sub) */
  g: RegExp;
  /** sticky (for match at a position) */
  y: RegExp;
  /** whole-string (fullmatch) */
  full: RegExp;
}

/** Compile a Python pattern. flags: "i" (re.I), "s" (re.S). */
export function pyRe(src: string, flags = ""): PyRe {
  const dotAll = flags.includes("s");
  const t = translate(src, dotAll);
  const f = "u" + (flags.includes("i") ? "i" : "") + (dotAll ? "s" : "");
  return {
    src,
    g: new RegExp(t, f + "g"),
    y: new RegExp(t, f + "y"),
    full: new RegExp(`^(?:${t})(?![\\s\\S])`, f),
  };
}

/** re.search(pattern, s[, pos]) */
export function search(r: PyRe, s: string, pos = 0): RegExpExecArray | null {
  r.g.lastIndex = pos;
  return r.g.exec(s);
}
/** re.match(pattern, s[, pos]) — anchored at pos */
export function match(r: PyRe, s: string, pos = 0): RegExpExecArray | null {
  r.y.lastIndex = pos;
  return r.y.exec(s);
}
/** re.fullmatch */
export function fullmatch(r: PyRe, s: string): RegExpExecArray | null {
  return r.full.exec(s);
}
/** re.finditer */
export function* finditer(r: PyRe, s: string): Generator<RegExpExecArray> {
  let pos = 0;
  while (pos <= s.length) {
    r.g.lastIndex = pos;
    const m = r.g.exec(s);
    if (!m) return;
    yield m;
    pos = m.index + m[0].length;
    if (m[0].length === 0) pos += 1;
  }
}
/** re.sub(pattern, repl, s, count) — repl a function, or a string where \1 / \g<1> are groups */
export function sub(r: PyRe, repl: string | ((m: RegExpExecArray) => string), s: string, count = 0): string {
  let out = "";
  let last = 0;
  let n = 0;
  for (const m of finditer(r, s)) {
    out += s.slice(last, m.index);
    out +=
      typeof repl === "function"
        ? repl(m)
        : repl.replace(/\\(\d+)|\\g<(\d+)>/g, (_, a, b) => m[Number(a ?? b)] ?? "");
    last = m.index + m[0].length;
    n++;
    if (count && n >= count) break;
  }
  return out + s.slice(last);
}
/** re.split(pattern, s, maxsplit) — patterns here have no capturing groups */
export function split(r: PyRe, s: string, maxsplit = 0): string[] {
  const out: string[] = [];
  let last = 0;
  let n = 0;
  for (const m of finditer(r, s)) {
    if (m[0].length === 0 && m.index === last && false) continue;
    out.push(s.slice(last, m.index));
    for (let k = 1; k < m.length; k++) out.push(m[k] as string);
    last = m.index + m[0].length;
    n++;
    if (maxsplit && n >= maxsplit) break;
  }
  out.push(s.slice(last));
  return out;
}
/** re.findall: groups → the tuple (or the one group), else the match */
export function findall(r: PyRe, s: string): (string | string[])[] {
  const out: (string | string[])[] = [];
  for (const m of finditer(r, s)) {
    if (m.length === 1) out.push(m[0]);
    else if (m.length === 2) out.push(m[1] ?? "");
    else out.push(m.slice(1).map((x) => x ?? ""));
  }
  return out;
}

// ── strings ──────────────────────────────────────────────────────────────────────────────────────────────────────────

/** Code-point order (Python's str comparison). */
export function cmpStr(a: string, b: string): number {
  if (a === b) return 0;
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a.codePointAt(i)!;
    const y = b.codePointAt(i)!;
    if (x !== y) return x < y ? -1 : 1;
    if (x > 0xffff) i++;
  }
  return a.length < b.length ? -1 : a.length > b.length ? 1 : 0;
}

/** Python tuple comparison over strings/numbers/null-as-lowest (only what the sorts here compare). */
export function cmpTuple(a: readonly (string | number)[], b: readonly (string | number)[]): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const x = a[i]!;
    const y = b[i]!;
    const c = typeof x === "string" && typeof y === "string" ? cmpStr(x, y) : x < y ? -1 : x > y ? 1 : 0;
    if (c) return c;
  }
  return a.length - b.length;
}

const NONPRINT = /[\p{C}\p{Z}]/u;

/** repr() of a str (or None). */
export function repr(s: string | null | undefined): string {
  if (s === null || s === undefined) return "None";
  const q = s.includes("'") && !s.includes('"') ? '"' : "'";
  let out = q;
  for (const ch of s) {
    const cp = ch.codePointAt(0)!;
    if (ch === "\\") out += "\\\\";
    else if (ch === q) out += "\\" + q;
    else if (ch === "\t") out += "\\t";
    else if (ch === "\n") out += "\\n";
    else if (ch === "\r") out += "\\r";
    else if (cp < 0x20 || cp === 0x7f) out += "\\x" + cp.toString(16).padStart(2, "0");
    else if (cp > 0x7f && ch !== " " && NONPRINT.test(ch)) {
      if (cp <= 0xff) out += "\\x" + cp.toString(16).padStart(2, "0");
      else if (cp <= 0xffff) out += "\\u" + cp.toString(16).padStart(4, "0");
      else out += "\\U" + cp.toString(16).padStart(8, "0");
    } else out += ch;
  }
  return out + q;
}

/** repr() of a list of str: "['A', 'B(i)']" */
export const reprList = (xs: readonly (string | null)[]): string => "[" + xs.map((x) => repr(x)).join(", ") + "]";

/** str.capitalize() */
export const capitalize = (s: string): string => (s ? s[0]!.toUpperCase() + s.slice(1).toLowerCase() : s);

/** int(text) for a str: whitespace-stripped, optional sign, digits (underscores between); else ValueError. */
export function pyInt(text: string | number): number {
  if (typeof text === "number") return Math.trunc(text);
  const t = strip(text);
  const m = /^([+-]?)(\p{Nd}(?:_?\p{Nd})*)$/u.exec(t);
  if (!m) throw new PyError("ValueError", `invalid literal for int() with base 10: ${repr(text)}`);
  const digits = [...m[2]!.replace(/_/g, "")].map((d) => {
    const v = d.codePointAt(0)!;
    // Nd blocks are contiguous runs of ten; ASCII fast path
    return v >= 48 && v <= 57 ? v - 48 : Number(d.normalize("NFKD")) || 0;
  });
  const v = digits.reduce((a, d) => a * 10 + d, 0);
  return m[1] === "-" ? -v : v;
}

// ── floats ───────────────────────────────────────────────────────────────────────────────────────────────────────────

/** The exact decimal value of a finite double: [sign, digits (BigInt), scale] meaning ±digits / 10^scale. */
function exactDecimal(x: number): [number, bigint, number] {
  const sign = x < 0 || Object.is(x, -0) ? -1 : 1;
  const buf = new DataView(new ArrayBuffer(8));
  buf.setFloat64(0, Math.abs(x));
  const hi = buf.getUint32(0);
  const lo = buf.getUint32(4);
  const exp = (hi >>> 20) & 0x7ff;
  let mant = (BigInt(hi & 0xfffff) << 32n) | BigInt(lo);
  let e: number;
  if (exp === 0) e = -1074;
  else {
    mant |= 1n << 52n;
    e = exp - 1075;
  }
  if (e >= 0) return [sign, mant << BigInt(e), 0];
  return [sign, mant * 5n ** BigInt(-e), -e];
}

/** Round ±digits/10^scale to `nd` decimals, half-even. Returns [sign, integer, nd] (value = integer / 10^nd). */
function roundDecimal(x: number, nd: number): [number, bigint] {
  const [sign, digits, scale] = exactDecimal(x);
  if (scale <= nd) return [sign, digits * 10n ** BigInt(nd - scale)];
  const div = 10n ** BigInt(scale - nd);
  let q = digits / div;
  const r = digits % div;
  const twice = r * 2n;
  if (twice > div || (twice === div && q % 2n === 1n)) q += 1n;
  return [sign, q];
}

/** f"{x:.{nd}f}" */
export function formatFixed(x: number, nd: number): string {
  const [sign, q] = roundDecimal(x, nd);
  let s = q.toString();
  if (nd > 0) {
    s = s.padStart(nd + 1, "0");
    s = s.slice(0, -nd) + "." + s.slice(-nd);
  }
  return (sign < 0 && q !== 0n ? "-" : sign < 0 ? "-" : "") + s;
}

/** f"{x:+.0%}" / f"{x:.0%}" */
export function formatPercent(x: number, plus = false): string {
  const s = formatFixed(x * 100, 0);
  return (plus && !s.startsWith("-") ? "+" : "") + s + "%";
}

/** round(x, nd) for a float */
export function pyRound(x: number, nd: number): number {
  const [sign, q] = roundDecimal(x, nd);
  const s = q.toString().padStart(nd + 1, "0");
  return sign * Number(s.slice(0, s.length - nd) + "." + s.slice(s.length - nd));
}

/** repr(float) — the shortest round-trip digits, Python's layout ("1.0", "0.5", "1e-05", "1e+16"). */
export function floatRepr(x: number): string {
  if (Number.isNaN(x)) return "NaN";
  if (!Number.isFinite(x)) return x > 0 ? "Infinity" : "-Infinity";
  if (x === 0) return Object.is(x, -0) ? "-0.0" : "0.0";
  const [mant, expS] = x.toExponential().split("e");
  const exp = Number(expS);
  const neg = mant!.startsWith("-");
  const digits = mant!.replace("-", "").replace(".", "");
  let body: string;
  if (exp >= 16 || exp < -4) {
    const m = digits.length > 1 ? digits[0] + "." + digits.slice(1) : digits;
    body = `${m}e${exp < 0 ? "-" : "+"}${String(Math.abs(exp)).padStart(2, "0")}`;
  } else if (exp >= 0) {
    const intPart = digits.slice(0, exp + 1).padEnd(exp + 1, "0");
    const frac = digits.slice(exp + 1);
    body = `${intPart}.${frac || "0"}`;
  } else {
    body = `0.${"0".repeat(-exp - 1)}${digits}`;
  }
  return (neg ? "-" : "") + body;
}
