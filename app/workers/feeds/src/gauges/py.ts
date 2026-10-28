// T225: the few Python semantics the gauge feed's numbers depend on, so the port computes the SAME doubles as
// `pipeline/gauges/feed/*.py` (the spec). Each is small and pinned by the parity test against the Python run.
//   pyRound    round(x, n): correctly rounded on the double's exact value, ties to even (Math.round is neither)
//   pyRoundInt round(x): ties to even
//   pySum      sum(floats) on Python >= 3.12: Neumaier-compensated (a plain reduce differs in the last bits)
//   pyFloat    float(str): Python's accepted spellings, null where Python raises ValueError
//   pyStr      str(x) for a JSON value (float repr: 1e-05, 12.0)
//   erf        math.erf (JS has none)
//   csvRows    the csv module's default dialect (quotes, CRLF), DictReader on top
//   urlencode  urllib.parse.urlencode (quote_plus) — the recorded URLs are Python's bytes

/** Python's `round(x, n)` for n >= 0. */
export function pyRound(x: number, n: number): number {
  if (!Number.isFinite(x) || Math.abs(x) >= 1e21) return x;
  const neg = x < 0 || Object.is(x, -0);
  const a = Math.abs(x);
  // FAST: 25 digits past the kept ones, themselves correctly rounded, decide every case but an apparent exact half
  let s = a.toFixed(n + 25);
  let dot = s.indexOf(".");
  let tail = s.slice(dot + 1 + n);
  let up: boolean;
  if (tail !== HALF25) up = tail > HALF25;
  else {
    // the double's exact decimal expansion (toFixed is exact; 100 digits covers every double >= 2^-48 exactly)
    s = a.toFixed(100);
    dot = s.indexOf(".");
    tail = s.slice(dot + 1 + n);
    const r0 = tail.charCodeAt(0) - 48;
    if (r0 !== 5) up = r0 > 5;
    else if (/[1-9]/.test(tail.slice(1))) up = true;
    else {
      const lastCh = n > 0 ? s.charCodeAt(dot + n) : s.charCodeAt(dot - 1);
      up = (lastCh - 48) % 2 === 1; // a true tie: to even
    }
  }
  let kept = n > 0 ? s.slice(0, dot + 1 + n) : s.slice(0, dot);
  if (up) kept = incDecimal(kept);
  const v = Number(kept);
  return neg ? -v : v;
}
const HALF25 = "5" + "0".repeat(24);

/** "12.349" -> "12.350", "9.99" -> "10.00" */
function incDecimal(s: string): string {
  const c = s.split("");
  for (let i = c.length - 1; i >= 0; i--) {
    if (c[i] === ".") continue;
    if (c[i] !== "9") {
      c[i] = String.fromCharCode(c[i]!.charCodeAt(0) + 1);
      return c.join("");
    }
    c[i] = "0";
  }
  return "1" + c.join("");
}

/** Python's `round(x)` (no digits): nearest integer, ties to even. */
export function pyRoundInt(x: number): number {
  const f = Math.floor(x);
  const d = x - f;
  if (d > 0.5) return f + 1;
  if (d < 0.5) return f;
  return f % 2 === 0 ? f : f + 1;
}

/** Python's `sum()` of floats (CPython >= 3.12: Neumaier compensation). Empty: 0. */
export function pySum(xs: Iterable<number>): number {
  let f = 0;
  let c = 0;
  let first = true;
  for (const x of xs) {
    if (first) {
      f = x; // 0 + x (int start), then the float loop
      first = false;
      continue;
    }
    const t = f + x;
    if (Math.abs(f) >= Math.abs(x)) c += f - t + x;
    else c += x - t + f;
    f = t;
  }
  if (c && Number.isFinite(c)) f += c;
  return f;
}

const PY_FLOAT = /^[+-]?(?:(?:\d(?:_?\d)*)?\.?\d(?:_?\d)*(?:[eE][+-]?\d(?:_?\d)*)?|\d(?:_?\d)*\.(?:[eE][+-]?\d(?:_?\d)*)?|inf|infinity|nan)$/i;
const PLAIN = /^-?\d+(?:\.\d+)?$/;
const PY_WS = /^[\s\u001c-\u001f]+|[\s\u001c-\u001f]+$/g;

/** `float(s)` — null where Python raises ValueError. */
export function pyFloat(s: string | null | undefined): number | null {
  if (s == null) return null;
  if (PLAIN.test(s)) return Number(s); // the common spelling ("12.345"), same double as Python's float()
  const t = s.replace(PY_WS, "");
  if (!t || !PY_FLOAT.test(t)) return null;
  const low = t.toLowerCase().replace(/^[+-]/, "");
  const neg = t.startsWith("-");
  if (low === "inf" || low === "infinity") return neg ? -Infinity : Infinity;
  if (low === "nan") return NaN;
  return Number(t.replace(/_/g, ""));
}

/** Python's repr of a float. */
export function pyFloatRepr(x: number): string {
  if (Number.isNaN(x)) return "nan";
  if (!Number.isFinite(x)) return x > 0 ? "inf" : "-inf";
  if (x === 0) return Object.is(x, -0) ? "-0.0" : "0.0";
  // shortest round-trip digits, then Python's layout: plain for 1e-4 <= |x| < 1e16, else d.ddde±XX
  const [mant, expS] = Math.abs(x).toExponential().split("e");
  const exp = Number(expS);
  const digits = mant!.replace(".", "");
  const sign = x < 0 ? "-" : "";
  if (exp < -4 || exp >= 16) {
    const m = digits.length > 1 ? `${digits[0]}.${digits.slice(1)}` : digits;
    return `${sign}${m}e${exp < 0 ? "-" : "+"}${String(Math.abs(exp)).padStart(2, "0")}`;
  }
  let s: string;
  if (exp >= 0) {
    s = digits.length <= exp + 1 ? digits + "0".repeat(exp + 1 - digits.length) + ".0" : `${digits.slice(0, exp + 1)}.${digits.slice(exp + 1)}`;
  } else {
    s = `0.${"0".repeat(-exp - 1)}${digits}`;
  }
  return sign + s;
}

/** `str(v)` for a value out of JSON. A JSON number is a Python float unless integral (then int: no ".0"). */
export function pyStr(v: unknown): string {
  if (v === null || v === undefined) return "None";
  if (typeof v === "string") return v;
  if (typeof v === "boolean") return v ? "True" : "False";
  if (typeof v === "number") return Number.isInteger(v) && Math.abs(v) < 1e16 ? String(v) : pyFloatRepr(v);
  return JSON.stringify(v);
}

/** Python truthiness of a JSON value. */
export function truthy(v: unknown): boolean {
  if (v === null || v === undefined || v === false || v === 0 || v === "") return false;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v as object).length > 0;
  return true;
}

/** math.erf: the all-positive series 2/sqrt(pi) e^{-x^2} sum 2^n x^{2n+1} / (2n+1)!! (no cancellation), |x| < 6. */
export function erf(x: number): number {
  if (Number.isNaN(x)) return NaN;
  const a = Math.abs(x);
  if (a >= 6) return Math.sign(x);
  const x2 = 2 * a * a;
  let term = a;
  let sum = a;
  for (let n = 1; n < 500; n++) {
    term *= x2 / (2 * n + 1);
    sum += term;
    if (term < sum * 1e-18) break;
  }
  const v = (2 / Math.sqrt(Math.PI)) * Math.exp(-a * a) * sum;
  return x < 0 ? -v : v;
}

/** csv.reader with the default (excel) dialect: rows of fields; a blank line is `[]`. */
export function csvRows(text: string): string[][] {
  const out: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQ = false;
  let started = false; // anything on this line yet
  let quotedField = false;
  const n = text.length;
  for (let i = 0; i < n; i++) {
    const ch = text[i]!;
    if (inQ) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i++;
        } else inQ = false;
      } else field += ch;
      continue;
    }
    if (ch === '"' && field === "" && !quotedField) {
      inQ = true;
      quotedField = true;
      started = true;
      continue;
    }
    if (ch === ",") {
      row.push(field);
      field = "";
      quotedField = false;
      started = true;
      continue;
    }
    if (ch === "\r" || ch === "\n") {
      if (ch === "\r" && text[i + 1] === "\n") i++;
      if (started || field !== "") row.push(field);
      out.push(row);
      row = [];
      field = "";
      started = false;
      quotedField = false;
      continue;
    }
    field += ch;
    started = true;
  }
  if (started || field !== "") {
    row.push(field);
    out.push(row);
  }
  return out;
}

/** csv.DictReader: the first row names the columns; blank rows skipped; a short row's missing fields are null. */
export function dictRows(text: string): Array<Record<string, string | null>> {
  const rows = csvRows(text);
  if (!rows.length) return [];
  const head = rows[0]!;
  const out: Array<Record<string, string | null>> = [];
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i]!;
    if (!r.length) continue;
    const d: Record<string, string | null> = {};
    head.forEach((h, k) => {
      d[h] = k < r.length ? r[k]! : null;
    });
    out.push(d);
  }
  return out;
}

const SAFE = /[A-Za-z0-9_.\-~]/;
function quotePlus(s: string): string {
  let o = "";
  for (const ch of s) {
    if (ch === " ") o += "+";
    else if (SAFE.test(ch)) o += ch;
    else for (const b of new TextEncoder().encode(ch)) o += "%" + b.toString(16).toUpperCase().padStart(2, "0");
  }
  return o;
}

/** urllib.parse.urlencode over (key, value) pairs. */
export function urlencode(pairs: Array<[string, string | number]>): string {
  return pairs.map(([k, v]) => `${quotePlus(String(k))}=${quotePlus(typeof v === "number" ? pyStr(v) : v)}`).join("&");
}

/** `str.splitlines()` */
export function splitlines(s: string): string[] {
  const parts = s.split(/\r\n|[\n\r\v\f\x1c\x1d\x1e\x85\u2028\u2029]/);
  if (parts.length && parts[parts.length - 1] === "") parts.pop();
  return parts;
}

/** A Promise pool of `n`, results in input order (ThreadPoolExecutor.map). */
export async function pool<T, R>(items: readonly T[], n: number, fn: (t: T) => Promise<R>): Promise<R[]> {
  const out = new Array<R>(items.length);
  let next = 0;
  const lanes = Array.from({ length: Math.max(1, Math.min(n, items.length)) }, async () => {
    while (next < items.length) {
      const k = next++;
      out[k] = await fn(items[k]!);
    }
  });
  await Promise.all(lanes);
  return out;
}

/** json.dumps(v, sort_keys=True, separators=(",", ":")) up to number spelling (see the parity report). */
export function dumpsSorted(v: unknown): string {
  if (v === null || typeof v !== "object") return JSON.stringify(v ?? null);
  if (Array.isArray(v)) return `[${v.map((x) => (x === undefined ? "null" : dumpsSorted(x))).join(",")}]`;
  const keys = Object.keys(v as object).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${keys.map((k) => `${JSON.stringify(k)}:${dumpsSorted((v as Record<string, unknown>)[k])}`).join(",")}}`;
}
