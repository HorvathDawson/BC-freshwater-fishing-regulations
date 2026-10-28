// T225 DFO port: Python's `json` byte for byte. The feed files are `json.dumps(obj, ensure_ascii=False,
// separators=(",", ":"), sort_keys=True) + "\n"`, and Python keeps int vs float apart ("1.0" stays "1.0"), so a float
// is a `PyFloat` here — produced by the port where Python makes a float, and by `loads` wherever the text had one.
import { cmpStr, floatRepr } from "./py";

export class PyFloat {
  constructor(public readonly v: number) {}
  valueOf(): number {
    return this.v;
  }
}

export type PyJson = null | boolean | number | string | PyFloat | PyJson[] | { [k: string]: PyJson };

export interface DumpOptions {
  sortKeys?: boolean;
  ensureAscii?: boolean;
  /** (",", ":") when true, else Python's default (", ", ": ") */
  compact?: boolean;
}

const ESC: Record<string, string> = { '"': '\\"', "\\": "\\\\", "\n": "\\n", "\r": "\\r", "\t": "\\t", "\b": "\\b", "\f": "\\f" };

function str(s: string, ascii: boolean): string {
  let out = '"';
  for (let i = 0; i < s.length; i++) {
    const c = s[i]!;
    const e = ESC[c];
    if (e) out += e;
    else {
      const code = s.charCodeAt(i);
      if (code < 0x20 || (ascii && code > 0x7e)) out += "\\u" + code.toString(16).padStart(4, "0");
      else out += c;
    }
  }
  return out + '"';
}

export function dumps(v: unknown, o: DumpOptions = {}): string {
  const ascii = o.ensureAscii ?? true;
  const [item, kv] = o.compact ? [",", ":"] : [", ", ": "];
  const go = (x: unknown): string => {
    if (x === null || x === undefined) return "null";
    if (x === true) return "true";
    if (x === false) return "false";
    if (x instanceof PyFloat) return floatRepr(x.v);
    if (typeof x === "number") {
      if (!Number.isInteger(x)) return floatRepr(x); // a non-integral JS number can only be a Python float
      return String(x);
    }
    if (typeof x === "string") return str(x, ascii);
    if (Array.isArray(x)) return x.length ? "[" + x.map(go).join(item) + "]" : "[]";
    const keys = Object.keys(x as object).filter((k) => (x as Record<string, unknown>)[k] !== undefined);
    if (o.sortKeys) keys.sort(cmpStr);
    if (!keys.length) return "{}";
    return "{" + keys.map((k) => str(k, ascii) + kv + go((x as Record<string, unknown>)[k])).join(item) + "}";
  };
  return go(v);
}

/** The feed file's bytes: `json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"`. */
export const feedText = (v: unknown): string => dumps(v, { sortKeys: true, ensureAscii: false, compact: true }) + "\n";

/** json.loads, keeping a number written with "." or an exponent as a `PyFloat`. */
export function loads(text: string): PyJson {
  let i = 0;
  const ws = () => {
    while (i < text.length && " \t\n\r".includes(text[i]!)) i++;
  };
  const val = (): PyJson => {
    ws();
    const c = text[i];
    if (c === "{") {
      i++;
      const o: { [k: string]: PyJson } = {};
      ws();
      if (text[i] === "}") {
        i++;
        return o;
      }
      for (;;) {
        ws();
        const k = val();
        if (typeof k !== "string") throw new SyntaxError(`json: key at ${i}`);
        ws();
        if (text[i++] !== ":") throw new SyntaxError(`json: ':' at ${i}`);
        o[k] = val();
        ws();
        const d = text[i++];
        if (d === "}") return o;
        if (d !== ",") throw new SyntaxError(`json: ',' at ${i}`);
      }
    }
    if (c === "[") {
      i++;
      const a: PyJson[] = [];
      ws();
      if (text[i] === "]") {
        i++;
        return a;
      }
      for (;;) {
        a.push(val());
        ws();
        const d = text[i++];
        if (d === "]") return a;
        if (d !== ",") throw new SyntaxError(`json: ',' at ${i}`);
      }
    }
    if (c === '"') {
      const start = i;
      i++;
      while (text[i] !== '"') {
        if (text[i] === "\\") i++;
        i++;
        if (i >= text.length) throw new SyntaxError("json: unterminated string");
      }
      i++;
      return JSON.parse(text.slice(start, i)) as string;
    }
    const m = /^-?\d+(\.\d+)?([eE][+-]?\d+)?|^true|^false|^null|^NaN|^-?Infinity/.exec(text.slice(i, i + 400));
    if (!m) throw new SyntaxError(`json: value at ${i}`);
    i += m[0].length;
    const t = m[0];
    if (t === "true") return true;
    if (t === "false") return false;
    if (t === "null") return null;
    if (t === "NaN") return new PyFloat(NaN);
    if (t.endsWith("Infinity")) return new PyFloat(t.startsWith("-") ? -Infinity : Infinity);
    return m[1] || m[2] ? new PyFloat(Number(t)) : Number(t);
  };
  const out = val();
  ws();
  if (i !== text.length) throw new SyntaxError(`json: extra data at ${i}`);
  return out;
}
