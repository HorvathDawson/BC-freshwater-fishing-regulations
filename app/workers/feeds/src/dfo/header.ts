// T225 port of pipeline/regs/dfo_salmon/header.py: the region header as typed notes (parse_header, digest, resolve).
import type { EntryFile } from "./entries";
import { sha1, sha256 } from "./hash";
import { parseHtml, Str, Tag } from "./html";
import { dropNotes, key } from "./names";
import { contentSlice, RE_H1, RE_TABLE } from "./parse";
import { dumps } from "./pyjson";
import { findall, fullmatch, match, pyInt, pyRe, rstrip, search, split, strip, sub } from "./py";
import { dayIndex } from "./typed";

export type Note = Record<string, unknown> & { id?: string; k: string; t: string };

const SP: [string, string][] = [
  ["chinook", "CH"],
  ["coho", "CO"],
  ["sockeye", "SK"],
  ["pink", "PK"],
  ["chum", "CM"],
];
const SP_MAP = Object.fromEntries(SP);
const MON: Record<string, number> = Object.fromEntries(
  "jan feb mar apr may jun jul aug sep oct nov dec".split(" ").map((m, i) => [m, i + 1]),
);
const RE_STOP = pyRe(
  "<h[1-6][^>]*>\\s*(?:Related links|Salmon limits, openings and closures)|<(?:p|li)[^>]*>\\s*(?:This table refers to|Unless otherwise stated in the table|There is no fishing for salmon in [^<]* except for the opportunities)",
  "i",
);
const DATE = "(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\\.?\\s+(\\d{1,2})";
const RE_LEADIN = pyRe("\\s*You must be aware of these [^:]*:\\s*$", "i");
const RE_MUS = pyRe("Management Units \\d", "i");
const RE_WINDOW = pyRe(`\\bfrom\\s+${DATE}\\s+(?:to|-|–)\\s+${DATE}`, "i");
const RE_H1_ANY = pyRe("<h1\\b[^>]*>", "i");
const RE_WS = pyRe("\\s+");

const norm = (t: string): string =>
  strip(sub(RE_WS, " ", t.replace(/\xa0/g, " ").replace(/“/g, '"').replace(/”/g, '"').replace(/’/g, "'")));

function ownText(node: Tag): string {
  const bits: string[] = [];
  for (const c of node.children) {
    if (c instanceof Tag) {
      if (c.name === "ul" || c.name === "ol" || c.name === "li") continue;
      bits.push(c.getText(" ", true));
    } else bits.push((c as Str).data);
  }
  return norm(bits.join(" "));
}

function items(html: string): [string, string | null, boolean][] {
  if (!search(RE_H1, html)) {
    const h1 = search(RE_H1_ANY, html);
    if (h1) {
      const rest = html.slice(h1.index);
      const at = rest.indexOf(h1[0]);
      html = rest.slice(0, at) + '<h1 id="wb-cont">' + rest.slice(at + h1[0].length);
    }
  }
  const tm = search(RE_TABLE, html);
  let frag = contentSlice(html, tm ? tm.index : null);
  const stop = search(RE_STOP, frag);
  if (stop) frag = frag.slice(0, stop.index);
  const soup = parseHtml(frag);
  const out: [string, string | null, boolean][] = [];
  for (const node of soup.findAll(["li", "p"])) {
    if (node.name === "p" && node.findParent("li")) continue;
    const text = ownText(node);
    if (!text || [...text].length < 4 || search(RE_MUS, text)) continue;
    const a = node.find("a");
    if (a !== null && norm(a.getText(" ", true)) === text) continue;
    const parent = node.findParent("li");
    const lead = parent !== null ? ownText(parent) : null;
    out.push([text, lead, !!node.find(["ul", "ol"])]);
  }
  return out;
}

const SP_RE = SP.map(([name, code]) => [pyRe(`\\b${name}\\b`), code] as const);
const species = (text: string): string[] => {
  const low = text.toLowerCase();
  return SP_RE.filter(([re]) => search(re, low)).map(([, c]) => c);
};
const day = (mon: string, d: string): number => dayIndex(MON[mon.slice(0, 3).toLowerCase()]!, pyInt(d)) - 1;
const windows = (text: string): number[][] =>
  (findall(RE_WINDOW, text) as string[][]).map(([a, b, c, d]) => [day(a!, b!), day(c!, d!)]);

const RE_THE = pyRe("^the\\s+", "i");
const RE_PLURAL = pyRe("(.+?) and (.+?) (Rivers|Creeks|Lakes)");
const RE_NAME_SPLIT = pyRe(",?\\s+and\\s+the\\s+|;\\s*");
function names(phrase: string): string[] {
  let p = rstrip(strip(sub(RE_WINDOW, "", phrase)), ".,;: ");
  p = sub(RE_THE, "", p);
  const m = fullmatch(RE_PLURAL, p);
  if (m) {
    const kind = m[3]!.slice(0, -1);
    return [`${m[1]} ${kind}`, `${m[2]} ${kind}`];
  }
  return split(RE_NAME_SPLIT, p)
    .filter((x) => strip(x))
    .map((x) => sub(RE_THE, "", strip(x)));
}

const water = (name: string, ws = false) => ({ n: name, ws, key: key(name) + (ws ? "+ws" : "") });

const R = {
  adultLead: pyRe("adult (chinook|coho)", "i"),
  overCm: pyRe("(?:over|greater than)\\s+(\\d+)\\s*cm:?\\s*$", "i"),
  baitLead: pyRe("bait ban", "i"),
  except: pyRe("except:?\\s*(.+?)(?:,?\\s*unless otherwise described below)?[.:]?$", "i"),
  direction: pyRe("\\b(downstream|upstream|above|below|between)\\b", "i"),
  digit: pyRe("\\d"),
  closed: pyRe("no fishing for salmon in region .* until further notice"),
  facility: pyRe("within (\\d+)\\s*met(?:er|re)s of any government facility"),
  watershed: pyRe("(.+?) watershed:\\s*(.+)", "i"),
  limit: pyRe("limit"),
  isNum: pyRe("\\bis\\s+(?:one|two|three|four|\\w+)?\\s*\\(?(\\d+)\\)?|\\bis\\s+(\\d+)\\s+fish", "i"),
  riverEnd: pyRe("(River|Creek|Lake)$"),
  daily: pyRe("daily limit for all species of pacific salmon .*? is (\\d+)"),
  moreThan: pyRe("more than (\\d+) salmon"),
  annual: pyRe("(annual|monthly) limit (?:for |of )?(?:(\\d+) )?(\\w+)(?: salmon)? \\(?over (\\d+) ?cm\\.?\\)?(?: is (\\d+))?"),
  minSize: pyRe("all retained (.+?) must measure (\\d+)\\s*cm or more"),
  measured: pyRe("measurements are taken from"),
  cls: pyRe('"?(adult|jack)"? (chinook|coho)'),
  jack: pyRe("between (\\d+)\\s*-\\s*(\\d+)\\s*cm"),
  overCm2: pyRe("(?:over|greater than) (\\d+)\\s*cm"),
  place: pyRe("cm in the (.+?)\\.?$"),
  inRegion: pyRe("\\bin region\\b"),
  adultOrBait: pyRe("adult|bait ban", "i"),
  except2: pyRe("\\s+except\\b", "i"),
  leadOk: pyRe("adult (chinook|coho)|bait ban", "i"),
};

export function classify(text: string, lead: string | null = null): Note | null {
  const full = lead ? `${lead} ${text}` : text;
  const low = text.toLowerCase();
  if (lead && search(R.adultLead, lead)) {
    const m = search(R.overCm, lead);
    const sp = species(lead.split("defined")[0]!);
    const on = windows(text);
    return {
      k: "adult",
      t: full,
      sp,
      ...(m ? { cm: pyInt(m[1]!) } : {}),
      w: names(text).map((n) => water(n)),
      ...(on.length ? { on } : {}),
    };
  }
  if (lead && search(R.baitLead, lead)) {
    const m = search(R.except, text);
    const out: Note = { k: "bait_ban", t: full };
    if (m) {
      const parts = m[1]!
        .split(";")
        .map((x) => strip(x))
        .filter((x) => x);
      const whole = parts.filter((x) => !search(R.direction, x));
      out.x = whole.map((x) => water(x));
      out.xp = parts.filter((x) => !whole.includes(x));
    }
    return out;
  }
  if (text.endsWith(":") && !search(R.digit, text) && !low.includes("measured") && !low.includes("taken from")) return null;
  if (search(R.closed, low)) return { k: "closed", t: text };
  if (low.includes("daylight hours")) return { k: "daylight", t: text };
  let m = search(R.facility, low);
  if (m) return { k: "facility", t: text, m: pyInt(m[1]!) };
  m = match(R.watershed, text);
  if (m && search(R.limit, low)) {
    const n = search(R.isNum, m[2]!);
    const g = n ? [n[1], n[2]].find((x) => x) : undefined;
    const num = g !== undefined ? pyInt(g) : null;
    const river = strip(m[1]!);
    return {
      k: "aggregate",
      t: text,
      sp: species(m[2]!),
      ...(num !== null ? { n: num } : {}),
      ...(low.includes("hatchery") ? { hatchery: true } : {}),
      w: [water(search(R.riverEnd, river) ? river : `${river} River`, true)],
    };
  }
  m = search(R.daily, low);
  if (m) return { k: "daily", t: text, n: pyInt(m[1]!) };
  if (low.includes("possession limit")) {
    const mm = search(R.moreThan, low);
    return { k: "possession", t: text, ...(mm ? { n: pyInt(mm[1]!) } : {}) };
  }
  m = search(R.annual, low);
  if (m) {
    const n = m[2] || m[5];
    return { k: m[1]!, t: text, sp: species(m[3]!), cm: pyInt(m[4]!), ...(n ? { n: pyInt(n) } : {}) };
  }
  m = search(R.minSize, low);
  if (m) return { k: "min_size", t: text, sp: species(m[1]!), cm: pyInt(m[2]!) };
  if (search(R.measured, low)) return { k: "measure", t: text.split(" You must")[0]! };
  m = search(R.cls, low);
  if (m) {
    const cls = m[1]!;
    const sp = SP_MAP[m[2]!]!;
    if (cls === "jack") {
      const r = search(R.jack, low);
      return { k: "jack", t: text, sp: [sp], ...(r ? { cm: [pyInt(r[1]!), pyInt(r[2]!)] } : {}) };
    }
    const r = search(R.overCm2, low);
    const out: Note = { k: "adult", t: text, sp: [sp], ...(r ? { cm: pyInt(r[1]!) } : {}) };
    const place = search(R.place, text);
    if (place && !search(R.inRegion, low)) out.w = names(place[1]!).map((n) => water(n));
    return out;
  }
  if (low.includes("bait ban")) return { k: "bait_ban", t: text };
  return { k: "other", t: text };
}

export function parseHeader(html: string, slug: string): Note[] {
  const notes: Note[] = [];
  const seen = new Set<string>();
  const its = items(html);
  for (const [raw, lead, hasKids] of its) {
    const text = strip(sub(RE_LEADIN, "", raw));
    if (!text) continue;
    let n: Note | null;
    if (hasKids && !search(R.adultOrBait, text)) n = classify(text);
    else if (hasKids) {
      const head = split(R.except2, text, 1)[0]!;
      n = text.toLowerCase().includes("adult") ? classify(head) : null;
      if (n) {
        const kids = its.filter(([, l]) => l === raw).map(([t]) => rstrip(t, ". "));
        n.t = kids.length ? `${rstrip(text, ":")}: ${kids.join("; ")}.` : rstrip(text, ":");
      }
    } else n = classify(text, lead && search(R.leadOk, lead) ? lead : null);
    if (!n) continue;
    const h = sha1(n.t).slice(0, 8);
    const withId: Note = { id: `${slug}:${n.k}:${h}`, ...n };
    if (!seen.has(withId.id!)) {
      seen.add(withId.id!);
      notes.push(withId);
    }
  }
  return notes;
}

/** sha256 of json.dumps(notes, sort_keys=True, ensure_ascii=False) — default separators */
export const digest = (notes: Note[]): string => sha256(dumps(notes, { sortKeys: true, ensureAscii: false }));

// ── names → items ────────────────────────────────────────────────────────────────────────────────────────────────────
function bindings(ef: EntryFile) {
  const exact = new Map<string, string[][]>();
  const loose = new Map<string, string[][]>();
  const add = (m: Map<string, string[][]>, k: string, v: string[]) => {
    const l = m.get(k);
    if (l) l.push(v);
    else m.set(k, [v]);
  };
  for (const w of ef.waters) {
    if (!w.item_ids.length) continue;
    for (const n of [w.name, ...w.aliases]) {
      const ids = [...w.item_ids].sort();
      add(exact, key(n), ids);
      add(loose, key(dropNotes(n)), ids);
    }
  }
  return { exact, loose };
}

function itemsOf(name: string, b: ReturnType<typeof bindings>): string[] | null {
  const k = key(name);
  for (const table of [b.exact, b.loose]) {
    const hits = new Map<string, string[]>();
    for (const x of table.get(k) ?? []) hits.set(JSON.stringify(x), x);
    if (hits.size === 1) return [...hits.values()][0]!;
    if (hits.size > 1) return null;
  }
  return null;
}

const RE_SLASH = pyRe("(.+?)/(.+?) (River|Creek)");

/** header.resolve without `upstream` (the live run passes none): (scopes, unresolved) */
export function resolve(notes: Note[], ef: EntryFile): [Record<string, string[]>, { note: string; name: string }[]] {
  const b = bindings(ef);
  const scopes: Record<string, string[]> = {};
  const unresolved: { note: string; name: string }[] = [];
  for (const n of notes)
    for (const w of [...((n.w as { n: string; key: string }[]) || []), ...((n.x as { n: string; key: string }[]) || [])]) {
      if (w.key in scopes) continue;
      let its = itemsOf(w.n, b);
      if (its === null && w.n.includes("/")) {
        const m = fullmatch(RE_SLASH, w.n);
        if (m) {
          const parts = [itemsOf(`${m[1]} ${m[3]}`, b), itemsOf(`${m[2]} ${m[3]}`, b)];
          if (parts.every((p) => p && p.length)) its = [...new Set(parts.flatMap((p) => p!))].sort();
        }
      }
      if (its === null) {
        unresolved.push({ note: n.id!, name: w.n });
        continue;
      }
      scopes[w.key] = its;
    }
  return [scopes, unresolved];
}
