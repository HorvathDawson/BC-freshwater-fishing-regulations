// T225 port of pipeline/regs/dfo_salmon/untangle.py (untangle + verify, and the scope reading locations needs).
import type { ParsedRegion, RegRow, Section } from "./parse";
import { cmpTuple, match, PyError, pyRe, rstrip, search, strip, sub } from "./py";

const RE_WS = pyRe("\\s+");
const RE_NAME_NOTE = pyRe("\\s*(?:—\\s*)?\\bNote:\\s*(?P<note>.+)$", "s");
const RE_TRIBS = pyRe("\\(?\\b(?P<verb>in|ex)cluding\\s+t\\w*ributaries\\b\\)?", "i");
const RE_ALIAS_BRACKET = pyRe("\\[\\s*(?:also known as\\s*)?(?P<alias>[^\\]]+?)\\s*\\]", "i");
const RE_ALIAS_PAREN = pyRe("\\(\\s*(?P<alias>[A-Z][\\w' ]*?)\\s*\\)");
const RE_PAREN_SCOPE = pyRe("\\(\\s*(?P<scope>(?:up|down)stream[^)]*|above[^)]*|below[^)]*)\\)", "i");

const ANCHOR_TYPES: [string, ReturnType<typeof pyRe>][] = [
  ["latlon", pyRe("\\d+\\s*°|\\d+\\.\\d+['′]\\s*[NW]")],
  ["tidal_boundary", pyRe("\\btidal\\s+(?:water\\s+)?boundary\\b", "i")],
  ["confluence", pyRe("\\bconfluence\\b|\\bjunction\\b|\\bmouth of\\b|\\bwhere .{0,40} enters\\b", "i")],
  ["lake_end", pyRe("\\b(?:outlet|inlet)\\b", "i")],
  ["dam_or_hatchery", pyRe("\\bdams?\\b|\\bweirs?\\b|\\bfishway\\b|\\bhatchery\\b|\\btailrace\\b|\\bfish (?:counting )?fence\\b|\\bpower station\\b", "i")],
  ["natural_feature", pyRe("\\b(?:falls|canyon|pool|rapids)\\b(?!\\s+(?:\\w+\\s+)?(?:regional|provincial)?\\s*park\\b)", "i")],
  ["bridge", pyRe("\\bbridges?\\b|\\btrestle\\b|\\boverpass\\b", "i")],
  ["road", pyRe("\\b(?:highway|hwy|road|street|avenue)\\b", "i")],
  ["powerline", pyRe("\\bpower\\s?lines?\\b", "i")],
  ["place_name", pyRe("\\bparks?\\b|\\blodge\\b|\\bresort\\b|\\bcampground\\b", "i")],
  ["boundary_sign", pyRe("\\bsigns?\\b", "i")],
];
const RE_FROM_TO_TEXT = pyRe("\\bfrom\\b.+?\\bto\\b|\\b(?:up|down)stream\\s+(?:of|from)\\b.+?\\bto\\b", "is");
const RE_UPSTREAM_TEXT = pyRe("\\b(?:upstream|above)\\b", "i");
const RE_DOWNSTREAM_TEXT = pyRe("\\b(?:downstream|below)\\b", "i");
const RE_NAMED_STREAM = pyRe("\\b([A-Z][\\w'’]*(?:\\s+[A-Z][\\w'’]*){0,3}\\s+(?:River|Creek|Cr|Ck|Lake|Slough))\\b");
const RE_BETWEEN_AND = pyRe("\\bbetween\\b.+?\\band\\b", "is");
const RE_TRAILING_CLAUSE = pyRe("\\b(?:note:|except\\b|unless\\b)", "i");
const RE_WITHIN_SIGNS = pyRe("\\bwithin\\b.*\\bsigns?\\b");
const RE_ALL_TRIBS = pyRe("\\ball\\s+tributaries\\b");
const RE_WATERS_WITHIN = pyRe("\\bwaters within\\b.*\\bsigns?\\b");
const RE_EXCL_TRIBS_ONLY = pyRe("^\\s*excluding\\s+tributaries\\s*$");
const RE_NAMED_TRIBS = pyRe("^[A-Z][\\w' ]+(?:Creek|River|Lake)\\b");
const RE_R_DOT = pyRe("\\bR\\.(?=\\s|$)");
const RE_CK_DOT = pyRe("\\b(?:Ck|Cr)\\.(?=\\s|$)");

export const norm = (t: string): string => strip(sub(RE_WS, " ", (t || "").replace(/\xa0/g, " ")));

export function anchorTypes(scope: string): string[] {
  const text = norm(scope);
  if (!text) return [];
  const found: [number, string][] = [];
  for (const [name, pat] of ANCHOR_TYPES) {
    const m = search(pat, text);
    if (m) found.push([m.index, name]);
  }
  if (!found.some(([, n]) => n === "confluence")) {
    const m = search(RE_NAMED_STREAM, text);
    if (m) found.push([m.index, "confluence"]);
  }
  found.sort((a, b) => cmpTuple(a, b));
  return found.map(([, n]) => n);
}

export interface Rule {
  species: string;
  dates: string;
  limits_gear: string;
  fishery_notices: { text: string; href: string }[];
  source: string;
  row_index: number;
}
const ruleFromRow = (r: RegRow): Rule => ({
  species: r.species || "All",
  dates: r.dates,
  limits_gear: r.limits_gear,
  fishery_notices: r.fishery_notices,
  source: r.source,
  row_index: r.row_index,
});

export interface Reach {
  index: number;
  scope: string;
  kind: string;
  anchor_types: string[];
  excludes: string[];
  mainstem_only: boolean;
  tributaries: boolean | null;
  rules: Rule[];
}

export interface Water {
  name: string;
  region: string;
  section: string | null;
  aliases: string[];
  name_note: string | null;
  tributaries: boolean | null;
  reaches: Reach[];
  inherits: string[];
}

export interface ScopeDefault {
  key: string;
  kind: string;
  scope: string;
  areas: number[];
  falls_back_to: string[];
  rules: Rule[];
}

export interface Untangled {
  region: string;
  region_number: number;
  region_name: string;
  url: string;
  date_modified: string | null;
  preamble: string[];
  defaults: ScopeDefault[];
  waters: Water[];
  sections: Section[];
  cross_references: { from: string; to: string; section: string }[];
}

type NameParts = [string, string[], string | null, boolean | null, string | null];

export function splitName(raw: string): NameParts {
  let text = norm(raw);
  let note: string | null = null;
  let m = search(RE_NAME_NOTE, text);
  if (m) {
    note = norm(m.groups!.note!);
    text = strip(text.slice(0, m.index));
  }
  let tribs: boolean | null = null;
  m = search(RE_TRIBS, text);
  if (m) {
    tribs = m.groups!.verb!.toLowerCase() === "in";
    text = strip(text.slice(0, m.index) + " " + text.slice(m.index + m[0].length));
  }
  let embedded: string | null = null;
  m = search(RE_PAREN_SCOPE, text);
  if (m) {
    embedded = norm(m.groups!.scope!);
    text = strip(text.slice(0, m.index) + " " + text.slice(m.index + m[0].length));
  }
  const aliases: string[] = [];
  m = search(RE_ALIAS_BRACKET, text);
  if (m) {
    aliases.push(norm(m.groups!.alias!));
    text = strip(text.slice(0, m.index) + " " + text.slice(m.index + m[0].length));
  }
  m = search(RE_ALIAS_PAREN, text);
  if (m) {
    const aliasWord = norm(m.groups!.alias!);
    const rest = norm(text.slice(0, m.index) + " " + text.slice(m.index + m[0].length));
    const tail = norm(text.slice(m.index + m[0].length));
    aliases.push(tail ? norm(`${aliasWord} ${tail}`) : aliasWord);
    text = rest;
  }
  text = sub(RE_R_DOT, "River", text);
  text = sub(RE_CK_DOT, "Creek", text);
  text = norm(rstrip(text, " .,-–—"));
  return [text, aliases, note, tribs, embedded];
}

export function classifyScope(scope: string): [string, string[], boolean, boolean | null] {
  const text = norm(scope);
  const mc = search(RE_TRAILING_CLAUSE, text);
  let head = mc ? strip(text.slice(0, mc.index)) : text;
  if (!head) head = text;
  const low = head.toLowerCase();
  let tribs: boolean | null = null;
  const m = search(RE_TRIBS, text);
  if (m) tribs = m.groups!.verb!.toLowerCase() === "in";
  const mainstem = low.includes("mainstem");
  if (!head || low === "all" || low === "all waters") return ["whole_water", [], mainstem, tribs];
  const types = anchorTypes(head);
  let kind: string;
  if (search(RE_BETWEEN_AND, head) && !search(RE_WITHIN_SIGNS, low)) kind = "between";
  else if (search(RE_ALL_TRIBS, low)) kind = "tributary_set";
  else if (search(RE_WATERS_WITHIN, low)) kind = "sign_bounded_zone";
  else if (low.includes("excluding") && !search(RE_EXCL_TRIBS_ONLY, low)) kind = "whole_water_excluding";
  else if (search(RE_FROM_TO_TEXT, head)) kind = "between";
  else if (search(RE_UPSTREAM_TEXT, head)) kind = "upstream_of";
  else if (search(RE_DOWNSTREAM_TEXT, head)) kind = "downstream_of";
  else if (tribs !== null && !types.length) kind = "tributaries_only";
  else if (match(RE_NAMED_TRIBS, text)) kind = "named_tributaries";
  else kind = "described";
  return [kind, types, mainstem, tribs];
}

function inheritanceChain(sectionKey: string | null, parsed: ParsedRegion): string[] {
  if (!sectionKey) return [];
  const chain = [sectionKey];
  const parent = sectionKey.split("(")[0]!;
  if (parent !== sectionKey) chain.push(parent);
  const byKey = new Map<string, Section>();
  for (const s of parsed.sections) byKey.set(s.key, s);
  if (chain.some((k) => byKey.get(k)?.falls_back_to_a) && byKey.has("A")) chain.push("A");
  return chain;
}

export function untangle(parsed: ParsedRegion): Untangled {
  const defaults: ScopeDefault[] = [];
  const regionRows = parsed.rows.filter((r) => r.precedence === 0);
  if (regionRows.length)
    defaults.push({
      key: regionRows[0]!.section_key || "region",
      kind: "region",
      scope: norm(regionRows[0]!.waters),
      areas: [],
      falls_back_to: [],
      rules: regionRows.map(ruleFromRow),
    });

  const catchalls = new Map<string, { key: [string | null, number, string, number[]]; rows: RegRow[] }>();
  for (const r of parsed.rows) {
    if (r.precedence !== 1 && r.precedence !== 2) continue;
    let label = norm(r.waters);
    if (!label || label.toLowerCase() === "all") label = norm(r.specific_area) || label;
    const k = JSON.stringify([r.section_key, r.precedence, label, r.areas]);
    const got = catchalls.get(k);
    if (got) got.rows.push(r);
    else catchalls.set(k, { key: [r.section_key, r.precedence, label, [...r.areas]], rows: [r] });
  }
  for (const { key: [sec, rank, scope, areas], rows } of catchalls.values()) {
    const bannerOnly = rows.every((x) => x.source === "section_banner");
    const chain = inheritanceChain(sec, parsed);
    defaults.push({
      key: sec || "region",
      kind: bannerOnly ? "closure" : rank === 2 ? "area" : "section",
      scope,
      areas: [...areas],
      falls_back_to: rank === 2 ? chain : chain.slice(1),
      rules: rows.map(ruleFromRow),
    });
  }

  const grouped = new Map<string, { sec: string | null; name: string; entries: [RegRow, NameParts][] }>();
  for (const r of parsed.rows) {
    if (r.precedence !== 3) continue;
    const parts = splitName(r.waters);
    const k = JSON.stringify([r.section_key, parts[0]]);
    const got = grouped.get(k);
    if (got) got.entries.push([r, parts]);
    else grouped.set(k, { sec: r.section_key, name: parts[0], entries: [[r, parts]] });
  }

  const waters: Water[] = [];
  for (const { sec, name, entries } of grouped.values()) {
    const first = entries[0]![1];
    const aliases = first[1];
    let note = first[2];
    let nameTribs = first[3];
    for (const [, parts] of entries.slice(1)) {
      for (const a of parts[1]) if (!aliases.includes(a)) aliases.push(a);
      note = note || parts[2];
      nameTribs = nameTribs !== null ? nameTribs : parts[3];
    }
    const water: Water = {
      name,
      region: parsed.region,
      section: sec,
      aliases,
      name_note: note,
      tributaries: nameTribs,
      reaches: [],
      inherits: inheritanceChain(sec, parsed),
    };
    const byScope = new Map<string, Reach>();
    for (const [r, parts] of entries) {
      const embedded = parts[4];
      const scope = norm(r.specific_area);
      const full = embedded && scope ? norm(`${embedded}; ${scope}`) : scope || embedded || "";
      if (!byScope.has(full)) {
        const [kind, types, mainstem, tribs] = classifyScope(full);
        byScope.set(full, {
          index: byScope.size,
          scope: full,
          kind,
          anchor_types: types,
          excludes: [...r.specific_area_bullets],
          mainstem_only: mainstem,
          tributaries: tribs !== null ? tribs : nameTribs,
          rules: [],
        });
      }
      const reach = byScope.get(full)!;
      for (const b of r.specific_area_bullets) if (!reach.excludes.includes(b)) reach.excludes.push(b);
      reach.rules.push(ruleFromRow(r));
    }
    water.reaches = [...byScope.values()];
    waters.push(water);
  }
  // sort by (str(section), name): str(None) is "None"
  waters.sort((a, b) => cmpTuple([a.section === null ? "None" : a.section, a.name], [b.section === null ? "None" : b.section, b.name]));

  return {
    region: parsed.region,
    region_number: parsed.region_number,
    region_name: parsed.region_name,
    url: parsed.url,
    date_modified: parsed.date_modified,
    preamble: parsed.preamble,
    defaults,
    waters,
    sections: [...parsed.sections],
    cross_references: parsed.cross_references,
  };
}

/** Every source row lands in exactly one rule; AssertionError (as the Python's assert) if not. */
export function verify(parsed: ParsedRegion, out: Untangled): void {
  const seen: number[] = [];
  for (const d of out.defaults) for (const r of d.rules) seen.push(r.row_index);
  for (const w of out.waters) for (const reach of w.reaches) for (const r of reach.rules) seen.push(r.row_index);
  const expected = parsed.rows.map((r) => r.row_index).sort((a, b) => a - b);
  const got = [...seen].sort((a, b) => a - b);
  if (got.length === expected.length && got.every((v, i) => v === expected[i])) return;
  const gotSet = new Set(got);
  const missing = [...new Set(expected)].filter((i) => !gotSet.has(i)).sort((a, b) => a - b).slice(0, 8);
  const dup = [...new Set(got.filter((i) => got.indexOf(i) !== got.lastIndexOf(i)))].sort((a, b) => a - b).slice(0, 8);
  throw new PyError(
    "AssertionError",
    `region ${parsed.region}: ${got.length} rules from ${expected.length} rows; missing=[${missing.join(", ")}] duplicated=[${dup.join(", ")}]`,
  );
}
