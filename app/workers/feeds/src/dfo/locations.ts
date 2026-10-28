// T225 port of pipeline/regs/dfo_salmon/locations.py: normalize / landmarks / fingerprint, and `extract`.
import { sha256 } from "./hash";
import { cmpStr, finditer, pyRe, strip, sub } from "./py";
import { classifyScope, type Untangled } from "./untangle";

const SYNONYMS: [ReturnType<typeof pyRe>, string][] = [
  [pyRe("\\bhwy\\.?\\b"), "highway"],
  [pyRe("#\\s*(\\d)"), "\\1"],
  [pyRe("\\bmetres?\\b|\\bmeters?\\b"), "m"],
  [pyRe("\\bkilometres?\\b|\\bkilometers?\\b"), "km"],
  [pyRe("\\bapprox\\.?\\b|\\bapproximately\\b"), "approx"],
  [pyRe("\\brd\\.?\\b"), "road"],
  [pyRe("\\bck\\.?\\b|\\bcr\\.?\\b"), "creek"],
  [pyRe("\\br\\.(?=\\s|$)"), "river"],
  [pyRe("\\s*&\\s*"), " and "],
];
const STOPWORDS = new Set([
  "the", "a", "of", "at", "on", "in", "to", "from", "and", "located", "approx", "fishing", "boundary", "sign", "signs",
  "white", "triangular", "waters", "water", "mainstem", "m", "km", "upstream", "downstream", "above", "below",
  "between", "river", "creek", "lake", "bridge", "including", "excluding", "tributaries",
]);
const RE_NONALNUM = pyRe("[^a-z0-9 ]+");
const RE_WS = pyRe("\\s+");

export function normalize(text: string): string {
  let t = (text || "").toLowerCase().replace(/’/g, "'").replace(/–/g, "-").replace(/—/g, "-");
  for (const [pat, rep] of SYNONYMS) t = sub(pat, rep, t);
  return strip(sub(RE_WS, " ", sub(RE_NONALNUM, " ", t)));
}

export function landmarks(text: string): string[] {
  const words = normalize(text).split(" ").filter((w) => w);
  return [...new Set(words.filter((w) => !STOPWORDS.has(w) && w.length > 2))].sort(cmpStr);
}

export function fingerprint(section: string | null, water: string, specificArea: string): string {
  return sha256(`${section || ""}|${normalize(water)}|${normalize(specificArea)}`).slice(0, 16);
}

export interface Location {
  fingerprint: string;
  region: string;
  region_number: number;
  kind: string;
  precedence: number;
  section: string | null;
  inherits: string[];
  water: string;
  aliases: string[];
  specific_area: string;
  op: string;
  anchor_types: string[];
  tributaries: boolean | null;
  excludes: string[];
  areas: number[];
  landmarks: string[];
  notes: string[];
  narrows_extent: boolean;
}

export interface RuleRecord {
  fingerprint: string;
  species: string;
  dates: string;
  limits_gear: string;
  fishery_notices: { text: string; href: string }[];
  source: string;
  order: number;
}

export interface Structure {
  sections: string[];
  n_locations: number;
  n_waters: number;
  n_defaults: number;
  precedences: string[];
}

export function structureSignature(u: Untangled): Structure {
  return {
    sections: u.sections.map((s) => s.key),
    n_locations: u.waters.length + u.defaults.length,
    n_waters: u.waters.length,
    n_defaults: u.defaults.length,
    precedences: [...new Set(u.defaults.map((d) => d.kind))].sort(cmpStr),
  };
}

const NOTE_PATTERNS: [ReturnType<typeof pyRe>, boolean][] = [
  [pyRe("within\\s+a?\\s*\\d+\\s*m\\w*\\s+radius[^.:]*", "i"), true],
  [pyRe("(?:also\\s+)?closed\\s+(?:east|west|north|south)\\w*\\s+of\\s+a\\s+line[^.]*", "i"), true],
  [pyRe("Note:\\s*[^.]*\\.", "i"), false],
];

function notesFor(text: string): [string[], boolean] {
  let out: string[] = [];
  let narrows = false;
  for (const [pat, spatial] of NOTE_PATTERNS)
    for (const m of finditer(pat, text || "")) {
      const frag = strip(sub(RE_WS, " ", m[0]), " .;,");
      if (!frag || out.some((e) => e.includes(frag))) continue;
      out = out.filter((e) => !frag.includes(e));
      out.push(frag);
      narrows = narrows || spatial;
    }
  return [out, narrows];
}

const KIND_MAP: Record<string, string> = {
  region: "region_default",
  section: "section_default",
  area: "area_default",
  closure: "closure",
};
const PRECEDENCE: Record<string, number> = { region: 0, section: 1, area: 2 };

export function extract(u: Untangled): [Location[], RuleRecord[], Structure] {
  const locations: Location[] = [];
  const rules: RuleRecord[] = [];
  const seen = new Set<string>();
  let order = 0;
  const add = (loc: Location, ruleRows: { species: string; dates: string; limits_gear: string; fishery_notices: RuleRecord["fishery_notices"]; source: string }[]) => {
    if (!seen.has(loc.fingerprint)) {
      seen.add(loc.fingerprint);
      locations.push(loc);
    }
    for (const r of ruleRows) {
      rules.push({
        fingerprint: loc.fingerprint,
        species: r.species || "All",
        dates: r.dates,
        limits_gear: r.limits_gear,
        fishery_notices: r.fishery_notices,
        source: r.source,
        order,
      });
      order += 1;
    }
  };

  for (const d of u.defaults) {
    const [op, aTypes, , tribs] = classifyScope(d.scope);
    const [notes, narrows] = notesFor(d.scope);
    add(
      {
        fingerprint: fingerprint(d.key, "", d.scope),
        region: u.region,
        region_number: u.region_number,
        kind: KIND_MAP[d.kind] ?? d.kind,
        precedence: PRECEDENCE[d.kind] ?? 1,
        section: d.key,
        inherits: [...d.falls_back_to],
        water: "",
        aliases: [],
        specific_area: d.scope,
        op,
        anchor_types: aTypes,
        tributaries: tribs,
        excludes: [],
        areas: [...d.areas],
        landmarks: landmarks(d.scope),
        notes,
        narrows_extent: narrows,
      },
      d.rules,
    );
  }
  for (const w of u.waters)
    for (const reach of w.reaches) {
      const [op, aTypes, , tribs] = classifyScope(reach.scope);
      const [notes, narrows] = notesFor(reach.scope);
      void tribs;
      add(
        {
          fingerprint: fingerprint(w.section, w.name, reach.scope),
          region: u.region,
          region_number: u.region_number,
          kind: "water",
          precedence: 3,
          section: w.section,
          inherits: [...w.inherits],
          water: w.name,
          aliases: [...w.aliases],
          specific_area: reach.scope,
          op,
          anchor_types: aTypes,
          tributaries: reach.tributaries !== null ? reach.tributaries : w.tributaries,
          excludes: [...reach.excludes],
          areas: [],
          landmarks: landmarks(`${w.name} ${reach.scope}`),
          notes: [...notes, ...(w.name_note ? [w.name_note] : [])],
          narrows_extent: narrows,
        },
        reach.rules,
      );
    }
  return [locations, rules, structureSignature(u)];
}
