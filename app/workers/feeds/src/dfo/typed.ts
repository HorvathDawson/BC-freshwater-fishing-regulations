// T225 port of pipeline/regs/dfo_salmon/typed.py (+ dates.py, and the slice of pipeline.regs.parsing.catalogue a
// DFO row can reach): one scraped row → its typed rules, as `CatalogueRule.model_dump(mode="json", exclude_defaults=
// True, by_alias=True)` would write them.
//
// THE VALIDATORS ARE PORTED TOO, as far as a DFO row can reach them: a rule the catalogue refuses raises, and a raise
// HOLDS THE WHOLE REGION ("the page did not parse (ValidationError: ...)") — that sentence lands in index.json, so the
// pydantic message is reproduced (its first 120 characters are what the feed keeps).
import { getCloseMatches } from "./difflib";
import { PyFloat } from "./pyjson";
import { capitalize, floatRepr, fullmatch, PyError, pyInt, pyRe, repr, rstrip, search, split, splitWs, strip, sub, translate } from "./py";

// ── the calendar (pipeline.common.calendar_spec) ─────────────────────────────────────────────────────────────────────
export const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
export const LAST_DAY = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const DAYS_BEFORE = LAST_DAY.map((_, i) => LAST_DAY.slice(0, i).reduce((a, b) => a + b, 0));
export const dayIndex = (month: number, day: number): number => {
  if (month < 1 || month > 12) throw new PyError("ValueError", `calendar: month ${month} is not 1..12`);
  return DAYS_BEFORE[month - 1]! + day;
};
export const YEAR = 366;
export function monthDay(day: number): [number, number] {
  let m = 1;
  while (day > LAST_DAY[m - 1]!) {
    day -= LAST_DAY[m - 1]!;
    m += 1;
  }
  return [m, day];
}

// ── pydantic's error text (what `str(ValidationError)` prints) ──────────────────────────────────────────────────────
const PYD = "https://errors.pydantic.dev/2.13/v/";
const truncRepr = (s: string): string => (s.length > 50 ? `${s.slice(0, 25)}...${s.slice(-24)}` : s);
interface LineErr {
  loc: string | null;
  msg: string;
  type: string;
  input: string;
  inputType: string;
}
function validationError(model: string, errs: LineErr[]): PyError {
  const head = `${errs.length} validation error${errs.length > 1 ? "s" : ""} for ${model}`;
  const body = errs
    .map(
      (e) =>
        `${e.loc !== null ? e.loc + "\n" : ""}  ${e.msg} [type=${e.type}, input_value=${truncRepr(e.input)}, input_type=${e.inputType}]\n    For further information visit ${PYD}${e.type}`,
    )
    .join("\n");
  return new PyError("ValidationError", `${head}\n${body}`);
}
const valueError = (model: string, msg: string, input: string, loc: string | null = null): PyError =>
  validationError(model, [{ loc, msg: `Value error, ${msg}`, type: "value_error", input, inputType: "dict" }]);

// ── DateRange / Clock ────────────────────────────────────────────────────────────────────────────────────────────────
export interface DateRange {
  from_month: number;
  from_day: number;
  to_month: number;
  to_day: number;
}

function dateRange(fm: number, fd: number, tm: number, td: number): DateRange {
  const errs: LineErr[] = [];
  const field = (name: string, v: number, lo: number, hi: number) => {
    if (v < lo) errs.push({ loc: name, msg: `Input should be greater than or equal to ${lo}`, type: "greater_than_equal", input: String(v), inputType: "int" });
    else if (v > hi) errs.push({ loc: name, msg: `Input should be less than or equal to ${hi}`, type: "less_than_equal", input: String(v), inputType: "int" });
  };
  field("from_month", fm, 1, 12);
  field("from_day", fd, 1, 31);
  field("to_month", tm, 1, 12);
  field("to_day", td, 1, 31);
  if (errs.length) throw validationError("DateRange", errs);
  for (const [m, d, side] of [
    [fm, fd, "from"],
    [tm, td, "to"],
  ] as const)
    if (d > LAST_DAY[m - 1]!)
      throw valueError(
        "DateRange",
        `${side}: day ${d} does not exist in month ${m}`,
        `{'from_month': ${fm}, 'from_day': ${fd}, 'to_month': ${tm}, 'to_day': ${td}}`,
      );
  return { from_month: fm, from_day: fd, to_month: tm, to_day: td };
}

const CLOCK_AT = /^([01]\p{Nd}|2[0-3]):[0-5]\p{Nd}$/u;
interface Clock {
  at: string;
}
function clock(at: string): Clock {
  if (!CLOCK_AT.test(at))
    throw validationError("Clock", [
      { loc: "at", msg: "String should match pattern '^([01]\\d|2[0-3]):[0-5]\\d$'", type: "string_pattern_mismatch", input: repr(at), inputType: "str" },
    ]);
  return { at };
}

const RE_CLOCK = pyRe("(\\d{1,2}):(\\d{2})");
const RE_SOLAR = pyRe("(?:(one|an|\\d+)\\s*(hour|hours|minute|minutes|min)\\s*)?(before|after)?\\s*(sunrise|sunset)");

/** catalogue.parse_clock — a DFO time is always "HH:MM"; a solar time is not a `Clock` the feed can carry here */
function parseClock(text: string): Clock | { solar: string; offset_min?: number } | null {
  const t = splitWs(text || "").join(" ").toLowerCase().replace(/ hours/g, "");
  if (!t) return null;
  let m = fullmatch(RE_CLOCK, t);
  if (m) return clock(`${String(pyInt(m[1]!)).padStart(2, "0")}:${m[2]}`);
  m = fullmatch(RE_SOLAR, t);
  if (!m) return null;
  const [, n, unit, side, ev] = m;
  let mins = 0;
  if (n) {
    const v = n === "one" || n === "an" ? 1 : pyInt(n);
    mins = v * (unit!.startsWith("hour") ? 60 : 1);
    if (side === "before") mins = -mins;
  }
  return mins ? { solar: ev!, offset_min: mins } : { solar: ev! };
}

const MONTH_NO: Record<string, number> = Object.fromEntries(MONTHS.map((m, i) => [m.toLowerCase(), i + 1]));
function month(name: string): number | null {
  const n = strip(name).toLowerCase();
  return MONTH_NO[n.startsWith("sept") ? "sep" : n.slice(0, 3)] ?? null;
}
const RE_POINT_DAY = pyRe("([A-Za-z]+)\\.?\\s*(\\d{1,2})");
const RE_POINT_MONTH = pyRe("([A-Za-z]+)\\.?");
function point(text: string): [number, number | null] | null {
  const t = rstrip(strip(text), ","); // .strip().rstrip(",")
  let m = fullmatch(RE_POINT_DAY, t);
  if (m) {
    const mo = month(m[1]!);
    return mo ? [mo, pyInt(m[2]!)] : null;
  }
  m = fullmatch(RE_POINT_MONTH, t);
  if (m) {
    const mo = month(m[1]!);
    return mo ? [mo, null] : null;
  }
  return null;
}
const RE_RANGE_SEP = pyRe("\\s*(?:-|\\u2013|\\u2014|\\bto\\b)\\s*");
const RE_BARE_DAY2 = pyRe("(\\d{1,2})");

/** catalogue.parse_date_range */
export function parseDateRange(text: string): DateRange | null {
  const t = splitWs(text || "").join(" ");
  if (!t) return null;
  const parts = split(RE_RANGE_SEP, t, 1);
  const lo = point(parts[0]!);
  if (!lo) return null;
  if (parts.length === 1) {
    if (lo[1] !== null) return null;
    return dateRange(lo[0], 1, lo[0], LAST_DAY[lo[0] - 1]!);
  }
  let hi = point(parts[1]!);
  if (!hi) {
    const m = fullmatch(RE_BARE_DAY2, strip(parts[1]!));
    if (!m || lo[1] === null) return null;
    hi = [lo[0], pyInt(m[1]!)];
  }
  if (lo[1] === null || hi[1] === null) return null;
  return dateRange(lo[0], lo[1], hi[0], hi[1]);
}

// ── dates.py: parse_date_window ──────────────────────────────────────────────────────────────────────────────────────
const DW_MONTHS: Record<string, number> = {
  jan: 1, january: 1, feb: 2, february: 2, mar: 3, march: 3, apr: 4, april: 4, may: 5, jun: 6, june: 6, jul: 7,
  july: 7, aug: 8, august: 8, sep: 9, sept: 9, september: 9, oct: 10, october: 10, nov: 11, november: 11, dec: 12,
  december: 12,
};
const RE_SEP = pyRe("\\s*(?:-|–|—|to|through|thru|until)\\s*", "i");
const RE_DATE = pyRe("^([A-Za-z]+)\\.?\\s+(\\d{1,2})$");
const RE_BARE_DAY = pyRe("^(\\d{1,2})$");
function dwOne(token: string): [number, number] | null {
  const m = search(RE_DATE, strip(token));
  if (!m) return null;
  const mon = DW_MONTHS[m[1]!.toLowerCase()];
  const day = pyInt(m[2]!);
  if (mon === undefined || !(day >= 1 && day <= LAST_DAY[mon - 1]!)) return null;
  return [mon, day];
}
function parseDateWindow(text: string): boolean {
  if (!text || !strip(text)) return false;
  text = strip(text.replace(/\*/g, ""));
  const parts = split(RE_SEP, text, 1);
  if (parts.length === 1) return dwOne(parts[0]!) !== null;
  const a = dwOne(parts[0]!);
  if (!a) return false;
  if (dwOne(parts[1]!)) return true;
  const m = search(RE_BARE_DAY, strip(parts[1]!));
  if (m) {
    const day = pyInt(m[1]!);
    return day >= 1 && day <= LAST_DAY[a[0] - 1]!;
  }
  return false;
}

// ── typed.py ─────────────────────────────────────────────────────────────────────────────────────────────────────────
const SPECIES: Record<string, string[]> = {
  all: ["SA"],
  chinook: ["CH"],
  coho: ["CO"],
  sockeye: ["SK"],
  pink: ["PK"],
  chum: ["CM"],
  "sockeye, pink and chum": ["SK", "PK", "CM"],
  "sockeye, pink, chum": ["SK", "PK", "CM"],
  eulachon: ["EU"],
};
const RE_ONLY_OVER = pyRe("only\\s+(\\d+)\\s+(?:of\\s+which\\s+may\\s+be\\s+)?(?:over|greater\\s+than|more\\s+than)\\s+(\\d+)\\s*cm", "i");
const RE_WHICH_MAY = pyRe("(\\d+)\\s+of\\s+which\\s+may\\s+be\\s+(?:more\\s+than|greater\\s+than|over)\\s+(\\d+)\\s*cm", "i");
const RE_NONE_OVER = pyRe("none\\s+over\\s+(\\d+)\\s*cm", "i");
const RE_MAX_SIZE = pyRe("(?:maximum\\s+size\\s+(\\d+)\\s*cm|(\\d+)\\s*cm\\s+or\\s+less)", "i");
const RE_TIMES = pyRe("from\\s+(\\d{1,2}:\\d{2})\\s*h?\\s+(?:until|to)\\s+(\\d{1,2}:\\d{2})\\s*h?", "i");
const RE_GAP = pyRe("less\\s+than\\s+(\\d+)\\s*mm\\s+from\\s+point\\s+to\\s+shank", "i");
const RE_FLY = pyRe("fly[- ]fishing\\s+only", "i");
const RE_FN = pyRe("\\bFN\\s*(\\d{3,4})\\b", "i");
const RE_TBD = pyRe("to\\s+be\\s+determined", "i");
const QUALIFIER = "(?:(?:hatchery[- ]marked|only|chinook|coho|sockeye|pink|chum|salmon)\\s+)*";
const RE_DAILY = pyRe(`\\b(\\d+)\\s+${QUALIFIER}per\\s+day\\b`, "i");
const RE_NO_FISHING = pyRe("\\bno\\s+fish(?:ing|inf)\\b|\\bfinfish\\s+closure\\b", "i");
const RE_NON_RETENTION = pyRe("\\bnon[- ]retention\\b|\\bno\\s+retention\\s+of\\b|\\bcatch\\s+and\\s+release\\b", "i");
const RE_HATCHERY = pyRe("hatchery[- ]marked", "i");
const RE_BAIT_BAN = pyRe("\\bbait\\s+ban\\b|\\bno\\s+natural\\s+bait\\b", "i");
const RE_BARBLESS = pyRe("\\bsingle,?\\s+barbless\\s+hook\\b", "i");
const RE_OPEN_ENDED = pyRe("\\buntil\\s+fu\\w*\\s+notice\\b", "i");
const RE_QUOTA_SPAN = pyRe("\\b\\d+\\s+(?:hatchery[- ]marked\\s+)?per\\s+day", "i");
const RE_BAIT_SPAN = pyRe("no\\s+natural\\s+bait[\\w\\s]*|bait\\s+ban", "i");
const RE_HOOK_SPAN = pyRe("single,?\\s+barbless\\s+hook[\\w\\s,.]*", "i");

interface Says {
  no_fishing: boolean;
  non_retention: boolean;
  hatchery_marked_only: boolean;
  bait_ban: boolean;
  single_barbless_hook: boolean;
  daily_limit: number | null;
}
export function decode(limits: string): Says {
  const text = limits || "";
  let daily: number | null = null;
  const m = search(RE_DAILY, text);
  if (m) daily = pyInt(m[1]!);
  else if (search(RE_NON_RETENTION, text) || search(RE_NO_FISHING, text)) daily = 0;
  return {
    no_fishing: !!search(RE_NO_FISHING, text),
    non_retention: !!search(RE_NON_RETENTION, text),
    hatchery_marked_only: !!search(RE_HATCHERY, text),
    bait_ban: !!search(RE_BAIT_BAN, text),
    single_barbless_hook: !!search(RE_BARBLESS, text),
    daily_limit: daily,
  };
}

const firstPassParses = (raw: string): boolean => !!search(RE_OPEN_ENDED, raw) || parseDateWindow(raw);

const DATE_MONTHS = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];
const RE_STRAY_SEP = pyRe("(\\d)\\s*[-–—]\\s+(?=to\\b)", "i");
const RE_TRAIL_YEAR = pyRe("\\s+(?:19|20)\\d{2}\\s*$");
const RE_WORD = pyRe("\\b[A-Za-z]{3,9}(?=\\.?\\s+\\d)");
function repairDates(raw: string): string {
  let out = sub(RE_STRAY_SEP, "\\1 ", raw);
  out = sub(RE_TRAIL_YEAR, "", out);
  out = sub(
    RE_WORD,
    (m) => {
      const w = m[0];
      const lw = w.toLowerCase();
      if (DATE_MONTHS.some((mo) => lw === mo || mo.startsWith(lw))) return w;
      const near = getCloseMatches(lw, DATE_MONTHS, 2, 0.8);
      return near.length === 1 ? capitalize(near[0]!.slice(0, 3)) : w;
    },
    out,
  );
  return out;
}

/** interpret_dates, as far as to_rules reads it: {parsed, repaired_from} */
function interpretDates(text: string): { parsed: boolean; repaired: boolean } {
  let raw = strip(text || "");
  if (!raw) return { parsed: false, repaired: false };
  let repaired = false;
  if (!firstPassParses(raw)) {
    const fixed = repairDates(raw);
    if (fixed !== raw && firstPassParses(fixed)) {
      repaired = true;
      raw = fixed;
    }
  }
  return { parsed: firstPassParses(raw), repaired };
}

const speciesFor = (text: string): string[] => [...(SPECIES[strip(text || "").toLowerCase()] ?? [])];
const span = (hay: string, re: ReturnType<typeof pyRe>): string | null => {
  const m = search(re, hay || "");
  return m ? m[0] : null;
};

// ── the CatalogueRule checks a DFO rule can reach (catalogue_parts/rule.py, checks.py) ─────────────────────────────
const EU_WHY =
  "not on the book's species list (p.80) — the fish are RB, ST, CT, GB, DV, LT, EB, LW, MW, LMB, SMB, KO, GR, BB, WSG, BCB, NP, YP, WP, GE, IN, CRA; the groups ALL_FIN_FISH, ALL_GAME_FISH, BASS, CHAR, PROTECTED_SPECIES, SALMON, TROUT_CHAR, WHITEFISH";
const FEDERAL_SALMON = new Set(["CH", "CO", "SK", "PK", "CM", "SA"]);
const speciesProblems = (codes: string[], where = "species"): string[] =>
  [...new Set(codes.filter((c) => !FEDERAL_SALMON.has(c)))].sort().map((c) => `${where}: unknown species code ${repr(c)} — ${EU_WHY}`);

const DASHES = /[‐‑‒–—―−]/g;
const SQ_QUOTE = new RegExp(translate("^\\s*[>|]\\s?", false), "gmu");
const SQ_BULLET = new RegExp(translate("^\\s*[-•]\\s+", false), "gmu");
const SQ_WS = new RegExp(translate("\\s+", false), "gu");
function squash(text: string): string {
  let t = (text || "").replace(/\[Includes Tributaries\]/g, "").replace(DASHES, "-").replace(/\*/g, "");
  t = t.replace(SQ_QUOTE, " ");
  t = t.replace(SQ_BULLET, " ");
  return strip(t.replace(SQ_WS, " ")).toLowerCase();
}
const RE_SNAGGED = pyRe("\\bsnag|\\bfoul[- ]?hook", "i");
const RE_HALF = pyRe("\\b(north|south|east|west)\\s+half\\s+of\\s+(?:the\\s+)?(?:river|stream|creek|channel)\\b", "i");
const RE_ADULT_CHINOOK = pyRe("\\badult\\s+chinook\\b", "i");
const RE_RANGE = pyRe("([a-z]+\\.?\\s*\\d{1,2})\\s*-\\s*([a-z]+\\.?\\s*\\d{1,2}|\\d{1,2})\\b");
const RE_ALL_YEAR = pyRe("\\ball year\\b|\\byear[- ]round\\b");
const RE_CUT_DATE = pyRe("\\b(jan|feb|mar|apr|jun|june|jul|july|aug|sep|sept|oct|nov|dec)\\.?\\s*$");
const RE_ALL_SPECIES = pyRe("\\ball (?:other )?(?:species|fish)\\b(?! combined)");

const rangeWords = (d: DateRange) => `${MONTHS[d.from_month - 1]} ${d.from_day}-${MONTHS[d.to_month - 1]} ${d.to_day}`;
function rangeDays(r: DateRange): number[] {
  const a = dayIndex(r.from_month, r.from_day);
  let b = dayIndex(r.to_month, r.to_day);
  if (r.to_month === 2 && r.to_day === 28) b += 1;
  const out: number[] = [];
  if (a <= b) for (let d = a; d <= b; d++) out.push(d);
  else {
    for (let d = a; d <= YEAR; d++) out.push(d);
    for (let d = 1; d <= b; d++) out.push(d);
  }
  return out;
}
const daysOf = (rs: DateRange[]) => new Set(rs.flatMap(rangeDays));
const sameRange = (a: DateRange, b: DateRange) =>
  a.from_month === b.from_month && a.from_day === b.from_day && a.to_month === b.to_month && a.to_day === b.to_day;

function printedRanges(text: string): DateRange[] {
  const out: DateRange[] = [];
  const s = squash(text);
  RE_RANGE.g.lastIndex = 0;
  for (let m = RE_RANGE.g.exec(s); m; m = RE_RANGE.g.exec(s)) {
    const got = parseDateRange(`${m[1]}-${m[2]}`);
    if (got) out.push(got);
    if (m[0].length === 0) RE_RANGE.g.lastIndex++;
  }
  return out;
}

function ownDatesCarried(rule: TypedRule): string | null {
  const sq = squash(rule.verbatim);
  if (search(RE_CUT_DATE, sq))
    return `its sentence ends in a month with no day (${repr([...sq].slice(-12).join(""))}) — a date cut off; restore it from the page`;
  const printed = printedRanges(rule.verbatim);
  if (!printed.length) return null;
  const mine = rule.when?.dates ?? [];
  const said = printed.map(rangeWords).join(", ");
  if (!mine.length) {
    if (search(RE_ALL_YEAR, sq)) return null;
    return `its sentence prints ${said} but it has no \`when\` — an absent \`when\` is ALL YEAR; give it the printed dates`;
  }
  if (mine.every((d) => printed.some((p) => sameRange(d, p)))) return null;
  const a = daysOf(mine);
  const printedDays = daysOf(printed);
  const comp = new Set<number>();
  for (let d = 1; d <= YEAR; d++) if (!printedDays.has(d)) comp.add(d);
  if (a.size === comp.size && [...a].every((d) => comp.has(d))) return null;
  return `its \`when\` (${mine.map(rangeWords).join(", ")}) is neither the dates its sentence prints (${said}), some of them, nor their complement`;
}

const pyList = (xs: string[]) => "[" + xs.map((x) => repr(x)).join(", ") + "]";

/** The model validators, in pydantic's order; raises the ValidationError the Python would. */
function validate(r: TypedRule): void {
  const t = r.type;
  const take = r.take ?? null;
  // field validation first: GearClause._shape (the one bound a DFO row can get wrong)
  (r.gear ?? []).forEach((g, i) => {
    const v = (g.max as PyFloat | undefined)?.v;
    if (g.slot === "hook_gap_mm" && v !== undefined && v < 10)
      throw valueError(
        "CatalogueRule",
        `hook_gap_mm ${floatRepr(v)} is smaller than any gap the book prints — the synopsis states this in cm as well as mm, so confirm the unit ('3 cm' is 30)`,
        `{'slot': 'hook_gap_mm', 'max': ${v}}`,
        `gear.${i}`,
      );
  });
  // _caught_is_a_fish_condition
  if (t === "retention_limit" && take === 0 && search(RE_SNAGGED, r.verbatim || ""))
    throw valueError(
      "CatalogueRule",
      "a release of a snagged (foul-hooked) fish binds the FISH, however it was hooked: `caught: [foul_hooked]`, not `while: [snagging]` (user ruling Q38)",
      "{...}",
    );
  // _check
  const e: string[] = [];
  const species = r.species ?? [];
  if (t === "retention_limit" && !species.length) e.push("retention_limit needs species (use ALL_GAME_FISH for everything)");
  e.push(...speciesProblems(species));
  if (t === "retention_limit") {
    if (take === 0 && r.may_target === undefined)
      e.push("take=0 needs an explicit may_target: false = may not fish for it, true = fish for it and release it");
    if (take !== null && take < 0) e.push("take cannot be negative");
    if (take !== null && r.lengths && r.lengths.length && r.lengths.every((b) => b.take !== undefined))
      e.push(
        `take=${take} fills no band — every range in \`lengths\` carries its own take. Leave \`take\` out: 'no trout over 50 cm' is [{min_cm: 50, take: 0}], a size rule that says nothing about smaller fish`,
      );
  } else {
    if (take !== null) e.push(`take belongs to retention_limit, not ${t}`);
    if (r.within) e.push(`within belongs to retention_limit, not ${t}`);
    if (r.lengths && r.lengths.length)
      e.push(`lengths belongs to retention_limit, not ${t} — a size a licence depends on is the licensing record's \`doing.lengths\``);
  }
  if (t === "bait_restriction" || t === "tackle_restriction") {
    if (species.length)
      e.push(
        `${t} must not carry \`species\` — a bait or hook rule binds all species you may catch. If the rule applies only when fishing FOR something, use \`when_targeting\`.`,
      );
    e.push(...speciesProblems(r.when_targeting ?? [], "when_targeting"));
  } else if (r.when_targeting && r.when_targeting.length) e.push("when_targeting belongs to bait_restriction and tackle_restriction");
  if ((t === "tackle_restriction" || t === "bait_restriction") && !(r.gear && r.gear.length))
    e.push(`${t} needs \`gear\`, \`conduct\` or \`exempts\` — it states nothing else`);
  if (take !== null && take < 0) e.push("take cannot be negative");
  const half = search(RE_HALF, r.verbatim || "");
  if (half) {
    const p = half[1]!.toLowerCase();
    e.push(`the sentence says the ${p} half of the channel — set side: ${p} (the rule holds on that half only)`);
  }
  if (search(RE_ADULT_CHINOOK, r.verbatim || ""))
    e.push("the sentence says 'adult chinook' — set life_stage: adult (the rule holds for adult chinook only, p.77)");
  const own = ownDatesCarried(r);
  if (own) e.push(own);
  if (t === "retention_limit" && search(RE_ALL_SPECIES, squash(r.verbatim)))
    e.push(
      `its sentence says 'all (other) species / all fish', which is GAME FISH and never crayfish — species ['ALL_GAME_FISH'] with 'CRA' in species_except (it has species ${pyList(species)}, except [])`,
    );
  if (e.length) throw valueError("CatalogueRule", `${r.rule_id}: ` + e.join("; "), "{...}");
}

// ── the rule, as dumped ──────────────────────────────────────────────────────────────────────────────────────────────
export interface When {
  dates?: DateRange[];
  hours?: { start: unknown; end: unknown };
  unparsed?: string[];
}
export interface LengthBand {
  min_cm?: number;
  max_cm?: number;
  take?: number;
}
export interface TypedRule {
  rule_id: string;
  type: string;
  verbatim: string;
  notice?: string;
  when?: When;
  species?: string[];
  take?: number;
  may_target?: boolean;
  origin?: string;
  review_reason?: string;
  lengths?: LengthBand[];
  within?: string;
  gear?: Record<string, unknown>[];
  when_targeting?: string[];
}

function whenOf(windows: string[], times: RegExpExecArray | null): When | null {
  const dates: DateRange[] = [];
  const unparsed: string[] = [];
  for (const w of windows) {
    const got = parseDateRange(w);
    if (got) dates.push(got);
    else unparsed.push(w);
  }
  let hours: When["hours"] | null = null;
  if (times) {
    const a = parseClock(times[1]!);
    const b = parseClock(times[2]!);
    if (!a || !b) throw new PyError("ValueError", `times ${repr(times[0])} do not parse`);
    hours = { start: a, end: b };
  }
  if (!dates.length && !unparsed.length && !hours) return null;
  const w: When = {};
  if (dates.length) w.dates = dates;
  if (hours) w.hours = hours;
  if (unparsed.length) w.unparsed = unparsed;
  return w;
}

export interface ScrapedRule {
  species: string;
  dates: string;
  limits_gear: string;
}

/** typed.to_rules: one scraped row → the rules it states (dumped). */
export function toRules(rec: ScrapedRule, ruleBase: string, order: number): TypedRule[] {
  const gear = strip(rec.limits_gear || "");
  const says = decode(gear);
  const codes = speciesFor(rec.species || "");
  const rawDates = strip(rec.dates || "");
  const when = interpretDates(rawDates);
  const windows = !rawDates ? [] : when.repaired ? [repairDates(rawDates)] : [rawDates];
  const unreadable = !!rawDates && !when.parsed;
  const fn = search(RE_FN, gear);
  const notice = fn ? `FN${String(pyInt(fn[1]!)).padStart(4, "0")}` : null;
  const rid = `${ruleBase}.r${order}`;
  const out: TypedRule[] = [];

  const make = (fields: Omit<TypedRule, "verbatim"> & { verbatim?: string }, times: RegExpExecArray | null = null): TypedRule => {
    // base(): verbatim / notice / when, then the kwargs (kwargs win)
    const base: Partial<TypedRule> = { verbatim: gear || rec.species || "row" };
    if (notice) base.notice = notice;
    const w = whenOf(windows, times);
    if (w) base.when = w;
    const merged: TypedRule = { ...(base as TypedRule), ...stripDefaults(fields) };
    validate(merged);
    return merged;
  };

  if (search(RE_TBD, gear))
    return [make({ rule_id: rid, type: "advisory", review_reason: "DFO has not set this row yet ('to be determined')" })];
  if (unreadable)
    return [make({ rule_id: rid, type: "advisory", review_reason: `dates do not parse to a calendar window: ${repr(rawDates)}` })];
  if (!codes.length && gear)
    return [make({ rule_id: rid, type: "advisory", review_reason: `unmapped species cell ${repr(rec.species)}` })];

  let take: number | null = null;
  let mayTarget: boolean | null = null;
  let zeroQuota = false;
  if (says.no_fishing) [take, mayTarget] = [0, false];
  else if (says.non_retention) [take, mayTarget] = [0, true];
  else if (says.daily_limit !== null) {
    take = says.daily_limit;
    if (take === 0) [take, mayTarget, zeroQuota] = [0, false, true];
  }

  if (take !== null) {
    const times = search(RE_TIMES, gear);
    const quoted = span(gear, RE_QUOTA_SPAN);
    out.push(
      make(
        {
          rule_id: rid,
          type: "retention_limit",
          species: codes,
          take,
          may_target: mayTarget ?? undefined,
          origin: says.hatchery_marked_only ? "hatchery" : undefined,
          review_reason: zeroQuota
            ? "a bare '0 per day' says the quota is zero but not whether you may fish at all; published closed, confirm against the page"
            : "",
          verbatim: quoted || gear || rec.species || "row",
        },
        times,
      ),
    );
    const sub = search(RE_ONLY_OVER, gear) ?? search(RE_WHICH_MAY, gear);
    const noneOver = sub ? null : search(RE_NONE_OVER, gear);
    const mx = sub || noneOver ? null : search(RE_MAX_SIZE, gear);
    if (sub)
      out.push(
        make({ rule_id: `${rid}b`, type: "retention_limit", species: codes, take: pyInt(sub[1]!), lengths: [{ min_cm: pyInt(sub[2]!) }], within: rid, verbatim: sub[0] }),
      );
    else if (noneOver)
      out.push(
        make({
          rule_id: `${rid}b`,
          type: "retention_limit",
          species: codes,
          may_target: true,
          lengths: [{ min_cm: pyInt(noneOver[1]!), take: 0 }],
          within: rid,
          verbatim: noneOver[0],
        }),
      );
    else if (mx) {
      const cm = pyInt(mx[1] || mx[2]!);
      out.push(make({ rule_id: `${rid}b`, type: "retention_limit", species: codes, may_target: true, lengths: [{ min_cm: cm, take: 0 }], within: rid, verbatim: mx[0] }));
    }
  }

  if (says.bait_ban) {
    const quoted = span(gear, RE_BAIT_SPAN);
    out.push(make({ rule_id: `${rid}_bait`, type: "bait_restriction", gear: [{ slot: "bait", ban: ["any_bait"] }], when_targeting: codes, verbatim: quoted || gear }));
  }
  if (says.single_barbless_hook) {
    const gap = search(RE_GAP, gear);
    const quoted = span(gear, RE_HOOK_SPAN);
    const g: Record<string, unknown>[] = [
      { slot: "barb", only: ["barbless"] },
      { slot: "points_per_hook", max: new PyFloat(1) },
    ];
    if (gap) g.push({ slot: "hook_gap_mm", max: new PyFloat(pyInt(gap[1]!)) });
    out.push(make({ rule_id: `${rid}_hook`, type: "tackle_restriction", gear: g, when_targeting: codes, verbatim: quoted || gear }));
  }
  if (search(RE_FLY, gear))
    out.push(
      make({ rule_id: `${rid}_fly`, type: "tackle_restriction", gear: [{ slot: "method", only: ["fly_fishing"] }], when_targeting: codes, verbatim: span(gear, RE_FLY) || gear }),
    );
  if (!out.length)
    out.push(make({ rule_id: rid, type: "advisory", review_reason: "row states no limit, closure or gear rule the scrape could type" }));
  return out;
}

/** exclude_defaults: drop what equals the field default ([] / "" / null / undefined). */
function stripDefaults<T extends object>(o: T): Partial<T> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(o)) {
    if (v === undefined || v === null || v === "" || (Array.isArray(v) && v.length === 0)) continue;
    out[k] = v;
  }
  return out as Partial<T>;
}

/** typed.row_text */
export const rowText = (rec: ScrapedRule): string =>
  [strip(rec.species || ""), strip(rec.dates || ""), strip(rec.limits_gear || "")].filter((p) => p).join(" | ");
