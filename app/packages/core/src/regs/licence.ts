/**
 * T33 — the Licence tab for one angler (`Who`): the question, the two choices, the profile's label, then the answer in draw
 * order (classified-water boxes, the under-16 box, the "You need" list, the paper-licence line, the not-yet-mapped box,
 * the guiding fold drawn open) and the source line that opens the `licsrc:` sheet ("Where each one comes from").
 *
 * Ports (inventory names @ 7fb6809f, regs v38): page v36 LX, DOC, AXES, whoTxt, actTxt, priceTxt, pathHtml, srcOf,
 * reqCard, otherCard, licParts (+ whenTxt, alsoTxt, docGi, fishOf, orderAct); prototype licence (+ one,
 * asNeeds), KEYOF, render (the licence-list patches).
 *
 * OWNER RULE 2026-10-21: nothing here decides a regulation. Which documents an angler needs, every requirement, exemption,
 * designation, price and path is the stored licence answer (`licence_frame` → holds / profiles / under_16); this module
 * SELECTS the angler's profile (and, for the under-16 "as a 16+ angler" path, the stored profile that path names), GROUPS
 * the answer into blocks and WORDS it. Licensing never affects open / closed (memory "licensing rewrite decisions"); the
 * angler is whoever the reader says (D8). The prototype's render-time steelhead-stamp DROP (a stamp removed from the list
 * where the card says steelhead are closed, round 20i O3) DECIDES a requirement and is NOT ported: the pipeline must ship
 * it (decisions D21 (b); conformance exclusions.ts STEELHEAD_DROP).
 */
import type { DocumentNeed, Path_, Prices, PrintedRequirement, RequirementPath, When } from "../answers.generated";
import type { Holds, LicenceValue, ProfileAnswer, RegsInputs } from "./inputs";
import type { LicenceBlockVM, LicenceVM, NeedItemVM, Who } from "./model";
import { cap, expand, join, lc, lcNames, type SpeciesBook } from "./names";
import type { ProfileKey, SheetKey } from "./sheetKey";
import { AGE_DIM, profileIndex } from "./tap";
import { rangeTxt, whenDates, type RuleWhen } from "./time";
import { LIC, WORDS } from "./words";


// ── the angler ──────────────────────────────────────────────────────────────────────────────────────────────────────

/** prototype KEYOF: the answer key for an age and a residency. */
export function keyOf(who: Who): ProfileKey {
  const u = who.age === "u16";
  return who.residency === "non_resident" ? (u ? "u16_non" : "adult_non")
    : who.residency === "non_resident_alien" ? (u ? "u16_alien" : "adult_alien") : (u ? "u16_res" : "adult");
}

/** The profile's index into `static licence.profiles` (guidance non_guided, status none — the app asks neither). */
export function profileOf(who: Who, s: RegsInputs["statics"]): number {
  return profileIndex(who, s.profileDims, s.profiles);
}

/** page v36's `state.who`: the four licence dimensions of an app `Who`. */
const dimsOf = (who: Who): Record<string, string> => ({ residency: who.residency, age: AGE_DIM[who.age], guidance: "non_guided", status: "none" });

/** page v36 profileIx over any dims object (the under-16 `as` path names another stored profile); -1 when absent. */
const profileIxOf = (dims: Record<string, string>, s: RegsInputs["statics"]): number =>
  s.profiles.indexOf(s.profileDims.map(([d]) => dims[d]).join("/"));

// ── the export licensing words ──────────────────────────────────────────────────────────────────────────────────────

export interface LicWords {
  readonly key: string; readonly kind: string; readonly label: string; readonly verbatim: string; readonly placement: string;
  readonly fields: Readonly<Record<string, any>>; readonly parts?: Readonly<Record<string, string>>;
  readonly prov: { readonly entry_name?: string; readonly why?: string | null; readonly uncertain?: boolean };
  readonly period?: { readonly says: string; readonly dates?: readonly unknown[] } | null;
  readonly stamp_waiver?: { readonly says: string } | null;
  readonly not_yet_mapped?: { readonly part: string; readonly says: string } | null;
}

/** page v36 LX: the licensing record `lix` (export words), keyed `${entry_id}::${record_id}`; a stand-in when absent. */
export function lx(inp: RegsInputs, lix: number): LicWords {
  const l = inp.lics.get(lix) as (Omit<LicWords, "key"> & { entry_id: string; record_id: string }) | undefined;
  if (!l) return { key: `#L${lix}`, kind: "?", label: LIC.MISSING_LIC(lix), verbatim: "", placement: "", fields: {}, prov: {} };
  return { ...l, key: `${l.entry_id}::${l.record_id}`, fields: l.fields ?? {}, prov: l.prov ?? {}, verbatim: l.verbatim ?? "" };
}

/** page v36 DOC: a document's name (export.licences), else its code in words. */
const docName = (inp: RegsInputs, d: string): string =>
  (inp.statics.exportLicences as Record<string, { name?: string }> | null)?.[d]?.name || d.replace(/_/g, " ");

// ── words ───────────────────────────────────────────────────────────────────────────────────────────────────────────

type AxisName = keyof typeof LIC.AXES;
/** page v36 whoTxt: a licence `who` in words ("16 or older", "canadian, other province"); null = every angler. */
export function whoTxt(who: Readonly<Record<string, unknown>> | null | undefined): string {
  if (!who) return LIC.EVERY_ANGLER;
  return Object.entries(who).map(([a, l]) => (Array.isArray(l) ? l : [l]).map((v) => {
    const t = String((LIC.AXES as Record<string, readonly (readonly [string, string])[]>)[a as AxisName]?.find((x) => x[0] === v)?.[1] ?? v);
    return a === "status" ? (v === "indian_bc_resident" ? LIC.WHO_STATUS_FN : t) : t.replace(/^[A-Z](?=[a-z])/, (c) => c.toLowerCase());
  }).join(WORDS.OR)).join(LIC.SEP);
}

/** page v36 actTxt: a licence `when` as its act ("To fish here", "To keep salmon", "To fish for steelhead"). */
export function actTxt(w: When, sp: SpeciesBook): string {
  const codes = w.species ?? null;
  const name = (codes ? lcNames(sp, expand(sp, codes)) || codes.map((c) => (sp.groups[c]?.name || c).toLowerCase()).join(LIC.SEP) : "") || WORDS.FISH;
  const ls = (w.lengths ?? []) as readonly { min_cm?: number | null; max_cm?: number | null }[];
  const len = ls.length ? " " + ls.map((x) => x.min_cm != null && x.max_cm != null ? LIC.LEN_RANGE(x.min_cm, x.max_cm)
    : x.min_cm != null ? LIC.LEN_OVER(x.min_cm) : LIC.LEN_UNDER(x.max_cm!)).join(LIC.SEP) : "";
  switch (w.act) {
    case "fishing": return LIC.ACT_FISHING;
    case "targeting": return LIC.ACT_TARGETING(name);
    case "retaining": return LIC.ACT_RETAINING(name + len);
    case "retaining_recorded": return LIC.ACT_RECORDED;
    case "guiding": return LIC.ACT_GUIDING;
    default: return String(w.act);
  }
}

/** page v36 priceTxt: "$41.15 a year · $11.43 a day · $22.86 for 8 days". */
export function priceTxt(p: Prices): string {
  return [p.year != null ? LIC.PER_YEAR(p.year.toFixed(2)) : "", ...(p.day ?? []).map((x) => LIC.PER_DAY(x.toFixed(2))),
    ...(p.eight_days ?? []).map((x) => LIC.PER_8_DAYS(x.toFixed(2)))].filter(Boolean).join(LIC.DOT);
}

/** page v36 srcOf: who a licensing record speaks for. */
export function srcOf(l: LicWords): string {
  return l.placement === "province" ? LIC.SRC.province : l.placement === "on_designation" ? LIC.SRC.on_designation
    : l.fields.authority === "superior" ? LIC.SRC.superior : LIC.SRC.water;
}

/** page v36 pathHtml, in plain words: one way to meet a requirement. */
function pathTxt(inp: RegsInputs, p: Path_): string {
  const bits: string[] = [];
  if ("need" in p) bits.push(p.need?.length ? p.need.map((d) => docName(inp, d)).join(LIC.PLUS) : LIC.PATH_EXEMPT);
  if (p.accompanied_by) bits.push(LIC.PATH_WITH(whoTxt((p.accompanied_by.who ?? p.accompanied_by) as Record<string, unknown>))
    + (p.quota === "counts_to_companion" ? LIC.PATH_COUNTS : ""));
  if (p.as) bits.push(LIC.PATH_AS(whoTxt(((p.as as { who?: unknown }).who ?? p.as) as Record<string, unknown>)));
  return bits.join(" ") + (p.alt != null ? LIC.PATH_ALT : "");
}

// ── the sources (the "Where each one comes from" fold → the licsrc: sheet, T36) ───────────────────────────────────────

/** One requirement as page v36 reqCard draws it (the licsrc: sheet's card). */
export interface ReqCardVM {
  readonly key: string; readonly label: string; readonly src: string;   // src = srcOf + " +N" when the book prints it N more times
  readonly lost: string | null;                                         // displaced: why it is not valid here
  readonly paths: readonly string[];                                    // each way to meet it (drawn with "or" between)
  readonly freed: string | null; readonly conduct: readonly string[];   // freed replaces conduct
  readonly notIf: string | null;
  readonly terms: readonly { key: string; label: string }[];
}

const orderAct = (t: string): number => t === LIC.ACT_FISHING ? 0 : t.startsWith(LIC.ACT_TARGETING("")) ? 1 : t.startsWith(LIC.ACT_RETAINING("")) ? 2 : 3;

function reqCard(inp: RegsInputs, x: RequirementPath, H: Holds): ReqCardVM {
  const l = lx(inp, x.req), also = H.also_printed?.[String(x.req)] ?? [];
  const by = x.presumes_by != null ? lx(inp, x.presumes_by) : null;
  return {
    key: l.key, label: l.label, src: srcOf(l) + (also.length ? LIC.SRC_MORE(also.length) : ""),
    lost: x.displaced_by?.length ? LIC.DISPLACED : null,
    paths: x.paths.map((p) => pathTxt(inp, p)),
    freed: x.presumes_freed ? LIC.FREED : null,
    conduct: x.presumes_freed ? [] : ((l.fields.conduct ?? []) as string[]).map((c) => inp.statics.exportConduct?.[c] || c),
    notIf: !x.presumes_freed && by ? LIC.NOT_IF(whoTxt(by.fields.who).replace(/\.$/, "")) : null,
    terms: (x.terms ?? []).map((t) => lx(inp, t)).map((t) => ({ key: t.key, label: t.label })),
  };
}

/** The requirement cards of the angler's answer, grouped by act and ordered as page v36 (to fish here, to fish for…,
 *  to keep…, the rest) — the prototype's licsrc: sheet body (T36 draws it, titled LIC.SOURCES_HEAD). */
export function licenceSources(inp: RegsInputs, p: ProfileAnswer, h: Holds): readonly { act: string; cards: readonly ReqCardVM[] }[] {
  const sp = inp.statics.exportSpecies, groups = new Map<string, RequirementPath[]>();
  for (const x of p.requirements) { const t = actTxt(x.when, sp); if (!groups.has(t)) groups.set(t, []); groups.get(t)!.push(x); }
  return [...groups.entries()].sort((a, b) => orderAct(a[0]) - orderAct(b[0]))
    .map(([act, list]) => ({ act, cards: list.map((x) => reqCard(inp, x, h)) }));
}

/** The sheet's plain lines (page v36 reqCard text, one per block element) — what the golden licsrc: sheet holds. */
export function licenceSourceLines(groups: ReturnType<typeof licenceSources>): readonly string[] {
  return groups.flatMap((g) => [g.act, ...g.cards.flatMap((c) => [
    `${c.label} ${c.src}`, ...(c.lost ? [c.lost] : []), ...c.paths.flatMap((x, i) => i ? [LIC.OR, x] : [x]),
    ...(c.freed ? [c.freed] : c.conduct), ...(c.notIf ? [c.notIf] : []), ...c.terms.map((t) => t.label)])]);
}

/** The source line's words (prototype licence.one: every card's `.src` before the folds move): the requirement cards in
 *  sheet order, then the guiding cards, then the other anglers' cards — first seen, once. */
export function licenceWho(inp: RegsInputs, p: ProfileAnswer, h: Holds): readonly string[] {
  const words = [...licenceSources(inp, p, h).flatMap((g) => g.cards.map((c) => c.src)),
    ...[...(p.guiding ?? []), ...(p.others ?? [])].map((o) => srcOf(lx(inp, o.req)))];
  return words.filter((w, i) => words.indexOf(w) === i);
}

// ── the answer ──────────────────────────────────────────────────────────────────────────────────────────────────────

type Item = NeedItemVM & { readonly docs: readonly string[] };

const isAnswer = (p: unknown): p is ProfileAnswer => !!p && typeof p === "object" && "documents" in p;
const isHolds = (h: LicenceValue["holds"]): h is Holds => !("tidal" in h);

/** page v36 licParts for one stored profile answer, as typed blocks (the dropped pieces — the profile selects, "Showing
 *  the answer for:", the under-16 residency list, the "Rules for other anglers" fold, "All licence sources" — are the
 *  prototype's removals; the "Where each one comes from" fold is `licenceSources`). */
function answerBlocks(inp: RegsInputs, F: LicenceValue & { holds: Holds }, who: Who, P: ProfileAnswer,
                      gloss: ReadonlySet<string>, source: SheetKey): LicenceBlockVM[] {
  const sp = inp.statics.exportSpecies, H = F.holds, DOC = (d: string) => docName(inp, d);
  const gi = (id: string): string[] => gloss.has(id) ? [id] : [];
  const out: LicenceBlockVM[] = [];
  const des = (H.designations ?? []).map((d) => lx(inp, d));
  const datesOf = (w: unknown) => whenDates(w as RuleWhen).map(([a, b]) => rangeTxt(a, b)).join(LIC.SEP);
  const stampDates = des.filter((d) => d.fields.steelhead_stamp_during).map((d) => datesOf(d.fields.steelhead_stamp_during.when))[0] || "";
  for (const d of des) {
    const f = d.fields;
    out.push({ cls: "classbox", head: d.period?.says ?? LIC.CLASS_HEAD(String(f.classified)), terms: gi(LIC.CLASS_TERM),
      body: LIC.UNIT(String(f.unit_name || f.unit))
        + (f.steelhead_stamp_during ? LIC.STAMP_DURING(datesOf(f.steelhead_stamp_during.when)) + (H.stamp_period ? LIC.IN_FORCE : "") + LIC.STAMP_OTHER_TIME : "")
        + (d.stamp_waiver ? LIC.STOP + " " + d.stamp_waiver.says.replace(/\.$/, "") : f.steelhead_stamp_waived ? LIC.STAMP_WAIVED : "") + LIC.STOP,
      source });
  }
  if (H.contested) out.push({ cls: "flag", text: LIC.CONTESTED });
  // page v36's unresolved-licensing flags ("Check: … couldn't be placed on the map") need the store's item.unresolved, which
  // T09 does not load yet (no item carries one in the current release) — not drawn
  if (P.exempt) out.push({ cls: "scopenote", text: LIC.EXEMPT_FROM(P.exempt.from.map(DOC).join(LIC.SEP)), source });
  if (P.none_needed) out.push({ cls: "classbox", head: LIC.NONE_HEAD, terms: [], source: null,
    body: (who.age === "u16" ? LIC.NONE_U16 : P.exempt ? LIC.NONE_EXEMPT : LIC.NONE_ANY) + " " + LIC.NONE_TAIL });

  // whenTxt / alsoTxt / docGi
  const perSays = des.map((d) => d.period?.says).filter(Boolean)[0];
  const whenTxt = (w: When): string => {
    const t = actTxt(w, sp);
    return w.on === "classified_period" && perSays ? LIC.WHILE_CLASSIFIED(lc(perSays.replace(/^Classified \(Class (I+)\)/, "Class $1")))
      : w.on === "steelhead_period" ? LIC.NEEDED_AT_ALL + (stampDates ? " " + stampDates : "") + LIC.WHATEVER
      : t === LIC.ACT_FISHING ? LIC.NEEDED_AT_ALL
      : t.startsWith(LIC.ACT_TARGETING("")) ? LIC.ONLY_IF(lc(t.replace(/^To /, ""))) + LIC.EVEN_RELEASE
      : t.startsWith(LIC.ACT_RETAINING("")) ? LIC.ONLY_IF(lc(t.replace(/^To /, ""))) : t;
  };
  const alsoTxt = (w: When): string => {
    const t = actTxt(w, sp);
    return t.startsWith(LIC.ACT_TARGETING("")) ? LIC.OTHER_TIME(lc(t.replace(/^To /, ""))) : lc(whenTxt(w));
  };
  const docGi = (d: string): string[] => d === "steelhead_stamp" ? gi("steelhead_stamp") : d === "classified_waters_licence" ? gi("classified_waters")
    : /conservation surcharge/i.test(DOC(d)) ? gi("conservation_surcharge") : [];
  const altsOf = (b: DocumentNeed): string[] => (b.or ?? []).map((o) => o.need.map(DOC).join(LIC.PLUS));
  const buy: Item[] = P.documents.map((b) => {
    const alts = altsOf(b);
    return { name: cap([DOC(b.doc), ...alts].join(` ${LIC.OR} `)), docs: [b.doc], terms: docGi(b.doc),
      when: [whenTxt(b.when), ...(b.also_when ?? []).map(alsoTxt)].join(LIC.ALSO_SEP) + (alts.length ? LIC.EITHER : ""),
      price: priceTxt(b.prices ?? {}) || null };
  });

  // under 16: no document of their own, and a fishing requirement met with a licensed adult or as an adult
  const youthReq = !P.documents.length && P.requirements.find((x) => x.when.act === "fishing" && x.paths.some((p) => p.accompanied_by || p.as));
  if (youthReq) {
    const asP = youthReq.paths.find((p) => p.as), acc = youthReq.paths.find((p) => p.accompanied_by);
    let adult: ProfileAnswer | null = null;
    if (asP) {
      const me = dimsOf(who), as = asP.as as Record<string, string | readonly string[]>;
      const dims = { ...me, ...Object.fromEntries(Object.entries(as).map(([k, v]) => {
        const vs = Array.isArray(v) ? v as readonly string[] : [v as string];
        return [k, vs.includes(me[k]!) ? me[k]! : vs[0]!];
      })) };
      const a = F.profiles[profileIxOf(dims, inp.statics)];
      adult = isAnswer(a) ? a : null;
    }
    const adocs = (adult?.documents ?? []).filter((b) => b.base).map((b) => cap([DOC(b.doc), ...altsOf(b)].join(` ${LIC.OR} `)));
    const counts = acc?.quota === "counts_to_companion";
    out.push({ cls: "classbox youth", head: LIC.YOUTH_HEAD, terms: gi(LIC.YOUTH_TERM),
      body: ((acc ? LIC.YOUTH_WITH(whoTxt((acc.accompanied_by!.who ?? {}) as Record<string, unknown>)) + (adocs.length ? LIC.YOUTH_DOCS(adocs.join(LIC.SEP)) : "")
        + LIC.STOP + (counts ? LIC.YOUTH_COUNTS_S : "") : "")
        + (asP ? LIC.YOUTH_BUY(adocs.length ? LIC.YOUTH_THE(adocs.join(LIC.PLUS)) : LIC.YOUTH_ADULT_NEEDS) : "")).trim() });
    buy.push({ name: LIC.YOUTH_NOTHING, docs: [], terms: [], when: counts ? LIC.YOUTH_COUNTS : LIC.YOUTH_HOLD, price: null });
    if (asP && adocs.length) buy.push({ name: adocs.join(LIC.PLUS), docs: [], terms: [], when: LIC.YOUTH_OWN,
      price: priceTxt(adult!.documents.find((b) => b.base)?.prices ?? {}) || null });
  }
  if (buy.length) out.push({ cls: "needs", label: LIC.YOU_NEED, items: buy, onlyIf: null, priceNote: buy.some((b) => b.price) ? LIC.PRICE_NOTE : null });

  // the paper licence: the record duties that reach this part (answers display part paper_licence)
  const paperRules = (inp.item?.parts[inp.partIx]?.paper_licence ?? []).map((r) => inp.rules.get(r));
  if (P.requirements.some((x) => x.when.act === "retaining_recorded") && paperRules.length) {
    const fishOf = (f: Readonly<Record<string, any>>): string => {
      const min = ((f.lengths ?? []) as { min_cm?: number | null }[]).find((l) => l.min_cm != null);
      return `${f.life_stage === "adult" ? LIC.ADULT : ""}${f.origin ? f.origin + " " : ""}${lcNames(sp, expand(sp, f.species ?? []), WORDS.OR)}${min ? " " + LIC.LEN_OVER(min.min_cm!) : ""}`;
    };
    const fish = [...new Set(paperRules.map((r) => fishOf(r.f)))];
    out.push({ cls: "paper", lead: LIC.PAPER_LEAD(/^[aeiou]/.test(fish[0]!) ? LIC.AN : LIC.A, join(fish, WORDS.OR)), strong: LIC.PAPER_STRONG, tail: LIC.PAPER_TAIL });
  }
  for (const l of (H.not_yet_mapped ?? []).map((x) => lx(inp, x)))
    out.push({ cls: "nymbox", label: LIC.NYM_HEAD, note: LIC.NYM_NOTE,
      says: LIC.NYM_SAYS(cap(l.parts?.in_part || l.not_yet_mapped?.part || ""), l.parts?.need || DOC(l.fields.satisfied_by?.[0]?.hold?.[0] || ""), l.parts?.doing || LIC.NYM_DOING) });
  if (!P.requirements.length && !P.none_needed) out.push({ cls: "muted", text: LIC.NOTHING });
  if ((P.guiding ?? []).length) out.push({ cls: "lostlist lopen", label: LIC.GUIDING, cards: P.guiding!.map((o) => otherCard(inp, o)) });
  return out;
}

/** page v36 otherCard (the guiding fold, drawn open): for whom, the record's label, each way to meet it. */
function otherCard(inp: RegsInputs, o: PrintedRequirement): { forWho: string; label: string; paths: readonly string[] } {
  const l = lx(inp, o.req);
  return { forWho: LIC.FOR(whoTxt(l.fields.who) + (l.fields.who_except ? LIC.NOT_WHO(whoTxt(l.fields.who_except)) : "")), label: l.label,
           paths: o.paths.map((p) => pathTxt(inp, p)) };
}

/** prototype licence.asNeeds: with no "You need" list, every box that is not also a box of the ADULT RESIDENT's answer
 *  becomes an item of a "You need" list where the first such box was (its head the name, its body the when). */
function asNeeds(blocks: LicenceBlockVM[], adult: readonly LicenceBlockVM[] | null): LicenceBlockVM[] {
  if (blocks.some((b) => b.cls === "needs") || !adult?.some((b) => b.cls === "needs")) return blocks;
  const isBox = (b: LicenceBlockVM): b is Extract<LicenceBlockVM, { cls: "classbox" | "classbox youth" }> => b.cls === "classbox" || b.cls === "classbox youth";
  const shared = new Set(adult.filter(isBox).map((b) => licenceBlockLines(b).join(" ")));
  const own = blocks.filter((b) => isBox(b) && !shared.has(licenceBlockLines(b).join(" "))) as Extract<LicenceBlockVM, { cls: "classbox" | "classbox youth" }>[];
  if (!own.length) return blocks;
  const at = blocks.indexOf(own[0]!);
  const needs: LicenceBlockVM = { cls: "needs", label: LIC.YOU_NEED, onlyIf: null, priceNote: null,
    items: own.map((b): Item => ({ name: b.head, terms: b.terms, when: b.body, price: null, docs: [] })) };
  return [...blocks.slice(0, at), needs, ...blocks.slice(at).filter((b) => !own.includes(b as never))];
}

/**
 * The prototype render()'s licence-list patches that WORD the list (decisions D21; isolated so a data fix deletes them):
 * (b) the salmon stamp's short line keeps its record's "(not kokanee)" — only when a stored requirement this document
 *     meets says so in its own label; (c) the conditional documents ("Only if …") move under "Also, only if…" unless none
 *     or all are conditional. The prototype's (a) — dropping the steelhead stamp where the card says steelhead are closed —
 *     decides a requirement and is NOT here (owner rule 2026-10-21; a pipeline need).
 */
export function patchNeeds<T extends NeedItemVM>(items: readonly T[], kokanee: (it: T) => boolean):
  { items: readonly T[]; onlyIf: { label: string; items: readonly T[] } | null } {
  const ko = items.map((it) => /salmon/i.test(it.name + " " + it.when) && !it.when.includes("not kokanee") && kokanee(it)
    ? { ...it, when: it.when.replace(/\.?$/, "") + " " + LIC.NOT_KOKANEE } : it);
  const only = ko.filter((it) => /^Only if\b/i.test(it.when.trim()));
  if (!only.length || only.length === ko.length) return { items: ko, onlyIf: null };
  return { items: ko.filter((it) => !only.includes(it)), onlyIf: { label: LIC.ONLY_IF_HEAD, items: only } };
}

/** The question and the two choices (prototype licence: "Who is fishing?" ⓘ, age, then residency). */
function selectors(who: Who, gloss: ReadonlySet<string>): Pick<LicenceVM, "question" | "questionTerms" | "axes"> {
  return {
    question: LIC.QUESTION, questionTerms: gloss.has(LIC.Q_TERM) ? [LIC.Q_TERM] : [],
    axes: [
      { axis: "age", aria: LIC.QUESTION, options: LIC.AXES.age.map(([v, t]) => ({ value: v === "under_16" ? "u16" : "adult", text: t, checked: v === AGE_DIM[who.age] })) },
      { axis: "res", aria: LIC.QUESTION, options: LIC.AXES.residency.map(([v, t]) => ({ value: v, text: t, checked: v === who.residency })) },
    ],
  };
}

const EMPTY: LicenceVM = Object.freeze({ question: null, questionTerms: [], axes: [], label: "", blocks: [], src: null });

/**
 * The Licence tab for `who`. Null outside B.C. (no key). A closed day draws the prototype's empty section (D16 (a)); a
 * tidal key, page v36's "Tidal water" box with the stored tidal licence words.
 */
export function buildLicence(inp: RegsInputs, who: Who, ctx: { readonly closed: boolean; readonly glossary: ReadonlySet<string> }): LicenceVM | null {
  if (!inp.key) return null;
  if (ctx.closed || !inp.frames) return EMPTY;
  const F = inp.frames.licence, pix = profileOf(who, inp.statics), source: SheetKey = `licsrc:${keyOf(who)}`;
  const head = { ...selectors(who, ctx.glossary), label: inp.statics.profileLabels[pix] ?? "" };
  if (!isHolds(F.holds)) {
    const t = F.holds.tidal as { licence?: string; note?: string };
    return { ...head, blocks: [{ cls: "classbox", head: LIC.TIDAL, terms: [], body: t.licence || t.note || "", source: null }], src: null };
  }
  const H = F.holds, P = F.profiles[pix];
  if (!isAnswer(P)) return { ...head, blocks: [], src: null };
  const FH = F as LicenceValue & { holds: Holds };
  const adultP = F.profiles[profileOf({ age: "adult", residency: "resident" }, inp.statics)];
  const adult = isAnswer(adultP) ? answerBlocks(inp, FH, { age: "adult", residency: "resident" }, adultP, ctx.glossary, source) : null;
  // (b) the "(not kokanee)" words: a stored requirement of this answer that the document meets says them in its label
  const saysNotKokanee = (doc: string): boolean => P.requirements.some((x) => x.paths.some((p) => p.need?.includes(doc))
    && lx(inp, x.req).label.includes(LIC.NOT_KOKANEE));
  const bare = (it: NeedItemVM): NeedItemVM => ({ name: it.name, terms: it.terms, when: it.when, price: it.price });
  const blocks = asNeeds(answerBlocks(inp, FH, who, P, ctx.glossary, source), adult).map((b): LicenceBlockVM => {
    if (b.cls !== "needs") return b;
    const p = patchNeeds(b.items as readonly Item[], (it) => (it.docs ?? []).some(saysNotKokanee));
    return { ...b, items: p.items.map(bare), onlyIf: p.onlyIf && { label: p.onlyIf.label, items: p.onlyIf.items.map(bare) } };
  });
  const words = licenceWho(inp, P, H);
  return { ...head, blocks,
    src: { who: (words.join(LIC.SRC_SEP) || LIC.SOURCES) + LIC.SRC_GO, aria: LIC.SRC_ARIA(words.join(LIC.SEP)), open: source } };
}

// ── plain lines (the golden's blocks[].lines) ───────────────────────────────────────────────────────────────────────

const marks = (terms: readonly string[]): string[] => terms.map(() => WORDS.TERM_ONE);
/** One need as the golden's li text: name, ⓘ per term, when, price. */
export const needLine = (it: NeedItemVM): string => [it.name, ...marks(it.terms), it.when, it.price].filter(Boolean).join(" ");

/** A block's plain lines, one per drawn element — the golden's `blocks[].lines`. */
export function licenceBlockLines(b: LicenceBlockVM): readonly string[] {
  switch (b.cls) {
    case "classbox": return [[b.head, ...marks(b.terms), b.body, b.source ? LIC.SOURCE : ""].filter(Boolean).join(" ")];
    case "classbox youth": return [[b.head, ...marks(b.terms), b.body].filter(Boolean).join(" ")];
    case "flag": return [`${LIC.FLAG} ${b.text}`];
    case "scopenote": return [[b.text, b.source ? LIC.SOURCE : ""].filter(Boolean).join(" ")];
    case "needs": return [b.label, ...b.items.map(needLine), ...(b.onlyIf ? [b.onlyIf.label, ...b.onlyIf.items.map(needLine)] : []),
      ...(b.priceNote ? [b.priceNote] : [])];
    case "paper": return [(b.lead + b.strong + b.tail).replace(/\s+/g, " ").trim()];
    case "nymbox": return [b.label, b.says, b.note];
    case "muted": return [b.text];
    case "lostlist lopen": return [b.label, ...b.cards.flatMap((c) => [c.forWho, c.label, ...c.paths.flatMap((p, i) => i ? [LIC.OR, p] : [p])])];
  }
}
