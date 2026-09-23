/**
 * Licensing, out of the bundle. DATA ACCESS ONLY — no answer is composed here.
 *
 * What a person must hold on a water, as whom, doing what, is one sentence composed in `core/`
 * (AGENTS 33: where a test can pin the whole string). This reads the records it is composed
 * from: which designations, not-classified assertions, requirements and alternatives bind a
 * section, and the records that bind no section at all — province-wide requirements, the ones
 * that hold wherever a designation is in force, sale terms and exemptions.
 *
 * LICENSING NEVER DECIDES OPEN OR CLOSED. Nothing here feeds `evaluate`.
 *
 * THE UNSAFE DIRECTION IS UNDER-REQUIRING. A record the build could not place (`uncertain`),
 * a tributary binding whose walk was not done (`trib_pending`), and a designation on a section a
 * not-classified record also binds (`contested`) are all returned — flagged — so a screen can say
 * "check", never "nothing needed".
 *
 * Written against `pipeline/deliver/bundle/schema.sql`. The record tables are small (about a
 * hundred rows), so they are read once and held; only the section sets are asked per viewport.
 */
import type { SectionId } from "../index";
import { json, str, type Cell, type Db, type Row } from "./db";
import { placeholders } from "./queries";

// ---- the shapes -----------------------------------------------------------------------

/** How a record reaches a section. See `licensing_set.via` in schema.sql. */
export type LicensingVia = "reach" | "trib" | "trib_pending" | "contested";
/** Where a placed record ended up. `unresolved` records are `uncertain`. */
export type Placement = "sections" | "province" | "on_designation" | "unresolved";

export type Residency = "resident" | "non_resident" | "non_resident_alien";
export type Age = "under_16" | "16_plus";
export type Guidance = "guided" | "non_guided";
export type AnglerStatus = "indian_bc_resident" | "metis" | "disabled";

/** WHICH ANGLERS: per axis, the members included. An absent axis is every member. */
export interface Who {
  readonly residency?: readonly Residency[];
  readonly age?: readonly Age[];
  readonly guidance?: readonly Guidance[];
  readonly status?: readonly AnglerStatus[];
}

/** The trigger: what the angler is doing. `lengths` only with `retaining`. */
export interface Doing {
  readonly act: "fishing" | "targeting" | "retaining" | "retaining_recorded" | "guiding";
  readonly species?: readonly string[];
  readonly species_except?: readonly string[];
  readonly origin?: string;
  readonly lengths?: readonly { readonly min_cm?: number; readonly max_cm?: number }[];
}

/** One way to satisfy a requirement: exactly one of `hold` / `accompanied_by` / `as`. */
export interface Path {
  readonly hold?: readonly string[];
  readonly accompanied_by?: { readonly who: Who; readonly holding: "what_this_fishing_requires" };
  readonly as?: Who;
  /** A NOTE, never arithmetic: whose limit the catch counts against. */
  readonly quota?: "own" | "counts_to_companion";
}

/** The catalogue's `When` as the record carries it (by alias). */
export interface RawWhen {
  readonly dates?: readonly { from_month: number; from_day: number; to_month: number; to_day: number }[];
  readonly weekdays?: readonly string[];
  readonly unparsed?: readonly string[];
}

interface Base {
  readonly entryId: string;
  readonly id: string;
  /** GENERATED from the structure by the pipeline — never authored. */
  readonly label: string;
  /** The sentence, quoted from the entry's passage. */
  readonly verbatim: string;
  /** What a curator still has to settle, or null. */
  readonly reviewReason: string | null;
}
interface PlacedBase extends Base {
  readonly placement: Placement;
  /** The build could not place it. It must read "check", never "none". */
  readonly uncertain: boolean;
  /** Why it could not be placed, when it could not. */
  readonly unresolved: string | null;
}

export interface Designation extends PlacedBase {
  readonly kind: "designation";
  readonly classified: "I" | "II";
  /** The licence unit a non-resident's day licence names. Shared unit = shared licence. */
  readonly unit: string;
  readonly unitName: string;
  /** The classified period; null = all year (which includes "when open"). */
  readonly when: RawWhen | null;
  readonly stampDuring: { readonly when: RawWhen; readonly verbatim: string } | null;
  readonly stampWaived: { readonly verbatim: string } | null;
  /** Dormant while any of these closure rules (in this entry) is in force. */
  readonly suspendedWhile: readonly { readonly rule_id: string; readonly verbatim: string }[];
}
export interface NotClassified extends PlacedBase {
  readonly kind: "not_classified";
}
export interface Requirement extends PlacedBase {
  readonly kind: "requirement";
  /** Any ONE of these satisfies it. Empty when it is a duty (`conduct`). */
  readonly satisfiedBy: readonly Path[];
  readonly conduct: readonly string[];
  readonly who: Who | null;
  readonly whoExcept: Who | null;
  readonly doing: Doing;
  readonly water: string | null;
  /** Holds wherever a designation is in force (and, with sections, only where both hold). */
  readonly onDesignation: "classified_period" | "steelhead_period" | null;
  /** `superior`: a National Park — provincial documents are not valid there. */
  readonly authority: "superior" | null;
  readonly when: RawWhen | null;
}
export interface LicenceTerms extends Base {
  readonly kind: "licence_terms";
  readonly document: string;
  readonly who: Who | null;
  readonly classified: "I" | "II" | null;
  /** Licence units these terms are about; empty = every unit. */
  readonly units: readonly string[];
  /** The terms themselves (sold, covers, day limits, allocation, fee …), as the record says. */
  readonly terms: Readonly<Record<string, unknown>>;
}
export interface Exemption extends Base {
  readonly kind: "exemption";
  readonly who: Who;
  readonly documents: readonly string[];
}
export interface Alternative extends PlacedBase {
  readonly kind: "alternative";
  readonly alternativeTo: { readonly entryId: string; readonly id: string };
  readonly satisfiedBy: readonly Path[];
}

export interface Licence {
  readonly id: string;
  readonly name: string;
  /** Sold to an angler under the Wildlife Act — what "any licence or stamp" means. */
  readonly provincial: boolean;
}

/** A record as it binds ONE section. */
export interface Bound<T> {
  readonly record: T;
  readonly via: LicensingVia;
}

export interface SectionLicensing {
  readonly designations: readonly Bound<Designation>[];
  readonly notClassified: readonly Bound<NotClassified>[];
  readonly requirements: readonly Bound<Requirement>[];
  readonly alternatives: readonly Bound<Alternative>[];
}

/** Everything that binds no section: read once, applied by the reader's own logic. */
export interface Unplaced {
  /** Every angler in B.C. (subject to who/doing): the basic licence, the salmon stamp … */
  readonly province: readonly Requirement[];
  /** Wherever a designation is in force: the Classified Waters Licence, the classified stamp. */
  readonly onDesignation: readonly Requirement[];
  /** Could not be placed. Each must render as "check". */
  readonly unresolved: readonly (Designation | NotClassified | Requirement | Alternative)[];
  readonly terms: readonly LicenceTerms[];
  readonly exemptions: readonly Exemption[];
}

export interface LicensingReader {
  /** What binds each of these sections. A section with nothing is ABSENT, not empty. */
  forSections(sections: readonly SectionId[]): Promise<ReadonlyMap<SectionId, SectionLicensing>>;
  unplaced(): Promise<Unplaced>;
  licences(): Promise<readonly Licence[]>;
}

// ---- the queries ----------------------------------------------------------------------

export const DESIGNATIONS = "SELECT * FROM designation ORDER BY entry_id, designation_id";
export const NOT_CLASSIFIED = "SELECT * FROM not_classified ORDER BY entry_id, not_classified_id";
export const REQUIREMENTS = "SELECT * FROM requirement ORDER BY entry_id, req_id";
export const TERMS = "SELECT * FROM licence_terms ORDER BY entry_id, terms_id";
export const EXEMPTIONS = "SELECT * FROM exemption ORDER BY entry_id, exemption_id";
export const ALTERNATIVES = "SELECT * FROM alternative ORDER BY entry_id, alternative_id";
export const LICENCES = "SELECT doc_id, name, provincial FROM licence ORDER BY doc_id";
/** Two joins and no expansion, exactly as `rulesForSections` reads the rule sets. */
export const licensingForSections = (n: number) =>
  "SELECT sl.sid, ls.kind, ls.entry_id, ls.record_id, ls.via " +
  "FROM section_licensing sl JOIN licensing_set ls ON ls.set_id = sl.set_id " +
  `WHERE sl.sid IN (${placeholders(n)})`;

// ---- the reader -----------------------------------------------------------------------

const VIAS = new Set<LicensingVia>(["reach", "trib", "trib_pending", "contested"]);
const PLACEMENTS = new Set<Placement>(["sections", "province", "on_designation", "unresolved"]);

type Rec = Record<string, unknown>;
const opt = <T>(v: unknown): T | null => (v === undefined || v === null ? null : (v as T));
const nullable = (v: Cell): string | null => (v == null || v === "" ? null : String(v));

function base(r: Row): Base {
  return { entryId: str(r.entry_id), id: "", label: str(r.label), verbatim: str(r.verbatim),
           reviewReason: nullable(r.review_reason) };
}
function placed(r: Row, where: string): Omit<PlacedBase, keyof Base> {
  const placement = str(r.placement) as Placement;
  // A placement this client does not know is a build that added one without telling us. It
  // must not silently read as bound; it reads as uncertain.
  const known = PLACEMENTS.has(placement);
  if (!known) console.warn(`${where}: unknown placement ${JSON.stringify(placement)}`);
  return { placement: known ? placement : "unresolved",
           uncertain: !known || Number(r.uncertain) === 1,
           unresolved: nullable(r.unresolved) };
}
const recordOf = (r: Row, where: string): Rec => json<Rec>(r.record, `${where} record`);

function toDesignation(r: Row): Designation {
  const where = `designation ${str(r.entry_id)}#${str(r.designation_id)}`;
  const x = recordOf(r, where);
  return {
    ...base(r), id: str(r.designation_id), ...placed(r, where), kind: "designation",
    classified: str(r.classified) as "I" | "II", unit: str(r.unit), unitName: str(r.unit_name),
    when: opt<RawWhen>(x.when),
    stampDuring: opt(x.steelhead_stamp_during), stampWaived: opt(x.steelhead_stamp_waived),
    suspendedWhile: (x.suspended_while as Designation["suspendedWhile"]) ?? [],
  };
}
function toNotClassified(r: Row): NotClassified {
  const where = `not_classified ${str(r.entry_id)}#${str(r.not_classified_id)}`;
  return { ...base(r), id: str(r.not_classified_id), ...placed(r, where), kind: "not_classified" };
}
function toRequirement(r: Row): Requirement {
  const where = `requirement ${str(r.entry_id)}#${str(r.req_id)}`;
  const x = recordOf(r, where);
  return {
    ...base(r), id: str(r.req_id), ...placed(r, where), kind: "requirement",
    satisfiedBy: (x.satisfied_by as Path[]) ?? [], conduct: (x.conduct as string[]) ?? [],
    who: opt<Who>(x.who), whoExcept: opt<Who>(x.who_except), doing: x.doing as Doing,
    water: nullable(r.water),
    onDesignation: nullable(r.on_designation) as Requirement["onDesignation"],
    authority: nullable(r.authority) as Requirement["authority"],
    when: opt<RawWhen>(x.when),
  };
}
/** The fields of a terms record that are not its identity — the terms themselves. */
const NOT_TERMS = new Set(["kind", "id", "document", "who", "classified", "units", "verbatim",
                           "review_reason"]);
function toTerms(r: Row): LicenceTerms {
  const x = recordOf(r, `licence_terms ${str(r.entry_id)}#${str(r.terms_id)}`);
  return {
    ...base(r), id: str(r.terms_id), kind: "licence_terms", document: str(r.document),
    who: opt<Who>(x.who), classified: nullable(r.classified) as "I" | "II" | null,
    units: json<string[]>(r.units, "licence_terms units"),
    terms: Object.fromEntries(Object.entries(x).filter(([k]) => !NOT_TERMS.has(k))),
  };
}
function toExemption(r: Row): Exemption {
  const x = recordOf(r, `exemption ${str(r.entry_id)}#${str(r.exemption_id)}`);
  return { ...base(r), id: str(r.exemption_id), kind: "exemption", who: x.who as Who,
           documents: json<string[]>(r.documents, "exemption documents") };
}
function toAlternative(r: Row): Alternative {
  const where = `alternative ${str(r.entry_id)}#${str(r.alternative_id)}`;
  const x = recordOf(r, where);
  return {
    ...base(r), id: str(r.alternative_id), ...placed(r, where), kind: "alternative",
    alternativeTo: { entryId: str(r.alternative_to_entry), id: str(r.alternative_to_id) },
    satisfiedBy: (x.satisfied_by as Path[]) ?? [],
  };
}

interface Records {
  designation: Map<string, Designation>;
  not_classified: Map<string, NotClassified>;
  requirement: Map<string, Requirement>;
  alternative: Map<string, Alternative>;
  terms: LicenceTerms[];
  exemptions: Exemption[];
}
const key = (entryId: string, id: string) => `${entryId}\u0000${id}`;

export function makeLicensingReader(db: Db): LicensingReader {
  let loaded: Promise<Records> | null = null;
  const records = (): Promise<Records> => (loaded ??= (async () => {
    const byKey = <T extends Base>(xs: T[]) => new Map(xs.map((x) => [key(x.entryId, x.id), x]));
    return {
      designation: byKey((await db.all(DESIGNATIONS)).map(toDesignation)),
      not_classified: byKey((await db.all(NOT_CLASSIFIED)).map(toNotClassified)),
      requirement: byKey((await db.all(REQUIREMENTS)).map(toRequirement)),
      alternative: byKey((await db.all(ALTERNATIVES)).map(toAlternative)),
      terms: (await db.all(TERMS)).map(toTerms),
      exemptions: (await db.all(EXEMPTIONS)).map(toExemption),
    };
  })());

  return {
    async forSections(sections) {
      const out = new Map<SectionId, {
        designations: Bound<Designation>[]; notClassified: Bound<NotClassified>[];
        requirements: Bound<Requirement>[]; alternatives: Bound<Alternative>[];
      }>();
      if (sections.length === 0) return out;
      const recs = await records();
      // Chunked for the same reason as the rule sets: SQLite's bound-parameter ceiling.
      const CHUNK = 900;
      for (let i = 0; i < sections.length; i += CHUNK) {
        const chunk = sections.slice(i, i + CHUNK);
        for (const r of await db.all(licensingForSections(chunk.length), ...chunk)) {
          const sid = Number(r.sid) as SectionId;
          const kind = str(r.kind);
          const via = str(r.via) as LicensingVia;
          const k = key(str(r.entry_id), str(r.record_id));
          // An unknown `via` reads as contested — "check" — never as a clean binding.
          const how: LicensingVia = VIAS.has(via) ? via : "contested";
          const slot = out.get(sid) ?? { designations: [], notClassified: [], requirements: [],
                                         alternatives: [] };
          const miss = () => { throw new Error(
            `licensing_set names ${kind} ${str(r.entry_id)}#${str(r.record_id)}, which the ` +
            "bundle does not hold — the build checks this, so the bundle is damaged"); };
          if (kind === "designation")
            slot.designations.push({ record: recs.designation.get(k) ?? miss(), via: how });
          else if (kind === "not_classified")
            slot.notClassified.push({ record: recs.not_classified.get(k) ?? miss(), via: how });
          else if (kind === "requirement")
            slot.requirements.push({ record: recs.requirement.get(k) ?? miss(), via: how });
          else if (kind === "alternative")
            slot.alternatives.push({ record: recs.alternative.get(k) ?? miss(), via: how });
          else throw new Error(`licensing_set: unknown kind ${JSON.stringify(kind)}`);
          out.set(sid, slot);
        }
      }
      return out;
    },

    async unplaced() {
      const recs = await records();
      const reqs = [...recs.requirement.values()];
      const all = [...recs.designation.values(), ...recs.not_classified.values(), ...reqs,
                   ...recs.alternative.values()];
      return {
        province: reqs.filter((r) => r.placement === "province"),
        onDesignation: reqs.filter((r) => r.placement === "on_designation"),
        unresolved: all.filter((r) => r.placement === "unresolved"),
        terms: recs.terms,
        exemptions: recs.exemptions,
      };
    },

    async licences() {
      return (await db.all(LICENCES)).map((r) => ({
        id: str(r.doc_id), name: str(r.name), provincial: Number(r.provincial) === 1 }));
    },
  };
}
