import type {
  Alternative, Boundary, Designation, Doing, Exemption, LicenceTerms, LicensingRecord,
  NotClassified, Path, Ref, Requirement, When,
} from "../../types";
import { ExtentEditor } from "../ExtentEditor";
import { ExcludesEditor } from "../ExcludesEditor";
import { SpeciesPicker } from "../SpeciesPicker";
import { WhenEditor } from "../WhenEditor";
import { LengthsEditor } from "./LengthsEditor";
import { WhoEditor } from "./WhoEditor";
import {
  Check, Checks, ErrorsAt, F, Num, Pick, RawJson, RowTools, Tags, Text, Tri, move, orNone, put,
  useVocab,
} from "../../model";

interface Ctx {
  path: string;                        // "licensing.2"
  rules: string[];                     // this entry's rule ids — a suspension names one
  boundaries: Boundary[];
  itemNames: Record<string, string>;
  matched: string[];
}

/** A template per kind, holding only the fields the model requires, so "+ add" starts from a
 *  record the validator can talk about field by field. */
export function blankRecord(kind: string, id: string): LicensingRecord {
  switch (kind) {
    case "designation":
      return { kind, id, classified: "II", unit: "", unit_name: "", verbatim: "" };
    case "not_classified":
      return { kind, id, verbatim: "" };
    case "requirement":
      return { kind, id, doing: { act: "fishing" }, satisfied_by: [{ hold: ["basic_licence"] }], verbatim: "" };
    case "licence_terms":
      return { kind, id, document: "classified_waters_licence", verbatim: "" };
    case "exemption":
      return { kind, id, who: {}, documents: [], verbatim: "" };
    default:
      return { kind: "alternative", id, alternative_to: { entry_id: "", id: "" }, satisfied_by: [],
               extents: [], verbatim: "" };
  }
}

function RefEditor({ value, onChange, path }: { value?: Ref | null; onChange: (r: Ref) => void; path: string }) {
  const r = value ?? { entry_id: "", id: "" };
  return (
    <span className="ref">
      <F path={`${path}.entry_id`}><Text value={r.entry_id} placeholder="entry id" label="entry_id"
        onChange={(x) => onChange({ ...r, entry_id: x ?? "" })} /></F>
      <F path={`${path}.id`}><Text value={r.id} placeholder="record id" label="id"
        onChange={(x) => onChange({ ...r, id: x ?? "" })} /></F>
    </span>
  );
}

/** `satisfied_by` — ANY ONE of these paths satisfies the requirement. A path is exactly one of
 *  `hold` (ALL of these documents), `accompanied_by` (a companion `who`), or `as` (satisfy it as
 *  this `who` instead); the last two say whose quota the catch is (`quota`, a NOTE the reader
 *  renders — not arithmetic). */
function PathsEditor({ value, onChange, path }: { value?: Path[]; onChange: (p: Path[] | undefined) => void; path: string }) {
  const v = useVocab();
  const paths = value ?? [];
  const emit = (next: Path[]) => onChange(next);
  const set = (i: number, p: Path) => emit(paths.map((x, j) => (j === i ? p : x)));
  const docWords = Object.fromEntries(v.documents.map((d) => [d.doc, d.words]));
  return (
    <div className="paths">
      {paths.map((p, i) => {
        const mode = p.accompanied_by != null ? "accompanied_by" : p.as != null ? "as" : "hold";
        const at = `${path}.${i}`;
        return (
          <div className="clause" key={i} data-path={at}>
            <div className="clause-head">
              <span className="dim">path {i + 1}{i > 0 ? " — or" : ""}</span>
              <select aria-label="path kind" value={mode} onChange={(e) => {
                const m = e.target.value;
                set(i, m === "hold" ? { hold: [] }
                  : m === "accompanied_by" ? { accompanied_by: { who: { age: ["16_plus"] } }, quota: v.path_quota[0] }
                  : { as: { residency: ["resident"] }, quota: v.path_quota[0] });
              }}>
                <option value="hold">hold documents</option>
                <option value="accompanied_by">be accompanied</option>
                <option value="as">satisfy it as someone else</option>
              </select>
              <RowTools i={i} n={paths.length} onMove={(d) => emit(move(paths, i, d))}
                onRemove={() => emit(paths.filter((_, j) => j !== i))} />
            </div>
            <ErrorsAt path={at} />
            {mode === "hold" && (
              <F path={`${at}.hold`} deep hint="ALL of these">
                <Checks values={p.hold} options={v.documents.map((d) => d.doc)} words={docWords}
                  onChange={(x) => set(i, put(p, { hold: x }))} />
              </F>
            )}
            {mode === "accompanied_by" && p.accompanied_by && (
              <F path={`${at}.accompanied_by.who`} k="accompanied_by.who" deep
                hint="the companion holds whatever this fishing requires of them">
                <WhoEditor value={p.accompanied_by.who}
                  onChange={(w) => set(i, put(p, { accompanied_by: { ...p.accompanied_by!, who: w ?? {} } }))} />
              </F>
            )}
            {mode === "as" && (
              <F path={`${at}.as`} k="as" deep>
                <WhoEditor value={p.as} onChange={(w) => set(i, put(p, { as: w ?? {} }))} />
              </F>
            )}
            {(mode !== "hold" || p.quota != null) && (
              <F path={`${at}.quota`} hint="counts_to_companion = the catch counts against the companion's limit (a note, rendered)">
                <Pick value={p.quota} options={v.path_quota} label="quota" onChange={(x) => set(i, put(p, { quota: x }))} />
              </F>
            )}
          </div>
        );
      })}
      <button type="button" className="btn small" onClick={() => emit([...paths, { hold: [] }])}>+ add path</button>
    </div>
  );
}

function DoingEditor({ value, onChange, path }: { value: Doing; onChange: (d: Doing) => void; path: string }) {
  const v = useVocab();
  const set = (patch: Partial<Doing>) => onChange(put(value, patch));
  return (
    <div className="doing">
      <F path={`${path}.act`}><Pick value={value.act} options={v.doing_acts} none={null} label="act"
        onChange={(x) => set({ act: x ?? "fishing" })} /></F>
      <F path={`${path}.species`} deep hint="targeting / retaining only">
        <SpeciesPicker values={value.species} options={v.species} onChange={(x) => set({ species: x })} />
      </F>
      <F path={`${path}.species_except`} deep>
        <SpeciesPicker values={value.species_except} options={v.species} onChange={(x) => set({ species_except: x })} />
      </F>
      <F path={`${path}.origin`}><Pick value={value.origin} options={v.origins} label="origin"
        onChange={(x) => set({ origin: x })} /></F>
      <F path={`${path}.lengths`} hint="WHICH fish you keep (retaining only) — never how many">
        <LengthsEditor value={value.lengths} path={`${path}.lengths`} noTake
          onChange={(x) => set({ lengths: x })} />
      </F>
    </div>
  );
}

function WhenField({ value, onChange, path }: { value?: When | null; onChange: (w: When | undefined) => void; path: string }) {
  return (
    <F path={path} deep hint="blank = all year">
      <WhenEditor value={value ?? undefined} onChange={onChange} />
    </F>
  );
}

function Where({ rec, set, ctx, tributaries = true }: {
  rec: { extents?: unknown; includes_tributaries?: boolean | null };
  set: (patch: Record<string, unknown>) => void; ctx: Ctx; tributaries?: boolean;
}) {
  const exts = (rec.extents as Designation["extents"]) ?? [];
  return (
    <>
      <F path={`${ctx.path}.extents`} hint="blank takes the entry's, at placement">
        <ExtentEditor extents={exts} boundaries={ctx.boundaries} itemNames={ctx.itemNames}
          path={`${ctx.path}.extents`} onChange={(x) => set({ extents: x.length ? x : undefined })} />
      </F>
      {tributaries && (
        <F path={`${ctx.path}.includes_tributaries`} hint="unset inherits the entry's">
          <Tri value={rec.includes_tributaries} unset="inherit the entry's"
            onChange={(x) => set({ includes_tributaries: x })} />
        </F>
      )}
    </>
  );
}

function DesignationFields({ rec, onChange, ctx }: { rec: Designation; onChange: (r: Designation) => void; ctx: Ctx }) {
  const v = useVocab();
  const set = (patch: Partial<Designation>) => onChange(put(rec, patch));
  const p = ctx.path;
  const stamp = rec.steelhead_stamp_during ? "during" : rec.steelhead_stamp_waived ? "waived" : "none";
  return (
    <>
      <div className="grid3">
        <F path={`${p}.classified`}><Pick value={rec.classified} options={v.classified} none={null} label="classified"
          onChange={(x) => set({ classified: x ?? "II" })} /></F>
        <F path={`${p}.unit`} hint="the licence unit id — a slug"><Text value={rec.unit} label="unit"
          onChange={(x) => set({ unit: x ?? "" })} /></F>
        <F path={`${p}.unit_name`} hint="as the page prints it"><Text value={rec.unit_name} label="unit_name"
          onChange={(x) => set({ unit_name: x ?? "" })} /></F>
      </div>
      <WhenField path={`${p}.when`} value={rec.when} onChange={(x) => set({ when: x })} />
      <Where rec={rec} set={(x) => set(x as Partial<Designation>)} ctx={ctx} />
      <F path={`${p}.tributaries_only`}><Check value={rec.tributaries_only} label="tributaries_only"
        onChange={(x) => set({ tributaries_only: x })} /></F>
      <F path={`${p}.tributary_excludes`} deep>
        <ExcludesEditor itemIds={ctx.matched} excludes={rec.tributary_excludes ?? []}
          onChange={(x) => set({ tributary_excludes: x.length ? x : undefined })} />
      </F>
      <F path={`${p}.steelhead_stamp`} k="steelhead stamp" hint="the classified-water stamp: runs during a period, is waived, or the page says neither">
        <span className="radio-row">
          {(["none", "during", "waived"] as const).map((m) => (
            <label key={m} className="check">
              <input type="radio" name={`${p}-stamp`} checked={stamp === m} onChange={() => set({
                steelhead_stamp_during: m === "during" ? { when: { dates: [] }, verbatim: "" } : undefined,
                steelhead_stamp_waived: m === "waived" ? { verbatim: "" } : undefined,
              })} />
              {m === "none" ? "neither" : m === "during" ? "steelhead_stamp_during" : "steelhead_stamp_waived"}
            </label>
          ))}
        </span>
      </F>
      {rec.steelhead_stamp_during && (
        <div className="sub-record">
          <WhenField path={`${p}.steelhead_stamp_during.when`} value={rec.steelhead_stamp_during.when}
            onChange={(x) => set({ steelhead_stamp_during: { ...rec.steelhead_stamp_during!, when: x ?? {} } })} />
          <F path={`${p}.steelhead_stamp_during.verbatim`}>
            <Text area value={rec.steelhead_stamp_during.verbatim}
              onChange={(x) => set({ steelhead_stamp_during: { ...rec.steelhead_stamp_during!, verbatim: x ?? "" } })} />
          </F>
          <ErrorsAt path={`${p}.steelhead_stamp_during`} />
        </div>
      )}
      {rec.steelhead_stamp_waived && (
        <div className="sub-record">
          <F path={`${p}.steelhead_stamp_waived.verbatim`}>
            <Text area value={rec.steelhead_stamp_waived.verbatim}
              onChange={(x) => set({ steelhead_stamp_waived: { verbatim: x ?? "" } })} />
          </F>
        </div>
      )}
      <F path={`${p}.suspended_while`} hint="dormant while a closure rule of this entry binds">
        {(rec.suspended_while ?? []).map((s, i) => (
          <div key={i} className="band-row">
            <F path={`${p}.suspended_while.${i}.rule_id`}>
              <Pick value={s.rule_id} options={ctx.rules} none={null} label="rule_id"
                onChange={(x) => set({ suspended_while: (rec.suspended_while ?? []).map((y, j) => j === i ? { ...y, rule_id: x ?? "" } : y) })} />
            </F>
            <F path={`${p}.suspended_while.${i}.verbatim`}>
              <Text value={s.verbatim} label="verbatim"
                onChange={(x) => set({ suspended_while: (rec.suspended_while ?? []).map((y, j) => j === i ? { ...y, verbatim: x ?? "" } : y) })} />
            </F>
            <button type="button" className="x" title="remove"
              onClick={() => set({ suspended_while: orNone((rec.suspended_while ?? []).filter((_, j) => j !== i)) })}>×</button>
          </div>
        ))}
        <button type="button" className="btn small" onClick={() => set({
          suspended_while: [...(rec.suspended_while ?? []), { rule_id: ctx.rules[0] ?? "", verbatim: "" }] })}>
          + suspension
        </button>
      </F>
    </>
  );
}

function RequirementFields({ rec, onChange, ctx }: { rec: Requirement; onChange: (r: Requirement) => void; ctx: Ctx }) {
  const v = useVocab();
  const set = (patch: Partial<Requirement>) => onChange(put(rec, patch));
  const p = ctx.path;
  return (
    <>
      <F path={`${p}.doing`} k="doing" hint="the trigger">
        <DoingEditor value={rec.doing} path={`${p}.doing`} onChange={(d) => set({ doing: d })} />
      </F>
      <F path={`${p}.who`} deep hint="blank = every angler">
        <WhoEditor value={rec.who} onChange={(x) => set({ who: x })} />
      </F>
      <F path={`${p}.who_except`} deep>
        <WhoEditor value={rec.who_except} onChange={(x) => set({ who_except: x })} />
      </F>
      <F path={`${p}.satisfied_by`} hint="what to hold — or use conduct for a duty; exactly one of the two">
        <PathsEditor value={rec.satisfied_by} path={`${p}.satisfied_by`}
          onChange={(x) => set({ satisfied_by: orNone(x) })} />
      </F>
      <F path={`${p}.conduct`} deep>
        <Checks values={rec.conduct} options={v.conduct.map((c) => c.act)}
          words={Object.fromEntries(v.conduct.map((c) => [c.act, c.words]))} onChange={(x) => set({ conduct: x })} />
      </F>
      <div className="grid3">
        <F path={`${p}.water`}><Pick value={rec.water} options={v.water_kinds} label="water" onChange={(x) => set({ water: x })} /></F>
        <F path={`${p}.on`} hint="where a designation is in force"><Pick value={rec.on} options={v.requirement_on} label="on"
          onChange={(x) => set({ on: x })} /></F>
        <F path={`${p}.authority`}><Pick value={rec.authority} options={v.authority} none="provincial" label="authority"
          onChange={(x) => set({ authority: x })} /></F>
      </div>
      <WhenField path={`${p}.when`} value={rec.when} onChange={(x) => set({ when: x })} />
      <Where rec={rec} set={(x) => set(x as Partial<Requirement>)} ctx={ctx} />
      <F path={`${p}.restates`} hint="the obligation elsewhere this row's words repeat">
        <Check value={rec.restates != null} label="restates"
          onChange={(on) => set({ restates: on ? { entry_id: "", id: "" } : undefined })}>restates another record</Check>
        {rec.restates && <RefEditor value={rec.restates} path={`${p}.restates`} onChange={(r) => set({ restates: r })} />}
      </F>
    </>
  );
}

function TermsFields({ rec, onChange, ctx }: { rec: LicenceTerms; onChange: (r: LicenceTerms) => void; ctx: Ctx }) {
  const v = useVocab();
  const set = (patch: Partial<LicenceTerms>) => onChange(put(rec, patch));
  const p = ctx.path;
  const words = Object.fromEntries(v.documents.map((d) => [d.doc, d.words]));
  return (
    <>
      <div className="grid3">
        <F path={`${p}.document`}><Pick value={rec.document} options={v.documents.map((d) => d.doc)} none={null}
          words={words} label="document" onChange={(x) => set({ document: x ?? rec.document })} /></F>
        <F path={`${p}.classified`}><Pick value={rec.classified} options={v.classified} label="classified"
          onChange={(x) => set({ classified: x })} /></F>
        <F path={`${p}.sold`}><Pick value={rec.sold} options={v.terms_sold} label="sold" onChange={(x) => set({ sold: x })} /></F>
        <F path={`${p}.covers`}><Pick value={rec.covers} options={v.terms_covers} label="covers" onChange={(x) => set({ covers: x })} /></F>
        <F path={`${p}.allocation`}><Pick value={rec.allocation} options={v.terms_allocation} label="allocation"
          onChange={(x) => set({ allocation: x })} /></F>
        <F path={`${p}.fee_cad`}><Num value={rec.fee_cad} label="fee_cad" onChange={(x) => set({ fee_cad: x })} /></F>
        <F path={`${p}.max_consecutive_days`}><Num value={rec.max_consecutive_days} step={1}
          onChange={(x) => set({ max_consecutive_days: x })} /></F>
        <F path={`${p}.max_days_per_licence_year`}><Num value={rec.max_days_per_licence_year} step={1}
          onChange={(x) => set({ max_days_per_licence_year: x })} /></F>
        <F path={`${p}.max_per_licence_year`}><Num value={rec.max_per_licence_year} step={1}
          onChange={(x) => set({ max_per_licence_year: x })} /></F>
        <F path={`${p}.max_units_per_licence_year`}><Num value={rec.max_units_per_licence_year} step={1}
          onChange={(x) => set({ max_units_per_licence_year: x })} /></F>
        <F path={`${p}.unlimited_days`}><Check value={rec.unlimited_days} label="unlimited_days"
          onChange={(x) => set({ unlimited_days: x })} /></F>
        <F path={`${p}.needs`} deep><Checks values={rec.needs} options={v.terms_needs} onChange={(x) => set({ needs: x })} /></F>
      </div>
      <F path={`${p}.units`} deep hint="licence unit ids; blank = every unit">
        <Tags values={rec.units} label="units" onChange={(x) => set({ units: x })} />
      </F>
      <F path={`${p}.who`} deep><WhoEditor value={rec.who} onChange={(x) => set({ who: x })} /></F>
    </>
  );
}

function ExemptionFields({ rec, onChange, ctx }: { rec: Exemption; onChange: (r: Exemption) => void; ctx: Ctx }) {
  const v = useVocab();
  const set = (patch: Partial<Exemption>) => onChange(put(rec, patch));
  return (
    <>
      <F path={`${ctx.path}.who`} deep hint="checked against the sentence's own words">
        <WhoEditor value={rec.who} onChange={(x) => set({ who: x ?? {} })} />
      </F>
      <F path={`${ctx.path}.documents`} deep hint="released from ALL of these">
        <Checks values={rec.documents} options={v.documents.map((d) => d.doc)}
          words={Object.fromEntries(v.documents.map((d) => [d.doc, d.words]))}
          onChange={(x) => set({ documents: x ?? [] })} />
      </F>
    </>
  );
}

function AlternativeFields({ rec, onChange, ctx }: { rec: Alternative; onChange: (r: Alternative) => void; ctx: Ctx }) {
  const set = (patch: Partial<Alternative>) => onChange(put(rec, patch));
  return (
    <>
      <F path={`${ctx.path}.alternative_to`} hint="the requirement this adds a path to">
        <RefEditor value={rec.alternative_to} path={`${ctx.path}.alternative_to`} onChange={(r) => set({ alternative_to: r })} />
      </F>
      <F path={`${ctx.path}.satisfied_by`}>
        <PathsEditor value={rec.satisfied_by} path={`${ctx.path}.satisfied_by`} onChange={(x) => set({ satisfied_by: x ?? [] })} />
      </F>
      <F path={`${ctx.path}.extents`} hint="REQUIRED — an alternative is accepted somewhere, never everywhere">
        <ExtentEditor extents={rec.extents ?? []} boundaries={ctx.boundaries} itemNames={ctx.itemNames}
          path={`${ctx.path}.extents`} onChange={(x) => set({ extents: x })} />
      </F>
    </>
  );
}

/** Every field of one licensing record, by kind. `kind` is the union's tag and is fixed once the
 *  record exists — a different kind is a different record (remove it and add the other). */
export function LicensingEditor({ rec, onChange, ctx }: {
  rec: LicensingRecord; onChange: (r: LicensingRecord) => void; ctx: Ctx;
}) {
  const p = ctx.path;
  const common = (patch: Partial<LicensingRecord>) => onChange(put(rec, patch as Partial<typeof rec>));
  return (
    <div className="record-editor">
      <div className="grid2">
        <F path={`${p}.kind`} hint="fixed — a different kind is a different record"><code>{rec.kind}</code></F>
        <F path={`${p}.id`} hint="unique within this entry"><Text value={rec.id} label="id"
          onChange={(x) => common({ id: x ?? "" })} /></F>
      </div>
      <F path={`${p}.verbatim`} hint="a contiguous quote of the entry's regs_verbatim">
        <Text area value={rec.verbatim} label="verbatim" onChange={(x) => common({ verbatim: x ?? "" })} />
      </F>
      {rec.kind === "designation" && <DesignationFields rec={rec} onChange={onChange} ctx={ctx} />}
      {rec.kind === "not_classified" && (
        <Where rec={rec as NotClassified} set={(x) => onChange(put(rec, x as Partial<NotClassified>))} ctx={ctx}
          tributaries={false} />
      )}
      {rec.kind === "requirement" && <RequirementFields rec={rec} onChange={onChange} ctx={ctx} />}
      {rec.kind === "licence_terms" && <TermsFields rec={rec} onChange={onChange} ctx={ctx} />}
      {rec.kind === "exemption" && <ExemptionFields rec={rec} onChange={onChange} ctx={ctx} />}
      {rec.kind === "alternative" && <AlternativeFields rec={rec} onChange={onChange} ctx={ctx} />}
      <F path={`${p}.review_reason`}>
        <Text area value={rec.review_reason} label="review_reason" onChange={(x) => common({ review_reason: x })} />
      </F>
      <ErrorsAt path={p} />
      <RawJson value={withoutLabel(rec)} label="raw JSON (this record)" onChange={(x) => onChange(x as LicensingRecord)} />
    </div>
  );
}

function withoutLabel<T extends { label?: string }>(x: T): Omit<T, "label"> {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { label, ...rest } = x;
  return rest;
}
