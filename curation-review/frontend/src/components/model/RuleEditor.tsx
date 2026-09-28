import { useContext, type ReactNode } from "react";
import type { Boundary, Rule } from "../../types";
import { ExtentEditor } from "../ExtentEditor";
import { ExcludesEditor } from "../ExcludesEditor";
import { SpeciesPicker } from "../SpeciesPicker";
import { WhenEditor } from "../WhenEditor";
import { GearEditor } from "./GearEditor";
import { LengthsEditor } from "./LengthsEditor";
import { ExemptsEditor } from "./ExemptsEditor";
import { WhoEditor } from "./WhoEditor";
import {
  Check, Checks, ErrorsAt, ErrorsCtx, F, Num, Pick, RawJson, Tags, Text, Tri, fieldErrors, put, useVocab,
} from "../../model";

const RETENTION_TYPES = ["retention_limit", "stop_fishing_after_quota"];
const VESSEL_TYPES = ["vessel_rule"];
const RETENTION_KEYS: (keyof Rule)[] = ["take", "unlimited", "may_target", "per_daily", "within",
  "lengths", "record_retention"];
const VESSEL_KEYS: (keyof Rule)[] = ["aspect", "level", "max_power_kw", "max_kmh"];

function said(r: Rule, keys: (keyof Rule)[]): boolean {
  return keys.some((k) => {
    const v = r[k];
    return v !== undefined && v !== null && v !== false && !(Array.isArray(v) && v.length === 0);
  });
}

/** A group of fields, open when it matters for this type, holds a value, or holds an error. */
function Group({ title, path, keys, open, children }: {
  title: string; path: string; keys: string[]; open: boolean; children: ReactNode;
}) {
  const errors = useContext(ErrorsCtx);
  const hot = keys.some((k) => {
    const { own, named } = fieldErrors(errors, `${path}.${k}`, true);
    return own.length + named.length > 0;
  });
  return (
    <details className={`group${hot ? " hot" : ""}`} open={open || hot}>
      <summary>{title}</summary>
      {children}
    </details>
  );
}

interface Props {
  rule: Rule;
  path: string;                           // "rules.3"
  onChange: (r: Rule) => void;
  siblings: string[];                     // the entry's other rule ids
  boundaries: Boundary[];
  itemNames: Record<string, string>;
  matched: string[];
}

/** Every field of a `CatalogueRule`, labelled with the model's own key. Groups follow the model's
 *  own sections (who/what/when · retention · gear · vessel · relations · where · review); a group
 *  the rule's type does not use starts closed, and opens when it holds a value or an error. */
export function RuleEditor({ rule: r, path, onChange, siblings, boundaries, itemNames, matched }: Props) {
  const v = useVocab();
  const set = (patch: Partial<Rule>) => onChange(put(r, patch));
  const others = siblings.filter((s) => s !== r.rule_id);
  const isRetention = RETENTION_TYPES.includes(r.type);
  const isVessel = VESSEL_TYPES.includes(r.type);
  const isGear = ["bait_restriction", "tackle_restriction", "method_rule", "handling_rule"].includes(r.type);

  return (
    <div className="rule-editor">
      <div className="grid2">
        <F path={`${path}.type`}>
          <select aria-label="type" value={r.type} onChange={(e) => set({ type: e.target.value })}>
            {v.rule_types.map((t) => <option key={t.type} value={t.type}>{t.type} · {t.family}</option>)}
          </select>
        </F>
        <F path={`${path}.obligation`} hint="law (must) or advice (should)">
          <Pick value={r.obligation} options={v.obligations} none="must (default)"
            onChange={(x) => set({ obligation: x })} />
        </F>
      </div>
      <F path={`${path}.verbatim`} hint="must be a contiguous quote of the entry's regs_verbatim">
        <Text area value={r.verbatim} label="verbatim" onChange={(x) => set({ verbatim: x ?? "" })} />
      </F>

      <Group title="who / what" path={path} open
        keys={["species", "species_except", "when_targeting", "closed_to", "closed_to_except", "water", "origin", "life_stage"]}>
        <F path={`${path}.species`} deep>
          <SpeciesPicker values={r.species} options={v.species} label="species"
            onChange={(x) => set({ species: x })} />
        </F>
        <F path={`${path}.species_except`} deep>
          <SpeciesPicker values={r.species_except} options={v.species} label="species_except"
            onChange={(x) => set({ species_except: x })} />
        </F>
        {(["bait_restriction", "tackle_restriction"].includes(r.type) || (r.when_targeting ?? []).length > 0) && (
          <F path={`${path}.when_targeting`} deep hint="the species a bait/tackle rule is ABOUT — not what you may catch">
            <SpeciesPicker values={r.when_targeting} options={v.species} label="when_targeting"
              onChange={(x) => set({ when_targeting: x })} />
          </F>
        )}
        {(r.type === "angler_closure" || r.closed_to != null) && (
          <F path={`${path}.closed_to`} deep hint="WHO the water is closed to — must match the sentence's residency/guidance words">
            <WhoEditor value={r.closed_to} onChange={(x) => set({ closed_to: x })} />
          </F>
        )}
        {(r.type === "angler_closure" || (r.closed_to_except ?? []).length > 0) && (
          <F path={`${path}.closed_to_except`} deep hint="the anglers INSIDE closed_to the water stays open to — each must meet closed_to">
            <div className="who-list">
              {(r.closed_to_except ?? []).map((w, k) => (
                <div key={k} className="who-item">
                  <WhoEditor value={w} onChange={(x) => {
                    const next = [...(r.closed_to_except ?? [])];
                    if (x) next[k] = x; else next.splice(k, 1);
                    set({ closed_to_except: next.length ? next : undefined });
                  }} />
                </div>
              ))}
              <button type="button" onClick={() => set({ closed_to_except: [...(r.closed_to_except ?? []), { age: ["16_plus"] }] })}>
                + except
              </button>
            </div>
          </F>
        )}
        <div className="grid2">
          <F path={`${path}.water`}><Pick value={r.water} options={v.water_kinds} label="water"
            onChange={(x) => set({ water: x })} /></F>
          <F path={`${path}.origin`}><Pick value={r.origin} options={v.origins} label="origin"
            onChange={(x) => set({ origin: x })} /></F>
          <F path={`${path}.life_stage`} hint="a life stage the book defines, as the sentence prints it ('adult chinook', p.77)">
            <Pick value={r.life_stage} options={v.life_stages} label="life_stage" none="every stage"
              onChange={(x) => set({ life_stage: x })} /></F>
        </div>
      </Group>

      <Group title="retention" path={path} open={isRetention || said(r, RETENTION_KEYS)}
        keys={[...RETENTION_KEYS, "period"] as string[]}>
        <div className="grid3">
          <F path={`${path}.take`}><Num label="take" value={r.take} step={1} onChange={(x) => set({ take: x })} /></F>
          <F path={`${path}.unlimited`}><Check label="unlimited" value={r.unlimited} onChange={(x) => set({ unlimited: x })} /></F>
          <F path={`${path}.may_target`} hint="with take 0: false = may not fish for it, true = catch and release">
            <Tri label="may_target" value={r.may_target} onChange={(x) => set({ may_target: x })}
              yes="true — fish for it, release" no="false — may not fish for it" />
          </F>
          <F path={`${path}.period`}><Pick value={r.period} options={v.periods} none="daily (default)" label="period"
            onChange={(x) => set({ period: x })} /></F>
          <F path={`${path}.per_daily`} hint="possession = N daily quotas"><Num label="per_daily" value={r.per_daily} step={1}
            onChange={(x) => set({ per_daily: x })} /></F>
          <F path={`${path}.within`} hint="the parent quota's rule id">
            <input type="text" aria-label="within" list={`${path}-within`} value={r.within ?? ""}
              onChange={(e) => set({ within: e.target.value || undefined })} />
            <datalist id={`${path}-within`}>{others.map((s) => <option key={s} value={s} />)}</datalist>
          </F>
          <F path={`${path}.record_retention`}><Check label="record_retention" value={r.record_retention}
            onChange={(x) => set({ record_retention: x })} /></F>
        </div>
        <F path={`${path}.lengths`} hint="ordered size ranges; first match wins">
          <LengthsEditor value={r.lengths} path={`${path}.lengths`} onChange={(x) => set({ lengths: x })} />
        </F>
      </Group>

      <Group title="when" path={path} open={r.when != null} keys={["when"]}>
        <F path={`${path}.when`} deep>
          <WhenEditor value={r.when ?? undefined} onChange={(x) => set({ when: x })} />
        </F>
      </Group>

      <Group title="gear · while · conduct" path={path}
        open={isGear || said(r, ["gear", "while", "conduct"])} keys={["gear", "while", "conduct"]}>
        <F path={`${path}.gear`}>
          <GearEditor value={r.gear} path={`${path}.gear`} onChange={(x) => set({ gear: x })} />
        </F>
        <F path={`${path}.while`} deep hint="binds only while doing this">
          <Checks values={r.while} options={v.while} onChange={(x) => set({ while: x })} />
        </F>
        <F path={`${path}.conduct`} deep>
          <Checks values={r.conduct} options={v.conduct.map((c) => c.act)}
            words={Object.fromEntries(v.conduct.map((c) => [c.act, c.words]))}
            onChange={(x) => set({ conduct: x })} />
        </F>
      </Group>

      <Group title="vessel" path={path} open={isVessel || said(r, VESSEL_KEYS)} keys={VESSEL_KEYS as string[]}>
        <div className="grid2">
          <F path={`${path}.aspect`}><Pick value={r.aspect} options={v.vessel_aspects} label="aspect"
            onChange={(x) => set({ aspect: x })} /></F>
          <F path={`${path}.level`}><Pick value={r.level} options={v.propulsion_levels} label="level"
            onChange={(x) => set({ level: x })} /></F>
          <F path={`${path}.max_power_kw`}><Num label="max_power_kw" value={r.max_power_kw}
            onChange={(x) => set({ max_power_kw: x })} /></F>
          <F path={`${path}.max_kmh`}><Num label="max_kmh" value={r.max_kmh} onChange={(x) => set({ max_kmh: x })} /></F>
        </div>
      </Group>

      <Group title="relations" path={path}
        open={said(r, ["exempts", "suspended_while", "derived_from", "condition_of", "standing", "authority", "notice"])}
        keys={["exempts", "suspended_while", "derived_from", "condition_of", "standing", "authority", "notice"]}>
        <F path={`${path}.exempts`} hint="what this rule LIFTS">
          <ExemptsEditor value={r.exempts} path={`${path}.exempts`} siblings={others}
            onChange={(x) => set({ exempts: x })} />
        </F>
        <div className="grid2">
          <F path={`${path}.suspended_while`} hint="dormant while that rule binds">
            <Pick value={r.suspended_while} options={others} label="suspended_while"
              onChange={(x) => set({ suspended_while: x })} />
          </F>
          <F path={`${path}.condition_of`} hint="the rule this is the proviso of">
            <Pick value={r.condition_of} options={others} label="condition_of"
              onChange={(x) => set({ condition_of: x })} />
          </F>
          <F path={`${path}.derived_from`} hint="the rule whose sentence asserts this one">
            <Pick value={r.derived_from} options={others} label="derived_from"
              onChange={(x) => set({ derived_from: x })} />
          </F>
          <F path={`${path}.authority`}>
            <Pick value={r.authority} options={v.authority} label="authority" none="provincial"
              onChange={(x) => set({ authority: x })} />
          </F>
          <F path={`${path}.notice`} hint="a DFO fishery notice number, FN####">
            <Text value={r.notice} label="notice" onChange={(x) => set({ notice: x })} />
          </F>
          <F path={`${path}.standing`} hint="its extent is unknowable — needs a review_reason">
            <Check label="standing" value={r.standing} onChange={(x) => set({ standing: x })} />
          </F>
        </div>
      </Group>

      <Group title="where" path={path} open
        keys={["extents", "includes_tributaries", "tributaries_only", "tributary_excludes", "extent_text", "undrawn_part", "side", "unresolved_locators"]}>
        <F path={`${path}.extents`} hint="the rule's own reach — never inherited from the entry">
          <ExtentEditor extents={r.extents ?? []} boundaries={boundaries} itemNames={itemNames}
            path={`${path}.extents`} onChange={(x) => set({ extents: x.length ? x : undefined })} />
        </F>
        <div className="grid2">
          <F path={`${path}.includes_tributaries`} hint="unset inherits the entry's">
            <Tri label="includes_tributaries" value={r.includes_tributaries} unset="inherit the entry's"
              onChange={(x) => set({ includes_tributaries: x })} />
          </F>
          <F path={`${path}.tributaries_only`} hint="the tributaries WITHOUT the mainstem">
            <Check label="tributaries_only" value={r.tributaries_only} onChange={(x) => set({ tributaries_only: x })} />
          </F>
        </div>
        <F path={`${path}.tributary_excludes`} deep>
          <ExcludesEditor itemIds={matched} excludes={r.tributary_excludes ?? []}
            onChange={(x) => set({ tributary_excludes: x.length ? x : undefined })} />
        </F>
        <F path={`${path}.extent_text`} hint="the place in the page's words, when no cut-point expresses it">
          <Text value={r.extent_text} label="extent_text" onChange={(x) => set({ extent_text: x })} />
        </F>
        <F path={`${path}.undrawn_part`} hint="holds only in this part of what the extents draw, and nothing draws the part — shown as a note, never coloured (replaces extent_text)">
          <Text value={r.undrawn_part} label="undrawn_part" onChange={(x) => set({ undrawn_part: x })} />
        </F>
        <F path={`${path}.side`} hint="holds on this half of the river's channel only ('on the west half of river') — placed on the stretch, shown beside the other half's rules">
          <Pick value={r.side} options={v.channel_sides} label="side" none="the whole width"
            onChange={(x) => set({ side: x })} />
        </F>
        <F path={`${path}.unresolved_locators`} deep hint="phrases nobody could bind — needs a review_reason">
          <Tags values={r.unresolved_locators} label="unresolved_locators"
            onChange={(x) => set({ unresolved_locators: x })} />
        </F>
      </Group>

      <F path={`${path}.review_reason`} hint="a reason present IS the flag">
        <Text area value={r.review_reason} label="review_reason" onChange={(x) => set({ review_reason: x })} />
      </F>
      <ErrorsAt path={path} />
      <RawJson value={stripLabel(r)} label="raw JSON (this rule)" onChange={(x) => onChange(x as Rule)} />
    </div>
  );
}

function stripLabel<T extends { label?: string }>(x: T): Omit<T, "label"> {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const { label, ...rest } = x;
  return rest;
}
