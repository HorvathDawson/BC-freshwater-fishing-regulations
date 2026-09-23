import { useEffect, useMemo, useState } from "react";
import type { Boundary, Entry, EntryDetail as EntryDetailT, EntryReaches, ItemSearchResult, Rule, SpeciesOption } from "../types";
import { api, type ValidationError } from "../api";
import { reachIdentities } from "../format";
import { ExtentEditor } from "./ExtentEditor";
import { SpeciesPicker } from "./SpeciesPicker";
import { AttachItem } from "./AttachItem";
import { ITEM_COLORS, MapPanel } from "./MapPanel";
import { SplitEditor } from "./SplitEditor";

interface Props {
  detail: EntryDetailT;
  speciesOptions: SpeciesOption[];
  onSaved: () => void;
  /** jump to another entry (a related row over the same water) */
  onNavigate?: (entryId: string) => void;
  /** bumped after a graph rebuild — forces the map to refetch geometry for the same item */
  reloadKey?: number;
}

// The 15 catalogue types, fetched from the backend so this list cannot drift from the model.
// It used to hold the 6 retired coarse kinds, every one of which the model now refuses.
const FALLBACK_TYPES = [
  "retention_limit", "stop_fishing_after_quota", "bait_restriction", "tackle_restriction",
  "method_rule", "vessel_rule", "angling_from_vessel_prohibited", "navigation_duty",
  "angler_closure", "handling_rule", "hazard", "advisory",
  "program_membership", "facility",
];

// Curated-split state vs the built graph, for the colour-coded chip badge.
function boundaryState(b: Boundary): { cls: string; text: string; title: string } | null {
  if (!b.curated) return null;
  if (b.in_graph && b.in_splits) {
    if (b.live && b.live.label !== b.label)
      return { cls: "edited", text: "edited", title: "differs from the built graph — rebuild to apply" };
    return { cls: "synced", text: "in sync", title: "splits.json matches the built graph" };
  }
  if (b.in_splits && !b.in_graph)
    return { cls: "new", text: "new", title: "in splits.json, not yet built — rebuild to add" };
  if (b.in_graph && !b.in_splits)
    return { cls: "orphan", text: "orphan", title: "removed from splits.json — rebuild to drop from the graph" };
  return null;
}

/* The book the synopsis rows were read out of — the same file `extract_synopsis.py` downloads.
   `source_pages` are its PDF pages, so #page= lands on the right one. */
const SYNOPSIS_PDF =
  "https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/" +
  "fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf";

export function EntryDetail({ detail, speciesOptions, onSaved, onNavigate,
                              reloadKey = 0 }: Props) {
  const { item, unused_curated_splits, match, source_image } = detail;
  const related = detail.related_entries ?? [];
  const boundaries = item?.boundaries ?? [];
  const speciesName = useMemo(
    () => Object.fromEntries(speciesOptions.map((o) => [o.code, o.name])),
    [speciesOptions],
  );

  // Editable working copy of the entry, reset whenever a new entry loads.
  // A combined override puts several registry items behind ONE synopsis row ("CHILLIWACK / VEDDER
  // RIVERS"; the Fraser plus its named channels) — the boundary menu below is their union.
  const alsoItems = detail.also_items ?? [];
  // Every water this entry covers, primary first — a combined entry must be reviewable as a whole
  // AND water-by-water, so each is clickable to focus the map on just that body.
  const coveredItems = item ? [{ id: item.id, name: item.name, kind: item.kind }, ...alsoItems] : [];
  const [focusItemId, setFocusItemId] = useState<string | null>(null);
  const [entry, setEntry] = useState<Entry>(() => structuredClone(detail.entry));
  const [errors, setErrors] = useState<string[]>([]);
  const [toast, setToast] = useState<string>("");
  const [saving, setSaving] = useState(false);
  const [selBoundary, setSelBoundary] = useState<string | null>(null); // clicked split -> map highlight + info
  const [pendingPoint, setPendingPoint] = useState<{ lon: number; lat: number } | null>(null); // live coord "show on map"
  const [reaches, setReaches] = useState<EntryReaches | null>(null); // resolved reach per rule/extent
  const [ruleTypes, setRuleTypes] = useState<string[]>(FALLBACK_TYPES);

  useEffect(() => {
    fetch("/api/rule-types")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => { if (Array.isArray(d) && d.length) setRuleTypes(d.map((x: {type: string}) => x.type)); })
      .catch(() => { /* keep the fallback list */ });
  }, []);

  // Which distinct reach each rule lands on. Several rules almost always share one, and the authored
  // extent text ("downstream of Vedder Crossing Bridge") does not reveal which WATER that is.
  const reachOf = useMemo(
    () => reachIdentities(reaches, entry.rules.map((r) => r.rule_id)),
    [reaches, entry.rules],
  );

  // registry id -> name, over every water this entry covers, so an extent can say which it spans
  const itemNames = useMemo(() => {
    const m: Record<string, string> = {};
    if (detail.item) m[detail.item.id] = detail.item.name;
    for (const a of detail.also_items ?? []) m[a.id] = a.name;
    return m;
  }, [detail]);

  // The resolved reach is derived from the SAVED entry + the built graph, so it is refetched when the
  // entry changes or a rebuild lands — not on every keystroke. Editing an extent therefore shows its
  // new reach after Save, which is also when the binding has actually been validated.
  useEffect(() => {
    let live = true;
    api.reaches(detail.entry.entry_id)
      .then((r) => { if (live) setReaches(r); })
      .catch(() => { if (live) setReaches(null); });
    return () => { live = false; };
  }, [detail.entry.entry_id, reloadKey]);


  useEffect(() => {
    setEntry(structuredClone(detail.entry));
    setErrors([]);
    setToast("");
  }, [detail.entry]);

  const mapItemId = item?.id ?? entry.matched[0] ?? null;
  // A catalogue entry records the items it covers in `matched`; a water row with none is unbound.
  const isNoRegistry = detail.kind !== "zone" && (entry.matched ?? []).length === 0;

  // Flag when the printed synopsis symbol (`symbols`, copied from the row at ingest) says the reg
  // extends to tributaries but the entry's `includes_tributaries` is off.
  const symbolSaysTributaries = (entry.symbols ?? []).some((s: string) => /incl.*trib/i.test(s));
  const tribFlagMismatch = symbolSaysTributaries && entry.includes_tributaries === false;

  const dirty = useMemo(
    () => JSON.stringify(entry) !== JSON.stringify(detail.entry),
    [entry, detail.entry],
  );

  function patchRule(idx: number, patch: Partial<Rule>) {
    setEntry((e) => ({
      ...e,
      rules: e.rules.map((r, i) => (i === idx ? { ...r, ...patch } : r)),
    }));
  }

  function removeRule(idx: number) {
    setEntry((e) => ({ ...e, rules: e.rules.filter((_, i) => i !== idx) }));
  }

  function addRule() {
    setEntry((e) => {
      const nums = e.rules.map((r) => Number(r.rule_id.match(/\.r(\d+)$/)?.[1] ?? 0));
      const n = (nums.length ? Math.max(...nums) : 0) + 1;
      const blank: Rule = {
        rule_id: `${e.entry_id}.r${n}`,
        type: "advisory",
        extents: [],
        review_reason: "manually added — set the type and quote its sentence, then bind",
        verbatim: "",
        unresolved_locators: [],
        species: [],
      };
      return { ...e, rules: [...e.rules, blank] };
    });
  }

  function attach(chosen: ItemSearchResult) {
    // `matched` is the whole record of what the entry covers; nothing else is flipped.
    setEntry((e) => ({ ...e, matched: [chosen.id] }));
    setToast(`attached ${chosen.name} — reload after save to load its boundaries`);
  }

  async function doSave() {
    setErrors([]);
    setToast("");
    setSaving(true);
    try {
      const res = await api.save(entry.entry_id, detail.region, entry);
      if (res.ok) {
        setToast("Saved ✓");
        onSaved();
      } else {
        setErrors(res.errors);
      }
    } catch (err) {
      const ve = err as ValidationError;
      if (ve.errors) setErrors(ve.errors);
      else setErrors([String((err as Error).message ?? err)]);
    } finally {
      setSaving(false);
    }
  }

  // "See Cowichan Lake" -> "Cowichan Lake". The curator can override it in registry_note; this is
  // just so the banner can NAME the target instead of saying "another entry".
  const pointerTarget =
    (entry.registry_note || "").trim() ||
    (/^\s*see\s+(.+?)\s*$/im.exec((entry.regs_verbatim || "").split("\n")[0] || "")?.[1] ?? "");

  return (
    <div className="detail">
      <div className="detail-cols">
        <div className="detail-content">
      {/* Reference-only banner. A pointer row LOOKS like a normal entry — same name, same shape,
          a rule or two parsed out of "See Cowichan Lake" — so the only thing separating it from a
          real regulation was a checkbox at the bottom of the page. Curators confirmed pointer rows
          as if they carried rules. State it at the top, before anything else is read. */}
      {entry.reference_only && (
        <div className="reference-banner">
          <span className="reference-banner-icon">↪</span>
          <div>
            <strong>Reference only — this row carries no regulations of its own.</strong>
            <div className="reference-banner-sub">
              The synopsis lists this water here only to send you somewhere else
              {pointerTarget ? (
                <> — see <b>{pointerTarget}</b></>
              ) : null}
              . Nothing below is a rule that applies to it; don&rsquo;t bind extents or confirm it as
              a regulation. It stays an entry so a search for this name still finds something.
            </div>
          </div>
        </div>
      )}

      {/* Identity header */}
      <div className={`identity${entry.reference_only ? " is-reference" : ""}`}>
        <h2>
          {entry.name}{" "}
          {entry.reference_only && (
            <span className="badge reference" title="pointer row — carries no regulations of its own">
              ↪ reference only
            </span>
          )}
        </h2>
        <div className="sub">
          <span>region {entry.region || detail.region}</span>
          {/* The catalogue keeps no `identity` block: name and region are flat, and the MUs the
              ROW was printed under are encoded in entry_id after the `@` — which is what makes the
              id stable when matching moves. */}
          {entry.entry_id.includes("@") && (
            <span>MU {entry.entry_id.split("@")[1].split("+").join(", ")}</span>
          )}
          <span
            className={`badge ${isNoRegistry ? "no_registry" : "confirmed"}`}
            title={entry.registry_note}
          >
            {isNoRegistry ? "NO REGISTRY" : "matched"}
          </span>
          {item ? (
            <span className="dim">
              item:{" "}
              {coveredItems.map((ci, i) => (
                <span key={ci.id}>
                  {i > 0 && " + "}
                  <a
                    href="#"
                    title={`${ci.id} — click to focus the map on this water`}
                    style={{ borderBottom: `2px solid ${ITEM_COLORS[i % ITEM_COLORS.length]}` }}
                    onClick={(ev) => {
                      ev.preventDefault();
                      setFocusItemId((cur) => (cur === ci.id ? null : ci.id));
                    }}
                  >
                    {ci.name}
                  </a>
                </span>
              ))}
              {focusItemId && (
                <button className="btn" style={{ padding: "0 6px", marginLeft: 6 }}
                        onClick={() => setFocusItemId(null)}>
                  show all
                </button>
              )}
            </span>
          ) : detail.kind === "zone" ? (
            /* A regional rule names no water on purpose. Saying "unmatched" here reported
               the entry as broken when it is exactly what it should be. */
            <span className="chip-tag new" title="a regional or provincial rule — its reach is an area, carried on the entry, not a named water">
              regional rule · applies by area
            </span>
          ) : (
            <span className="dim">
              item: none {match.status ? `(${match.status})` : ""}
            </span>
          )}
        </div>
        {related.length > 0 && (
          <div className="dim" style={{ marginTop: 4 }}>
            Same water, reviewed separately:{" "}
            {related.map((r, i) => (
              <span key={r.entry_id}>
                {i > 0 && ", "}
                <a
                  href="#"
                  onClick={(ev) => { ev.preventDefault(); onNavigate?.(r.entry_id); }}
                  title={`covers ${r.shared_items.map((x) => x.name).join(", ")} — ${r.n_rules} rule(s)`}
                >
                  {r.name}
                </a>
                {r.pointer && <span className="badge"> ↪ pointer</span>}
              </span>
            ))}
          </div>
        )}
        {entry.registry_note && (
          <div className="dim" style={{ marginTop: 4 }}>
            {entry.registry_note}
          </div>
        )}
      </div>

      {/* Durable agent-review state (persisted at ingest) */}
      {entry.parse_review?.verdict && (
        <div className={`parse-review ${entry.parse_review.verdict}`}>
          <strong>
            agent review: {entry.parse_review.verdict.replace("_", " ")}
          </strong>
          {entry.parse_review.model && (
            <span className="dim"> · {entry.parse_review.model}{entry.parse_review.reviewed_at ? ` @ ${entry.parse_review.reviewed_at}` : ""}</span>
          )}
          {entry.parse_review.issues.length > 0 && (
            <ul>
              {entry.parse_review.issues.map((iss, i) => (
                <li key={i}>
                  <span className={`sev ${iss.severity}`}>{iss.severity}</span> {iss.problem}
                  {iss.fix && <div className="dim">→ {iss.fix}</div>}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      {/* no_registry attach flow */}
      {isNoRegistry && (
        <div className="section">
          <AttachItem matched={entry.matched} onAttach={attach} />
        </div>
      )}

      {/* Unused curated splits warning */}
      {unused_curated_splits.length > 0 && (
        <div className="section">
          <div className="unused-warning">
            <strong>⚠ Unused curated splits</strong>
            <div>Curated cut(s) never used by any rule:</div>
            <ul>
              {unused_curated_splits.map((u) => (
                <li key={u.id}>
                  <strong>{u.label}</strong>{" "}
                  <span className="dim">
                    ({u.id}
                    {u.anchor_type ? ` · ${u.anchor_type}` : ""})
                  </span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      )}

      {/* Source synopsis row-crop — always shown so the curator reads the original alongside the
          parse — and now WHICH PAGE it was printed on, so a curator who wants the surrounding
          context can open the book rather than hunting for the row. `entry.source_pages` is a
          list because seven MU 6-1 lakes are printed twice. */}
      {(source_image || (entry.source_pages ?? []).length > 0) && (
        <div className="section source-image">
          <div className="dim" style={{ marginBottom: 4 }}>
            {detail.kind === "zone" ? "regional / provincial chapter" : "source row (synopsis)"}
            {(entry.source_pages ?? []).length > 0 && (
              <> · {(entry.source_pages ?? []).length > 1 ? "pages" : "page"}{" "}
                {(entry.source_pages ?? []).map((n: number, i: number) => (
                  <span key={n}>
                    {i > 0 && ", "}
                    <a href={`${SYNOPSIS_PDF}#page=${n}`} target="_blank" rel="noreferrer">{n}</a>
                  </span>
                ))}
              </>
            )}
          </div>
          {source_image && (
            <a href={`/api/row-image/${source_image}`} target="_blank" rel="noreferrer" title="open full size">
              <img src={`/api/row-image/${source_image}`} alt="source regulation row crop" />
            </a>
          )}
        </div>
      )}

      {/* Global reach — the entry-level scope that applies to EVERY rule (e.g. a row named
          "Elk River (downstream of Elko Dam)" scopes the whole entry downstream of that split). */}
      <div className="section">
        <h3>Global reach (applies to all rules)</h3>
        <div className="dim" style={{ marginBottom: 6 }}>
          The reach this whole entry covers — e.g. <em>downstream of</em> a dam/lake split. Every rule
          below inherits it; leave empty if the entry covers the whole waterbody.
        </div>
        <ExtentEditor
          extents={entry.extents ?? []}
          boundaries={boundaries}
          itemNames={itemNames}
          onChange={(next) => setEntry((s) => ({ ...s, scope: next }))}
        />
      </div>

      {/* Side-by-side: original text vs parsed rules */}
      <div className="section">
        <h3>Original regs ↔ parsed rules</h3>
        <div className="stacked">
          <div>
            {tribFlagMismatch && (
              <div className="trib-warning">
                ⚠ The synopsis symbol flags this row <strong>[Includes Tributaries]</strong> but this
                entry's global tributaries flag is <strong>off</strong>. Verify — the parser defaulted
                many entries to false.
              </div>
            )}
            <div className="verbatim">{entry.regs_verbatim}</div>
          </div>
          <div className="rules-list">
            {entry.rules.map((rule, idx) => (
              <div
                className={`rule${rule.review_reason ? " needs_review" : ""}`}
                key={rule.rule_id}
              >
                <div className="rule-head">
                  <span className="badge">{rule.type}</span>
                  <span className="rule-id">{rule.rule_id}</span>
                  {(() => {
                    const rc = reachOf[rule.rule_id];
                    if (!rc) {
                      return reaches ? (
                        <span className="badge warn" title="this rule's extent does not resolve to any geometry — an area scope, an unbound locator, or a cut that is not on this water">
                          no reach
                        </span>
                      ) : null;
                    }
                    return (
                      <span
                        className="reach-tag"
                        style={{ borderColor: rc.color, color: rc.color }}
                        title={`reach ${rc.key}: ${rc.n} section${rc.n === 1 ? "" : "s"} on ${rc.waters.join(", ")}. Rules sharing this tag cover the same water; pick reach ${rc.key} on the map to see it.`}
                      >
                        <i style={{ background: rc.color }} />
                        {rc.key} · {rc.waters.join(", ") || "—"} · {rc.n}
                      </span>
                    );
                  })()}
                  <button
                    className="btn"
                    style={{ marginLeft: "auto", padding: "1px 8px" }}
                    disabled={entry.rules.length <= 1}
                    title={entry.rules.length <= 1 ? "an entry needs at least 1 rule" : "remove this rule (not applicable)"}
                    onClick={() => removeRule(idx)}
                  >
                    remove rule
                  </button>
                </div>
                <div className="details">{rule.label}</div>
                {rule.verbatim && <div className="rule-verbatim">{rule.verbatim}</div>}

                {rule.species.length > 0 && (
                  <div className="field">
                    <span className="k">species</span>
                    {rule.species.map((c) => speciesName[c] ?? c).join(", ")}
                  </div>
                )}
                {rule.species.length === 0 && (
                  <div className="field dim">species: ALL</div>
                )}
                {(rule.windows ?? []).length > 0 && (
                  <div className="field">
                    <span className="k">windows</span>
                    {(rule.windows ?? []).join("; ")}
                    {rule.windows_are === "excepts" && (
                      <span className="chip-tag orphan" title="these dates say when the rule does NOT apply">
                        excepts
                      </span>
                    )}
                  </div>
                )}

                {/* What this rule LIFTS. In the catalogue an exemption is a FIELD, not a type,
                    and it takes the type of whatever it lifts — so this is structured data now,
                    not a fixed vocabulary of seven ids. Shown, not edited: editing it safely means
                    editing the rule it points at. */}
                {rule.exempts != null && (
                  <div className="field">
                    <span className="k">exempts</span>
                    <code className="dim">{JSON.stringify(rule.exempts)}</code>
                  </div>
                )}
                {rule.extent_text && (
                  <div className="field dim" title="the reach in the page's own words — no cut-point expresses it">
                    “{rule.extent_text}”
                  </div>
                )}
                {rule.tributaries_only && (
                  <div className="field">
                    <span className="k">tributaries</span>
                    <span className="chip-tag new" title="walks the tributaries WITHOUT the mainstem">
                      tributaries only
                    </span>
                  </div>
                )}

                {(rule.review_reason || (rule.unresolved_locators ?? []).length > 0) && (
                  <div className="review-flag">
                    <strong>⚠ needs review</strong>
                    {rule.review_reason && <div>{rule.review_reason}</div>}
                    {(rule.unresolved_locators ?? []).length > 0 && (
                      <div className="locators">
                        unresolved: {(rule.unresolved_locators ?? []).join(" · ")}
                      </div>
                    )}
                  </div>
                )}

                {/* Per-rule edit controls */}
                <details style={{ marginTop: 8 }} open={!rule.verbatim}>
                  <summary className="dim" style={{ cursor: "pointer" }}>
                    edit rule (type · verbatim · binding · species)
                  </summary>
                  <div className="rule-edit">
                    <div className="field">
                      <span className="k">type</span>
                      <select value={rule.type}
                        onChange={(e) => patchRule(idx, { type: e.target.value })}>
                        {ruleTypes.map((t) => <option key={t} value={t}>{t}</option>)}
                      </select>
                    </div>
                    <div className="field">
                      <span className="k">label</span>
                      {/* GENERATED from type + conditions. It was a free-text box, and a label
                          typed beside a number drifts from it — which is why the prose field was
                          removed. Read-only here: change the conditions and the label follows. */}
                      <span className="grow dim" title="generated from the rule's type and conditions — not editable">
                        {rule.label || "—"}
                      </span>
                    </div>
                    <div className="field">
                      <span className="k">verbatim</span>
                      <textarea className="grow" rows={2} spellCheck={false} value={rule.verbatim}
                        onChange={(e) => patchRule(idx, { verbatim: e.target.value })}
                        placeholder="exact verbatim substring of the regs (left panel)" />
                      <span className="hint">must be an exact substring of the entry's regs_verbatim.</span>
                    </div>
                    <div className="field">
                      <span className="k">extents</span>
                      <ExtentEditor
                        extents={(rule.extents ?? [])}
                        boundaries={boundaries}
                        itemNames={itemNames}
                        onChange={(next) => patchRule(idx, { extents: next })}
                      />
                    </div>
                    <div className="field">
                      <span className="k">species</span>
                      <SpeciesPicker
                        values={rule.species}
                        options={speciesOptions}
                        onChange={(next) => patchRule(idx, { species: next })}
                      />
                    </div>
                    <div className="field">
                      <span className="k">windows</span>
                      <div className="dates-editor">
                        {(rule.windows ?? []).map((d: string, di: number) => (
                          <span className="date-row" key={di}>
                            <input
                              value={d}
                              placeholder="e.g. Dec 1–Apr 30"
                              onChange={(e) =>
                                patchRule(idx, { windows: (rule.windows ?? []).map((x: string, j: number) => (j === di ? e.target.value : x)) })
                              }
                            />
                            <button
                              type="button"
                              className="x"
                              title="remove date"
                              onClick={() => patchRule(idx, { windows: (rule.windows ?? []).filter((_: string, j: number) => j !== di) })}
                            >
                              ×
                            </button>
                          </span>
                        ))}
                        <button
                          type="button"
                          className="btn"
                          style={{ padding: "1px 8px" }}
                          onClick={() => patchRule(idx, { windows: [...(rule.windows ?? []), ""] })}
                        >
                          + add date
                        </button>
                      </div>
                    </div>
                    <div className="field">
                      <span className="k">tributaries</span>
                      {/* The catalogue keeps "does this water include its tributaries" on the
                          ENTRY (`tributaries.included`, from the synopsis asterisk). A RULE can
                          only narrow to the tributaries alone — there is no per-rule include /
                          exclude / inherit any more, because two rules on one water disagreeing
                          about what the water IS was never expressible in the book. */}
                      <select
                        value={rule.tributaries_only ? "only" : "water"}
                        onChange={(e) => patchRule(idx, { tributaries_only: e.target.value === "only" })}
                      >
                        <option value="water">the water (entry decides tributaries)</option>
                        <option value="only">tributaries only</option>
                      </select>
                    </div>
                  </div>
                </details>
              </div>
            ))}
            <button className="btn" style={{ padding: "3px 10px", marginTop: 6 }} onClick={addRule}>
              + add rule
            </button>
          </div>
        </div>
      </div>

      {/* Bindable boundaries */}
      <div className="section">
        <h3>
          Bindable boundaries{" "}
          {item ? `(${[item.name, ...alsoItems.map((a) => a.name)].join(" + ")})` : ""}
        </h3>
        {boundaries.length === 0 ? (
          <div className="dim">No boundaries (no matched item / auto-only).</div>
        ) : (
          <div className="boundaries">
            {boundaries.map((b) => (
              <button
                type="button"
                className={`boundary-chip${b.curated ? " curated" : ""}${selBoundary === b.id ? " selected" : ""}`}
                key={b.id}
                title={`${b.ref || ""} ${b.wbk || ""}`}
                onClick={() => setSelBoundary((cur) => (cur === b.id ? null : b.id))}
              >
                {b.curated ? "★ " : ""}
                <span title={b.live && b.live.label !== b.label ? `built: ${b.label}` : undefined}>
                  {(b.live?.label ?? b.label) || b.id}
                </span>
                <span className="anchor">{b.kind}</span>
                {alsoItems.length > 0 && b.item_id && (
                  <span
                    className="anchor"
                    title={`this cut-point is on ${b.item_id} — an item_id-scoped extent may only bind cut-points on its own water`}
                    style={{ color: ITEM_COLORS[
                      Math.max(0, coveredItems.findIndex((c) => c.id === b.item_id)) % ITEM_COLORS.length] }}
                  >
                    {coveredItems.find((c) => c.id === b.item_id)?.name ?? b.item_id}
                  </span>
                )}
                {(() => {
                  const st = boundaryState(b);
                  return st ? <span className={`chip-tag ${st.cls}`} title={st.title}>{st.text}</span> : null;
                })()}
              </button>
            ))}
          </div>
        )}
        <div className="dim" style={{ marginTop: 6 }}>
          ★ = curated split · click a boundary to highlight it on the map + see its details · lake/auto boundaries show teal
        </div>
        {(() => {
          const b = boundaries.find((x) => x.id === selBoundary);
          if (!b) return null;
          const m = (b.meta ?? {}) as Record<string, unknown>;
          const rows: [string, unknown][] = [
            ["id", b.id], ["label", b.label], ["kind", b.kind], ["ref", b.ref],
            ["anchor_type", m.anchor_type], ["route_measure", m.route_measure], ["blk", m.blk],
            ["picked_up", m.picked_up], ["offset_m", m.offset_m], ["concern", m.concern],
            ["tributary_wsc", m.tributary_wsc], ["wbk", b.wbk],
          ];
          return (
            <div className="split-info">
              <strong>{b.curated ? "Curated split" : "Boundary"}: {b.label || b.id}</strong>
              <div className="dim">as built in the current graph (edit the live splits.json below; rebuild to apply)</div>
              <table>
                <tbody>
                  {rows.filter(([, v]) => v !== undefined && v !== null && v !== "").map(([k, v]) => (
                    <tr key={k}><td className="k">{k}</td><td>{String(v)}</td></tr>
                  ))}
                </tbody>
              </table>
              <div className="dim" style={{ marginTop: 4 }}>
                {b.in_graph ? "✓ in graph" : "✗ not in graph"} · {b.in_splits ? "✓ in splits.json" : "✗ not in splits.json"}
                {b.in_splits && !b.in_graph && " — new split, pending rebuild"}
                {b.in_graph && b.in_splits && b.live && b.live.label !== b.label && " — edited in splits.json, pending rebuild"}
                {b.in_graph && !b.in_splits && b.curated && " — in graph but removed from splits.json (will vanish on rebuild)"}
              </div>
              {!b.curated && <div className="dim">auto boundary (lake/outlet/headwaters) — not a curated split</div>}
              {b.curated && <SplitEditor splitId={b.id} onChanged={onSaved} onPreviewPoint={setPendingPoint} />}
            </div>
          );
        })()}
      </div>

      {/* Entry-level tributaries */}
      <div className="section">
        <h3>Tributaries</h3>
        {/* The catalogue keeps ONE flag here: does this water include its tributaries, from the
            synopsis asterisk. "only" moved onto the RULE (`tributaries_only`), because it is a
            property of a restriction and not of the water; and the hand-curated carve-outs are
            gone — an EXCEPT is now an extent on the rule that states it. */}
        <label>
          <input
            type="checkbox"
            checked={entry.includes_tributaries === true}
            onChange={(e) => setEntry((s) => ({ ...s, includes_tributaries: e.target.checked }))}
          />{" "}
          includes tributaries
        </label>
        {entry.includes_tributaries == null && (
          <div className="dim" style={{ marginTop: 4 }}>
            not stated by the row — inherits the default
          </div>
        )}
      </div>

      {/* Errors / toast */}
      {errors.length > 0 && (
        <div className="errors">
          <strong>Validation failed</strong>
          <ul>
            {errors.map((e, i) => (
              <li key={i}>{e}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Actions */}
      <div className="actions">
        <button
          className="btn"
          disabled={saving || !dirty}
          onClick={() => doSave()}
        >
          Save edit
        </button>
        {dirty && <span className="dim">unsaved changes</span>}
        {toast && <span className="toast">{toast}</span>}
      </div>
        </div>
        <div className="detail-map">
          <MapPanel
            itemId={mapItemId}
            alsoItemIds={alsoItems.map((a) => a.id)}
            focusItemId={focusItemId}
            referencedSplitIds={(entry.rules ?? []).flatMap((r) => (r.extents ?? []).flatMap((ex) => ex.splits ?? []))}
            unusedSplitIds={unused_curated_splits.map((u) => u.id)}
            selectedSplitId={selBoundary}
            onSelectSplit={setSelBoundary}
            pendingPoint={pendingPoint}
            reaches={reaches}
            rules={entry.rules.map((r) => ({
              rule_id: r.rule_id,
              type: r.type,
              label: r.label,
            }))}
            reachOf={reachOf}
            entryId={entry.entry_id}
            reloadKey={reloadKey}
          />
        </div>
      </div>
    </div>
  );
}
