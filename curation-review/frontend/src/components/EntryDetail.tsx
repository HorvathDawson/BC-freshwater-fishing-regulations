import { useEffect, useMemo, useState } from "react";
import type { Boundary, Entry, EntryDetail as EntryDetailT, ItemSearchResult, Rule, SpeciesOption } from "../types";
import { api, type ValidationError } from "../api";
import { humanExtent, rawExtent } from "../format";
import { ExtentEditor } from "./ExtentEditor";
import { SpeciesPicker } from "./SpeciesPicker";
import { AttachItem } from "./AttachItem";
import { MapPanel } from "./MapPanel";
import { SplitEditor } from "./SplitEditor";
import { ExcludesEditor } from "./ExcludesEditor";

interface Props {
  detail: EntryDetailT;
  curator: string;
  speciesOptions: SpeciesOption[];
  onSaved: () => void;
  /** after a successful confirm+lock — parent advances to the next unlocked entry in the region */
  onConfirmed?: () => void;
  /** bumped after a graph rebuild — forces the map to refetch geometry for the same item */
  reloadKey?: number;
}

const RESTRICTION_TYPES = ["closure", "harvest", "gear_restriction", "vessel_restriction", "licensing", "note"];

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

export function EntryDetail({ detail, curator, speciesOptions, onSaved, onConfirmed, reloadKey = 0 }: Props) {
  const { item, unused_curated_splits, match, source_image } = detail;
  const boundaries = item?.boundaries ?? [];
  const speciesName = useMemo(
    () => Object.fromEntries(speciesOptions.map((o) => [o.code, o.name])),
    [speciesOptions],
  );

  // Editable working copy of the entry, reset whenever a new entry loads.
  const [entry, setEntry] = useState<Entry>(() => structuredClone(detail.entry));
  const [errors, setErrors] = useState<string[]>([]);
  const [toast, setToast] = useState<string>("");
  const [saving, setSaving] = useState(false);
  const [selBoundary, setSelBoundary] = useState<string | null>(null); // clicked split -> map highlight + info
  const [pendingPoint, setPendingPoint] = useState<{ lon: number; lat: number } | null>(null); // live coord "show on map"

  useEffect(() => {
    setEntry(structuredClone(detail.entry));
    setErrors([]);
    setToast("");
  }, [detail.entry]);

  const mapItemId = item?.id ?? entry.matched[0] ?? null;
  const isNoRegistry = entry.registry_status === "no_registry";

  // Parser skewed most entries to tributaries.included=false; flag when the source says the reg extends
  // to tributaries but the global flag is off. Prefer the authoritative synopsis symbol (source_symbols,
  // injected at ingest); fall back to the raw-text marker for entries with no captured symbol.
  const symbolSaysTributaries = (entry.source_symbols ?? []).some((s) => /incl.*trib/i.test(s));
  const rawSaysTributaries =
    /\[\s*includes?\s+tributaries\s*\]|includ\w*\s+(?:all\s+|its\s+)?tributar|all\s+tributaries/i.test(
      entry.regs_verbatim,
    );
  const tribFlagMismatch =
    (symbolSaysTributaries || rawSaysTributaries) && entry.tributaries.included === false;

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
        restriction_type: "note",
        details: "",
        extents: [],
        dates: [],
        includes_tributaries: null,
        tributary_excludes: [],
        sections_override: null,
        needs_review: true,
        review_reason: "manually added — set rule_text + details, then bind",
        rule_text: "",
        location_text: "",
        exception: "",
        display_location: "",
        unresolved_locators: [],
        species: [],
      };
      return { ...e, rules: [...e.rules, blank] };
    });
  }

  function attach(chosen: ItemSearchResult) {
    // Spec: set entry.matched = [chosen.id]. Also flip registry_status to
    // "matched" so extents can be bound (a no_registry entry rejects extents
    // server-side); reset the flag if the curator clears the attachment.
    setEntry((e) => ({
      ...e,
      matched: [chosen.id],
      registry_status: "matched",
    }));
    setToast(`attached ${chosen.name} — reload after save to load its boundaries`);
  }

  async function doSave(lock: boolean) {
    setErrors([]);
    setToast("");
    let reviewedBy = curator;
    if (lock && !reviewedBy) {
      setErrors(["Set a curator name first (top-right) before confirming."]);
      return;
    }
    setSaving(true);
    try {
      const res = lock
        ? await api.confirm(entry.entry_id, detail.region, entry, reviewedBy)
        : await api.save(entry.entry_id, detail.region, entry);
      if (res.ok) {
        setToast(lock ? "Confirmed & locked ✓" : "Saved ✓");
        // on confirm, advance to the next unlocked entry (parent); otherwise just refresh in place
        if (lock && onConfirmed) onConfirmed();
        else onSaved();
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

  return (
    <div className="detail">
      <div className="detail-cols">
        <div className="detail-content">
      {/* Identity header */}
      <div className="identity">
        <h2>
          {entry.identity.name}{" "}
          {entry.locked && <span className="badge lock">🔒 locked</span>}
          {entry.revisit && <span className="badge revisit">↻ revisit later</span>}
        </h2>
        <div className="sub">
          <span>region {entry.identity.region || detail.region}</span>
          {entry.identity.mus.length > 0 && (
            <span>MU {entry.identity.mus.join(", ")}</span>
          )}
          <span
            className={`badge ${isNoRegistry ? "no_registry" : "confirmed"}`}
            title={entry.registry_note}
          >
            {isNoRegistry ? "NO REGISTRY" : "matched"}
          </span>
          {item ? (
            <span className="dim">
              item: {item.name} ({item.id})
            </span>
          ) : (
            <span className="dim">
              item: none {match.status ? `(${match.status})` : ""}
            </span>
          )}
          {entry.reviewed_by && (
            <span className="dim">
              reviewed by {entry.reviewed_by} @ {entry.reviewed_at}
            </span>
          )}
        </div>
        {entry.registry_note && (
          <div className="dim" style={{ marginTop: 4 }}>
            {entry.registry_note}
          </div>
        )}
      </div>

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

      {/* Source synopsis row-crop — always shown so the curator reads the original alongside the parse */}
      {source_image && (
        <div className="section source-image">
          <div className="dim" style={{ marginBottom: 4 }}>source row (synopsis)</div>
          <a href={`/api/row-image/${source_image}`} target="_blank" rel="noreferrer" title="open full size">
            <img src={`/api/row-image/${source_image}`} alt="source regulation row crop" />
          </a>
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
          extents={entry.scope}
          boundaries={boundaries}
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
                ⚠ The synopsis flags this row <strong>[Includes Tributaries]</strong>
                {symbolSaysTributaries ? " (source symbol)" : " (raw text)"} but this entry's global
                tributaries flag is <strong>off</strong>. Verify — the parser defaulted many entries to
                false.
              </div>
            )}
            <div className="verbatim">{entry.regs_verbatim}</div>
          </div>
          <div className="rules-list">
            {entry.rules.map((rule, idx) => (
              <div
                className={`rule${rule.needs_review ? " needs_review" : ""}`}
                key={rule.rule_id}
              >
                <div className="rule-head">
                  <span className="badge">{rule.restriction_type}</span>
                  <span className="rule-id">{rule.rule_id}</span>
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
                <div className="details">{rule.details}</div>
                {rule.rule_text && <div className="rule-verbatim">{rule.rule_text}</div>}

                {rule.species.length > 0 && (
                  <div className="field">
                    <span className="k">species</span>
                    {rule.species.map((c) => speciesName[c] ?? c).join(", ")}
                  </div>
                )}
                {rule.species.length === 0 && (
                  <div className="field dim">species: ALL</div>
                )}
                {rule.dates.length > 0 && (
                  <div className="field">
                    <span className="k">dates</span>
                    {rule.dates.join("; ")}
                  </div>
                )}

                {/* Binding: human + raw */}
                <div className="field">
                  <span className="k">binding</span>
                  {rule.extents.length === 0 ? (
                    <span className="dim">(unbound)</span>
                  ) : (
                    rule.extents.map((ex, i) => (
                      <span key={i} style={{ marginRight: 10 }}>
                        <span className="human-binding">
                          {humanExtent(ex, boundaries)}
                        </span>{" "}
                        <span className="raw-binding">{rawExtent(ex)}</span>
                      </span>
                    ))
                  )}
                </div>
                {(rule.display_location || rule.location_text) && (
                  <div className="field dim">
                    “{rule.display_location || rule.location_text}”
                  </div>
                )}
                {rule.includes_tributaries != null && (
                  <div className="field">
                    <span className="k">tributaries</span>
                    <span className={`chip-tag ${rule.includes_tributaries ? "new" : "orphan"}`}>
                      {rule.includes_tributaries ? "includes tributaries" : "excludes tributaries"}
                    </span>
                    <span className="dim"> (rule override)</span>
                  </div>
                )}

                {(rule.needs_review || rule.unresolved_locators.length > 0) && (
                  <div className="review-flag">
                    <strong>⚠ needs review</strong>
                    {rule.review_reason && <div>{rule.review_reason}</div>}
                    {rule.unresolved_locators.length > 0 && (
                      <div className="locators">
                        unresolved: {rule.unresolved_locators.join(" · ")}
                      </div>
                    )}
                  </div>
                )}

                {/* Per-rule edit controls */}
                <details style={{ marginTop: 8 }} open={!rule.rule_text}>
                  <summary className="dim" style={{ cursor: "pointer" }}>
                    edit rule (type · details · verbatim · binding · species)
                  </summary>
                  <div className="rule-edit">
                    <div className="field">
                      <span className="k">type</span>
                      <select value={rule.restriction_type}
                        onChange={(e) => patchRule(idx, { restriction_type: e.target.value as Rule["restriction_type"] })}>
                        {RESTRICTION_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
                      </select>
                    </div>
                    <div className="field">
                      <span className="k">details</span>
                      <input className="grow" value={rule.details}
                        onChange={(e) => patchRule(idx, { details: e.target.value })} placeholder="concise summary" />
                    </div>
                    <div className="field">
                      <span className="k">rule_text</span>
                      <textarea className="grow" rows={2} spellCheck={false} value={rule.rule_text}
                        onChange={(e) => patchRule(idx, { rule_text: e.target.value })}
                        placeholder="exact verbatim substring of the regs (left panel)" />
                      <span className="hint">must be an exact substring of the entry's regs_verbatim.</span>
                    </div>
                    <div className="field">
                      <span className="k">extents</span>
                      <ExtentEditor
                        extents={rule.extents}
                        boundaries={boundaries}
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
                      <span className="k">dates</span>
                      <div className="dates-editor">
                        {rule.dates.map((d, di) => (
                          <span className="date-row" key={di}>
                            <input
                              value={d}
                              placeholder="e.g. Dec 1–Apr 30"
                              onChange={(e) =>
                                patchRule(idx, { dates: rule.dates.map((x, j) => (j === di ? e.target.value : x)) })
                              }
                            />
                            <button
                              type="button"
                              className="x"
                              title="remove date"
                              onClick={() => patchRule(idx, { dates: rule.dates.filter((_, j) => j !== di) })}
                            >
                              ×
                            </button>
                          </span>
                        ))}
                        <button
                          type="button"
                          className="btn"
                          style={{ padding: "1px 8px" }}
                          onClick={() => patchRule(idx, { dates: [...rule.dates, ""] })}
                        >
                          + add date
                        </button>
                      </div>
                    </div>
                    <div className="field">
                      <span className="k">tributaries</span>
                      <select
                        value={rule.includes_tributaries == null ? "inherit" : rule.includes_tributaries ? "include" : "exclude"}
                        onChange={(e) => {
                          const v = e.target.value;
                          patchRule(idx, { includes_tributaries: v === "inherit" ? null : v === "include" });
                        }}
                      >
                        <option value="inherit">inherit entry</option>
                        <option value="include">includes tributaries</option>
                        <option value="exclude">excludes tributaries</option>
                      </select>
                    </div>
                    <div className="field">
                      <span className="k">tributary carve-outs</span>
                      <span className="hint">
                        EXCEPT … for THIS rule only (e.g. “No Fishing in tributaries except Quinsam River”). Leave empty unless this one rule drops a tributary the others keep.
                      </span>
                      <ExcludesEditor
                        itemIds={Array.from(new Set([...(item?.id ? [item.id] : []), ...entry.matched]))}
                        excludes={rule.tributary_excludes ?? []}
                        onChange={(next) => patchRule(idx, { tributary_excludes: next })}
                      />
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
        <h3>Bindable boundaries {item ? `(${item.name})` : ""}</h3>
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
                {(() => {
                  const st = boundaryState(b);
                  return st ? <span className={`chip-tag ${st.cls}`} title={st.title}>{st.text}</span> : null;
                })()}
              </button>
            ))}
          </div>
        )}
        <div className="dim" style={{ marginTop: 6 }}>
          ★ = curated split · click a boundary to highlight it on the map + see its details
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
        <label style={{ marginRight: 16 }}>
          <input
            type="checkbox"
            checked={entry.tributaries.included}
            onChange={(e) =>
              setEntry((s) => ({
                ...s,
                tributaries: { ...s.tributaries, included: e.target.checked },
              }))
            }
          />{" "}
          included
        </label>
        <label>
          <input
            type="checkbox"
            checked={entry.tributaries.only}
            onChange={(e) =>
              setEntry((s) => ({
                ...s,
                tributaries: {
                  ...s.tributaries,
                  only: e.target.checked,
                  included: e.target.checked ? true : s.tributaries.included,
                },
              }))
            }
          />{" "}
          only
        </label>
        <div style={{ marginTop: 8 }}>
          <div className="dim" style={{ marginBottom: 4 }}>
            Carve-outs (EXCEPT …) — read the raw regs; subtracted from the tributary set:
          </div>
          <ExcludesEditor
            itemIds={Array.from(new Set([...(item?.id ? [item.id] : []), ...entry.matched]))}
            excludes={entry.tributaries.excludes}
            onChange={(next) =>
              setEntry((s) => ({ ...s, tributaries: { ...s.tributaries, excludes: next } }))
            }
          />
        </div>
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
        <label className="revisit" title="Conditionally accept: confirm now but flag it to revisit later">
          <input
            type="checkbox"
            checked={entry.revisit}
            onChange={(e) => setEntry((s) => ({ ...s, revisit: e.target.checked }))}
          />{" "}
          revisit later
          <input
            type="text"
            className="revisit-note"
            placeholder="why? (optional comment)"
            value={entry.revisit_note}
            disabled={!entry.revisit}
            onChange={(e) => setEntry((s) => ({ ...s, revisit_note: e.target.value }))}
          />
        </label>
        <button
          className="btn"
          disabled={saving || !dirty}
          onClick={() => doSave(false)}
        >
          Save edit
        </button>
        <button
          className="btn primary"
          disabled={saving}
          onClick={() => doSave(true)}
        >
          {entry.revisit ? "Confirm (revisit later)" : "Confirm & lock"}
        </button>
        {dirty && <span className="dim">unsaved changes</span>}
        {toast && <span className="toast">{toast}</span>}
      </div>
        </div>
        <div className="detail-map">
          <MapPanel
            itemId={mapItemId}
            referencedSplitIds={entry.rules.flatMap((r) => r.extents.flatMap((ex) => ex.splits))}
            unusedSplitIds={unused_curated_splits.map((u) => u.id)}
            selectedSplitId={selBoundary}
            onSelectSplit={setSelBoundary}
            pendingPoint={pendingPoint}
            reloadKey={reloadKey}
          />
        </div>
      </div>
    </div>
  );
}
