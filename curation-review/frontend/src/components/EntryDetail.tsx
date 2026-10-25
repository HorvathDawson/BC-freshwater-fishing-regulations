import { useEffect, useMemo, useState } from "react";
import type { Boundary, EntryDetail as EntryDetailT, EntryReaches, Mode, ReviewView } from "../types";
import { api } from "../api";
import { describeDiagnostic, reachIdentities } from "../format";
import { VerifyBar, type QueueNav } from "./VerifyBar";
import { Evidence } from "./Evidence";
import { EntryReview } from "./EntryReview";
import { Prechecks } from "./Prechecks";
import { ITEM_COLORS, MapPanel } from "./MapPanel";

interface Props {
  detail: EntryDetailT;
  /** jump to another entry (a related row over the same water) */
  onNavigate?: (entryId: string) => void;
  /** bumped after a graph rebuild — forces the map to refetch geometry for the same item */
  reloadKey?: number;
  /** where this entry sits in the queue being worked through, and how to move */
  nav?: QueueNav;
  /** the checks, the problem categories and the commands the server reviews with (T134) */
  mode: Mode;
  /** a mark was recorded; `final` when the entry is done */
  onMarked: (final: boolean) => void;
}

// Curated-split state vs the built graph, for the colour-coded chip badge.
function boundaryState(b: Boundary): { cls: string; text: string; title: string } | null {
  if (!b.curated) return null;
  if (b.minted)
    return { cls: "synced", text: "minted", title: "a position the build cut itself (gauge, length, area) — not in splits.json by design" };
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

/* The book the entries were read from: the repo's copy, served by the backend. `source_pages`
   are PRINTED page numbers and the PDF's index differs by 2-6 (the unnumbered centre gloss), so a
   link goes through the printed -> PDF map. It used to put the printed number straight into
   gov.bc.ca's `#page=`, which opened two to six pages early — Region 4's tables for the Dean. */
const SYNOPSIS_PDF = "/api/synopsis.pdf";

/* ONE ENTRY UNDER REVIEW (T134, T184). READ ONLY: the screen writes one thing, a mark (the
   checklist in `EntryReview`); a wrong entry is a mark that becomes a ticket, and the fix is a
   reviewed change to the curated files (T185) — there are no editors here. Evidence first, then
   the checklist with the verdict bar always visible, then what the checklist is answered from:
   the entry's waters, its scope, every rule with where it binds, its licensing, and the map. */
export function EntryDetail({ detail, onNavigate, reloadKey = 0, nav, mode, onMarked }: Props) {
  // T134: what the checklist is answered from — evidence, coverage, tier, impact, the app link
  const [view, setView] = useState<ReviewView | null>(null);
  const [viewDate, setViewDate] = useState<string | undefined>(undefined);
  const [viewError, setViewError] = useState("");
  const [mapBig, setMapBig] = useState(false);
  useEffect(() => {
    let live = true;
    setViewError("");
    api.review(detail.entry.entry_id, viewDate)
      .then((v) => { if (live) setView(v); })
      .catch((e) => { if (live) { setView(null); setViewError(String(e)); } });
    return () => { live = false; };
  }, [detail.entry, viewDate, reloadKey]);
  const { item, unused_curated_splits } = detail;
  const entry = detail.entry;
  const related = detail.related_entries ?? [];
  const boundaries = item?.boundaries ?? [];

  // A combined override puts several registry items behind ONE synopsis row ("CHILLIWACK / VEDDER
  // RIVERS"; the Fraser plus its named channels) — the boundary list below is their union.
  const alsoItems = detail.also_items ?? [];
  // Every water this entry covers, primary first — a combined entry must be reviewable as a whole
  // AND water-by-water, so each is clickable to focus the map on just that body.
  const coveredItems = item ? [{ id: item.id, name: item.name, kind: item.kind }, ...alsoItems] : [];
  const [focusItemId, setFocusItemId] = useState<string | null>(null);
  const [selBoundary, setSelBoundary] = useState<string | null>(null); // clicked split -> map highlight + info
  const [reaches, setReaches] = useState<EntryReaches | null>(null); // resolved reach per rule/extent
  const [pdfPages, setPdfPages] = useState<Record<string, number>>({});
  useEffect(() => { api.synopsisPages().then(setPdfPages).catch(() => {}); }, []);
  const rules = entry.rules ?? [];
  const licensing = entry.licensing ?? [];

  // Which distinct reach each rule lands on. Several rules almost always share one, and the authored
  // extent text ("downstream of Vedder Crossing Bridge") does not reveal which WATER that is.
  const reachOf = useMemo(
    () => reachIdentities(reaches, rules.map((r) => r.rule_id)),
    [reaches, rules],
  );

  // registry id -> name, over every water this entry covers
  const itemNames = useMemo(() => {
    const m: Record<string, string> = {};
    if (detail.item) m[detail.item.id] = detail.item.name;
    for (const a of detail.also_items ?? []) m[a.id] = a.name;
    return m;
  }, [detail]);

  // The resolved reach is the entry as it is in the file + the built graph: refetched when the
  // entry changes on disk (a reviewed change lands) or a rebuild lands.
  const [reachBusy, setReachBusy] = useState(false);
  useEffect(() => {
    let live = true;
    setReachBusy(true);
    api.reaches(entry.entry_id)
      .then((r) => { if (live) setReaches(r); })
      .catch(() => { if (live) setReaches(null); })
      .finally(() => { if (live) setReachBusy(false); });
    return () => { live = false; };
  }, [entry, reloadKey]);

  const matched = entry.matched ?? [];
  const mapItemId = item?.id ?? matched[0] ?? null;
  // A catalogue entry records the items it covers in `matched`; a water row with none is unbound.
  const isNoRegistry = detail.kind !== "zone" && item == null;

  // Flag when the printed synopsis symbol (`symbols`, copied from the row at ingest) says the reg
  // extends to tributaries but the entry's `includes_tributaries` is off.
  const symbolSaysTributaries = (entry.symbols ?? []).some((s: string) => /incl.*trib/i.test(s));
  const tribFlagMismatch = symbolSaysTributaries && entry.includes_tributaries === false;

  return (
    <div className="detail review-mode">
      <div className={`detail-cols${mapBig ? " map-big" : ""}`}>
        <div className="detail-content">
      <VerifyBar entryId={entry.entry_id} marks={detail.marks} nav={nav} vintage={detail.vintage} />
      {/* EVIDENCE FIRST, THEN THE CHECKLIST (T134): the raw name, the legible crop, the page with
          it marked and the coverage strip; then T/R/P/A with the verdict bar always visible. */}
      {view ? (
        <>
          <Evidence view={view} pdfPages={pdfPages} />
          <Prechecks view={view} />
          <EntryReview detail={detail} view={view} mode={mode} onMarked={onMarked}
            onToggleMap={() => setMapBig((x) => !x)} onDate={setViewDate} />
        </>
      ) : (
        <div className="section dim">{viewError || "loading the evidence…"}</div>
      )}
      {/* Identity header */}
      <div className="entry-head">
        <div className="sub">
          <span>region {entry.region || detail.region}</span>
          {entry.entry_id.includes("@") && (
            <span>MU {entry.entry_id.split("@")[1].split("+").join(", ")}</span>
          )}
          <span className={`badge ${isNoRegistry ? "no_registry" : "confirmed"}`}>
            {isNoRegistry ? "NO REGISTRY" : detail.kind === "zone" ? "zone" : "matched"}
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
            /* A regional rule names no water on purpose. */
            <span className="chip-tag new" title="a regional or provincial rule — its reach is an area, carried on its rules, not a named water">
              regional rule · applies by area
            </span>
          ) : (
            <span className="dim">item: none — `matched` is empty, so this entry binds nothing</span>
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
        {/* PASS-THROUGH FIELDS, copied from the synopsis row by the batch; `entry_id` encodes the
            row (name slug + the MUs it was printed under). */}
        <details className="identity-fields">
          <summary className="dim">identity (pass-through from the synopsis row)</summary>
          <table>
            <tbody>
              <tr><td className="k">entry_id</td><td><code>{entry.entry_id}</code></td></tr>
              <tr><td className="k">name</td><td>{entry.name}</td></tr>
              <tr><td className="k">display_name</td><td>{entry.display_name || <span className="dim">—</span>}</td></tr>
              <tr><td className="k">region</td><td>{entry.region || <span className="dim">—</span>}</td></tr>
              <tr><td className="k">source_pages</td><td>{(entry.source_pages ?? []).join(", ") || "—"}</td></tr>
              <tr><td className="k">symbols</td><td title="the printed glyphs, 1:1 — a cross-check, never a binding">
                {(entry.symbols ?? []).length ? (entry.symbols ?? []).map((g) => <span key={g} className="tag">{g}</span>) : <span className="dim">none printed</span>}
              </td></tr>
            </tbody>
          </table>
        </details>
      </div>

      {/* matched — the items this entry covers. AUTHORITATIVE: empty binds nothing. A zone entry
          binds by its AREA, carried on its rules (T134). */}
      <div className="section">
        <div className="mfield">
          <span className="k">matched</span>
          <div className="v">
            <span className="tag-input">
              {matched.map((id) => (
                <span className="tag" key={id}>
                  {itemNames[id] ?? id} <span className="dim">{id}</span>
                </span>
              ))}
              {matched.length === 0 && <span className="dim">none</span>}
            </span>
            <div className="hint">{detail.kind === "zone"
              ? "a regional rule names no water: it binds by the area on its rules"
              : "the registry items this row regulates, primary first — empty means it binds nothing"}</div>
          </div>
        </div>
      </div>

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

      {/* The source row crop and the page are the EVIDENCE panel's (T134, above): legible, with the
          row or passage marked on its page. Every other printed page of the entry (seven MU 6-1
          lakes are printed twice) is linked here. */}
      {(entry.source_pages ?? []).length > 1 && (
        <div className="section dim">
          also printed on{" "}
          {(entry.source_pages ?? []).map((n: number, i: number) => (
            <span key={n}>
              {i > 0 && ", "}
              <a href={`${SYNOPSIS_PDF}#page=${pdfPages[String(n)] ?? n}`} target="_blank" rel="noreferrer">p.{n}</a>
              {" "}(<a href={`/api/synopsis/page/${n}.png`} target="_blank" rel="noreferrer">page image</a>)
            </span>
          ))}
        </div>
      )}

      {/* The entry's own fields: its reach (which CLIPS every rule, and is never a rule's reach),
          whether it includes tributaries, and the scope note. */}
      <div className="section">
        <h3>Entry scope (clips every rule)</h3>
        <div className="scope-read" data-testid="scope-read">
          <div><span className="k">extents</span> <code>{JSON.stringify(entry.extents ?? [])}</code></div>
          <div><span className="k">includes_tributaries</span>{" "}
            {entry.includes_tributaries == null ? <span className="dim">not stated by the row</span>
              : String(entry.includes_tributaries)}</div>
          {entry.scope_note && <div><span className="k">scope_note</span> {entry.scope_note}</div>}
          {(entry.see ?? []).length > 0 && <div><span className="k">see</span> <code>{JSON.stringify(entry.see)}</code></div>}
          {entry.anadromous_rainbow && <div><span className="k">anadromous_rainbow</span> true</div>}
          {tribFlagMismatch && (
            <div className="trib-warning">⚠ The printed symbol says <strong>[Includes Tributaries]</strong> but
              <code> includes_tributaries</code> is false.</div>
          )}
        </div>
      </div>

      {/* Every rule and every licensing record as the app will say it — the GENERATED label — with
          the sentence it quotes and where it binds. The row itself is the coverage strip above. */}
      <div className="section">
        <h3>Rules ({rules.length})</h3>
        <div className="dim" style={{ marginBottom: 6 }}>
          Each rule: the generated label (what the app says), the sentence it quotes, and WHERE it
          binds — the reach builder's answer for the entry as it is in the file
          {reachBusy ? " (computing…)" : ""}.
        </div>
        {(entry.see ?? []).length > 0 && rules.length === 0 && (
          <div className="dim">A pointer row: it carries no rules of its own; the entry it points to does.</div>
        )}
        <div className="rules-list">
          {rules.map((rule) => {
            const rc = reachOf[rule.rule_id];
            return (
              <div className={`rule${rule.review_reason ? " flagged" : ""}`}
                   key={rule.rule_id} data-rule={rule.rule_id}>
                <div className="rule-head">
                  <span className="badge">{rule.type}</span>
                  <span className="rule-id">{rule.rule_id}</span>
                  {rc ? (
                    <span className="reach-tag" style={{ borderColor: rc.color, color: rc.color }}
                      title={`reach ${rc.key}: ${rc.n} section${rc.n === 1 ? "" : "s"} on ${rc.waters.join(", ")}`}>
                      <i style={{ background: rc.color }} />
                      {rc.key} · {rc.waters.join(", ") || "—"} · {rc.n}
                    </span>
                  ) : reaches ? (
                    <span className="badge warn" title="this rule's extent does not resolve to any geometry">no reach</span>
                  ) : null}
                </div>
                <div className="label" title="GENERATED by catalogue.label — what the app will say">
                  {rule.label ?? <span className="dim">— does not validate —</span>}
                </div>
                {rule.verbatim && <div className="rule-verbatim">{rule.verbatim}</div>}
                {rule.review_reason && (
                  <div className="review-flag"><strong>⚠ review_reason</strong> {rule.review_reason}</div>
                )}
                {rule.undrawn_part && (
                  <div className="review-flag"><strong>not yet mapped</strong> holds only in “{String(rule.undrawn_part)}”,
                    which nothing draws — shown as a note on the water, never colouring it</div>
                )}
                {(rule.unresolved_locators ?? []).length > 0 && (
                  <div className="review-flag"><strong>unresolved place</strong> {(rule.unresolved_locators ?? []).join("; ")}</div>
                )}
                {(() => {
                  const v = reaches?.verdict?.[rule.rule_id];
                  if (!v) return null;
                  const bound = v.outcome === "bound";
                  // T133: until the entry is built this is the builder's answer for the file NOW,
                  // not what the bundle ships — a preview, never the binding to sign off
                  const preview = detail.vintage && detail.vintage.state !== "current";
                  return (
                    <div className={`binding${bound ? "" : " unbound"}`} data-testid={`binding-${rule.rule_id}`}>
                      {preview && <span className="badge warn" title="awaiting build: not what the bundle ships yet">preview</span>}{" "}
                      <strong>{bound ? `binds ${v.n_sections.toLocaleString()} section(s)` : `${v.outcome}`}</strong>
                      {v.reason && <> · {v.reason}{v.detail ? `: ${v.detail}` : ""}</>}
                      {rule.side && <> · {String(rule.side)} half only</>}
                      {v.tributaries_pending && <> · <span className="warn-text">tributaries pending</span></>}
                      {v.diagnostics.length > 0 && (
                        <ul>{v.diagnostics.map((d, i) => <li key={i}>{describeDiagnostic(d)}</li>)}</ul>
                      )}
                    </div>
                  );
                })()}
              </div>
            );
          })}
        </div>
      </div>

      <div className="section">
        <h3>Licensing ({licensing.length})</h3>
        <div className="dim" style={{ marginBottom: 6 }}>
          What you must hold, and what this water is. Never votes on open/closed.
        </div>
        <div className="rules-list">
          {licensing.map((rec) => (
            <div className={`rule${rec.review_reason ? " flagged" : ""}`} key={rec.id} data-record={rec.id}>
              <div className="rule-head">
                <span className="badge">{rec.kind}</span>
                <span className="rule-id">{rec.id}</span>
              </div>
              <div className="label" title="GENERATED by catalogue.licensing_label — what the app will say">
                {rec.label ?? <span className="dim">— does not validate —</span>}
              </div>
              {rec.verbatim && <div className="rule-verbatim">{rec.verbatim}</div>}
              {rec.review_reason && (
                <div className="review-flag"><strong>⚠ review_reason</strong> {String(rec.review_reason)}</div>
              )}
            </div>
          ))}
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
              <div className="dim">as built in the current graph</div>
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
              {b.curated && b.live?.review?.note && (
                <div className={"split-review" + (b.live.review.grade ? " proposal" : "")}>
                  {b.live.review.grade && <div className="split-review-grade">auto-proposal, grade <strong>{b.live.review.grade}</strong></div>}
                  <div className="split-review-note">{b.live.review.note}</div>
                </div>
              )}
            </div>
          );
        })()}
      </div>
        </div>
        <div className="detail-map">
          <MapPanel
            itemId={mapItemId}
            alsoItemIds={alsoItems.map((a) => a.id)}
            focusItemId={focusItemId}
            referencedSplitIds={rules.flatMap((r) => (r.extents ?? []).flatMap((ex) => ex.splits ?? []))}
            unusedSplitIds={unused_curated_splits.map((u) => u.id)}
            selectedSplitId={selBoundary}
            onSelectSplit={setSelBoundary}
            reaches={reaches}
            rules={rules.map((r) => ({
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
