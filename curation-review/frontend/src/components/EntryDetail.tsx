import { useEffect, useMemo, useState } from "react";
import type {
  Boundary, EntryDetail as EntryDetailT, EntryReaches, EntrySplits, ExtentFix, Mode, NamedRef,
  NamedSplit, ReviewView, RuleScopeWords, TribCall,
} from "../types";
import { ExtentFixEditor } from "./ExtentFixEditor";
import { api } from "../api";
import { describeDiagnostic, reachIdentities } from "../format";
import { VerifyBar, type QueueNav } from "./VerifyBar";
import { Evidence } from "./Evidence";
import { RuleSpecies } from "./RuleSpecies";
import { EntryReview, markWords, useEntryReview, type MarkExtras } from "./EntryReview";
import { Finding, findingsOf, Prechecks } from "./Prechecks";
import { ITEM_COLORS, MapPanel } from "./MapPanel";
import { VerdictBar } from "./useMarking";

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

// A merged place carries every name (owner 2026-10-24): "canonical (also: …)".
function splitLabel(b: Boundary): string {
  const base = (b.live?.label ?? b.label) || b.id;
  return b.names && b.names.length ? `${base} (also: ${b.names.join(", ")})` : base;
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
   are PRINTED page numbers and the PDF's index differs by 2-6, so a link goes through the
   printed -> PDF map. */
const SYNOPSIS_PDF = "/api/synopsis.pdf";

/** T189: the reviewer's tributary call on one target — "should include" / "should not"; clicking
 *  the call already made takes it back. Recorded in the mark (`Mark.tributaries`), never a file. */
function TribCallButtons({ k, extras }: { k: string; extras: MarkExtras }) {
  const now = extras.trib[k];
  const b = (call: TribCall, words: string) => (
    <button type="button" className={`ans trib-call${now === call ? " on" : ""}`}
      aria-pressed={now === call} data-trib-call={`${k}:${call}`}
      onClick={(ev) => { ev.currentTarget.blur(); extras.setTrib(k, now === call ? null : call); }}>
      {words}
    </button>
  );
  return (
    <span className="trib-calls" title="your call, recorded with the mark">
      {b("include", "should include")}{b("exclude", "should not")}
    </span>
  );
}

/** A yes / no chip. */
const YesNo = ({ yes, words }: { yes: boolean; words?: string }) => (
  <span className={`yn ${yes ? "yn-yes" : "yn-no"}`}>{words ?? (yes ? "yes" : "no")}</span>
);

/* ONE ENTRY UNDER REVIEW (T134, T184, T189). READ ONLY: the screen writes one thing, a mark.
   THE LAYOUT (T189, the owner's pilot): the source — the row as printed and as parsed — pinned at
   the top of the pane; the entry's own extent; one block per rule (its label and the words it
   quotes, then WHERE it holds in plain words, tributaries yes or no, carve-outs, and a click to
   draw it on the map); licensing; the checklist; the verdict bar pinned at the foot. Each fact is
   shown once: a pre-check on a rule is in that rule's block, the splits a rule names are on it. */
export function EntryDetail({ detail, onNavigate, reloadKey = 0, nav, mode, onMarked }: Props) {
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
  // a ZONE entry holds region-wide (T205): its reach is an area far too large to load or draw
  const zone = detail.kind === "zone";
  const entry = detail.entry;
  const related = detail.related_entries ?? [];
  const boundaries = item?.boundaries ?? [];
  const alsoItems = detail.also_items ?? [];
  const coveredItems = item ? [{ id: item.id, name: item.name, kind: item.kind }, ...alsoItems] : [];
  const [focusItemId, setFocusItemId] = useState<string | null>(null);
  const [selBoundary, setSelBoundary] = useState<string | null>(null);
  const [showRule, setShowRule] = useState<{ id: string | null; n: number }>({ id: null, n: 0 });
  const [reaches, setReaches] = useState<EntryReaches | null>(null);
  const [pdfPages, setPdfPages] = useState<Record<string, number>>({});
  useEffect(() => { api.synopsisPages().then(setPdfPages).catch(() => {}); }, []);
  const rules = entry.rules ?? [];
  const licensing = entry.licensing ?? [];

  // T189: the tributary calls and "lake split later", per entry — carried by the mark
  const [trib, setTribs] = useState<Record<string, TribCall>>({});
  const [lakeSplit, setLakeSplit] = useState(false);
  // T191: the proposed extents (in the mark), the preview drawn, and a map-click pick in progress
  const [fixes, setFixes] = useState<Record<string, ExtentFix>>({});
  const [fixPreview, setFixPreview] = useState<GeoJSON.FeatureCollection | null>(null);
  const [pickCb, setPickCb] = useState<{ cb: ((ll: [number, number]) => void) | null }>({ cb: null });
  useEffect(() => {
    setTribs({}); setLakeSplit(false); setShowRule({ id: null, n: 0 });
    setFixes({}); setFixPreview(null); setPickCb({ cb: null });
  }, [entry.entry_id]);
  const extras: MarkExtras = {
    trib, lakeSplit, setLakeSplit, fixes,
    setFix: (k, fix) => setFixes((f) => {
      const next = { ...f };
      if (fix) next[k] = fix; else delete next[k];
      return next;
    }),
    setTrib: (k, call) => setTribs((t) => {
      const next = { ...t };
      if (call) next[k] = call; else delete next[k];
      return next;
    }),
  };
  const r = useEntryReview({ detail, view, mode, onMarked, onToggleMap: () => setMapBig((x) => !x),
                             extras });

  // EVERY SPLIT THE RULES NAME (T184): labelled on the map; each rule block lists its own
  const [named, setNamed] = useState<EntrySplits | null>(null);
  useEffect(() => {
    let live = true;
    if (zone) { setNamed(null); return; }
    api.splits(entry.entry_id).then((s) => { if (live) setNamed(s); })
      .catch(() => { if (live) setNamed(null); });
    return () => { live = false; };
  }, [entry, reloadKey]);
  const namedBy = useMemo(() => {
    const m: Record<string, NamedSplit & { why?: string; placed: boolean }> = {};
    for (const f of named?.features ?? []) m[f.properties.split_id] = { ...f.properties, placed: true };
    for (const u of named?.unplaced ?? []) m[u.split_id] = { ...u, placed: false };
    return m;
  }, [named]);
  const referenced = useMemo(
    () => rules.flatMap((x) => (x.extents ?? []).flatMap((ex) => ex.splits ?? [])), [rules]);
  const reachOf = useMemo(() => reachIdentities(reaches, rules.map((x) => x.rule_id)),
    [reaches, rules]);

  const [reachBusy, setReachBusy] = useState(false);
  useEffect(() => {
    let live = true;
    if (zone) { setReaches(null); setReachBusy(false); return; }
    setReachBusy(true);
    api.reaches(entry.entry_id)
      .then((x) => { if (live) setReaches(x); })
      .catch(() => { if (live) setReaches(null); })
      .finally(() => { if (live) setReachBusy(false); });
    return () => { live = false; };
  }, [entry, reloadKey]);

  const matched = entry.matched ?? [];
  const mapItemId = item?.id ?? matched[0] ?? null;
  const isNoRegistry = !zone && item == null;
  const symbolSaysTributaries = (entry.symbols ?? []).some((s: string) => /incl.*trib/i.test(s));
  const tribFlagMismatch = symbolSaysTributaries && entry.includes_tributaries === false;
  const es = view?.scope.entry;
  const itemColor = (id: string) =>
    ITEM_COLORS[Math.max(0, coveredItems.findIndex((c) => c.id === id)) % ITEM_COLORS.length];

  // T191: the splits already on this map — the ones the rules name, then the items' boundaries
  const mapSplits = useMemo(() => {
    const out: NamedRef[] = [];
    const seen = new Set<string>();
    for (const f of named?.features ?? []) {
      seen.add(f.properties.split_id);
      out.push({ id: f.properties.split_id, name: f.properties.name || f.properties.label });
    }
    for (const b of boundaries) {
      if (seen.has(b.id) || !b.curated) continue;
      seen.add(b.id);
      out.push({ id: b.id, name: `${splitLabel(b)} (${b.kind}${
        b.live?.label || b.label ? ` · ${b.id.split("__").pop()}` : ""})` });
    }
    return out;
  }, [named, boundaries]);
  const splitAt = (id: string): [number, number] | null =>
    (named?.features ?? []).find((f) => f.properties.split_id === id)?.geometry.coordinates ?? null;
  function fixEditor(scope: string, currentOp?: string) {
    return (
      <ExtentFixEditor scope={scope} entryId={entry.entry_id} currentOp={currentOp}
        mapSplits={mapSplits} waters={coveredItems.map((c) => ({ id: c.id, name: c.name }))}
        value={fixes[scope]} onChange={(f) => extras.setFix(scope, f)}
        onPreview={setFixPreview} onPick={(cb) => setPickCb({ cb })} splitAt={splitAt} />
    );
  }

  function splitChip(id: string, name: string) {
    const s = namedBy[id];
    return (
      <button type="button" className={`split-chip${selBoundary === id ? " selected" : ""}`}
        title={`${id} — click to fly to it on the map`} onClick={() => setSelBoundary(id)}>
        {name}
        {s?.to_verify && <span className="badge warn">to verify{s.grade ? ` · ${s.grade}` : ""}</span>}
        {s && !s.placed && <span className="badge warn">not on the map: {s.why}</span>}
      </button>
    );
  }

  /** WHERE ONE RULE HOLDS (T189): the extent in words, then tributaries, carve-outs, the
   *  binding, and the pre-checks on it. */
  function ruleExtent(rid: string, sw: RuleScopeWords) {
    const found = view ? findingsOf(view, rid) : [];
    const undrawn = found.find((f) => f.code === "undrawn_part");
    const parserWhy = undrawn?.what.split(" — the parser: ")[1];
    // the extent shows the undrawn part and the unresolved places: their findings are not repeated
    const rest = found.filter((f) => f.code !== "undrawn_part" && f.code !== "unresolved_locator");
    const otherItems = sw.items.filter((i) => !matched.includes(i.id) || matched.length > 1);
    return (
      <div className="rule-extent" data-testid={`where-${rid}`}>
        <div className="ext-row">
          <span className="k">where</span>
          <span className="ext-where">{sw.where}</span>
        </div>
        {sw.book_says && sw.book_says !== sw.where && (
          <div className="ext-row"><span className="k">book</span>“{sw.book_says}”</div>
        )}
        {otherItems.length > 0 && (
          <div className="ext-row"><span className="k">on</span>
            {otherItems.map((i) => (
              <span key={i.id} className="item-ref" style={{ borderBottomColor: itemColor(i.id) }}
                title={i.id}>{i.name}</span>
            ))}
          </div>
        )}
        {sw.splits.length > 0 && (
          <div className="ext-row"><span className="k">splits</span>
            {sw.splits.map((s) => <span key={s.id}>{splitChip(s.id, s.name)}</span>)}
          </div>
        )}
        {sw.in_part && (
          <div className="ext-row warn-row"><span className="k">only in</span>
            <span>“{sw.in_part}” — <strong>not drawn</strong>: a note on the water, never colouring it
              {parserWhy && <span className="dim"> · the parser: {parserWhy}</span>}</span>
          </div>
        )}
        {sw.side && <div className="ext-row"><span className="k">side</span>{sw.side} half only</div>}
        {sw.unresolved.length > 0 && (
          <div className="ext-row warn-row"><span className="k">unplaced</span>
            names a place nothing draws: {sw.unresolved.join("; ")}</div>
        )}
        <div className="ext-row" data-testid={`trib-${rid}`}>
          <span className="k">tributaries</span>
          <YesNo yes={sw.tributaries.yes} words={sw.tributaries.only ? "tributaries only" : undefined} />
          <span className="dim">{sw.tributaries.why}</span>
          <TribCallButtons k={rid} extras={extras} />
        </div>
        <div className="ext-row" data-testid={`carve-${rid}`}>
          <span className="k">carve-outs</span>
          {sw.carve_outs.length + sw.exceptions.length === 0 ? <span className="dim">none</span> : (
            <span>{[...sw.carve_outs, ...sw.exceptions].join(" · ")}</span>
          )}
        </div>
        {(() => {
          const v = reaches?.verdict?.[rid];
          if (!v) return reachBusy ? <div className="ext-row dim">binding: computing…</div> : null;
          const bound = v.outcome === "bound";
          const preview = detail.vintage && detail.vintage.state !== "current";
          return (
            <div className={`binding${bound ? "" : " unbound"}`} data-testid={`binding-${rid}`}>
              {preview && <span className="badge warn" title="awaiting build: not what the bundle ships yet">preview</span>}{" "}
              <strong>{bound ? `binds ${v.n_sections.toLocaleString()} section(s)` : `${v.outcome}`}</strong>
              {v.reason && <> · {v.reason}{v.detail ? `: ${v.detail}` : ""}</>}
              {v.tributaries_pending && <> · <span className="warn-text">tributaries pending</span></>}
              {/* an ambiguous cut is its pre-check below (candidates and pick), not repeated here */}
              {v.diagnostics.some((d) => d.kind !== "ambiguous_cut") && (
                <ul>{v.diagnostics.filter((d) => d.kind !== "ambiguous_cut")
                  .map((d, i) => <li key={i}>{describeDiagnostic(d)}</li>)}</ul>
              )}
            </div>
          );
        })()}
        {rest.length > 0 && <ul className="rule-findings">{rest.map((f, i) => <Finding key={i} f={f} />)}</ul>}
        {fixEditor(rid, rules.find((x) => x.rule_id === rid)?.extents?.[0]?.op)}
      </div>
    );
  }

  return (
    <div className="detail review-mode">
      <div className={`detail-cols${mapBig ? " map-big" : ""}`}>
        <div className="detail-content">
          <VerifyBar entryId={entry.entry_id} marks={detail.marks} nav={nav} vintage={detail.vintage}
            reviewed={detail.reviewed} />
          {/* THE SOURCE, PINNED (T189): the row as printed and as parsed stay at the top while the
              rules scroll under them — the reviewer reads a rule against the book without
              scrolling up and down. */}
          {view ? (
            <div className="source-pin" data-testid="source-pin">
              <Evidence view={view} pdfPages={pdfPages} />
            </div>
          ) : (
            <div className="section dim">{viewError || "loading the evidence…"}</div>
          )}

          {/* THE ENTRY'S OWN EXTENT (T189): which waters, where, tributaries, carve-outs — it clips
              every rule below. */}
          <div className="section entry-scope" data-testid="entry-scope">
            <div className="scope-head">
              <h3>This entry</h3>
              <span className="dim">region {entry.region || detail.region}</span>
              {entry.entry_id.includes("@") && (
                <span className="dim">MU {entry.entry_id.split("@")[1].split("+").join(", ")}</span>
              )}
              {isNoRegistry && <span className="badge no_registry">NO REGISTRY — binds nothing</span>}
              {zone && <span className="chip-tag new" title="a regional or provincial rule — its reach is an area, carried on its rules">regional rule · applies by area</span>}
            </div>
            <div className="ext-row">
              <span className="k">waters</span>
              {coveredItems.length > 0 ? coveredItems.map((ci) => (
                <a key={ci.id} href="#" className={`item-ref${focusItemId === ci.id ? " on" : ""}`}
                  title={`${ci.id} — click to focus the map on this water`}
                  style={{ borderBottomColor: itemColor(ci.id) }}
                  onClick={(ev) => { ev.preventDefault(); setFocusItemId((c) => (c === ci.id ? null : ci.id)); }}>
                  {ci.name}
                </a>
              )) : <span className="dim">{zone ? "none — it binds by the area on its rules" : "none: `matched` names no item of this build"}</span>}
              {focusItemId && <button className="btn small" onClick={() => setFocusItemId(null)}>show all</button>}
            </div>
            {es && (
              <>
                <div className="ext-row"><span className="k">where</span>{es.where}</div>
                <div className="ext-row" data-testid="trib-entry">
                  <span className="k">tributaries</span>
                  <YesNo yes={es.tributaries.yes}
                    words={es.tributaries.stated ? undefined : "not stated (no)"} />
                  <span className="dim">
                    {es.rules_walking.length} of {rules.length} rule{rules.length === 1 ? "" : "s"} walk
                    their tributaries
                  </span>
                  <TribCallButtons k="entry" extras={extras} />
                </div>
                {tribFlagMismatch && (
                  <div className="ext-row warn-row">⚠ The printed symbol says <strong>[Includes
                    Tributaries]</strong> but the entry says no.</div>
                )}
                <div className="ext-row" data-testid="carve-entry">
                  <span className="k">carve-outs</span>
                  {es.carve_outs.length === 0 && !es.name_except ? <span className="dim">none</span> : (
                    <span>
                      {es.carve_outs.join(" · ")}
                      {es.name_except && <>{es.carve_outs.length ? " · " : ""}the heading says “{es.name_except}”</>}
                    </span>
                  )}
                </div>
              </>
            )}
            {/* T191: the entry's own extent, proposed (a zone entry is marked rule by rule) */}
            {!zone && fixEditor("entry", (entry.extents ?? [])[0]?.op)}
            {entry.scope_note && <div className="ext-row"><span className="k">note</span>{entry.scope_note}</div>}
            {(entry.see ?? []).length > 0 && (
              <div className="ext-row"><span className="k">see</span>
                {(entry.see ?? []).map((s) => s.verbatim).join("; ")}
                {rules.length === 0 && <span className="dim"> — a pointer row: the entry it points to carries the rules</span>}
              </div>
            )}
            {entry.anadromous_rainbow && <div className="ext-row"><span className="k">steelhead</span>anadromous rainbow trout are found here</div>}
            {related.length > 0 && (
              <div className="ext-row"><span className="k">same water</span>
                {related.map((x) => (
                  <a key={x.entry_id} href="#" className="item-ref"
                    onClick={(ev) => { ev.preventDefault(); onNavigate?.(x.entry_id); }}
                    title={`covers ${x.shared_items.map((y) => y.name).join(", ")} — ${x.n_rules} rule(s)`}>
                    {x.name}{x.pointer ? " ↪" : ""}
                  </a>
                ))}
              </div>
            )}
            {(entry.source_pages ?? []).length > 1 && (
              <div className="ext-row"><span className="k">printed</span>
                {(entry.source_pages ?? []).map((n: number) => (
                  <a key={n} href={`${SYNOPSIS_PDF}#page=${pdfPages[String(n)] ?? n}`} target="_blank" rel="noreferrer">p.{n}</a>
                ))}
              </div>
            )}
            {unused_curated_splits.length > 0 && (
              <div className="ext-row warn-row"><span className="k">unused</span>
                curated cut(s) no rule uses: {unused_curated_splits.map((u) => u.label).join(", ")}</div>
            )}
            <details className="identity-fields">
              <summary className="dim">identity</summary>
              <div className="dim"><code>{entry.entry_id}</code>
                {entry.display_name ? ` · shown as “${entry.display_name}”` : ""}
                {" · symbols: "}{(entry.symbols ?? []).join(", ") || "none printed"}</div>
            </details>
          </div>

          {view && <Prechecks view={view} />}

          {/* ONE BLOCK PER RULE (T189): what it says, then where it holds. */}
          <div className="section">
            <h3>Rules ({rules.length})</h3>
            <div className="rules-list">
              {rules.map((rule, i) => {
                const rc = reachOf[rule.rule_id];
                const sw = view?.scope.rules[rule.rule_id];
                const mk = r.zone ? r.marksBy[rule.rule_id] : undefined;
                return (
                  <div className={`rule${r.zone && i === r.at ? " active" : ""}`}
                    key={rule.rule_id} data-rule={rule.rule_id}>
                    <div className="rule-head">
                      <span className="badge">{rule.type}</span>
                      <span className="rule-id">{rule.rule_id}</span>
                      {rc ? (
                        <span className="reach-tag" style={{ borderColor: rc.color, color: rc.color }}
                          title={`reach ${rc.key}: ${rc.n} section${rc.n === 1 ? "" : "s"} on ${rc.waters.join(", ")}`}>
                          <i style={{ background: rc.color }} />
                          {rc.key} · {rc.n}
                        </span>
                      ) : reaches ? (
                        <span className="badge warn" title="this rule's extent does not resolve to any geometry">no reach</span>
                      ) : null}
                      {mk && <span className={`verify-chip ${mk.status}`}>{markWords(mk)}</span>}
                      {r.zone && i !== r.at && (
                        <button type="button" className="btn small" onClick={() => r.setAt(i)}>mark this rule</button>
                      )}
                    </div>
                    <div className="label" title="GENERATED by catalogue.label — what the app will say">
                      {rule.label ?? <span className="dim">— does not validate —</span>}
                    </div>
                    {rule.verbatim && rule.verbatim !== rule.label && (
                      <div className="rule-verbatim">{rule.verbatim}</div>
                    )}
                    <RuleSpecies rule={rule} />
                    {!zone && (
                      <button type="button" className="show-map-btn" data-testid={`show-${rule.rule_id}`}
                        onClick={() => setShowRule((x) => ({ id: rule.rule_id, n: x.n + 1 }))}
                        title="draw this rule's reach on the map">
                        <span aria-hidden="true">&#9906;</span> Show on map
                      </button>
                    )}
                    {sw ? ruleExtent(rule.rule_id, sw)
                      : <div className="dim">{viewError ? "" : "where it holds: loading…"}</div>}
                  </div>
                );
              })}
            </div>
          </div>

          {licensing.length > 0 && (
            <div className="section">
              <h3>Licensing ({licensing.length})</h3>
              <div className="dim" style={{ marginBottom: 6 }}>
                What you must hold, and what this water is. Never votes on open/closed.
              </div>
              <div className="rules-list">
                {licensing.map((rec) => {
                  const found = view ? findingsOf(view, rec.id) : [];
                  return (
                    <div className="rule" key={rec.id} data-record={rec.id}>
                      <div className="rule-head">
                        <span className="badge">{rec.kind}</span>
                        <span className="rule-id">{rec.id}</span>
                      </div>
                      <div className="label" title="GENERATED by catalogue.licensing_label — what the app will say">
                        {rec.label ?? <span className="dim">— does not validate —</span>}
                      </div>
                      {rec.verbatim && rec.verbatim !== rec.label && <div className="rule-verbatim">{rec.verbatim}</div>}
                      {found.length > 0 && <ul className="rule-findings">{found.map((f, j) => <Finding key={j} f={f} />)}</ul>}
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {view && <EntryReview r={r} view={view} mode={mode} extras={extras} onDate={setViewDate} />}

          {/* The item's cut-points — every boundary it could bind, beyond the ones the rules name
              (those are on their rules). Closed by default. */}
          <details className="section boundaries-box">
            <summary>
              Bindable boundaries ({boundaries.length}){" "}
              {item ? <span className="dim">on {[item.name, ...alsoItems.map((a) => a.name)].join(" + ")}</span> : ""}
            </summary>
            {boundaries.length === 0 ? (
              <div className="dim">No boundaries (no matched item / auto-only).</div>
            ) : (
              <div className="boundaries">
                {boundaries.map((b) => (
                  <button type="button"
                    className={`boundary-chip${b.curated ? " curated" : ""}${selBoundary === b.id ? " selected" : ""}`}
                    key={b.id} title={`${b.ref || ""} ${b.wbk || ""}`}
                    onClick={() => setSelBoundary((cur) => (cur === b.id ? null : b.id))}>
                    {b.curated ? "★ " : ""}
                    <span title={b.live && b.live.label !== b.label ? `built: ${b.label}` : undefined}>
                      {splitLabel(b)}
                    </span>
                    <span className="anchor">{b.kind}</span>
                    {alsoItems.length > 0 && b.item_id && (
                      <span className="anchor" style={{ color: itemColor(b.item_id) }}>
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
                  <strong>{b.curated ? "Curated split" : "Boundary"}: {splitLabel(b)}</strong>
                  <table>
                    <tbody>
                      {rows.filter(([, v]) => v !== undefined && v !== null && v !== "").map(([k, v]) => (
                        <tr key={k}><td className="k">{k}</td><td>{String(v)}</td></tr>
                      ))}
                    </tbody>
                  </table>
                  <div className="dim" style={{ marginTop: 4 }}>
                    {b.in_graph ? "✓ in graph" : "✗ not in graph"} · {b.in_splits ? "✓ in splits.json" : "✗ not in splits.json"}
                  </div>
                  {b.curated && b.live?.review?.note && (
                    <div className={"split-review" + (b.live.review.grade ? " proposal" : "")}>
                      {b.live.review.grade && <div className="split-review-grade">auto-proposal, grade <strong>{b.live.review.grade}</strong></div>}
                      <div className="split-review-note">{b.live.review.note}</div>
                    </div>
                  )}
                </div>
              );
            })()}
          </details>

          <VerdictBar checks={r.checks} m={r.m} held={r.held} />
        </div>
        <div className="detail-map">
          {zone ? (
            <div className="zone-no-map" data-testid="zone-no-map">
              <strong>No map for a zone entry.</strong> Its rules hold region-wide ({entry.region || detail.region}):
              the reach is an area far too large to load or draw, so nothing is fetched. Check the
              text and the rules against the printed passage on the left; where a rule holds is in
              its block, in words.
            </div>
          ) : (
          <MapPanel
            itemId={mapItemId}
            alsoItemIds={alsoItems.map((a) => a.id)}
            focusItemId={focusItemId}
            referencedSplitIds={referenced}
            unusedSplitIds={unused_curated_splits.map((u) => u.id)}
            selectedSplitId={selBoundary}
            onSelectSplit={setSelBoundary}
            reaches={reaches}
            rules={rules.map((x) => ({ rule_id: x.rule_id, type: x.type, label: x.label }))}
            reachOf={reachOf}
            entryId={entry.entry_id}
            reloadKey={reloadKey}
            namedSplits={named}
            showRule={showRule}
            preview={fixPreview}
            onPick={pickCb.cb}
          />
          )}
        </div>
      </div>
    </div>
  );
}
