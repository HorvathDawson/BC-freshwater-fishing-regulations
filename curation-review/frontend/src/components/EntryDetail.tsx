import { useEffect, useMemo, useRef, useState } from "react";
import type {
  Boundary, CheckResult, Entry, EntryDetail as EntryDetailT, EntryReaches, FieldError,
  ItemSearchResult, LicensingRecord, Rule,
} from "../types";
import { api, type EntryRefused } from "../api";
import { describeDiagnostic, reachIdentities } from "../format";
import { AnswerPanel } from "./AnswerPanel";
import { VerifyBar, type QueueNav } from "./VerifyBar";
import { ExtentEditor } from "./ExtentEditor";
import { AttachItem } from "./AttachItem";
import { ITEM_COLORS, MapPanel } from "./MapPanel";
import { SplitEditor } from "./SplitEditor";
import { RuleEditor } from "./model/RuleEditor";
import { LicensingEditor, blankRecord } from "./model/LicensingEditor";
import { SeeEditor } from "./model/SeeEditor";
import { Check, ErrorsAt, ErrorsCtx, F, Text, Tri, put, useVocab } from "../model";

interface Props {
  detail: EntryDetailT;
  onSaved: () => void;
  /** jump to another entry (a related row over the same water) */
  onNavigate?: (entryId: string) => void;
  /** bumped after a graph rebuild — forces the map to refetch geometry for the same item */
  reloadKey?: number;
  /** where this entry sits in the queue being worked through, and how to move */
  nav?: QueueNav;
  /** a mark was set or cleared — refresh the queue and progress */
  onMarked?: () => void;
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

/** Strip what the backend stamps on a served entry (each rule's / record's generated `label`),
 *  so "unsaved changes" compares what would be written. */
function stored(e: Entry): Entry {
  return {
    ...e,
    rules: (e.rules ?? []).map(({ label: _l, ...r }) => r as Rule),
    licensing: (e.licensing ?? []).map(({ label: _l, ...x }) => x as LicensingRecord),
  };
}

export function EntryDetail({ detail, onSaved, onNavigate, reloadKey = 0, nav, onMarked }: Props) {
  const vocab = useVocab();
  const { item, unused_curated_splits, source_image } = detail;
  const related = detail.related_entries ?? [];
  const boundaries = item?.boundaries ?? [];

  // Editable working copy of the entry, reset whenever a new entry loads.
  // A combined override puts several registry items behind ONE synopsis row ("CHILLIWACK / VEDDER
  // RIVERS"; the Fraser plus its named channels) — the boundary menu below is their union.
  const alsoItems = detail.also_items ?? [];
  // Every water this entry covers, primary first — a combined entry must be reviewable as a whole
  // AND water-by-water, so each is clickable to focus the map on just that body.
  const coveredItems = item ? [{ id: item.id, name: item.name, kind: item.kind }, ...alsoItems] : [];
  const [focusItemId, setFocusItemId] = useState<string | null>(null);
  const [entry, setEntry] = useState<Entry>(() => stored(structuredClone(detail.entry)));
  // the save's refusal, and the live check of the draft — both addressed to fields
  const [saveErrors, setSaveErrors] = useState<FieldError[]>([]);
  const [checked, setChecked] = useState<CheckResult | null>(null);
  const [openRule, setOpenRule] = useState<Record<string, boolean>>({});
  const [toast, setToast] = useState<string>("");
  const [saving, setSaving] = useState(false);
  const [selBoundary, setSelBoundary] = useState<string | null>(null); // clicked split -> map highlight + info
  const [pendingPoint, setPendingPoint] = useState<{ lon: number; lat: number } | null>(null); // live coord "show on map"
  const [reaches, setReaches] = useState<EntryReaches | null>(null); // resolved reach per rule/extent
  const [pdfPages, setPdfPages] = useState<Record<string, number>>({});
  useEffect(() => { api.synopsisPages().then(setPdfPages).catch(() => {}); }, []);
  // the whole printed page — open by default where there is no row crop (a region chapter)
  const [showPage, setShowPage] = useState<number | null>(null);
  useEffect(() => {
    setShowPage(!detail.source_image && (detail.entry.source_pages ?? []).length
      ? (detail.entry.source_pages ?? [])[0] : null);
  }, [detail.entry.entry_id, detail.source_image]);
  const rules = entry.rules ?? [];
  const licensing = entry.licensing ?? [];

  // THE LIVE CHECK. Every edit is sent to /api/check (debounced): the model validates the draft
  // and returns each error addressed to its field, plus the label every rule and record will
  // show. Nothing is written — that is Save's job, which runs the same check again.
  const seq = useRef(0);
  useEffect(() => {
    const n = ++seq.current;
    const ctl = new AbortController();
    const t = setTimeout(() => {
      api.check(entry, detail.region, ctl.signal)
        .then((r) => { if (n === seq.current) setChecked(r); })
        .catch(() => { /* aborted or offline: keep the last answer */ });
    }, 300);
    return () => { clearTimeout(t); ctl.abort(); };
  }, [entry, detail.region]);
  const liveErrors = checked?.errors ?? [];
  const errors = saveErrors.length ? saveErrors : liveErrors;

  // Which distinct reach each rule lands on. Several rules almost always share one, and the authored
  // extent text ("downstream of Vedder Crossing Bridge") does not reveal which WATER that is.
  const reachOf = useMemo(
    () => reachIdentities(reaches, rules.map((r) => r.rule_id)),
    [reaches, rules],
  );

  // registry id -> name, over every water this entry covers, so an extent can say which it spans
  const itemNames = useMemo(() => {
    const m: Record<string, string> = {};
    if (detail.item) m[detail.item.id] = detail.item.name;
    for (const a of detail.also_items ?? []) m[a.id] = a.name;
    return m;
  }, [detail]);

  // The resolved reach is derived from the SAVED entry + the built graph, so it is refetched when the
  // saved entry changes (a save reloads `detail.entry`) or a rebuild lands — not on every keystroke.
  // Keyed on the id alone it was NOT refetched after a save, so the map and every rule's binding
  // went on showing the reach of the entry as it was before the edit.
  const [reachBusy, setReachBusy] = useState(false);
  useEffect(() => {
    let live = true;
    setReachBusy(true);
    api.reaches(detail.entry.entry_id)
      .then((r) => { if (live) setReaches(r); })
      .catch(() => { if (live) setReaches(null); })
      .finally(() => { if (live) setReachBusy(false); });
    return () => { live = false; };
  }, [detail.entry, reloadKey]);


  useEffect(() => {
    // a reload after a save lands here: keep its "Saved ✓" toast, which is the confirmation
    setEntry(stored(structuredClone(detail.entry)));
    setSaveErrors([]);
  }, [detail.entry]);

  const matched = entry.matched ?? [];
  const mapItemId = item?.id ?? matched[0] ?? null;
  // A catalogue entry records the items it covers in `matched`; a water row with none is unbound.
  const isNoRegistry = detail.kind !== "zone" && item == null;

  // Flag when the printed synopsis symbol (`symbols`, copied from the row at ingest) says the reg
  // extends to tributaries but the entry's `includes_tributaries` is off.
  const symbolSaysTributaries = (entry.symbols ?? []).some((s: string) => /incl.*trib/i.test(s));
  const tribFlagMismatch = symbolSaysTributaries && entry.includes_tributaries === false;

  const dirty = useMemo(
    () => JSON.stringify(entry) !== JSON.stringify(stored(detail.entry)),
    [entry, detail.entry],
  );

  function edit(fn: (e: Entry) => Entry) {
    setSaveErrors([]);
    setEntry(fn);
  }
  const patchRule = (idx: number, r: Rule) =>
    edit((e) => ({ ...e, rules: (e.rules ?? []).map((x, i) => (i === idx ? r : x)) }));
  const removeRule = (idx: number) =>
    edit((e) => ({ ...e, rules: (e.rules ?? []).filter((_, i) => i !== idx) }));
  const patchRecord = (idx: number, x: LicensingRecord) =>
    edit((e) => ({ ...e, licensing: (e.licensing ?? []).map((y, i) => (i === idx ? x : y)) }));
  const removeRecord = (idx: number) =>
    edit((e) => ({ ...e, licensing: (e.licensing ?? []).filter((_, i) => i !== idx) }));

  function addRule() {
    edit((e) => {
      // a rule id is `<entry slug>.r<n>` — the slug is what the entry's other rules already use
      const slug = (e.rules?.[0]?.rule_id ?? `${e.entry_id.split(":").pop()?.split("@")[0]}.r0`)
        .replace(/\.r\d+[a-z]?$/, "");
      const nums = (e.rules ?? []).map((r) => Number(r.rule_id.match(/\.r(\d+)[a-z]?$/)?.[1] ?? 0));
      const n = (nums.length ? Math.max(...nums) : 0) + 1;
      const blank: Rule = {
        rule_id: `${slug}.r${n}`, type: "advisory", verbatim: "",
        review_reason: "manually added — set the type, quote its sentence, then say where",
      };
      setOpenRule((o) => ({ ...o, [blank.rule_id]: true }));
      return { ...e, rules: [...(e.rules ?? []), blank] };
    });
  }

  function addRecord(kind: string) {
    edit((e) => {
      const ids = new Set((e.licensing ?? []).map((x) => x.id));
      let n = 1;
      while (ids.has(`${kind}_${n}`)) n++;
      return { ...e, licensing: [...(e.licensing ?? []), blankRecord(kind, `${kind}_${n}`)] };
    });
  }

  function attach(chosen: ItemSearchResult) {
    // `matched` is the whole record of what the entry covers; nothing else is flipped.
    edit((e) => ({ ...e, matched: [...(e.matched ?? []).filter((x) => x !== chosen.id), chosen.id] }));
    setToast(`attached ${chosen.name} — save, then reload, to load its boundaries`);
  }

  async function doSave() {
    setSaveErrors([]);
    setToast("");
    setSaving(true);
    try {
      const res = await api.save(entry.entry_id, detail.region, entry);
      if (res.ok) {
        setToast("Saved ✓");
        onSaved();
      } else {
        setSaveErrors(res.errors);
      }
    } catch (err) {
      const fe = (err as EntryRefused).fieldErrors;
      setSaveErrors(fe ?? [{ path: "", msg: String((err as Error).message ?? err) }]);
    } finally {
      setSaving(false);
    }
  }

  const liveLabel = (kind: "rules" | "licensing", i: number, served?: string) => {
    const got = checked?.labels?.[kind]?.[i];
    return got === undefined ? served : got;
  };

  return (
    <ErrorsCtx.Provider value={errors}>
    <div className="detail">
      <div className="detail-cols">
        <div className="detail-content">
      <VerifyBar entryId={entry.entry_id} verification={detail.verification} dirty={dirty}
        nav={nav} onMarked={onMarked} />
      {/* Identity header */}
      <div className="entry-head">
        <h2>{entry.display_name || entry.name}</h2>
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
        {/* PASS-THROUGH FIELDS. They are copied from the synopsis row by the batch, and
            `entry_id` encodes the row (name slug + the MUs it was printed under). A re-parse
            compares the entry to the row by them, so editing one here would desync the entry from
            its source — the backend refuses a change to any of them. */}
        <details className="identity-fields">
          <summary className="dim">identity (pass-through from the synopsis row — read-only)</summary>
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
          <ErrorsAt path="entry_id" /><ErrorsAt path="name" /><ErrorsAt path="display_name" />
          <ErrorsAt path="region" /><ErrorsAt path="symbols" /><ErrorsAt path="source_pages" />
        </details>
        <ErrorsAt path="" />
      </div>

      {/* matched — the items this entry covers. AUTHORITATIVE: empty binds nothing. */}
      <div className="section">
        <F path="matched" deep hint="the registry items this row regulates, primary first — empty means it binds nothing">
          <span className="tag-input">
            {matched.map((id) => (
              <span className="tag" key={id}>
                {itemNames[id] ?? id} <span className="dim">{id}</span>
                <button type="button" aria-label={`remove ${id}`}
                  onClick={() => edit((e) => ({ ...e, matched: (e.matched ?? []).filter((x) => x !== id) }))}>×</button>
              </span>
            ))}
            {matched.length === 0 && <span className="dim">none</span>}
          </span>
        </F>
        {detail.match && matched.length === 0 && (
          <div className="dim">
            live matcher suggests: <code>{detail.match.item_id ?? "nothing"}</code> ({detail.match.status}
            {detail.match.reason ? ` — ${detail.match.reason}` : ""}) — attach it below if it is right
          </div>
        )}
        {(isNoRegistry || detail.kind !== "zone") && (
          <AttachItem matched={matched} onAttach={attach} />
        )}
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

      {/* Source synopsis row-crop — always shown so the curator reads the original alongside the
          parse — and now WHICH PAGE it was printed on, so a curator who wants the surrounding
          context can open the book rather than hunting for the row. `entry.source_pages` is a
          list because seven MU 6-1 lakes are printed twice. */}
      {(source_image || (entry.source_pages ?? []).length > 0) && (
        <div className={`section source-image${showPage != null ? " with-page" : ""}`}>
          <div className="dim" style={{ marginBottom: 4 }}>
            {detail.kind === "zone" ? "regional / provincial chapter" : "source row (synopsis)"}
            {(entry.source_pages ?? []).length > 0 && (
              <> · {(entry.source_pages ?? []).length > 1 ? "pages" : "page"}{" "}
                {(entry.source_pages ?? []).map((n: number, i: number) => (
                  <span key={n}>
                    {i > 0 && ", "}
                    <a href={`${SYNOPSIS_PDF}#page=${pdfPages[String(n)] ?? n}`} target="_blank" rel="noreferrer"
                      title={pdfPages[String(n)] ? `printed page ${n} = PDF page ${pdfPages[String(n)]}` : undefined}>{n}</a>
                    {" "}
                    <button type="button" className="btn small" data-testid={`show-page-${n}`}
                      onClick={() => setShowPage((cur) => (cur === n ? null : n))}>
                      {showPage === n ? "hide page" : "show page"}
                    </button>
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
          {showPage != null && (
            <div className="page-image">
              <a href={`/api/synopsis/page/${showPage}.png`} target="_blank" rel="noreferrer" title="open full size">
                <img src={`/api/synopsis/page/${showPage}.png`} alt={`synopsis printed page ${showPage}`} />
              </a>
            </div>
          )}
        </div>
      )}

      {/* The entry's own fields: its reach (which CLIPS every rule, and is never a rule's reach),
          whether it includes tributaries, and the scope note. */}
      <div className="section">
        <h3>Entry scope (clips every rule)</h3>
        <F path="extents" hint="the stretch this row is about — it clips every rule; each rule still states its own reach">
          <ExtentEditor
            extents={entry.extents ?? []}
            boundaries={boundaries}
            itemNames={itemNames}
            path="extents"
            onChange={(next) => edit((s) => put(s, { extents: next.length ? next : undefined }))}
          />
        </F>
        <F path="includes_tributaries" hint="from the synopsis symbol; a rule or record with its own unset inherits this">
          <Tri value={entry.includes_tributaries} unset="not stated by the row" yes="includes tributaries"
            no="does not include tributaries"
            onChange={(x) => edit((s) => put(s, { includes_tributaries: x }))} />
        </F>
        {tribFlagMismatch && (
          <div className="trib-warning">
            ⚠ The printed symbol says <strong>[Includes Tributaries]</strong> but
            <code> includes_tributaries</code> is false.
          </div>
        )}
        <F path="scope_note">
          <Text value={entry.scope_note} label="scope_note" onChange={(x) => edit((s) => put(s, { scope_note: x }))} />
        </F>
        {/* POINTERS ("See Lonzo Creek") — not rules, they bind nothing. The model refuses an
            advisory that is only a pointer, so this is the one place one can be written. */}
        <F path="see" hint="pointers to the row whose regulations govern — each names entries OR says why it names none">
          <SeeEditor value={entry.see} path="see"
            suggestions={related.map((r) => r.entry_id)}
            onChange={(x) => edit((s) => put(s, { see: x }))} />
        </F>
        <F path="anadromous_rainbow" hint="anadromous rainbow are found here: a rainbow over 50 cm IS a steelhead (p.80) — set where known, never inferred">
          <Check label="anadromous_rainbow" value={entry.anadromous_rainbow}
            onChange={(x) => edit((s) => put(s, { anadromous_rainbow: x }))}>
            anadromous rainbow trout are found in this water
          </Check>
        </F>
      </div>

      {/* The printed passage, then every rule and every licensing record as the app will say it
          — the GENERATED label, recomputed by the backend on every edit — with its editor. */}
      <div className="section">
        <h3>Original regs</h3>
        <div className="verbatim" data-testid="regs-verbatim">{entry.regs_verbatim}</div>
      </div>

      <div className="section">
        <h3>Rules ({rules.length})</h3>
        <div className="dim" style={{ marginBottom: 6 }}>
          Each rule: the generated label (what the app says), the sentence it quotes, and WHERE it
          binds — the reach builder's answer for the entry as saved
          {reachBusy ? " (computing…)" : ""}.
        </div>
        {dirty && (
          <div className="review-flag" data-testid="reach-stale">
            Unsaved edits: labels and errors follow the draft, but every binding and the map show
            the entry <strong>as last saved</strong>. Save to recompute them.
          </div>
        )}
        {(entry.see ?? []).length > 0 && rules.length === 0 && (
          <div className="dim">A pointer row: it carries no rules of its own; the entry it points to does.</div>
        )}
        <div className="rules-list">
          {rules.map((rule, idx) => {
            const path = `rules.${idx}`;
            const lab = liveLabel("rules", idx, rule.label);
            const bad = errors.some((e) => e.path === path || e.path.startsWith(path + "."));
            const rc = reachOf[rule.rule_id];
            return (
              <div className={`rule${rule.review_reason ? " flagged" : ""}${bad ? " has-error" : ""}`}
                   key={idx} data-rule={rule.rule_id}>
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
                  <button className="btn" style={{ marginLeft: "auto", padding: "1px 8px" }}
                    title="remove this rule" onClick={() => removeRule(idx)}>remove rule</button>
                </div>
                <div className="label" data-testid={`label-${path}`}
                  title="GENERATED from the fields below by catalogue.label — what the app will say">
                  {lab ?? <span className="dim">— does not validate; see the errors below —</span>}
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
                  return (
                    <div className={`binding${bound ? "" : " unbound"}`} data-testid={`binding-${path}`}>
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
                <ErrorsAt path={path} />
                <button type="button" className="btn small edit-toggle"
                  onClick={() => setOpenRule((o) => ({ ...o, [rule.rule_id]: !(o[rule.rule_id] ?? bad) }))}>
                  {(openRule[rule.rule_id] ?? bad) ? "▾ close editor" : "▸ edit rule"}
                </button>
                {(openRule[rule.rule_id] ?? bad) && (
                  <RuleEditor rule={rule} path={path} onChange={(r) => patchRule(idx, r)}
                    siblings={rules.map((r) => r.rule_id)} boundaries={boundaries}
                    itemNames={itemNames} matched={matched} />
                )}
              </div>
            );
          })}
          <button className="btn" style={{ padding: "3px 10px", marginTop: 6 }} onClick={addRule}>
            + add rule
          </button>
        </div>
      </div>

      <div className="section">
        <h3>Licensing ({licensing.length})</h3>
        <div className="dim" style={{ marginBottom: 6 }}>
          What you must hold, and what this water is. Never votes on open/closed.
        </div>
        <div className="rules-list">
          {licensing.map((rec, idx) => {
            const path = `licensing.${idx}`;
            const lab = liveLabel("licensing", idx, rec.label);
            const bad = errors.some((e) => e.path === path || e.path.startsWith(path + "."));
            const key = `lic:${idx}`;
            return (
              <div className={`rule${rec.review_reason ? " flagged" : ""}${bad ? " has-error" : ""}`}
                   key={idx} data-record={rec.id}>
                <div className="rule-head">
                  <span className="badge">{rec.kind}</span>
                  <span className="rule-id">{rec.id}</span>
                  <button className="btn" style={{ marginLeft: "auto", padding: "1px 8px" }}
                    onClick={() => removeRecord(idx)}>remove record</button>
                </div>
                <div className="label" data-testid={`label-${path}`}
                  title="GENERATED by catalogue.licensing_label — what the app will say">
                  {lab ?? <span className="dim">— does not validate; see the errors below —</span>}
                </div>
                {rec.verbatim && <div className="rule-verbatim">{rec.verbatim}</div>}
                {rec.review_reason && (
                  <div className="review-flag"><strong>⚠ review_reason</strong> {String(rec.review_reason)}</div>
                )}
                <ErrorsAt path={path} />
                <button type="button" className="btn small edit-toggle"
                  onClick={() => setOpenRule((o) => ({ ...o, [key]: !(o[key] ?? bad) }))}>
                  {(openRule[key] ?? bad) ? "▾ close editor" : "▸ edit record"}
                </button>
                {(openRule[key] ?? bad) && (
                  <LicensingEditor rec={rec} onChange={(x) => patchRecord(idx, x)}
                    ctx={{ path, rules: rules.map((r) => r.rule_id), boundaries, itemNames, matched }} />
                )}
              </div>
            );
          })}
          <div className="add-record">
            <select value="" aria-label="add licensing record"
              onChange={(e) => { if (e.target.value) addRecord(e.target.value); }}>
              <option value="">+ add licensing record…</option>
              {vocab.licensing_kinds.map((k) => <option key={k} value={k}>{k}</option>)}
            </select>
          </div>
        </div>
      </div>

      <AnswerPanel entry={detail.entry} version={detail.entry} verdict={reaches?.verdict} />

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

      {/* Errors / toast — every error, with the field it is addressed to. The same errors also
          sit on the controls above; this is the list to work down. */}
      {errors.length > 0 && (
        <div className="errors" data-testid="errors">
          <strong>{saveErrors.length ? "Save refused" : "The model refuses this draft"}</strong>
          <ul>
            {errors.map((e, i) => (
              <li key={i}>{e.path && <code>{e.path}</code>} {e.msg}</li>
            ))}
          </ul>
        </div>
      )}
      {(checked?.warnings ?? []).length > 0 && (
        <details className="warnings">
          <summary>{checked!.warnings.length} extent warning(s) — reported, not refused</summary>
          <ul>{checked!.warnings.map((w, i) => <li key={i}><code>{w.path}</code> {w.msg}</li>)}</ul>
        </details>
      )}

      {/* Actions */}
      <div className="actions">
        <button className="btn" disabled={saving || !dirty} onClick={() => doSave()}>
          Save edit
        </button>
        {dirty && <span className="dim">unsaved changes</span>}
        {dirty && checked && (checked.ok
          ? <span className="chip-tag synced">the model accepts this draft</span>
          : <span className="chip-tag orphan">{checked.errors.length} error(s)</span>)}
        {dirty && (
          <button className="btn" onClick={() => { setEntry(stored(structuredClone(detail.entry))); setSaveErrors([]); }}>
            discard
          </button>
        )}
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
    </ErrorsCtx.Provider>
  );
}
