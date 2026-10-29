import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { isTyping } from "../review";
import { MapPanel } from "./MapPanel";
import { TopNav, setBadge } from "./TopNav";

export const REPORTS_HASH = "reports/";

/* THE REPORTS TAB (T237): problem reports the app sent (the staging feeds worker's private/reports/), pulled by
   "Sync" through the backend (GET /api/reports: the worker with the run token). READ ONLY on the server side: the
   reports and each one's STATUS (new / looking / fixed / won't fix) are kept in this browser (localStorage) — a
   to-do list for the owner, never a mark (AGENTS 66). A report names its water (item id) and, from a part / section
   page, the tapped section's handle: resolved to a piece of this build's map only when the report's tiles are this
   build's vintage. */

type Reason = "rule" | "place" | "missing" | "name" | "other";
export type ReportStatus = "new" | "looking" | "fixed" | "wontfix";
const STATUSES: { k: ReportStatus; t: string; chip: string }[] = [
  { k: "new", t: "new", chip: "chip-tag new" },
  { k: "looking", t: "looking", chip: "chip-tag conf-medium" },
  { k: "fixed", t: "fixed", chip: "chip-tag conf-high" },
  { k: "wontfix", t: "won't fix", chip: "chip-tag" },
];
const REASON: Record<Reason, string> = {
  rule: "Wrong rule", place: "Wrong place on the map", missing: "Missing water", name: "Wrong name", other: "Something else",
};

export interface Report {
  id: string; created: string; received: string; reason: Reason; text: string; email: string | null;
  context: { screen?: string; tab?: string; place?: string; water?: string; part?: number; partLabel?: string;
             section?: number; day?: string; mode?: string; app?: string; env?: string; platform?: string;
             release?: string; tiles?: string; errors?: string[] };
  item: { id: string; name: string | null; kind: string | null; known: boolean } | null;
  section: { sid: number; node: string | null; items: string[]; vintage: "same" | "other" | "unknown" } | null;
}

const KEY_REPORTS = "curation-review.reports.v1";
const KEY_STATUS = "curation-review.reports.status.v1";
const KEY_SYNCED = "curation-review.reports.synced.v1";
function readLS<T>(k: string, d: T): T {
  try { const v = window.localStorage.getItem(k); return v ? (JSON.parse(v) as T) : d; } catch { return d; }
}
function writeLS(k: string, v: unknown) {
  try { window.localStorage.setItem(k, JSON.stringify(v)); } catch { /* per viewer only */ }
}

type Filter = "open" | ReportStatus | "all";

export function ReportsView() {
  const [reports, setReports] = useState<Report[]>(() => readLS<Report[]>(KEY_REPORTS, []));
  const [status, setStatusMap] = useState<Record<string, ReportStatus>>(() => readLS(KEY_STATUS, {}));
  const [synced, setSynced] = useState<string | null>(() => readLS<string | null>(KEY_SYNCED, null));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [filter, setFilter] = useState<Filter>("open");
  const [selected, setSelected] = useState<string | null>(() => {
    const h = decodeURIComponent(window.location.hash.slice(1));
    return h.startsWith(REPORTS_HASH) ? h.slice(REPORTS_HASH.length) || null : null;
  });
  const st = (id: string): ReportStatus => status[id] ?? "new";

  const sync = useCallback(() => {
    setBusy(true);
    setError("");
    fetch("/api/reports")
      .then(async (r) => {
        if (!r.ok) throw new Error(`${r.status}: ${(await r.json().catch(() => ({}))).detail ?? r.statusText}`);
        return r.json() as Promise<{ reports: Report[] }>;
      })
      .then((b) => {
        // merge: a report the worker no longer lists (older than its window) stays here with its status
        setReports((old) => {
          const byId = new Map(old.map((x) => [x.id, x]));
          for (const x of b.reports) byId.set(x.id, x);
          const next = [...byId.values()].sort((a, c) => (a.received < c.received ? 1 : a.received > c.received ? -1 : 0));
          writeLS(KEY_REPORTS, next);
          return next;
        });
        const at = new Date().toISOString();
        setSynced(at);
        writeLS(KEY_SYNCED, at);
      })
      .catch((e) => setError(`Sync failed — ${e instanceof Error ? e.message : e}. Showing what was synced before.`))
      .finally(() => setBusy(false));
  }, []);

  const setStatus = useCallback((id: string, s: ReportStatus) => {
    setStatusMap((m) => { const next = { ...m, [id]: s }; writeLS(KEY_STATUS, next); return next; });
  }, []);

  const counts = useMemo(() => {
    const c: Record<ReportStatus, number> = { new: 0, looking: 0, fixed: 0, wontfix: 0 };
    for (const r of reports) c[st(r.id)]++;
    return c;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [reports, status]);
  useEffect(() => { setBadge("reports", String(counts.new + counts.looking)); }, [counts]);

  const rows = useMemo(() => reports.filter((r) => {
    const s = st(r.id);
    return filter === "all" || (filter === "open" ? s === "new" || s === "looking" : s === filter);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [reports, status, filter]);
  const row = reports.find((r) => r.id === selected) ?? null;

  const open = useCallback((id: string) => {
    setSelected(id);
    window.history.replaceState(null, "", `#${REPORTS_HASH}${encodeURIComponent(id)}`);
  }, []);
  const idx = rows.findIndex((r) => r.id === selected);
  const nav = useRef({ next: null as string | null, prev: null as string | null });
  nav.current = { next: idx >= 0 && idx < rows.length - 1 ? rows[idx + 1].id : !selected && rows.length ? rows[0].id : null,
                  prev: idx > 0 ? rows[idx - 1].id : null };
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      if (ev.key === "j" && nav.current.next) { open(nav.current.next); ev.preventDefault(); }
      else if (ev.key === "k" && nav.current.prev) { open(nav.current.prev); ev.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);

  return (
    <div className="app reports-mode" data-testid="reports-mode">
      <div className="filterbar-wrap">
        <TopNav active="reports" right={<>
          <span className="progress" data-testid="reports-progress">
            <strong>{counts.new}</strong> new · {counts.looking} looking · {counts.fixed} fixed · {counts.wontfix} won't fix
            <span className="dim"> · {synced ? `synced ${synced.slice(0, 16).replace("T", " ")} UTC` : "never synced"}</span>
          </span>
          <select aria-label="reports filter" data-testid="reports-filter" value={filter}
            onChange={(e) => setFilter(e.target.value as Filter)}>
            <option value="open">open (new + looking)</option>
            {STATUSES.map((s) => <option key={s.k} value={s.k}>{s.t}</option>)}
            <option value="all">all</option>
          </select>
          <button className="btn small primary" data-testid="reports-sync" disabled={busy} onClick={sync}
            title="pull the reports from staging R2 (private/reports/) through the feeds worker">
            {busy ? "Syncing…" : "Sync"}</button>
        </>} />
      </div>
      {error && <div className="errors" style={{ margin: 8 }} data-testid="reports-error">{error}{" "}
        <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setError("")}>dismiss</button></div>}
      <div className="main">
        <div className="queue" data-testid="reports-queue">
          <div className="queue-group">{rows.length} report{rows.length === 1 ? "" : "s"}{reports.length ? "" : " — press Sync"}</div>
          {rows.map((r) => {
            const s = STATUSES.find((x) => x.k === st(r.id))!;
            return (
              <div key={r.id} data-report={r.id} className={`queue-row${r.id === selected ? " selected" : ""}`}
                onClick={() => open(r.id)}>
                <div className="name">
                  {r.item?.name ?? r.context.place ?? "(no water)"}
                  <span className={s.chip}>{s.t}</span>
                </div>
                <div className="meta">
                  <span>{REASON[r.reason] ?? r.reason}</span>
                  <span>{r.received.slice(0, 16).replace("T", " ")}</span>
                  {r.context.env && <span className="chip-tag">{r.context.env}</span>}
                </div>
                <div className="meta">{r.text.length > 140 ? `${r.text.slice(0, 140)}…` : r.text}</div>
              </div>
            );
          })}
        </div>
        {row ? (
          <ReportDetail key={row.id} r={row} status={st(row.id)} onStatus={(s) => setStatus(row.id, s)}
            nav={{ pos: idx + 1, total: rows.length,
                   onNext: nav.current.next ? () => open(nav.current.next!) : undefined,
                   onPrev: nav.current.prev ? () => open(nav.current.prev!) : undefined }} />
        ) : (
          <div className="detail"><div className="empty-state">{reports.length ? "Select a report." : "No reports yet — press Sync."}</div></div>
        )}
      </div>
    </div>
  );
}

interface Nav { pos: number; total: number; onNext?: () => void; onPrev?: () => void }

function ReportDetail({ r, status, onStatus, nav }: { r: Report; status: ReportStatus; onStatus: (s: ReportStatus) => void; nav: Nav }) {
  const c = r.context;
  const itemId = r.item?.id ?? r.section?.items[0] ?? null;
  const [section, setSection] = useState<GeoJSON.FeatureCollection | null>(null);
  useEffect(() => {
    setSection(null);
    if (!r.section || r.section.vintage !== "same" || !c.tiles) return;
    let live = true;
    fetch(`/api/reports/section?sid=${r.section.sid}&tiles=${encodeURIComponent(c.tiles)}`)
      .then((x) => (x.ok ? x.json() : null)).then((g) => live && g && setSection(g)).catch(() => {});
    return () => { live = false; };
  }, [r.section, c.tiles]);
  const facts: [string, string | undefined][] = [
    ["Where", [c.place, c.partLabel ? `· ${c.partLabel}` : null, c.part !== undefined ? `(part ${c.part})` : null].filter(Boolean).join(" ") || undefined],
    ["Screen", [c.screen, c.tab].filter(Boolean).join(" · ") || undefined],
    ["Day", c.day], ["Map colours", c.mode],
    ["App", [c.app, c.env ? `(${c.env})` : null].filter(Boolean).join(" ") || undefined], ["Platform", c.platform],
    ["Release", c.release], ["Tiles", c.tiles],
    ["Sent", `${r.created} · received ${r.received}`], ["Reply to", r.email ?? undefined],
  ];
  return (
    <div className="detail review-mode" data-testid="reports-detail">
      <div className="detail-cols">
        <div className="detail-content">
          <div className="verify-bar">
            <div className="verify-row">
              <strong>{REASON[r.reason] ?? r.reason}</strong>
              <span className="dim">{r.item?.name ?? c.place ?? "no water named"}</span>
              <span className="spacer" style={{ flex: 1 }} />
              <span className="verify-nav">
                <button className="btn small" disabled={!nav.onPrev} onClick={nav.onPrev}>‹ k</button>
                <span className="dim">{nav.pos || "–"}/{nav.total}</span>
                <button className="btn small" disabled={!nav.onNext} onClick={nav.onNext}>j ›</button>
              </span>
            </div>
            <div className="verify-row" role="radiogroup" aria-label="status" style={{ gap: 6 }}>
              <span className="dim">status</span>
              {STATUSES.map((s) => (
                <button key={s.k} role="radio" aria-checked={status === s.k} data-testid={`report-status-${s.k}`}
                  className={`btn small${status === s.k ? " primary" : ""}`} onClick={() => onStatus(s.k)}>{s.t}</button>
              ))}
              <span className="dim" style={{ fontSize: 11 }}>kept in this browser</span>
            </div>
          </div>
          <div className="section">
            <div className="verbatim" data-testid="report-text" style={{ whiteSpace: "pre-wrap" }}>{r.text}</div>
          </div>
          <div className="section">
            <h3>Context</h3>
            <table className="kv" style={{ fontSize: 12 }}><tbody>
              {facts.filter(([, v]) => v).map(([k, v]) => <tr key={k}><td className="dim" style={{ paddingRight: 10 }}>{k}</td><td>{v}</td></tr>)}
            </tbody></table>
            <div className="dim" style={{ fontSize: 12, marginTop: 6 }} data-testid="report-water">
              {itemId ? <>Water <code>{itemId}</code>{r.item && !r.item.known ? " — not in this build's registry" : ""}</> : "No water named."}
              {r.section && (r.section.vintage === "same"
                ? <> · tapped section <code>{r.section.node ?? r.section.sid}</code> (magenta)</>
                : <> · tapped section from {r.section.vintage === "other" ? "another map vintage" : "an unknown vintage"}: not shown</>)}
            </div>
            {c.errors?.length ? (
              <details style={{ marginTop: 6 }}><summary className="dim">recent errors ({c.errors.length})</summary>
                <pre style={{ fontSize: 11, whiteSpace: "pre-wrap" }}>{c.errors.join("\n")}</pre></details>
            ) : null}
            <div className="dim" style={{ fontSize: 11, marginTop: 6 }}>id {r.id}</div>
          </div>
        </div>
        <div className="detail-map" data-testid="reports-map">
          {itemId ? <MapPanel itemId={itemId} preview={section} /> : <div className="empty-state">No water to show.</div>}
        </div>
      </div>
    </div>
  );
}
