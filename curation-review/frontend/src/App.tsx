import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "./api";
import type {
  EntryDetail as EntryDetailT, Mode, PendingBuild, Progress, QueueOrder, QueueRow, RegionSummary,
  ReviewFilter, Status, Tier,
} from "./types";
import { ALL_REGIONS, FilterBar } from "./components/FilterBar";
import { QueueList } from "./components/QueueList";
import { EntryDetail } from "./components/EntryDetail";
import { ProgressBar } from "./components/ProgressBar";
import { isTyping, keepSelected, nextUnsure, paceSamples, recordPace } from "./review";

/** THE REVIEW TIERS (PLAN §3, `tiers.TIERS`) — entries only: a split point is checked by proxy on
 *  the map of each entry whose rules name it (T184), never as a target of its own. */
const TIERS: Tier[] = ["P1", "Z", "P2", "P3", "P4", "sweep"];

/* THE REVIEW SCREEN (T134, T184): the one queue on the left, the open entry on the right. It writes
   one thing, a reviewer's mark; there is no other mode (T185 retired the editors). The URL's hash is
   the open entry (`#r5:dean_river@5-9`), so a reload or a shared link resumes there. */
export default function App() {
  // the checks, the problem categories and the commands the server reviews with
  const [mode, setMode] = useState<Mode | null>(null);
  const [regions, setRegions] = useState<RegionSummary[]>([]);
  // THE ONE QUEUE (T135, PLAN §3): every region, in the review order, by default
  const [region, setRegion] = useState(ALL_REGIONS);
  const [status, setStatus] = useState<Status | "">("");
  const [tier, setTier] = useState<Tier | "">("");
  // the reviewer's pass: which marks to list, and in what order — the review order by default
  // (tier, then the most urgent risk factor; T135), or the book's (region, page)
  const [verify, setVerify] = useState<ReviewFilter>("");
  const [order, setOrder] = useState<QueueOrder>("risk");
  // THE PILOT FIRST (T136): while a pilot file exists the queue opens on its 30 targets, in its order
  const [pilotSize, setPilotSize] = useState(0);
  const [pilot, setPilot] = useState(true);
  const [rows, setRows] = useState<QueueRow[]>([]);
  const [loadingRows, setLoadingRows] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<EntryDetailT | null>(null);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [samples, setSamples] = useState<number[]>(paceSamples);
  const [error, setError] = useState<string>("");
  // bumped by refresh so the map (and anything keyed on it) refetches even for the same item — after
  // an atlas rebuild run from a terminal the backend drops its caches and the geometry is new
  const [reloadKey, setReloadKey] = useState(0);
  // when the open entry was opened — its time to the last mark is the reviewer's pace
  const openedAt = useRef<number>(Date.now());

  useEffect(() => {
    api.mode().then(setMode).catch((e) => setError(`mode: ${e}`));
    api.regions().then(setRegions).catch((e) => setError(String(e)));
    api.pilot().then((t) => { setPilotSize(t.length); if (!t.length) setPilot(false); }).catch(() => setPilot(false));
  }, []);

  const loadProgress = useCallback(() => {
    api.progress().then(setProgress).catch(() => {});
  }, []);
  useEffect(() => { loadProgress(); }, [loadProgress]);

  const selectedRef = useRef(selected);
  selectedRef.current = selected;
  const loadRows = useCallback(() => {
    if (!region) return;
    setLoadingRows(true);
    api
      .entries(region === ALL_REGIONS ? undefined : region, status, "", verify, order, tier, pilot)
      // THE OPEN ENTRY STAYS IN THE LIST after it is marked (a "to do" filter drops it otherwise)
      .then((fresh) => setRows((old) => keepSelected(fresh, old, selectedRef.current)))
      .catch((e) => setError(String(e)))
      .finally(() => setLoadingRows(false));
  }, [region, status, verify, order, tier, pilot]);

  useEffect(() => {
    loadRows();
  }, [loadRows]);

  const loadDetail = useCallback((entryId: string) => {
    setSelected(entryId);
    openedAt.current = Date.now();
    // the open entry is in the URL, so a reload or a shared link lands on it (resume in place)
    if (decodeURIComponent(window.location.hash.slice(1)) !== entryId)
      window.history.replaceState(null, "", `#${encodeURIComponent(entryId)}`);
    api
      .entry(entryId)
      .then(setDetail)
      .catch((e) => setError(String(e)));
  }, []);

  // Open what the URL names (#r5:dean_river@5-9), in its own region — on load, and whenever the
  // hash changes (a pasted link, the browser's back button). A reload resumes there.
  useEffect(() => {
    const open = () => {
      const id = decodeURIComponent(window.location.hash.slice(1));
      if (!id) return;
      setSelected(id);
      openedAt.current = Date.now();
      api.entry(id)
        // the entry's own region, unless the one queue (every region) is showing
        .then((d) => { setRegion((r) => (r === ALL_REGIONS ? r : d.region)); setDetail(d); })
        .catch((e) => setError(String(e)));
    };
    open();
    window.addEventListener("hashchange", open);
    return () => window.removeEventListener("hashchange", open);
  }, []);

  // Where the open entry sits in the list, and its neighbours — so a reviewer can walk the
  // whole queue in order without going back to the list.
  const list = rows.map((r) => ({ id: r.entry_id, verify: r.verify }));
  const idx = list.findIndex((r) => r.id === selected);
  const nav = {
    pos: idx + 1,
    total: list.length,
    onPrev: idx > 0 ? () => loadDetail(list[idx - 1].id) : undefined,
    onNext: idx >= 0 && idx < list.length - 1 ? () => loadDetail(list[idx + 1].id)
      : idx < 0 && list.length ? () => loadDetail(list[0].id) : undefined,
  };
  const navRef = useRef({ nav, list, idx });
  navRef.current = { nav, list, idx };

  // j / k: next / previous; u: the next entry marked unsure (PLAN §4 keys)
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      const N = navRef.current;
      if (ev.key === "j" && N.nav.onNext) { N.nav.onNext(); ev.preventDefault(); }
      else if (ev.key === "k" && N.nav.onPrev) { N.nav.onPrev(); ev.preventDefault(); }
      else if (ev.key === "u") {
        const i = nextUnsure(N.list, N.idx < 0 ? -1 : N.idx);
        if (i >= 0) { loadDetail(N.list[i].id); ev.preventDefault(); }
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [loadDetail]);

  // THE PENDING BUILD (T133, PLAN §5): the entries and splits changed since the last build, and the
  // command the owner runs. While anything waits it is re-read once a minute — the server re-reads
  // the reach run / bundle / release by their digests, so when the owner's rebuild lands the list
  // empties and the open entry clears "awaiting build" with no restart.
  const [pending, setPending] = useState<PendingBuild | null>(null);
  const loadPending = useCallback(() => {
    api.pendingBuild().then(setPending).catch(() => {});
  }, []);
  useEffect(() => { loadPending(); }, [loadPending]);
  const waiting = (pending?.entries.length ?? 0) + (pending?.splits.length ?? 0);
  useEffect(() => {
    if (!waiting) return;
    const t = window.setInterval(() => {
      api.pendingBuild().then((p) => {
        setPending(p);
        if (p.entries.length + p.splits.length < waiting) {
          loadRows();
          loadProgress();
          if (selected) api.entry(selected).then(setDetail).catch(() => {});
        }
      }).catch(() => {});
    }, 60_000);
    return () => window.clearInterval(t);
  }, [waiting, loadRows, loadProgress, selected]);

  // A mark was recorded. When the entry is done: its time is the reviewer's pace, and the next one
  // opens (j). The marked row stays listed (`keepSelected`).
  const onMarked = useCallback((final: boolean) => {
    if (final) setSamples(recordPace((Date.now() - openedAt.current) / 1000));
    loadRows();
    loadProgress();
    api.regions().then(setRegions).catch(() => {});
    if (!selected) return;
    if (final && navRef.current.nav.onNext) navRef.current.nav.onNext();
    else api.entry(selected).then(setDetail).catch(() => {});
  }, [loadRows, loadProgress, selected]);

  return (
    <div className="app">
      <FilterBar
        regions={regions}
        region={region}
        status={status}
        onRegion={(r) => {
          setRegion(r);
          setSelected(null);
          setDetail(null);
        }}
        onStatus={setStatus}
        verify={verify}
        onVerify={setVerify}
        order={order}
        onOrder={setOrder}
        onRefresh={() => {
          loadRows();
          loadProgress();
          api.regions().then(setRegions).catch(() => {});
          if (selected) api.entry(selected).then(setDetail).catch(() => {});
          loadPending();
          setReloadKey((k) => k + 1);
        }}
        slot={pilotSize > 0 ? (
          <button className={`btn small pilot-toggle${pilot ? " on" : ""}`} data-testid="pilot-toggle"
            aria-pressed={pilot} onClick={() => setPilot((p) => !p)}>
            {pilot
              ? `Pilot: ${rows.filter((r) => r.reviewed || (r.verify && r.verify !== "unmarked")).length}/${pilotSize} marked — show all`
              : `Show the ${pilotSize} pilot entries`}
          </button>
        ) : null}
        moreSet={tier ? 1 : 0}
        more={
          <>
            <div>
              <label>tier</label>
              <select value={tier} aria-label="tier" onChange={(e) => setTier(e.target.value as Tier | "")}>
                <option value="">all</option>
                {TIERS.map((t) => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <ProgressBar progress={progress} samples={samples} commands={mode?.commands} />
          </>
        }
      />
      {pending && waiting > 0 && (
        <div className="pending-build" data-testid="pending-build" style={{ margin: 8 }}>
          <strong>Awaiting build:</strong>{" "}
          {pending.entries.length} entr{pending.entries.length === 1 ? "y" : "ies"}
          {pending.splits.length > 0 && `, ${pending.splits.length} split${pending.splits.length === 1 ? "" : "s"}`}
          {" "}— only T and R can be marked on them until it runs. Run, in order:{" "}
          {pending.commands.map((c) => <code key={c} className="cmd">{c}</code>)}
          {pending.splits_checked === "ids" && (
            <span className="dim"> (the served atlas predates its split record: a MOVED split is not seen until the next atlas build)</span>
          )}
          <details className="pending-list" data-testid="pending-list">
            <summary>what is waiting</summary>
            <ul>
              {pending.splits.map((s) => (
                <li key={`s:${s.split_id}`}>split <code>{s.split_id}</code> — {s.reasons.join(", ")}</li>
              ))}
              {pending.entries.slice(0, 200).map((e) => (
                <li key={e.entry_id}>
                  <a href={`#${encodeURIComponent(e.entry_id)}`}>{e.name}</a> <span className="dim">r{e.region}</span> — {e.reasons.join(", ")}
                  {e.splits.length > 0 && <span className="dim"> ({e.splits.join(", ")})</span>}
                </li>
              ))}
              {pending.entries.length > 200 && <li className="dim">… {pending.entries.length - 200} more</li>}
            </ul>
          </details>
        </div>
      )}
      {error && (
        <div className="errors" style={{ margin: 8 }}>
          {error}{" "}
          <button className="btn" style={{ padding: "2px 8px" }} onClick={() => setError("")}>
            dismiss
          </button>
        </div>
      )}
      <div className="main">
        <QueueList
          rows={rows}
          selected={selected}
          loading={loadingRows}
          onSelect={loadDetail}
          facts={progress?.facts}
        />
        {detail && mode ? (
          <EntryDetail
            key={detail.entry.entry_id}
            detail={detail}
            onNavigate={loadDetail}
            reloadKey={reloadKey}
            nav={nav}
            mode={mode}
            onMarked={onMarked}
          />
        ) : (
          <div className="detail">
            <div className="empty-state">Select an entry from the queue.</div>
          </div>
        )}
      </div>
    </div>
  );
}
