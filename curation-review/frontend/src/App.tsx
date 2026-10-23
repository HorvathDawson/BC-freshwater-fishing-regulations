import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "./api";
import type {
  EntryDetail as EntryDetailT, Mode, PendingBuild, Progress, QueueOrder, QueueRow, RegionSummary,
  SplitCard, Status, Tier, Vocab, VerifyStatus,
} from "./types";
import { VocabCtx } from "./model";
import { FilterBar } from "./components/FilterBar";
import { QueueList } from "./components/QueueList";
import { EntryDetail } from "./components/EntryDetail";
import { SplitCardView } from "./components/SplitCardView";
import { ProgressBar } from "./components/ProgressBar";
import { isTyping, keepSelected, nextUnsure, paceSamples, recordPace } from "./review";

const TIERS: Tier[] = ["P1", "Z", "P2", "P3", "P4", "sweep"];
/** `#split:<id>` opens a split card; any other hash is an entry id (`#r5:dean_river@5-9`) */
const SPLIT_HASH = "split:";

export default function App() {
  // T134: review mode (marks only) or the owner's fix mode — the server says which
  const [mode, setMode] = useState<Mode | null>(null);
  const [work, setWork] = useState<"entries" | "splits">(
    decodeURIComponent(window.location.hash.slice(1)).startsWith(SPLIT_HASH) ? "splits" : "entries");
  const [regions, setRegions] = useState<RegionSummary[]>([]);
  const [region, setRegion] = useState("");
  const [status, setStatus] = useState<Status | "">("");
  const [tier, setTier] = useState<Tier | "">("");
  // the reviewer's pass: which marks to list, and in what order (the book's, by default — the
  // way to go through every entry; "attention" floats the suspicious ones up)
  const [verify, setVerify] = useState<VerifyStatus | "todo" | "">("");
  const [order, setOrder] = useState<QueueOrder>("book");
  const [rows, setRows] = useState<QueueRow[]>([]);
  const [loadingRows, setLoadingRows] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<EntryDetailT | null>(null);
  const [cards, setCards] = useState<SplitCard[]>([]);
  const [allSplits, setAllSplits] = useState(false);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [samples, setSamples] = useState<number[]>(paceSamples);
  // The model's vocabulary — every option list the editors offer, read off catalogue.py.
  const [vocab, setVocab] = useState<Vocab | null>(null);
  const [error, setError] = useState<string>("");
  // bumped by refresh so the map (and anything keyed on it) refetches even for the same item — after
  // an atlas rebuild run from a terminal the backend drops its caches and the geometry is new
  const [reloadKey, setReloadKey] = useState(0);
  // when the open entry (or split) was opened — its time to the last mark is the reviewer's pace
  const openedAt = useRef<number>(Date.now());

  useEffect(() => {
    api.mode().then(setMode).catch((e) => setError(`mode: ${e}`));
    api
      .regions()
      .then((r) => {
        setRegions(r);
        // an entry named in the URL picks its own region (below); otherwise the first
        if (r.length && !region && !window.location.hash) setRegion(r[0].id);
      })
      .catch((e) => setError(String(e)));
    api.vocab().then(setVocab).catch((e) => setError(`vocabulary: ${e}`));
    // eslint-disable-next-line react-hooks/exhaustive-deps
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
      .entries(region, status, "", verify, order, tier)
      // THE OPEN ENTRY STAYS IN THE LIST after it is marked (a "to do" filter drops it otherwise)
      .then((fresh) => setRows((old) => keepSelected(fresh, old, selectedRef.current)))
      .catch((e) => setError(String(e)))
      .finally(() => setLoadingRows(false));
  }, [region, status, verify, order, tier]);

  useEffect(() => {
    loadRows();
  }, [loadRows]);

  const loadCards = useCallback(() => {
    api.splitCards(allSplits).then((c) => setCards((old) => {
      // the open card stays in the list after it is marked, at its place
      const sel = selectedRef.current;
      if (!sel || c.some((x) => x.split_id === sel)) return c;
      const at = old.findIndex((x) => x.split_id === sel);
      return at < 0 ? c : [...c.slice(0, at), old[at], ...c.slice(at)];
    })).catch((e) => setError(String(e)));
  }, [allSplits]);
  useEffect(() => { if (work === "splits") loadCards(); }, [work, loadCards]);

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

  const openCard = useCallback((splitId: string) => {
    setSelected(splitId);
    openedAt.current = Date.now();
    const h = `${SPLIT_HASH}${splitId}`;
    if (decodeURIComponent(window.location.hash.slice(1)) !== h)
      window.history.replaceState(null, "", `#${encodeURIComponent(h)}`);
  }, []);

  // Open what the URL names (#r5:dean_river@5-9, or #split:<id>), in its own region — on load, and
  // whenever the hash changes (a pasted link, the browser's back button). A reload resumes there.
  useEffect(() => {
    const open = () => {
      const id = decodeURIComponent(window.location.hash.slice(1));
      if (!id) return;
      if (id.startsWith(SPLIT_HASH)) {
        setWork("splits");
        setSelected(id.slice(SPLIT_HASH.length));
        return;
      }
      setWork("entries");
      setSelected(id);
      api.entry(id)
        .then((d) => { setRegion(d.region); setDetail(d); })
        .catch((e) => setError(String(e)));
    };
    open();
    window.addEventListener("hashchange", open);
    return () => window.removeEventListener("hashchange", open);
  }, []);

  // Where the open entry sits in the list, and its neighbours — so a reviewer can walk the
  // whole region in order without going back to the list.
  const list: { id: string; verify?: string }[] = work === "splits"
    ? cards.map((c) => ({ id: c.split_id, verify: c.status }))
    : rows.map((r) => ({ id: r.entry_id, verify: r.verify }));
  const go = work === "splits" ? openCard : loadDetail;
  const idx = list.findIndex((r) => r.id === selected);
  const nav = {
    pos: idx + 1,
    total: list.length,
    onPrev: idx > 0 ? () => go(list[idx - 1].id) : undefined,
    onNext: idx >= 0 && idx < list.length - 1 ? () => go(list[idx + 1].id)
      : idx < 0 && list.length ? () => go(list[0].id) : undefined,
  };
  const navRef = useRef({ nav, list, idx, go });
  navRef.current = { nav, list, idx, go };

  // j / k: next / previous; u: the next entry marked unsure (PLAN §4 keys)
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      const N = navRef.current;
      if (ev.key === "j" && N.nav.onNext) { N.nav.onNext(); ev.preventDefault(); }
      else if (ev.key === "k" && N.nav.onPrev) { N.nav.onPrev(); ev.preventDefault(); }
      else if (ev.key === "u") {
        const i = nextUnsure(N.list, N.idx < 0 ? -1 : N.idx);
        if (i >= 0) { N.go(N.list[i].id); ev.preventDefault(); }
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // THE PENDING BUILD (T133, PLAN §5): the entries and splits edited since the last build, and the
  // command the owner runs. Re-read after a save and, while anything waits, once a minute — the
  // server re-reads the reach run / bundle / release by their digests, so when the owner's rebuild
  // lands the list empties and the open entry clears "awaiting build" with no restart.
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
          if (work === "splits") loadCards();
          if (selected && work === "entries") api.entry(selected).then(setDetail).catch(() => {});
        }
      }).catch(() => {});
    }, 60_000);
    return () => window.clearInterval(t);
  }, [waiting, loadRows, loadCards, loadProgress, selected, work]);

  // After a save, refresh both the queue and the open detail.
  const onSaved = useCallback(() => {
    loadRows();
    loadPending();
    loadProgress();
    api.regions().then(setRegions).catch(() => {});
    if (selected) api.entry(selected).then(setDetail).catch(() => {});
  }, [loadRows, loadPending, loadProgress, selected]);

  // A mark was recorded. When the entry is done: its time is the reviewer's pace, and the next one
  // opens (j). The marked row stays listed (`keepSelected`).
  const onMarked = useCallback((final: boolean) => {
    if (final) setSamples(recordPace((Date.now() - openedAt.current) / 1000));
    loadRows();
    loadProgress();
    if (work === "splits") loadCards();
    api.regions().then(setRegions).catch(() => {});
    if (work === "entries" && selected) {
      if (final && navRef.current.nav.onNext) navRef.current.nav.onNext();
      else api.entry(selected).then(setDetail).catch(() => {});
    } else if (final && navRef.current.nav.onNext) {
      navRef.current.nav.onNext();
    }
  }, [loadRows, loadProgress, loadCards, selected, work]);

  const card = work === "splits" ? cards.find((c) => c.split_id === selected) ?? null : null;

  return (
    <div className={`app${mode?.fix ? " fix-mode" : ""}`}>
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
          if (work === "splits") loadCards();
          api.regions().then(setRegions).catch(() => {});
          api.vocab().then(setVocab).catch(() => {});
          if (selected && work === "entries") api.entry(selected).then(setDetail).catch(() => {});
          loadPending();
          setReloadKey((k) => k + 1);
        }}
        slot={
          <>
            <div>
              <label>work</label>
              <select value={work} aria-label="work" onChange={(e) => {
                setWork(e.target.value as "entries" | "splits");
                setSelected(null);
                setDetail(null);
              }}>
                <option value="entries">entries</option>
                <option value="splits">split points (S0)</option>
              </select>
            </div>
            {work === "entries" ? (
              <div>
                <label>tier</label>
                <select value={tier} aria-label="tier" onChange={(e) => setTier(e.target.value as Tier | "")}>
                  <option value="">all</option>
                  {TIERS.map((t) => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
            ) : (
              <label className="dim">
                <input type="checkbox" checked={allSplits} onChange={(e) => setAllSplits(e.target.checked)} />
                {" "}every split, not only S0
              </label>
            )}
            <span className={`mode-chip ${mode?.fix ? "fix" : "review"}`} data-testid="mode"
              title={mode?.fix ? "fix mode: the editors write the catalogue and splits.json"
                : "review mode: the one thing this screen writes is a mark"}>
              {mode ? (mode.fix ? "FIX MODE" : "review mode") : "…"}
            </span>
            <ProgressBar progress={progress} samples={samples} />
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
          {mode?.fix && (
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
          )}
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
        {work === "splits" ? (
          <div className="queue">
            {cards.map((c) => (
              <div key={c.split_id} data-entry={c.split_id}
                className={`queue-row${selected === c.split_id ? " selected" : ""}`}
                onClick={() => openCard(c.split_id)}>
                <div className="name">{c.label || c.split_id}</div>
                <div className="meta">
                  {c.grade && <span className="badge warn">grade {c.grade}</span>}
                  {c.status !== "unmarked" && <span className={`verify-chip ${c.status}`}>{c.status}</span>}
                  {c.vintage && c.vintage.state !== "current" && <span className="verify-chip stale">awaiting build</span>}
                  <span className="dim">{c.waterbody}</span>
                </div>
              </div>
            ))}
            {!cards.length && <div className="empty-state">No split points to verify.</div>}
          </div>
        ) : (
          <QueueList
            rows={rows}
            selected={selected}
            loading={loadingRows}
            onSelect={loadDetail}
          />
        )}
        {work === "splits" ? (
          card && mode ? (
            <SplitCardView key={card.split_id} card={card} mode={mode} onMarked={() => onMarked(true)}
              nav={nav} reloadKey={reloadKey} />
          ) : (
            <div className="detail"><div className="empty-state">Select a split point.</div></div>
          )
        ) : detail && vocab && mode ? (
          <VocabCtx.Provider value={vocab}>
            <EntryDetail
              key={detail.entry.entry_id}
              detail={detail}
              onSaved={onSaved}
              onNavigate={loadDetail}
              reloadKey={reloadKey}
              nav={nav}
              mode={mode}
              onMarked={onMarked}
            />
          </VocabCtx.Provider>
        ) : (
          <div className="detail">
            <div className="empty-state">Select an entry from the queue.</div>
          </div>
        )}
      </div>
    </div>
  );
}
