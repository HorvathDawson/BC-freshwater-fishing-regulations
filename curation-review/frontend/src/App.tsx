import { useCallback, useEffect, useState } from "react";
import { api } from "./api";
import type {
  EntryDetail as EntryDetailT, QueueOrder, QueueRow, RegionSummary, Status, Vocab, VerifyStatus,
} from "./types";
import { VocabCtx } from "./model";
import { FilterBar } from "./components/FilterBar";
import { QueueList } from "./components/QueueList";
import { EntryDetail } from "./components/EntryDetail";
import { RebuildButton } from "./components/RebuildButton";

export default function App() {
  const [regions, setRegions] = useState<RegionSummary[]>([]);
  const [region, setRegion] = useState("");
  const [status, setStatus] = useState<Status | "">("");
  // the reviewer's pass: which marks to list, and in what order (the book's, by default — the
  // way to go through every entry; "attention" floats the suspicious ones up)
  const [verify, setVerify] = useState<VerifyStatus | "todo" | "">("");
  const [order, setOrder] = useState<QueueOrder>("book");
  const [progress, setProgress] = useState<{ total: number; verified: number } | null>(null);
  const [rows, setRows] = useState<QueueRow[]>([]);
  const [loadingRows, setLoadingRows] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<EntryDetailT | null>(null);
  // The model's vocabulary — every option list the editors offer, read off catalogue.py.
  const [vocab, setVocab] = useState<Vocab | null>(null);
  const [error, setError] = useState<string>("");
  // bumped after a graph rebuild so the map (and anything keyed on it) refetches even for the same item
  const [reloadKey, setReloadKey] = useState(0);

  // Load region summaries + the model vocabulary once.
  useEffect(() => {
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

  const loadRows = useCallback(() => {
    if (!region) return;
    setLoadingRows(true);
    api
      .entries(region, status, "", verify, order)
      .then(setRows)
      .catch((e) => setError(String(e)))
      .finally(() => setLoadingRows(false));
    api.verification().then(setProgress).catch(() => {});
  }, [region, status, verify, order]);

  useEffect(() => {
    loadRows();
  }, [loadRows]);

  const loadDetail = useCallback((entryId: string) => {
    setSelected(entryId);
    // the open entry is in the URL, so a reload or a shared link lands on it
    if (decodeURIComponent(window.location.hash.slice(1)) !== entryId)
      window.history.replaceState(null, "", `#${encodeURIComponent(entryId)}`);
    api
      .entry(entryId)
      .then(setDetail)
      .catch((e) => setError(String(e)));
  }, []);

  // Open the entry named in the URL (#r5:dean_river@5-9), in its own region — on load, and
  // whenever the hash changes (a pasted link, the browser's back button).
  useEffect(() => {
    const open = () => {
      const id = decodeURIComponent(window.location.hash.slice(1));
      if (!id) return;
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
  const idx = rows.findIndex((r) => r.entry_id === selected);
  const nav = {
    pos: idx + 1,
    total: rows.length,
    onPrev: idx > 0 ? () => loadDetail(rows[idx - 1].entry_id) : undefined,
    onNext: idx >= 0 && idx < rows.length - 1 ? () => loadDetail(rows[idx + 1].entry_id)
      : idx < 0 && rows.length ? () => loadDetail(rows[0].entry_id) : undefined,
  };
  const onMarked = useCallback(() => {
    loadRows();
    api.regions().then(setRegions).catch(() => {});
  }, [loadRows]);

  // After a save, refresh both the queue and the open detail.
  const onSaved = useCallback(() => {
    loadRows();
    api.regions().then(setRegions).catch(() => {});
    if (selected) api.entry(selected).then(setDetail).catch(() => {});
  }, [loadRows, selected]);

  // After a graph rebuild: the backend has dropped its caches, so reload every view — region summaries,
  // the vocabulary, the queue, and the open entry (its boundaries/geometry are now the freshly-built ones).
  // Bumping reloadKey forces the map to refetch geojson even when the same item stays selected.
  const onRebuilt = useCallback(() => {
    loadRows();
    api.regions().then(setRegions).catch(() => {});
    api.vocab().then(setVocab).catch(() => {});
    if (selected) api.entry(selected).then(setDetail).catch(() => {});
    setReloadKey((k) => k + 1);
  }, [loadRows, selected]);

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
        progress={progress}
        onRefresh={() => {
          loadRows();
          api.regions().then(setRegions).catch(() => {});
          if (selected) api.entry(selected).then(setDetail).catch(() => {});
        }}
        slot={<RebuildButton onRebuilt={onRebuilt} />}
      />
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
        />
        {detail && vocab ? (
          <VocabCtx.Provider value={vocab}>
            <EntryDetail
              key={detail.entry.entry_id}
              detail={detail}
              onSaved={onSaved}
              onNavigate={loadDetail}
              reloadKey={reloadKey}
              nav={nav}
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
