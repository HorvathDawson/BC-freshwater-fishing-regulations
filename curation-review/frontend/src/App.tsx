import { useCallback, useEffect, useState } from "react";
import { api } from "./api";
import type { EntryDetail as EntryDetailT, QueueRow, RegionSummary, Status, Vocab } from "./types";
import { VocabCtx } from "./model";
import { FilterBar } from "./components/FilterBar";
import { QueueList } from "./components/QueueList";
import { EntryDetail } from "./components/EntryDetail";
import { RebuildButton } from "./components/RebuildButton";

export default function App() {
  const [regions, setRegions] = useState<RegionSummary[]>([]);
  const [region, setRegion] = useState("");
  const [status, setStatus] = useState<Status | "">("");
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
        if (r.length && !region) setRegion(r[0].id);
      })
      .catch((e) => setError(String(e)));
    api.vocab().then(setVocab).catch((e) => setError(`vocabulary: ${e}`));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const loadRows = useCallback(() => {
    if (!region) return;
    setLoadingRows(true);
    api
      .entries(region, status)
      .then(setRows)
      .catch((e) => setError(String(e)))
      .finally(() => setLoadingRows(false));
  }, [region, status]);

  useEffect(() => {
    loadRows();
  }, [loadRows]);

  const loadDetail = useCallback((entryId: string) => {
    setSelected(entryId);
    api
      .entry(entryId)
      .then(setDetail)
      .catch((e) => setError(String(e)));
  }, []);

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
