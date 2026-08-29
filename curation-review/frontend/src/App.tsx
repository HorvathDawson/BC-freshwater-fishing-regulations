import { useCallback, useEffect, useState } from "react";
import { api } from "./api";
import type { EntryDetail as EntryDetailT, QueueRow, RegionSummary, SpeciesOption, Status } from "./types";
import { FilterBar } from "./components/FilterBar";
import { QueueList } from "./components/QueueList";
import { EntryDetail } from "./components/EntryDetail";
import { RebuildButton } from "./components/RebuildButton";

const CURATOR_KEY = "curation-review.curator";

export default function App() {
  const [regions, setRegions] = useState<RegionSummary[]>([]);
  const [region, setRegion] = useState("");
  const [status, setStatus] = useState<Status | "">("");
  const [rows, setRows] = useState<QueueRow[]>([]);
  const [loadingRows, setLoadingRows] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<EntryDetailT | null>(null);
  const [curator, setCurator] = useState(() => localStorage.getItem(CURATOR_KEY) ?? "");
  const [species, setSpecies] = useState<SpeciesOption[]>([]);
  const [error, setError] = useState<string>("");
  // bumped after a graph rebuild so the map (and anything keyed on it) refetches even for the same item
  const [reloadKey, setReloadKey] = useState(0);

  // Prompt once for a curator name (remembered in localStorage).
  useEffect(() => {
    if (!curator) askCurator();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function askCurator() {
    const name = window.prompt("Curator name (used to stamp confirms):", curator || "");
    if (name && name.trim()) {
      const v = name.trim();
      setCurator(v);
      localStorage.setItem(CURATOR_KEY, v);
    }
  }

  // Load region summaries + the species list once.
  useEffect(() => {
    api
      .regions()
      .then((r) => {
        setRegions(r);
        if (r.length && !region) setRegion(r[0].id);
      })
      .catch((e) => setError(String(e)));
    api.species().then(setSpecies).catch(() => {});
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

  // After a save/confirm, refresh both the queue and the open detail.
  const onSaved = useCallback(() => {
    loadRows();
    api.regions().then(setRegions).catch(() => {});
    if (selected) api.entry(selected).then(setDetail).catch(() => {});
  }, [loadRows, selected]);

  // After a confirm+lock: reload the queue, then jump to the next UNLOCKED entry in the region (so the
  // curator can keep confirming without hunting). Advancement is anchored to the entry's position in the
  // PRE-refresh queue, so it still moves to the neighbour that followed it even when a status filter
  // drops the just-locked row out of the fresh list (otherwise findIndex=-1 sent us back to the top).
  // Wraps around; falls back to refreshing the current entry if none are left unlocked.
  const onConfirmed = useCallback(() => {
    api.regions().then(setRegions).catch(() => {});
    if (!region) {
      onSaved();
      return;
    }
    // Order of entry_ids that followed `selected` in the queue as it looked when we confirmed.
    const prevIdx = rows.findIndex((r) => r.entry_id === selected);
    const followingIds =
      prevIdx >= 0
        ? [...rows.slice(prevIdx + 1), ...rows.slice(0, prevIdx)].map((r) => r.entry_id)
        : [];
    api
      .entries(region, status)
      .then((fresh) => {
        setRows(fresh);
        const byId = new Map(fresh.map((r) => [r.entry_id, r]));
        // Prefer the first still-present, unlocked neighbour in the original order.
        let next = followingIds.map((id) => byId.get(id)).find((r) => r && !r.locked) ?? null;
        if (!next) {
          // Fallbacks: the just-locked row is still visible (unfiltered view) → advance from it;
          // otherwise take the first unlocked entry in the fresh list.
          const cur = fresh.findIndex((r) => r.entry_id === selected);
          const ordered = cur >= 0 ? [...fresh.slice(cur + 1), ...fresh.slice(0, cur)] : fresh;
          next = ordered.find((r) => !r.locked) ?? null;
        }
        if (next) loadDetail(next.entry_id);
        else if (selected) api.entry(selected).then(setDetail).catch(() => {});
      })
      .catch((e) => setError(String(e)));
  }, [region, status, selected, rows, loadDetail, onSaved]);

  // After a graph rebuild: the backend has dropped its caches, so reload every view — region summaries,
  // species, the queue, and the open entry (its boundaries/geometry are now the freshly-built ones).
  // Bumping reloadKey forces the map to refetch geojson even when the same item stays selected.
  const onRebuilt = useCallback(() => {
    loadRows();
    api.regions().then(setRegions).catch(() => {});
    api.species().then(setSpecies).catch(() => {});
    if (selected) api.entry(selected).then(setDetail).catch(() => {});
    setReloadKey((k) => k + 1);
  }, [loadRows, selected]);

  return (
    <div className="app">
      <FilterBar
        regions={regions}
        region={region}
        status={status}
        curator={curator}
        onRegion={(r) => {
          setRegion(r);
          setSelected(null);
          setDetail(null);
        }}
        onStatus={setStatus}
        onSetCurator={askCurator}
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
        {detail ? (
          <EntryDetail
            key={detail.entry.entry_id}
            detail={detail}
            curator={curator}
            speciesOptions={species}
            onSaved={onSaved}
            onNavigate={setSelected}
            onConfirmed={onConfirmed}
            reloadKey={reloadKey}
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
