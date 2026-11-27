import type { IdList } from "../types";

/** T414: `/?ids=<file>` limits the entry queue to the ids that file lists (the yearly synopsis diff's
 *  `--ids-out`). The search param, not the hash: the hash stays the open entry. */
export const idsParam = (): string | null => new URLSearchParams(window.location.search).get("ids");

/** drop `?ids=` from the address, keeping the open entry (the hash) */
export function clearIdsParam() {
  const q = new URLSearchParams(window.location.search);
  q.delete("ids");
  const s = q.toString();
  window.history.replaceState(null, "", `${window.location.pathname}${s ? `?${s}` : ""}${window.location.hash}`);
}

interface Props {
  path: string;
  list: IdList | null;
  /** the list could not be read: the queue shows nothing, and says why */
  error: string;
  /** the list's entries the other filters (region, review, severity …) keep out of view */
  hidden: number;
  onClear: () => void;
}

/* ONLY THESE ENTRIES: the list's progress ("12 of 40 reviewed"), the ids that match no entry (each
   one shown — never dropped), and how many the other filters hide. */
export function IdsBar({ path, list, error, hidden, onClear }: Props) {
  return (
    <div className={`pending-build${error || list?.missing.length ? " warn" : ""}`} data-testid="ids-bar"
      style={{ margin: 8 }}>
      <strong>Only these entries:</strong> <code>{list?.path ?? path}</code>{" "}
      {error ? (
        <span data-testid="ids-error">— cannot read the list: {error}. The queue shows nothing until it is fixed.</span>
      ) : !list ? (
        <span className="dim">loading…</span>
      ) : (
        <>
          <span data-testid="ids-progress"
            title="reviewed: a mark in force (verified, wrong, unsure or deferred); a mark on an entry changed since does not count">
            — {list.reviewed} of {list.total} reviewed
            <span className="dim"> · {list.verified} verified</span>
          </span>
          {hidden > 0 && <span className="dim" data-testid="ids-hidden"> · {hidden} hidden by the other filters</span>}
          {list.repeats > 0 && <span className="dim"> · {list.repeats} repeated id{list.repeats === 1 ? "" : "s"} listed once</span>}
        </>
      )}{" "}
      <button className="btn small" onClick={onClear} title="show the whole queue again">× show all</button>
      {list && list.missing.length > 0 && (
        <div data-testid="ids-missing">
          {list.missing.length} id{list.missing.length === 1 ? "" : "s"} in the list match{list.missing.length === 1 ? "es" : ""}{" "}
          no entry in this build:{" "}
          {list.missing.map((id) => <code key={id} className="cmd">{id}</code>)}
        </div>
      )}
    </div>
  );
}
