import { useState } from "react";
import { api } from "../api";
import type { ItemSearchResult } from "../types";

interface Props {
  matched: string[];
  onAttach: (item: ItemSearchResult) => void;
}

// no_registry attach flow: search the registry, pick a result, set entry.matched.
export function AttachItem({ matched, onAttach }: Props) {
  const [q, setQ] = useState("");
  const [results, setResults] = useState<ItemSearchResult[]>([]);
  const [busy, setBusy] = useState(false);

  async function run(query: string) {
    setQ(query);
    if (!query.trim()) {
      setResults([]);
      return;
    }
    setBusy(true);
    try {
      setResults(await api.searchItems(query));
    } catch {
      setResults([]);
    } finally {
      setBusy(false);
    }
  }

  const search = (
    <div style={{ marginTop: 8 }}>
      <input
        type="text"
        placeholder="search registry by name…"
        value={q}
        onChange={(e) => run(e.target.value)}
      />
    </div>
  );
  const list = (
    <>
      {busy && <div className="dim" style={{ marginTop: 6 }}>searching…</div>}
      {results.length > 0 && (
        <ul className="search-results">
          {results.map((r) => (
            <li key={r.id} onClick={() => onAttach(r)}>
              <strong>{r.name}</strong>{" "}
              <span className="dim">
                {r.kind} · {r.id}
                {r.mus.length ? ` · MU ${r.mus.join(", ")}` : ""}
              </span>
            </li>
          ))}
        </ul>
      )}
    </>
  );
  // A matched entry is NOT a no-registry entry: the red "NO REGISTRY" box sat on every water row,
  // telling the reviewer a matched water was unbound. Adding a further water is a quiet option.
  if (matched.length > 0)
    return (
      <details className="attach-more">
        <summary className="dim">attach another water…</summary>
        {search}
        {list}
      </details>
    );

  return (
    <div className="attach">
      <strong>NO REGISTRY</strong> — attach a registry item, then bind its reaches
      below.
      <div style={{ marginTop: 8 }}>
        <input
          type="text"
          placeholder="search registry by name…"
          value={q}
          onChange={(e) => run(e.target.value)}
        />
      </div>
      {matched.length > 0 && (
        <div style={{ marginTop: 6 }} className="dim">
          currently matched: <code>{matched.join(", ")}</code>
        </div>
      )}
      {busy && <div className="dim" style={{ marginTop: 6 }}>searching…</div>}
      {results.length > 0 && (
        <ul className="search-results">
          {results.map((r) => (
            <li key={r.id} onClick={() => onAttach(r)}>
              <strong>{r.name}</strong>{" "}
              <span className="dim">
                {r.kind} · {r.id}
                {r.mus.length ? ` · MU ${r.mus.join(", ")}` : ""}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
