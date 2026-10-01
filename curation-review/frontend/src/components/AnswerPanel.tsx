import { useEffect, useMemo, useState } from "react";
import { api } from "../api";
import { useVocab } from "../model";
import type { Answer, AnswerWater, Entry, EntryInBundle, RuleVerdict } from "../types";

/* WHAT AN ANGLER IS TOLD. Everything else on the page is what the entry SAYS and where its rules
   bind; this is the answer that comes out the other end — `read.effective_rules` on one piece of
   water, one day, one fish, read from the live bundle. It is the "proper result" to check against
   the book.

   The bundle is built from the entry files, so it reflects the LAST BUNDLE BUILD: an edit saved
   here does not reach it until the bundle is rebuilt. The panel compares the bundle's copy of
   the entry with the file's and says so when they differ, rather than presenting an old answer
   as the edit's consequence. */

const STATE_WORDS: Record<string, string> = {
  speaks: "applies",
  beside: "applies beside (part time / part width)",
  shown: "shown (information)",
  not_yet_mapped: "not yet mapped (holds in a part nothing draws)",
};

function today(): string {
  const d = new Date();
  const p = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

interface Props {
  entry: Entry;
  /** bumped after a save, so the bundle comparison is redone against the file as saved */
  version: unknown;
  /** the reach builder's answer for the entry as saved — set against the bundle's section
   *  counts, since a change of WHERE (a tributary flag, an extent) leaves every label as it was */
  verdict?: Record<string, RuleVerdict>;
}

export function AnswerPanel({ entry, version, verdict }: Props) {
  const vocab = useVocab();
  const entryId = entry.entry_id;
  const [cmp, setCmp] = useState<EntryInBundle | null>(null);
  const [q, setQ] = useState("");
  const [waters, setWaters] = useState<AnswerWater[] | null>(null);
  const [water, setWater] = useState<string>("");
  const [sid, setSid] = useState<number | null>(null);
  const [day, setDay] = useState(today());
  // leaf species only: effective_rules answers for ONE fish
  const fishes = useMemo(() => vocab.species.filter((s) => !s.is_group), [vocab]);
  const firstNamed = useMemo(() => {
    const leaf = new Set(fishes.map((f) => f.code));
    for (const r of entry.rules ?? []) for (const c of r.species ?? []) if (leaf.has(c)) return c;
    return "RB";
  }, [entry, fishes]);
  const [fish, setFish] = useState(firstNamed);
  const [ans, setAns] = useState<Answer | null>(null);
  const [err, setErr] = useState("");
  const [showProvince, setShowProvince] = useState(false);

  useEffect(() => {
    let live = true;
    api.entryBundle(entryId).then((d) => live && setCmp(d)).catch((e) => live && setErr(String(e)));
    return () => { live = false; };
  }, [entryId, version]);

  useEffect(() => {
    let live = true;
    const t = setTimeout(() => {
      api.answerWaters(entryId, q)
        .then((w) => {
          if (!live) return;
          setWaters(w);
          // keep the water already chosen; else the entry's own primary water; else the largest
          const keep = w.find((x) => x.item_id === water)
            ?? w.find((x) => x.item_id === (entry.matched ?? [])[0]) ?? w[0];
          setWater(keep?.item_id ?? "");
          setSid(keep?.pieces[0]?.sid ?? null);
        })
        .catch((e) => live && setErr(String(e)));
    }, 250);
    return () => { live = false; clearTimeout(t); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [entryId, q]);

  useEffect(() => {
    if (sid == null || !day || !fish) { setAns(null); return; }
    let live = true;
    api.answer(sid, day, fish).then((a) => live && setAns(a)).catch((e) => live && setErr(String(e)));
    return () => { live = false; };
  }, [sid, day, fish]);

  const w = waters?.find((x) => x.item_id === water) ?? null;
  // The bundle predates the file when a rule reads differently there, or binds a different
  // number of sections than the builder now gives the saved entry.
  const moved = (rid: string, n: number) =>
    verdict?.[rid] != null && verdict[rid].n_sections !== n;
  const stale = cmp ? (cmp.rules.filter((r) => r.label_bundle !== r.label_now
    || moved(r.rule_id, r.n_sections))) : [];
  const shortId = (rid: string) => rid.split(".").pop();
  const ruleWord = (rid: string) => entry.rules?.find((r) => r.rule_id === rid)?.label ?? rid;

  const rows = ans?.rules ?? [];
  const province = rows.filter((r) => r.entry_id.startsWith("zp:"));
  const shown = showProvince ? rows : rows.filter((r) => !r.entry_id.startsWith("zp:"));

  return (
    <div className="section answer" data-testid="answer-panel">
      <h3>What an angler is told</h3>
      <div className="dim">
        From the live bundle{cmp ? ` (built ${cmp.bundle.built})` : ""} — the reference reader,
        <code> read.effective_rules</code>. It shows the last bundle build, not unsaved or newly
        saved edits.
      </div>
      {err && <div className="errors">{err}</div>}
      {cmp && !cmp.in_bundle && (
        <div className="review-flag">This entry is not in the bundle — rebuild the bundle to see its answer.</div>
      )}
      {cmp && cmp.in_bundle && stale.length > 0 && (
        <div className="review-flag" data-testid="bundle-stale">
          <strong>The bundle predates this entry.</strong> {stale.length} rule(s) read differently
          in the bundle, so the answer below is the OLD one until the bundle is rebuilt:
          <ul>
            {stale.map((r) => (
              <li key={r.rule_id}><code>{shortId(r.rule_id)}</code>{" "}
                {r.label_bundle !== r.label_now
                  ? <>bundle: {r.label_bundle ?? <em>absent</em>} · file: {r.label_now ?? <em>absent</em>}</>
                  : <>binds {r.n_sections.toLocaleString()} section(s) in the bundle, {" "}
                    {verdict?.[r.rule_id]?.n_sections.toLocaleString()} as saved</>}</li>
            ))}
          </ul>
        </div>
      )}

      {/* licensing: where each record binds, as the bundle placed it */}
      {cmp && cmp.licensing.length > 0 && (
        <div className="answer-lic">
          <div className="k">Licensing records — where they bind</div>
          {cmp.licensing.map((l) => (
            <div key={`${l.kind}:${l.id}`} className="answer-lic-row">
              <span className="badge">{l.kind}</span> {l.label}
              <div className="dim">
                {l.unresolved ? <>unplaced: {l.unresolved}</>
                  : <>{l.n_sections.toLocaleString()} section(s) on {l.n_waters} water(s)
                    {l.waters.length > 0 && ": "}
                    {l.waters.map((x) => `${x.name} ${x.n}`).join(", ")}
                    {l.n_waters > l.waters.length ? ", …" : ""}</>}
              </div>
            </div>
          ))}
        </div>
      )}

      <div className="answer-ask">
        <label>water
          <input placeholder="filter…" value={q} onChange={(e) => setQ(e.target.value)} style={{ width: 90 }} />
          <select value={water} aria-label="water"
            onChange={(e) => {
              setWater(e.target.value);
              setSid(waters?.find((x) => x.item_id === e.target.value)?.pieces[0]?.sid ?? null);
            }}>
            {(waters ?? []).map((x) => (
              <option key={x.item_id || "unnamed"} value={x.item_id}>
                {x.name} ({x.n_sections})
              </option>
            ))}
          </select>
        </label>
        <label>piece
          <select value={sid ?? ""} aria-label="piece" onChange={(e) => setSid(Number(e.target.value))}>
            {(w?.pieces ?? []).map((p, i) => (
              <option key={p.sid} value={p.sid}>
                {i + 1}: {p.n_sections} section(s) · {p.rules.map((r) => shortId(r.rule_id)
                  + (r.via === "trib" ? " (trib)" : "")).join(", ")}
              </option>
            ))}
          </select>
        </label>
        <label>date <input type="date" value={day} onChange={(e) => setDay(e.target.value)} /></label>
        <label>fish
          <select value={fish} aria-label="fish" onChange={(e) => setFish(e.target.value)}>
            {fishes.map((f) => <option key={f.code} value={f.code}>{f.name}</option>)}
          </select>
        </label>
      </div>
      {waters && waters.length === 0 && (
        <div className="dim">This entry's rules bind no section in the bundle.</div>
      )}
      {w && sid != null && (
        <div className="dim" style={{ marginTop: 4 }}>
          This entry's rules on this piece:{" "}
          {(w.pieces.find((p) => p.sid === sid)?.rules ?? []).map((r) => (
            <span key={r.rule_id} className="tag" title={ruleWord(r.rule_id)}>
              {shortId(r.rule_id)}{r.via === "trib" ? " · via tributary" : ""}
            </span>
          ))}
        </div>
      )}

      {ans && (
        <div className="answer-result" data-testid="answer-result">
          {ans.tidal && (
            <div className="review-flag">Tidal water ({ans.tidal}): provincial rules do not apply here.</div>
          )}
          <table>
            <tbody>
              {shown.map((r) => (
                <tr key={`${r.entry_id}::${r.rule_id}`}
                  className={r.entry_id === entryId ? "mine" : ""}>
                  <td><span className={`state ${r.state}`} title={STATE_WORDS[r.state] ?? r.state}>
                    {r.state === "speaks" ? "applies" : r.state.replace(/_/g, " ")}</span></td>
                  <td>{r.label}</td>
                  <td className="dim" title={r.entry_id}>
                    {r.entry_id === entryId ? "this entry" : r.entry_name || r.entry_id}
                    {" · "}{shortId(r.rule_id)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {province.length > 0 && (
            <button className="btn small" onClick={() => setShowProvince((v) => !v)}>
              {showProvince ? "hide" : "show"} {province.length} province-wide rule(s)
            </button>
          )}
          {ans.licensing.length > 0 && (
            <div className="answer-lic">
              <div className="k">Licensing here</div>
              {ans.licensing.map((l) => (
                <div key={`${l.entry_id}:${l.id}`} className={l.entry_id === entryId ? "mine" : ""}>
                  <span className="badge">{l.kind}</span> {l.label}
                  <span className="dim"> · {l.entry_id === entryId ? "this entry" : l.entry_id}
                    {l.via === "trib" ? " · via tributary" : ""}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
