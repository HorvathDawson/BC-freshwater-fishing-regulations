import { useCallback, useEffect, useRef, useState } from "react";
import { api, type EntryRefused } from "../api";
import type { Check, EntryReaches, EntrySplits, Mode, Pv } from "../types";
import { getReviewer, isTyping, setReviewer } from "../review";
import { MapPanel } from "./MapPanel";
import { PvBadge } from "./Preverify";
import { TopNav } from "./TopNav";

/** `#p1/` opens the P1 quick-confirm view */
export const P1_HASH = "p1/";

/* THE P1 PASS, MADE FAST (owner 2026-10-28, backend `p1.view`): the closures with a risk factor whose
   every check agrees — pre-verified ✓✓, no stale split, nothing unresolved, built — one card at a time:
   the closure text, the map, the second check's reasons. `y` records the owner's NORMAL mark (every
   check "y", POST /api/marks, the one write) — only when pressed; nothing is recorded otherwise. A card
   whose closure binds no section (the reach builder's answer, asked per card) cannot be accepted here.
   The rest of P1 is listed below in risk order (no match / unresolved first), each a link to its full review. */

interface Rule { rule_id: string; type: string; label?: string; verbatim?: string; closure: boolean }
interface Proposal { rule_id: string; kind: string; words?: string; reason?: string; status?: string; why?: string; method?: string }
interface Row {
  entry_id: string; name: string; region: string; page: number | null; risks: string[]; severity: string;
  pv: Pv | null; matched_item_id: string | null; matched_item_name: string | null; also_item_ids: string[];
}
interface Quick extends Row { regs_verbatim?: string; rules: Rule[]; proposals: Proposal[]; notes: string[] }
interface Rest extends Row { why_not_quick: string[] }
interface View {
  counts: { total: number; todo: number; quick: number; by_first_risk: Record<string, number>;
    by_any_risk: Record<string, number>; by_pv: Record<string, number>; not_quick_because: Record<string, number> };
  quick: Quick[];
  rest: Rest[];
}

const chips = (o: Record<string, number>) =>
  Object.entries(o).sort((a, b) => b[1] - a[1]).map(([k, n]) => <span key={k} className="chip-tag">{k} {n}</span>);

export function P1Quick({ mode }: { mode: Mode }) {
  const [view, setView] = useState<View | null>(null);
  const [error, setError] = useState("");
  const [at, setAt] = useState(0);
  const [done, setDone] = useState<Record<string, string>>({});
  const [reviewer, setName] = useState(getReviewer);
  const [busy, setBusy] = useState(false);
  const [reaches, setReaches] = useState<EntryReaches | null>(null);
  const [named, setNamed] = useState<EntrySplits | null>(null);

  useEffect(() => {
    fetch("/api/review/p1").then((r) => (r.ok ? r.json() : Promise.reject(`${r.status} /api/review/p1`)))
      .then(setView).catch((e) => setError(String(e)));
  }, []);

  const quick = view?.quick ?? [];
  const card = quick[at] ?? null;
  useEffect(() => {
    setReaches(null); setNamed(null);
    if (!card) return;
    api.reaches(card.entry_id).then(setReaches).catch(() => setReaches({ covered: [], rules: {}, verdict: {} }));
    api.splits(card.entry_id).then(setNamed).catch(() => {});
  }, [card?.entry_id]);

  // THE BINDING CHECK: every closure rule bound to at least one section by the reach builder
  const closures = card?.rules.filter((r) => r.closure) ?? [];
  const unbound = reaches?.verdict
    ? closures.filter((r) => { const v = reaches.verdict![r.rule_id]; return !v || v.outcome !== "bound" || v.n_sections < 1; })
    : null;
  const canAccept = !!card && !done[card.entry_id] && unbound !== null && unbound.length === 0 && !busy;

  const inFlight = useRef(false);
  const accept = useCallback(async () => {
    if (!card || inFlight.current || !canAccept) return;
    if (!reviewer.trim()) { setError("Type your name first: every mark names its reviewer."); return; }
    inFlight.current = true; setBusy(true); setError("");
    const answers = Object.fromEntries(mode.checks.entry.map((c: Check) => [c, "y" as const]));
    const why = card.proposals.map((p) => p.why).filter(Boolean).join("; ");
    try {
      const got = await api.postMark({
        scope: "entry", target: card.entry_id, rule_id: null, answers, problem: null, reviewer: reviewer.trim(),
        note: `P1 quick confirm — second check agrees${why ? `: ${why}` : ""}`,
      });
      setDone((d) => ({ ...d, [card.entry_id]: got.status }));
      setAt((i) => Math.min(i + 1, quick.length - 1));
    } catch (e) {
      const fe = (e as EntryRefused).fieldErrors;
      setError(fe ? fe.map((x) => (x.path ? `${x.path}: ` : "") + x.msg).join(" · ") : String(e));
    } finally { inFlight.current = false; setBusy(false); }
  }, [card, canAccept, reviewer, mode, quick.length]);

  const keys = useRef({ accept, card, n: quick.length });
  keys.current = { accept, card, n: quick.length };
  useEffect(() => {
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey || ev.repeat) return;
      const K = keys.current;
      if (ev.key === "y") { void K.accept(); ev.preventDefault(); }
      else if (ev.key === "j" || ev.key === "ArrowRight") { setAt((i) => Math.min(i + 1, Math.max(0, K.n - 1))); ev.preventDefault(); }
      else if (ev.key === "k" || ev.key === "ArrowLeft") { setAt((i) => Math.max(0, i - 1)); ev.preventDefault(); }
      else if (ev.key === "o" && K.card) { window.open(`/#${encodeURIComponent(K.card.entry_id)}`, "_blank", "noopener"); ev.preventDefault(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const c = view?.counts;
  return (
    <div className="app">
      <TopNav active="entries" sub="p1" right={c && (
        <span className="progress slim" data-testid="p1-progress">
          P1 to do {c.todo}/{c.total} · quick {quick.length - Object.keys(done).length}/{c.quick}
        </span>
      )} />
      {error && <div className="errors" style={{ margin: 8 }}>{error}</div>}
      {!view && !error && <div className="empty-state">loading P1… (the pre-checks run over every P1 entry)</div>}
      {view && (
        <div className="p1-quick" data-testid="p1-quick">
          <div className="p1-counts">
            <div><strong>first risk</strong> {chips(c!.by_first_risk)}</div>
            <div><strong>any risk</strong> {chips(c!.by_any_risk)}</div>
            <div><strong>2nd check</strong> {chips(c!.by_pv)}</div>
            <details><summary>why the other {c!.todo - c!.quick} are not quick</summary>{chips(c!.not_quick_because)}</details>
          </div>
          {card ? (
            <div className="p1-card" data-testid="p1-card" data-entry={card.entry_id}>
              <div className="p1-text">
                <div className="p1-head">
                  <span className="dim">{at + 1}/{quick.length}</span>{" "}
                  <strong>{card.name}</strong> <span className="dim">r{card.region} · p.{card.page ?? "?"} · {card.matched_item_name}</span>{" "}
                  <PvBadge pv={card.pv} />
                  {done[card.entry_id] && <span className="verify-chip verified" data-testid="p1-done">{done[card.entry_id]}</span>}
                </div>
                {card.regs_verbatim && <div className="p1-verbatim">{card.regs_verbatim}</div>}
                <ul className="p1-rules">
                  {closures.map((r) => {
                    const v = reaches?.verdict?.[r.rule_id];
                    return (
                      <li key={r.rule_id}>
                        <strong>{r.label ?? r.verbatim}</strong>
                        <span className="dim"> — {v ? `${v.n_sections} section${v.n_sections === 1 ? "" : "s"}` : "binding…"}</span>
                        {r.verbatim && r.verbatim !== r.label && <div className="dim">“{r.verbatim}”</div>}
                      </li>
                    );
                  })}
                </ul>
                <div className="p1-why"><strong>second check</strong>
                  <ul>{card.proposals.map((p, i) => (
                    <li key={i}><code>{p.rule_id}</code> {p.words}: {p.why}<div className="dim">{p.method}</div></li>
                  ))}</ul>
                </div>
                {card.notes.length > 0 && <div className="dim">{card.notes.join(" · ")}</div>}
                {unbound && unbound.length > 0 && (
                  <div className="errors">binds no section: {unbound.map((r) => r.rule_id).join(", ")} — use the full review</div>
                )}
                <div className="p1-actions">
                  <input className="reviewer" value={reviewer} placeholder="your name" aria-label="reviewer"
                    onChange={(e) => { setName(e.target.value); setReviewer(e.target.value); }} />
                  <button className="btn primary" disabled={!canAccept} onClick={() => void accept()} data-testid="p1-accept"
                    title="records your normal mark: every check y (key y)">accept <kbd>y</kbd></button>
                  <button className="btn" onClick={() => setAt((i) => Math.min(i + 1, quick.length - 1))}>skip <kbd>j</kbd></button>
                  <a className="btn" href={`/#${encodeURIComponent(card.entry_id)}`} target="_blank" rel="noreferrer">full review <kbd>o</kbd></a>
                  <span className="dim keys">y accept · j/k next/prev · o full review</span>
                </div>
              </div>
              <div className="p1-map">
                <MapPanel key={card.entry_id} itemId={card.matched_item_id} alsoItemIds={card.also_item_ids}
                  reaches={reaches} rules={closures.map((r) => ({ rule_id: r.rule_id, type: r.type, label: r.label }))}
                  entryId={card.entry_id} namedSplits={named}
                  showRule={closures[0] && reaches ? { id: closures[0].rule_id, n: 1 } : undefined} />
              </div>
            </div>
          ) : <div className="empty-state">No quick-confirm rows.</div>}
          <details className="p1-rest" open>
            <summary>the rest of P1 ({view.rest.length}), risk order — each needs the full review</summary>
            <ol>
              {view.rest.map((r) => (
                <li key={r.entry_id}>
                  <a href={`/#${encodeURIComponent(r.entry_id)}`}>{r.name}</a>{" "}
                  <span className="dim">r{r.region} · {r.risks.join(", ")} · {r.severity}</span> <PvBadge pv={r.pv} />
                  <span className="dim"> — {r.why_not_quick.join("; ")}</span>
                </li>
              ))}
            </ol>
          </details>
        </div>
      )}
    </div>
  );
}
