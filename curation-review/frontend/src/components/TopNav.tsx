import { useEffect, useState, type ReactNode } from "react";
import { isTyping } from "../review";

/* THE ONE TOP BAR (owner 2026-10-28: "make the tabs easier to switch between"). Every view renders this
   as its first row: the main tabs (short label, a done/total badge, a 1-9 / g+letter key), then — only for
   the active tab — its sub-tabs on a second row. A tab's filters live in a collapsible "Filters" fold
   (`useFold`, open/closed remembered per tab). Navigation is the URL: it sets the hash (and `?lowcuts`)
   and fires `hashchange`, which App reads, so every old link keeps working. */

export type TabId = "entries" | "lowcuts" | "dfo" | "tidal" | "sloughs" | "names" | "proposals";

interface Tab { id: TabId; label: string; key: string; title: string; hash: string }
const TABS: Tab[] = [
  { id: "entries", label: "Entries", key: "e", hash: "", title: "the synopsis entries queue" },
  { id: "dfo", label: "DFO", key: "d", hash: "dfo/", title: "T164: every scraped DFO salmon extent (sub-tabs: Cascade, Tidal)" },
  { id: "tidal", label: "Tidal", key: "t", hash: "tidal/", title: "T212: the proposed tidal boundary on every ocean-entering stream" },
  { id: "sloughs", label: "Sloughs", key: "s", hash: "sloughs/", title: "T55: slough / canal pieces drawn as lakes (sub-tab: small ponds Q154)" },
  { id: "names", label: "Names", key: "n", hash: "names/", title: "T224: mined names for waters — spot-check, accept / reject" },
  { id: "lowcuts", label: "Low cuts", key: "l", hash: "",
    title: "entries whose rules name a guessed (low-confidence) cut — check the cut on the satellite map" },
  { id: "proposals", label: "Proposals", key: "p", hash: "proposals", title: "T185: the edits your marks propose, waiting to apply (read only)" },
];

/** the sub-tabs of a main tab; `hash` is the whole target */
const SUBS: Partial<Record<TabId, { id: string; label: string; hash: string; title?: string }[]>> = {
  dfo: [
    { id: "extents", label: "Extents", hash: "dfo/" },
    { id: "cascade", label: "Cascade", hash: "cascade/skeena", title: "T229: which DFO places apply where (map)" },
    { id: "tidal", label: "Tidal", hash: "tidal/", title: "T212: the tidal boundaries" },
  ],
  sloughs: [
    { id: "sloughs", label: "Sloughs", hash: "sloughs/" },
    { id: "ponds", label: "Small ponds (Q154)", hash: "sloughs/q154/",
      title: "Q154: small unnamed ponds a river runs through — pool (river rules) or real lake?" },
  ],
};

/** go to a tab / sub-tab without a reload: the URL changes, App (listening to hashchange) swaps the view */
export function navigate(hash: string, lowcuts = false) {
  const search = lowcuts ? "?lowcuts" : "";
  window.history.replaceState(null, "", `${window.location.pathname}${search}${hash ? `#${hash}` : ""}`);
  window.dispatchEvent(new HashChangeEvent("hashchange"));
}

/* ---- badges: each tab's progress, read once a minute from the same endpoints the views use ---- */
type Badges = Partial<Record<TabId, string>>;
let badges: Badges = {};
let fetchedAt = 0;
const listeners = new Set<() => void>();
const emit = () => listeners.forEach((l) => l());
export function setBadge(id: TabId, text: string) {
  if (badges[id] === text) return;
  badges = { ...badges, [id]: text };
  emit();
}
const frac = (done?: number, total?: number) => (total == null ? "" : `${done ?? 0}/${total}`);
async function j(url: string): Promise<any> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(url);
  return r.json();
}
function refreshBadges() {
  if (Date.now() - fetchedAt < 60_000) return;
  fetchedAt = Date.now();
  j("/api/regions").then((rs: { total: number; by_verify?: Record<string, number> }[]) =>
    setBadge("entries", frac(rs.reduce((n, r) => n + (r.by_verify?.verified ?? 0), 0), rs.reduce((n, r) => n + r.total, 0)))).catch(() => {});
  j("/api/dfo/progress").then((p) => setBadge("dfo", frac(p.all?.done, p.all?.total))).catch(() => {});
  j("/api/tidal/progress").then((p) => setBadge("tidal", frac(p.done, p.total))).catch(() => {});
  j("/api/sloughs/progress").then((p) => setBadge("sloughs", frac(p.done, p.total))).catch(() => {});
  j("/api/names/progress").then((p) => setBadge("names", frac(p.review?.done, p.review?.total))).catch(() => {});
  j("/api/proposals").then((p) => setBadge("proposals", String(p.proposals?.length ?? 0))).catch(() => {});
}

export function TopNav({ active, sub, right }: {
  active: TabId;
  /** the active sub-tab's id (see SUBS) */
  sub?: string;
  /** the active view's slim progress, and its Filters toggle — kept on the bar's row */
  right?: ReactNode;
}) {
  const [, tick] = useState(0);
  useEffect(() => {
    const l = () => tick((n) => n + 1);
    listeners.add(l);
    refreshBadges();
    return () => { listeners.delete(l); };
  }, []);
  useEffect(() => {
    let g = 0; // a pending "g" (go to) press
    const onKey = (ev: KeyboardEvent) => {
      if (isTyping(ev.target) || ev.metaKey || ev.ctrlKey || ev.altKey) return;
      const go = (t: Tab) => { navigate(t.hash, t.id === "lowcuts"); ev.preventDefault(); };
      const n = Number(ev.key);
      if (n >= 1 && n <= TABS.length) { go(TABS[n - 1]); g = 0; return; }
      if (ev.key === "g") { g = Date.now(); return; }
      if (g && Date.now() - g < 1500) {
        const t = TABS.find((x) => x.key === ev.key);
        g = 0;
        if (t) go(t);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);
  const subs = SUBS[active];
  return (
    <div className="topnav" data-testid="topnav">
      <div className="topnav-row">
        <div className="mode-tabs" role="tablist">
          {TABS.map((t, i) => (
            <button key={t.id} role="tab" aria-selected={t.id === active} data-testid={`${t.id === "lowcuts" ? "low-cuts" : t.id}-tab`}
              className={`btn small topnav-tab${t.id === active ? " primary" : ""}`}
              title={`${t.title} — key ${i + 1}, or g ${t.key}`}
              onClick={() => navigate(t.hash, t.id === "lowcuts")}>
              <kbd>{i + 1}</kbd>{t.label}{badges[t.id] ? <span className="topnav-badge">{badges[t.id]}</span> : null}
            </button>
          ))}
        </div>
        <div className="spacer" />
        {right}
      </div>
      {subs && (
        <div className="topnav-row topnav-sub" role="tablist" data-testid="subnav">
          {subs.map((s) => (
            <button key={s.id} role="tab" aria-selected={s.id === sub} data-testid={s.id === "ponds" ? "ponds-tab" : `sub-${s.id}`}
              className={`btn small topnav-subtab${s.id === sub ? " primary" : ""}`} title={s.title}
              onClick={() => navigate(s.hash)}>{s.label}</button>
          ))}
        </div>
      )}
    </div>
  );
}

/* ---- the Filters fold: open/closed remembered per tab (localStorage; per viewer only) ---- */
const foldKey = (tab: string) => (tab === "entries" ? "curation-review.filters-open" : `curation-review.filters-open.${tab}`);
export function useFold(tab: string) {
  const [open, setOpen] = useState(() => {
    try { return window.localStorage.getItem(foldKey(tab)) === "1"; } catch { return false; }
  });
  const toggle = () => setOpen((o) => {
    try { window.localStorage.setItem(foldKey(tab), o ? "0" : "1"); } catch { /* per-viewer only */ }
    return !o;
  });
  return { open, toggle };
}

/** the Filters button; `set` = how many filters are away from their default (so a folded filter is never hidden) */
export function FoldToggle({ fold, set = 0, title }: { fold: { open: boolean; toggle: () => void }; set?: number; title?: string }) {
  return (
    <button type="button" className={`btn small${fold.open ? " primary" : ""}`} data-testid="filters-toggle"
      aria-expanded={fold.open} onClick={fold.toggle} title={title ?? "this tab's filters and options"}>
      Filters{set ? ` (${set})` : ""} {fold.open ? "▴" : "▾"}
    </button>
  );
}
