import type { Pv, PvStatus } from "../types";

/* PRE-VERIFIED (owner 2026-10-27): a SECOND, independent method's verdict on a proposal — DEM for a
   tidal boundary, OSM / GNIS / MoTI / the book for a DFO or entry split. "pre-verified" means two
   methods agree within tolerance; it is NOT an acceptance: the owner's mark is still the only write.
   One badge, one filter, one "pre-verified first" order, one "hide duplicates" switch, shared by the
   three queues (entries, DFO, Tidal). */

export interface PvFilter {
  /** "" any | a status | "none" (no proposal checked) */
  pv: "" | PvStatus | "none";
  first: boolean;
  hideDupes: boolean;
}
export const PV_DEFAULT: PvFilter = { pv: "", first: false, hideDupes: false };

const RANK: Record<string, number> = { "pre-verified": 0, disagree: 1, unchecked: 2 };
const WORDS: Record<PvStatus, string> = {
  "pre-verified": "pre-verified", disagree: "methods disagree", unchecked: "unchecked",
};

export function pvTitle(pv: Pv): string {
  const bits = [pv.why];
  if (pv.method) bits.push(`second method: ${pv.method}`);
  if (pv.first) bits.push(`first: ${pv.first}`);
  if (pv.delta_m != null) bits.push(`Δ ${Math.round(pv.delta_m)} m` + (pv.tolerance_m != null ? ` (tolerance ${Math.round(pv.tolerance_m)} m)` : ""));
  if (pv.duplicate_of) bits.push(`duplicate of ${pv.duplicate_of}`);
  bits.push("never an acceptance — your mark is");
  return bits.filter(Boolean).join("\n");
}

/** the badge; nothing when there is no verdict */
export function PvBadge({ pv }: { pv?: Pv | null }) {
  if (!pv) return null;
  return (
    <>
      <span className={`chip-tag pv pv-${pv.status}`} data-testid="pv-badge" data-pv={pv.status} title={pvTitle(pv)}>
        {pv.status === "pre-verified" ? "✓✓ " : pv.status === "disagree" ? "≠ " : ""}{WORDS[pv.status]}
        {pv.n != null && pv.n > 1 ? ` ${pv.n_pre}/${pv.n}` : ""}
      </span>
      {pv.duplicate_of && <span className="chip-tag pv-dup" title={`one decision covers both: ${pv.duplicate_of}`}>duplicate</span>}
    </>
  );
}

/** filter, then (stably) order: the queue as shown */
export function applyPv<T extends { pv?: Pv | null }>(rows: T[], f: PvFilter): T[] {
  let out = rows;
  if (f.hideDupes) out = out.filter((r) => !r.pv?.duplicate_of);
  if (f.pv === "none") out = out.filter((r) => !r.pv);
  else if (f.pv) out = out.filter((r) => r.pv?.status === f.pv);
  if (f.first) {
    out = out.map((r, i) => [r, i] as const)
      .sort((a, b) => (RANK[a[0].pv?.status ?? ""] ?? 3) - (RANK[b[0].pv?.status ?? ""] ?? 3) || a[1] - b[1])
      .map(([r]) => r);
  }
  return out;
}

/** counts for the select's labels */
function counts(rows: { pv?: Pv | null }[]) {
  const c: Record<string, number> = { "pre-verified": 0, disagree: 0, unchecked: 0, none: 0, dup: 0 };
  for (const r of rows) {
    c[r.pv?.status ?? "none"] += 1;
    if (r.pv?.duplicate_of) c.dup += 1;
  }
  return c;
}

export function PvControls({ value, onChange, rows, dupes = true }: {
  value: PvFilter; onChange: (f: PvFilter) => void; rows: { pv?: Pv | null }[]; dupes?: boolean;
}) {
  const c = counts(rows);
  const set = (p: Partial<PvFilter>) => onChange({ ...value, ...p });
  return (
    <>
      <div>
        <label title="a second, independent method's check of the proposal (never an acceptance)">2nd check</label>
        <select aria-label="pre-verify" data-testid="pv-filter" value={value.pv}
          onChange={(e) => set({ pv: e.target.value as PvFilter["pv"] })}>
          <option value="">any</option>
          <option value="pre-verified">pre-verified ({c["pre-verified"]})</option>
          <option value="disagree">methods disagree ({c.disagree})</option>
          <option value="unchecked">unchecked ({c.unchecked})</option>
          <option value="none">no proposal ({c.none})</option>
        </select>
      </div>
      <label className="check" title="the pre-verified proposals first, then the disagreements, then the rest — otherwise in the queue's own order">
        <input type="checkbox" data-testid="pv-first" checked={value.first} onChange={(e) => set({ first: e.target.checked })} />
        {" "}pre-verified first
      </label>
      {dupes && c.dup > 0 && (
        <label className="check" title="rewordings of the same place: one decision covers both">
          <input type="checkbox" data-testid="pv-hide-dupes" checked={value.hideDupes}
            onChange={(e) => set({ hideDupes: e.target.checked })} />
          {" "}hide {c.dup} duplicate{c.dup === 1 ? "" : "s"}
        </label>
      )}
    </>
  );
}
