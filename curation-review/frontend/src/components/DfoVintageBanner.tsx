import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import type { DfoVintageNote } from "../types";
import { Notice } from "./NoticeStack";

/* T366 ONE VINTAGE, ON SCREEN (DFO mode only). `/api/dfo/progress` `vintage` (`refresh.vintage`): a region whose typed
   rows are NOT from the saved DFO page (`typed_older` — the queue shows last vintage's rows: T361's cause), a saved page
   not fetched for a week in season (`raw_old`), or the check itself failing (`error`). Said plainly, with what to run.
   Rendered into the one notice dock (owner 2026-11-02), not over the queue. */

const REFRESH = (slugs: string[]) => `python -m pipeline.regs.dfo_salmon.live run --regions ${slugs.join(" ")}`;

function regionsOf(ns: DfoVintageNote[]): string[] {
  return [...new Set(ns.map((n) => n.region).filter((r): r is string => !!r))]
    .sort((a, b) => a.localeCompare(b, "en", { numeric: true }));
}
const words = (rs: string[]) => (rs.length === 1 ? `region ${rs[0]}` : `regions ${rs.join(", ")}`);

/** the banner's one-line summary — composed here so a test (or a reader) sees the whole sentence */
export function vintageSummary(ns: DfoVintageNote[]): string {
  const older = regionsOf(ns.filter((n) => n.kind === "typed_older"));
  const old = regionsOf(ns.filter((n) => n.kind === "raw_old"));
  const failed = ns.filter((n) => n.kind === "error" || (n.kind !== "typed_older" && n.kind !== "raw_old"));
  const bits: string[] = [];
  if (older.length) bits.push(`the DFO rows shown for ${words(older)} are OLDER than the saved DFO page — you would be reviewing last vintage's rows`);
  if (old.length) bits.push(`the saved DFO page for ${words(old)} has not been fetched for over a week (in season) — the live page may be newer`);
  if (failed.length) bits.push("the DFO vintage check FAILED — whether these rows are current is unknown");
  return bits.join("; ");
}

export function DfoVintageBanner({ notes }: { notes: DfoVintageNote[] }) {
  const [dock, setDock] = useState<Element | null>(null);
  useEffect(() => { setDock(document.querySelector('[data-testid="notice-dock"]')); }, []);
  if (!notes.length) return null;
  const regions = regionsOf(notes.filter((n) => n.kind === "typed_older" || n.kind === "raw_old"));
  const failed = notes.some((n) => n.kind !== "typed_older" && n.kind !== "raw_old");
  const stale = notes.some((n) => n.kind === "typed_older");
  const card = (
    <Notice id="dfo-vintage" contentKey={notes.map((n) => `${n.region}:${n.kind}:${n.text}`).join("|")}
      tone={failed || stale ? "error" : "warn"} critical={failed || stale} testid="dfo-vintage"
      pill={failed ? "DFO vintage check failed" : `DFO rows out of date: ${words(regions)}`}
      summary={<><strong>DFO vintage:</strong> {vintageSummary(notes)}.</>}>
      <ul>{notes.map((n, i) => <li key={i} data-testid="dfo-vintage-note">{n.text}</li>)}</ul>
      {regions.length > 0 && (
        <div>
          Run (network only, no credits), then reload this page:
          <ol>
            <li>preview, writes nothing: <code>{REFRESH(regions)} --dry-run</code></li>
            <li>refresh: <code>{REFRESH(regions)}</code></li>
            <li>queue any new / reworded place it lists: <code>python -m pipeline.regs.dfo_salmon.live queue --regions {regions.join(" ")}</code></li>
          </ol>
        </div>
      )}
      {failed && <div>The vintage check's error is above; the backend log has the traceback.</div>}
    </Notice>
  );
  // the dock is mounted beside the app (main.tsx); until it is found the card shows in place — never not at all
  return dock ? createPortal(card, dock) : card;
}
