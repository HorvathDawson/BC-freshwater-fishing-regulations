import { useState } from "react";
import type { Mode, SplitCard } from "../types";
import { Checklist } from "./Checklist";
import { MapPanel } from "./MapPanel";
import { heldWhileAwaiting } from "./EntryReview";
import { useMarking, VerdictBar } from "./useMarking";
import type { QueueNav } from "./VerifyBar";

interface Props {
  card: SplitCard;
  mode: Mode;
  onMarked: () => void;
  nav: QueueNav;
  reloadKey: number;
}

const WORD: Record<string, string> = {
  unmarked: "not marked", verified: "confirmed", wrong: "wrong", unsure: "unsure",
  deferred: "deferred", stale: "changed since marked",
};

/* A SPLIT CARD (tier S0, PLAN §4 phase 3): one split point still to verify — its label, its water,
   the curator's note and the auto-proposal grade, the map with the point ringed, and one check: P,
   "the cut is in the right place" (a `split` mark). A split the atlas does not hold as authored
   (added or moved in fix mode, not yet built) cannot be confirmed: it waits for the build. */
export function SplitCardView({ card, mode, onMarked, nav, reloadKey }: Props) {
  const [mapBig, setMapBig] = useState(false);
  const checks = mode.checks.split;
  const awaiting = !!card.vintage && card.vintage.state !== "current";
  const held = awaiting ? { P: "awaiting build — the atlas does not hold this split as authored" }
    : heldWhileAwaiting(false);
  const m = useMarking({
    key: card.split_id, resetOn: card.split_id, onToggleMap: () => setMapBig((x) => !x),
    mark: { scope: "split", target: card.split_id, checks, held },
    onRecorded: () => onMarked(),
  });
  return (
    <div className="detail review-mode">
      <div className={`detail-cols${mapBig ? " map-big" : ""}`}>
        <div className="detail-content">
          <div className="verify-bar" data-testid="split-bar">
            <div className="verify-row">
              <span className={`verify-chip ${card.status}`}>{WORD[card.status] ?? card.status}
                {card.status === "stale" && card.reasons.length > 0 && ` (${card.reasons.join(", ")})`}</span>
              {awaiting && (
                <span className="verify-chip stale" title={card.vintage!.reasons.join("; ")}>awaiting build</span>
              )}
              <span className="verify-nav">
                <button className="btn" disabled={!nav.onPrev} onClick={nav.onPrev} aria-label="previous split">←</button>
                <span className="dim">{nav.pos ? `${nav.pos} / ${nav.total}` : `– / ${nav.total}`}</span>
                <button className="btn" disabled={!nav.onNext} onClick={nav.onNext} aria-label="next split">→</button>
              </span>
            </div>
          </div>
          <div className="evidence" data-testid="split-card">
            <div className="evidence-head">
              <h2>{card.label || card.split_id}</h2>
              <span className="dim">{card.waterbody} · <code>{card.split_id}</code> · {card.kind}</span>
            </div>
            {card.grade && (
              <div className="split-review proposal">
                <div className="split-review-grade">auto-proposal, grade <strong>{card.grade}</strong> — a machine
                  placement nobody has confirmed on a map</div>
              </div>
            )}
            {card.note && <div className="split-review-note" data-testid="split-note">{card.note}</div>}
            {awaiting && card.vintage!.commands.length > 0 && (
              <div className="dim">{card.vintage!.commands.map((c) => <code key={c} className="cmd">{c}</code>)}</div>
            )}
          </div>
          <Checklist target="the split point" checks={checks} state={m.state} onChange={m.setState}
            held={held} problems={mode.problems} />
          <VerdictBar checks={checks} m={m} held={held} />
        </div>
        <div className="detail-map">
          <MapPanel itemId={card.item_id} selectedSplitId={card.split_id}
            referencedSplitIds={[card.split_id]} reloadKey={reloadKey} />
        </div>
      </div>
    </div>
  );
}
