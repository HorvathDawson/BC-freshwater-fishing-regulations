import type { ReviewView } from "../types";
import { SeverityChip } from "./QueueList";

interface Props {
  view: ReviewView;
  /** printed page -> PDF page, for the "open the PDF here" link */
  pdfPages: Record<string, number>;
}

/* EVIDENCE FIRST (PLAN §4 phase 3): the raw name as printed, where it is printed, a crop a reviewer
   can READ (the row, water column above regulation column, at 2.6 px/pt — the old crop showed the
   whole 530 pt table row at 24 %), the whole page with the row or passage marked, and the coverage
   strip: the printed words no rule quotes. */
export function Evidence({ view, pdfPages }: Props) {
  const ev = view.evidence;
  const cov = view.coverage;
  const printed = ev.printed ?? null;
  return (
    <div className="evidence" data-testid="evidence">
      <div className="evidence-head">
        <h2 data-testid="raw-name">{view.name}</h2>
        {view.display_name && view.display_name !== view.name && (
          <span className="dim">shown in the app as “{view.display_name}”</span>
        )}
      </div>
      <div className="evidence-sub">
        <SeverityChip level={view.severity} reasons={view.severity_reasons} />
        <span className={`tier-chip tier-${view.tier}`} title="review tier (PLAN §3)">{view.tier}</span>
        {view.risks.map((r) => <span key={r} className="badge dim" title="risk factor (tiers.RISKS)">{r}</span>)}
        <span>region {view.region}</span>
        {printed != null && (
          <span>
            printed p.{printed}{" "}
            <a href={`/api/synopsis.pdf#page=${pdfPages[String(printed)] ?? ev.pdf_page ?? printed}`}
              target="_blank" rel="noreferrer">open the PDF here</a>
          </span>
        )}
        {ev.kind === "passage" && ev.total ? (
          <span className={ev.found! / ev.total < 0.9 ? "warn-text" : "dim"}
            title="the passage's words found on the page, in order, in runs of 3+">
            passage found: {ev.found} of {ev.total} words
          </span>
        ) : null}
      </div>
      {(ev.kind === "none" || ev.kind === "missing") && (
        <div className="review-flag" data-testid="evidence-missing">
          <strong>{ev.kind === "missing" ? "No crop:" : "Not located:"}</strong> {ev.why}
        </div>
      )}
      {ev.crop_url && (
        <a href={ev.crop_url} target="_blank" rel="noreferrer" title="open full size">
          <img className="evidence-crop" data-testid="evidence-crop" src={ev.crop_url}
            alt={`${view.name} as printed`} />
        </a>
      )}
      <div className="evidence-actions">
        {/* the page opens in a tab of its own: the evidence is pinned at the top of the pane
            (T189), and a whole page inside it would push everything else off the screen */}
        {ev.page_url && (
          <a className="btn small" data-testid="show-page" href={ev.page_url} target="_blank"
            rel="noreferrer">page {printed ?? ""} with it marked</a>
        )}
        {ev.row_image && (
          <a className="dim" href={ev.row_image} target="_blank" rel="noreferrer"
            title="the extractor's own crop: the whole table row at 400 dpi">the extractor's row crop</a>
        )}
      </div>
      <div className="coverage" data-testid="coverage"
        title="the row as stored: green is quoted by a rule or record, red underlined is quoted by none">
        {cov.segments.map((s, i) => (
          <span key={i} className={`cov-${s.kind}`}>{s.text}</span>
        ))}
      </div>
      <div className="dim coverage-sum">
        {cov.unquoted_chars === 0 ? "every word of the row is quoted by a rule"
          : `${cov.unquoted_chars} letters/digits of the row are quoted by no rule (red)`}
        {cov.missing.length > 0 && (
          <span className="warn-text"> · not in the row: {cov.missing.join(", ")}</span>
        )}
      </div>
    </div>
  );
}
