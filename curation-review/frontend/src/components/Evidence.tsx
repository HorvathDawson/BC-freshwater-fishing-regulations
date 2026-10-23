import { useEffect, useState } from "react";
import type { ReviewView } from "../types";

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
  const [showPage, setShowPage] = useState(false);
  useEffect(() => { setShowPage(false); }, [view.entry_id]);
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
        <span className={`tier-chip tier-${view.tier}`} title="review tier (PLAN §3)">{view.tier}</span>
        {view.risks.map((r) => <span key={r} className="badge warn">{r}</span>)}
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
        {ev.page_url && (
          <button type="button" className="btn small" data-testid="show-page"
            onClick={() => setShowPage((x) => !x)}>
            {showPage ? "hide the page" : `show page ${printed ?? ""} with it marked`}
          </button>
        )}
        {ev.row_image && (
          <a className="dim" href={ev.row_image} target="_blank" rel="noreferrer"
            title="the extractor's own crop: the whole table row at 400 dpi">the extractor's row crop</a>
        )}
      </div>
      {showPage && ev.page_url && (
        <div className="page-image">
          <a href={ev.page_url} target="_blank" rel="noreferrer" title="open full size">
            <img src={ev.page_url} alt={`printed page ${printed} with the entry marked`} />
          </a>
        </div>
      )}
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
