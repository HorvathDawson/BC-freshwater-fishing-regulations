import React, { useEffect, useMemo, useRef, useState } from 'react';
import { X, Check, ExternalLink, Copy } from 'lucide-react';
import './IssueReport.css';

/** GitHub repo that receives pre-filled issue reports. */
const GITHUB_REPO = 'HorvathDawson/BC-freshwater-fishing-regulations';

/** Snapshot of app state attached to a report so a maintainer can reproduce it. */
export interface IssueReportContext {
    waterbodyName?: string;
    waterbodyId?: string;
    waterbodyType?: string;
    lat?: number;
    lng?: number;
    zoom?: number;
    dataVersion?: string;
    pageUrl?: string;
}

const CATEGORIES = [
    'Incorrect regulation',
    'Wrong location or boundary',
    'Missing waterbody',
    'Naming issue',
    'Bug or technical problem',
    'Other',
] as const;
type Category = (typeof CATEGORIES)[number];

interface IssueReportProps {
    isOpen: boolean;
    onClose: () => void;
    /** Called once when the modal opens to snapshot the current app context. */
    getContext?: () => IssueReportContext;
}

/**
 * Build the Markdown report body.  This is the single source of truth shared by
 * both delivery paths (the pre-filled GitHub issue and the "Copy report"
 * fallback), so the two can never drift out of sync.
 */
function buildReportBody(
    category: Category,
    description: string,
    contact: string,
    ctx: IssueReportContext,
): string {
    const lines: string[] = [
        '### Description',
        description.trim() || '_(none provided)_',
        '',
        '### Category',
        category,
        '',
    ];

    const context: string[] = [];
    if (ctx.waterbodyName) {
        const meta = [ctx.waterbodyType, ctx.waterbodyId ? `id ${ctx.waterbodyId}` : null]
            .filter(Boolean)
            .join(', ');
        context.push(`- Waterbody: ${ctx.waterbodyName}${meta ? ` (${meta})` : ''}`);
    }
    if (ctx.lat != null && ctx.lng != null) {
        const zoom = ctx.zoom != null ? `, zoom ${ctx.zoom.toFixed(1)}` : '';
        context.push(`- Map location: ${ctx.lat.toFixed(5)}, ${ctx.lng.toFixed(5)}${zoom}`);
    }
    if (ctx.pageUrl) context.push(`- Page: ${ctx.pageUrl}`);
    if (ctx.dataVersion) context.push(`- Data version: ${ctx.dataVersion}`);
    if (contact.trim()) context.push(`- Contact: ${contact.trim()}`);

    if (context.length) {
        lines.push('### Context', ...context, '');
    }
    return lines.join('\n').trim();
}

const IssueReport: React.FC<IssueReportProps> = ({ isOpen, onClose, getContext }) => {
    const [category, setCategory] = useState<Category>('Incorrect regulation');
    const [description, setDescription] = useState('');
    const [contact, setContact] = useState('');
    const [ctx, setCtx] = useState<IssueReportContext>({});
    const [copied, setCopied] = useState(false);

    // Keep the latest getContext without making it an effect dependency, so the
    // snapshot fires exactly once per open (not on every parent re-render).
    const getContextRef = useRef(getContext);
    getContextRef.current = getContext;

    // Fresh slate + context snapshot each time the modal opens.
    useEffect(() => {
        if (!isOpen) return;
        setCtx(getContextRef.current ? getContextRef.current() : {});
        setCategory('Incorrect regulation');
        setDescription('');
        setContact('');
        setCopied(false);
    }, [isOpen]);

    // Close on Escape.
    useEffect(() => {
        const onEsc = (e: KeyboardEvent) => {
            if (e.key === 'Escape' && isOpen) onClose();
        };
        window.addEventListener('keydown', onEsc);
        return () => window.removeEventListener('keydown', onEsc);
    }, [isOpen, onClose]);

    const body = useMemo(
        () => buildReportBody(category, description, contact, ctx),
        [category, description, contact, ctx],
    );

    const title = useMemo(() => {
        const wb = ctx.waterbodyName ? `: ${ctx.waterbodyName}` : '';
        return `[Report] ${category}${wb}`;
    }, [category, ctx.waterbodyName]);

    const githubUrl = useMemo(() => {
        const params = new URLSearchParams({ title, body });
        return `https://github.com/${GITHUB_REPO}/issues/new?${params.toString()}`;
    }, [title, body]);

    if (!isOpen) return null;

    const canSubmit = description.trim().length > 0;

    const handleCopy = async () => {
        try {
            await navigator.clipboard.writeText(`${title}\n\n${body}`);
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        } catch {
            // Clipboard API unavailable (e.g. insecure context) — nothing to do.
        }
    };

    return (
        <div className="issue-report-overlay" onClick={onClose} role="presentation">
            <div
                className="issue-report-modal"
                onClick={e => e.stopPropagation()}
                role="dialog"
                aria-modal="true"
                aria-labelledby="issue-report-title"
            >
                <button
                    className="issue-report-close"
                    onClick={onClose}
                    aria-label="Close issue report"
                >
                    <X size={20} />
                </button>
                <h2 id="issue-report-title">Feedback</h2>

                <div className="issue-report-content">
                    <p className="issue-report-intro">
                        Spotted a wrong regulation, a misplaced boundary, or a bug? Let us know.
                        Submitting opens a pre-filled GitHub issue — no GitHub account? Use
                        <strong> Copy report</strong> and send it however you like.
                    </p>

                    <label className="issue-report-field">
                        <span>Type of issue</span>
                        <select
                            value={category}
                            onChange={e => setCategory(e.target.value as Category)}
                        >
                            {CATEGORIES.map(c => (
                                <option key={c} value={c}>
                                    {c}
                                </option>
                            ))}
                        </select>
                    </label>

                    <label className="issue-report-field">
                        <span>
                            What&apos;s wrong? <span className="issue-report-req">*</span>
                        </span>
                        <textarea
                            value={description}
                            onChange={e => setDescription(e.target.value)}
                            placeholder="Describe the problem…"
                            rows={4}
                            required
                        />
                    </label>

                    <label className="issue-report-field">
                        <span>Contact (optional)</span>
                        <input
                            type="text"
                            value={contact}
                            onChange={e => setContact(e.target.value)}
                            placeholder="Email or name, if you'd like a reply"
                        />
                    </label>

                    <details className="issue-report-preview">
                        <summary>What gets sent</summary>
                        <pre>{body}</pre>
                    </details>
                </div>

                <div className="issue-report-actions">
                    <button
                        type="button"
                        className="issue-report-secondary"
                        onClick={handleCopy}
                        disabled={!canSubmit}
                    >
                        {copied ? (
                            <>
                                <Check size={15} /> Copied
                            </>
                        ) : (
                            <>
                                <Copy size={15} /> Copy report
                            </>
                        )}
                    </button>
                    <a
                        className={`issue-report-primary${canSubmit ? '' : ' disabled'}`}
                        href={canSubmit ? githubUrl : undefined}
                        target="_blank"
                        rel="noopener noreferrer"
                        aria-disabled={!canSubmit}
                        onClick={e => {
                            if (!canSubmit) {
                                e.preventDefault();
                                return;
                            }
                            onClose();
                        }}
                    >
                        <ExternalLink size={15} /> Open GitHub issue
                    </a>
                </div>
            </div>
        </div>
    );
};

/** Small footer link that opens the feedback modal (sits beside the Disclaimer link). */
export const IssueReportLink: React.FC<{ onClick: () => void }> = ({ onClick }) => (
    <button className="feedback-link" onClick={onClick} aria-label="Send feedback">
        Feedback
    </button>
);

export default IssueReport;
