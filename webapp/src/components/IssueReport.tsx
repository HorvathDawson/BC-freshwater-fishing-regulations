import React, { useEffect, useMemo, useRef, useState } from 'react';
import { X, Check, ExternalLink, Copy, Send } from 'lucide-react';
import './IssueReport.css';
import { API_BASE } from '../config/endpoints';

/** GitHub repo that receives pre-filled issue reports. */
const GITHUB_REPO = 'HorvathDawson/BC-freshwater-fishing-regulations';

/** Pragmatic email shape check (mirrors the worker's server-side validation).
 *  Deliberately loose — the goal is to catch typos, not enforce RFC 5322. */
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

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
    /** Extra diagnostics (map view internals, selected-feature metadata, …)
     *  rendered verbatim under "Technical details". Empty values are skipped. */
    details?: Record<string, string | number | boolean | null | undefined>;
}

/**
 * Capture universal browser/runtime environment for the report. This is
 * app-agnostic (viewport, UA, locale, …) so it lives here rather than in the
 * map component, keeping getContext focused on app/map/feature state.
 */
function collectEnvironment(): Record<string, string | number> {
    const env: Record<string, string | number> = {};
    if (typeof window !== 'undefined') {
        env['Viewport'] = `${window.innerWidth}\u00d7${window.innerHeight}`;
        env['Device pixel ratio'] = window.devicePixelRatio || 1;
        if (window.screen) env['Screen'] = `${window.screen.width}\u00d7${window.screen.height}`;
    }
    if (typeof navigator !== 'undefined') {
        env['User agent'] = navigator.userAgent;
        env['Language'] = navigator.language;
        env['Online'] = navigator.onLine ? 'yes' : 'no';
        if (navigator.maxTouchPoints != null) env['Touch points'] = navigator.maxTouchPoints;
    }
    try {
        env['Timezone'] = Intl.DateTimeFormat().resolvedOptions().timeZone;
    } catch {
        // Intl unavailable — skip.
    }
    env['Local time'] = new Date().toString();
    return env;
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
    email: string,
    ctx: IssueReportContext,
    environment: Record<string, string | number>,
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
    if (email.trim()) context.push(`- Contact email: ${email.trim()}`);

    if (context.length) {
        lines.push('### Context', ...context, '');
    }

    // Verbose diagnostics: app/map/feature internals + browser environment.
    // Merged into one section; empty values are omitted.
    const diagnostics: Record<string, string | number | boolean> = {};
    for (const [k, v] of Object.entries(ctx.details ?? {})) {
        if (v !== null && v !== undefined && v !== '') diagnostics[k] = v;
    }
    for (const [k, v] of Object.entries(environment)) {
        if (v !== null && v !== undefined && v !== '') diagnostics[k] = v;
    }
    const detailKeys = Object.keys(diagnostics);
    if (detailKeys.length) {
        lines.push('### Technical details');
        for (const k of detailKeys) lines.push(`- ${k}: ${diagnostics[k]}`);
        lines.push('');
    }

    return lines.join('\n').trim();
}

const IssueReport: React.FC<IssueReportProps> = ({ isOpen, onClose, getContext }) => {
    const [category, setCategory] = useState<Category>('Incorrect regulation');
    const [description, setDescription] = useState('');
    const [email, setEmail] = useState('');
    const [ctx, setCtx] = useState<IssueReportContext>({});
    const [environment, setEnvironment] = useState<Record<string, string | number>>({});
    const [copied, setCopied] = useState(false);
    const [submitState, setSubmitState] = useState<'idle' | 'sending' | 'sent' | 'error'>('idle');
    // Honeypot: hidden field; only bots fill it. Kept out of the report body.
    const [hp, setHp] = useState('');

    // Keep the latest getContext without making it an effect dependency, so the
    // snapshot fires exactly once per open (not on every parent re-render).
    const getContextRef = useRef(getContext);
    getContextRef.current = getContext;

    // Fresh slate + context snapshot each time the modal opens.
    useEffect(() => {
        if (!isOpen) return;
        setCtx(getContextRef.current ? getContextRef.current() : {});
        setEnvironment(collectEnvironment());
        setCategory('Incorrect regulation');
        setDescription('');
        setEmail('');
        setCopied(false);
        setSubmitState('idle');
        setHp('');
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
        () => buildReportBody(category, description, email, ctx, environment),
        [category, description, email, ctx, environment],
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

    const trimmedEmail = email.trim();
    // Optional: blank is fine; if provided it must look like an email.
    const emailValid = trimmedEmail === '' || EMAIL_RE.test(trimmedEmail);
    const canSubmit = description.trim().length > 0;
    // Sending also requires a well-formed email (Copy/GitHub don't).
    const canSend = canSubmit && emailValid;

    const handleCopy = async () => {
        try {
            await navigator.clipboard.writeText(`${title}\n\n${body}`);
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        } catch {
            // Clipboard API unavailable (e.g. insecure context) — nothing to do.
        }
    };

    // Send the report directly — no account needed. The worker stores it and
    // (if configured) emails it. On failure, the GitHub/Copy paths remain.
    const handleSend = async () => {
        if (!canSend || submitState === 'sending' || submitState === 'sent') return;
        setSubmitState('sending');
        try {
            const resp = await fetch(`${API_BASE}/api/feedback`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ title, body, email: trimmedEmail || undefined, hp }),
            });
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            setSubmitState('sent');
        } catch {
            setSubmitState('error');
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
                        <strong> Send report</strong> delivers it straight to the maintainers—no
                        account needed. Prefer GitHub, or want to send it yourself? Use the
                        <strong> GitHub</strong> or <strong>Copy</strong> options.
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
                        <span>Email (optional)</span>
                        <input
                            type="email"
                            value={email}
                            onChange={e => setEmail(e.target.value)}
                            placeholder="you@example.com — we'll email you a confirmation"
                            autoComplete="email"
                            inputMode="email"
                            aria-invalid={!emailValid}
                            aria-describedby={emailValid ? undefined : 'issue-report-email-error'}
                        />
                        {!emailValid && (
                            <span
                                id="issue-report-email-error"
                                className="issue-report-field-error"
                                role="alert"
                            >
                                Enter a valid email address, or leave it blank.
                            </span>
                        )}
                    </label>

                    <details className="issue-report-preview">
                        <summary>What gets sent</summary>
                        <pre>{body}</pre>
                    </details>

                    {/* Honeypot: hidden from users; bots that fill it are silently dropped. */}
                    <input
                        type="text"
                        className="issue-report-hp"
                        tabIndex={-1}
                        autoComplete="off"
                        aria-hidden="true"
                        value={hp}
                        onChange={e => setHp(e.target.value)}
                    />
                </div>

                {submitState === 'sent' && (
                    <p className="issue-report-status success" role="status">
                        <Check size={14} /> Thanks — your report was sent.
                        {trimmedEmail && ' A confirmation is on its way to your inbox.'}
                    </p>
                )}
                {submitState === 'error' && (
                    <p className="issue-report-status error" role="alert">
                        Couldn&apos;t send automatically. Use <strong>Copy report</strong> or the
                        GitHub option instead.
                    </p>
                )}

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
                        className={`issue-report-secondary${canSubmit ? '' : ' disabled'}`}
                        href={canSubmit ? githubUrl : undefined}
                        target="_blank"
                        rel="noopener noreferrer"
                        aria-disabled={!canSubmit}
                        onClick={e => {
                            if (!canSubmit) {
                                e.preventDefault();
                                return;
                            }
                        }}
                    >
                        <ExternalLink size={15} /> GitHub
                    </a>
                    <button
                        type="button"
                        className="issue-report-primary"
                        onClick={handleSend}
                        disabled={!canSend || submitState === 'sending' || submitState === 'sent'}
                    >
                        {submitState === 'sending' ? (
                            <>
                                <Send size={15} /> Sending…
                            </>
                        ) : submitState === 'sent' ? (
                            <>
                                <Check size={15} /> Sent
                            </>
                        ) : (
                            <>
                                <Send size={15} /> Send report
                            </>
                        )}
                    </button>
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
