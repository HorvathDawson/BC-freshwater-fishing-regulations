import { useCallback, useEffect, useLayoutEffect, useRef, useState, type ReactNode } from "react";

/* ONE NOTICE STACK (owner 2026-11-02): every banner and toast sits in one compact dock BELOW the app, in the
   page's flow — never over the map or the editor. Each notice has a close ×; a dismissed notice stays hidden
   until its CONTENT changes (its `contentKey`). A critical one (`critical`: a save NOT made, an orphaned
   mark) cannot be hidden: × shrinks it to a small red pill that still says how many. */

const STORE = "notice-dismissed";

function read(): Record<string, string> {
  try { return JSON.parse(sessionStorage.getItem(STORE) ?? "{}") as Record<string, string>; } catch { return {}; }
}

/** [dismissed, dismiss]: dismissed while the stored key equals `contentKey` (new content shows again) */
export function useDismissed(id: string, contentKey: string): [boolean, () => void, () => void] {
  const [map, setMap] = useState<Record<string, string>>(read);
  const write = useCallback((m: Record<string, string>) => {
    setMap(m);
    try { sessionStorage.setItem(STORE, JSON.stringify(m)); } catch { /* per viewer only */ }
  }, []);
  return [map[id] === contentKey,
          () => write({ ...read(), [id]: contentKey }),
          () => { const m = read(); delete m[id]; write(m); }];
}

export function CloseX({ onClick, title = "dismiss" }: { onClick: () => void; title?: string }) {
  return (
    <button type="button" className="notice-x" aria-label={title} title={title} data-testid="notice-close"
      onClick={onClick}>×</button>
  );
}

/** one notice of the stack: a tone, a one-line summary, optional details, its close */
export function Notice({ id, contentKey, tone, summary, children, critical = false, testid, pill }: {
  id: string; contentKey: string; tone: "error" | "warn" | "info"; summary: ReactNode; children?: ReactNode;
  critical?: boolean; testid?: string; pill?: string;
}) {
  const [hidden, hide, show] = useDismissed(id, contentKey);
  if (hidden && !critical) return null;
  if (hidden) {
    return (
      <button type="button" className={`notice-pill tone-${tone}`} data-testid={testid} role="alert"
        title="click to open" onClick={show}>{pill ?? summary}</button>
    );
  }
  return (
    <div className={`notice-card tone-${tone}`} data-testid={testid} role={tone === "info" ? "status" : "alert"}>
      <div className="notice-line">
        <span className="notice-summary">{summary}</span>
        <CloseX onClick={hide} title={critical ? "shrink (it stays as a small pill)" : "dismiss until it changes"} />
      </div>
      {children && <div className="notice-body">{children}</div>}
    </div>
  );
}

/** THE DOCK: a flex child below the app; it takes height only when it holds a notice */
export function NoticeDock({ children }: { children: ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);
  const [empty, setEmpty] = useState(true);
  useLayoutEffect(() => { setEmpty(!ref.current?.childElementCount); });
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const mo = new MutationObserver(() => setEmpty(!el.childElementCount));
    mo.observe(el, { childList: true });
    return () => mo.disconnect();
  }, []);
  return <div ref={ref} className={`notice-dock${empty ? " empty" : ""}`} data-testid="notice-dock">{children}</div>;
}
