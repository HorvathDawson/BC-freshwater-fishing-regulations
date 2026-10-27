import { useEffect, useRef } from "react";

/**
 * ESCAPE IS BACK on the desktop (T214 item 2, owner 2026-10-27): the same one back as ‹. `active` is false while a
 * layer is open (search, a sheet, a menu register with the back and have their own Escape: `useEscapeCloses`, the
 * Modal's), and at the browse root, where there is nothing to go back to. An Escape that something already took
 * (`defaultPrevented`), one typed in a field, or one under a dialog is left alone.
 */
const escapeIsBack = (e: Pick<KeyboardEvent, "key" | "defaultPrevented" | "target">, modalOpen: boolean): boolean => {
  if (e.key !== "Escape" || e.defaultPrevented || modalOpen) return false;
  const t = e.target as { tagName?: string; isContentEditable?: boolean } | null;
  return !(t && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName ?? "")));
};

export function useEscapeBack(back: () => boolean, active: boolean): void {
  const ref = useRef(back);
  ref.current = back;
  useEffect(() => {
    if (!active || typeof window === "undefined") return;
    const onKey = (e: KeyboardEvent) => {
      if (escapeIsBack(e, typeof document !== "undefined" && !!document.querySelector('[aria-modal="true"]'))) ref.current();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [active]);
}
