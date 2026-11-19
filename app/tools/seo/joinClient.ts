/**
 * (T911) THE SITE'S "JOIN THE TEST" CARD, in the reader's browser. The gauge page draws each Join the test as a plain
 * mailto link (it works with no script at all) beside a closed `<dialog>`; this wires the link to open its dialog as a
 * card: ✕, Escape and a click outside close it, focus goes in and back to the link, "Copy email" copies the address and
 * says "Copied", or, where the clipboard is refused, says so and shows the address to select (never silent: W46).
 * Every word is on the dialog (data-copied / data-refused, from core's WEB_SPOTS_WORDS.join.popup): nothing is typed here.
 * No import: the build (gauges.ts `joinScript`) ships this file as one plain script, self-hosted, calling `joinPopup` on load.
 */
interface ClipboardLike { writeText(text: string): Promise<void> }
export function joinPopup(doc: Document, clipboard: () => ClipboardLike | undefined): void {
  const open = new Map<HTMLDialogElement, HTMLElement>();
  const close = (d: HTMLDialogElement) => {
    if (d.open) d.close();
    const back = open.get(d); open.delete(d);
    if (back) back.focus();
  };
  for (const link of Array.from(doc.querySelectorAll<HTMLElement>("a[data-join]"))) {
    const d = doc.getElementById(link.getAttribute("data-join")!) as HTMLDialogElement | null;
    if (!d || typeof d.showModal !== "function") continue;           // no dialog support: the mailto link just works
    link.addEventListener("click", (e) => { e.preventDefault(); open.set(d, link); d.showModal(); });
    d.addEventListener("click", (e) => { if (e.target === d) close(d); });                   // the backdrop is the dialog itself
    d.addEventListener("cancel", (e) => { e.preventDefault(); close(d); });                  // Escape
    d.addEventListener("keydown", (e) => { if (e.key === "Escape") { e.preventDefault(); close(d); } });
    d.addEventListener("close", () => { const back = open.get(d); open.delete(d); if (back) back.focus(); });
    const x = d.querySelector<HTMLElement>("[data-close]");
    if (x) x.addEventListener("click", () => close(d));
    const copy = d.querySelector<HTMLElement>("[data-copy]");
    const said = d.querySelector<HTMLElement>("[data-said]");
    const addr = d.querySelector<HTMLElement>("[data-addr]");
    if (copy && said) copy.addEventListener("click", () => {
      const clip = clipboard();
      const refused = () => {
        said.textContent = d.getAttribute("data-refused") ?? "";
        if (addr) {
          addr.hidden = false;
          const range = doc.createRange(); range.selectNodeContents(addr);
          const sel = doc.getSelection(); if (sel) { sel.removeAllRanges(); sel.addRange(range); }
        }
      };
      if (!clip) { refused(); return; }
      clip.writeText(copy.getAttribute("data-copy") ?? "").then(
        () => { said.textContent = d.getAttribute("data-copied") ?? ""; if (addr) addr.hidden = true; }, refused);
    });
  }
}
