/**
 * (T910, owner 2026-11-19 "yes build all 3 on webapp") THE FIRST VISIT'S UNASKED DOWNLOADS WAIT FOR A QUIET PAGE. The idle
 * tier (core + the sql.js engine, T186) and the background tier (every file an unnamed tap reads, T253/T737) started on
 * the first idle callback after first paint: on a first visit that was while the map was still being built, and the
 * ~8 MiB they brought (each file verified, the regs rules decoded) shared the main thread and the line with the first
 * screen (Lighthouse: 9.3-10.4 MiB in its window, ~1 MiB of it the first screen's). Now they start after the page's
 * `load` event and then `QUIET_MS` with no long task (a task over 50 ms, the browser's own `longtask` entries); each
 * long task starts the spell again. A browser with no long-task entries (Safari, Firefox) waits `QUIET_MS` after load.
 * Never later than `QUIET_CAP_MS` after load: a page that is never quiet (an animating map) still gets its files.
 * Nothing the reader asks for waits for this: a tap, a search, a start link fetch at once, as before.
 */
/** The quiet spell (owner's 5 s). */
export const QUIET_MS = 5000;
/** At the latest this long after load, quiet or not. */
const QUIET_CAP_MS = 30_000;

export function afterLoadQuiet(go: () => void): () => void {
  let spell: ReturnType<typeof setTimeout> | null = null, cap: ReturnType<typeof setTimeout> | null = null;
  let observer: PerformanceObserver | null = null, over = false;
  const stop = () => {
    over = true;
    if (spell) clearTimeout(spell);
    if (cap) clearTimeout(cap);
    observer?.disconnect();
    window.removeEventListener("load", loaded);
  };
  const fire = () => { if (over) return; stop(); go(); };
  const quiet = () => { if (spell) clearTimeout(spell); spell = setTimeout(fire, QUIET_MS); };
  function loaded() {
    if (over) return;
    quiet();
    cap = setTimeout(fire, QUIET_CAP_MS);
    if (typeof PerformanceObserver !== "undefined" && PerformanceObserver.supportedEntryTypes?.includes("longtask")) {
      observer = new PerformanceObserver(quiet);
      observer.observe({ type: "longtask" });
    }
  }
  if (document.readyState === "complete") loaded();
  else window.addEventListener("load", loaded);
  return stop;
}
