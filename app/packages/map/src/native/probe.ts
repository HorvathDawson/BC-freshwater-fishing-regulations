/**
 * THE GREY-FRAME PROBE (T74) — development measurement only, the native counterpart of the colour-latency harness's
 * probe (handoff/PLAN/app-data/colour-latency). On while `globalThis.__colourProbe` is set (apps/mobile sets it from
 * `EXPO_PUBLIC_COLOUR_PROBE=1` in a development build); otherwise `colourProbe` returns null and the map registers no
 * frame listener at all.
 *
 * From a pan's start until 1.5 s after it settles, each rendered frame (as many as an async query keeps up with) asks
 * which sections are on screen and counts the ones the colouring should have labelled (`wanted`) whose label the map
 * has not acknowledged yet (`coloured`). A frame with any is GREY. One line per pan to the log:
 * `T74PROBE {"pan":n,"frames":f,"grey":g,"worst":share}` (`adb logcat -s ReactNativeJS`).
 */
export interface ColourProbe {
  moving(): void;
  settled(): void;
  frame(): void;
}

export function colourProbe(o: {
  query: () => Promise<readonly number[]>;
  coloured: (id: number) => boolean;
  wanted: (id: number) => boolean;
}): ColourProbe | null {
  if (!(globalThis as { __colourProbe?: boolean }).__colourProbe) return null;
  let pan = 0, on = false, busy = false, frames = 0, grey = 0, worst = 0;
  let timer: ReturnType<typeof setTimeout> | null = null;
  const report = () => {
    if (!frames) return;
    console.log(`T74PROBE ${JSON.stringify({ pan, frames, grey, worst: Math.round(worst * 1000) / 1000 })}`);
    frames = 0; grey = 0; worst = 0;
  };
  return {
    moving: () => {
      if (timer) { clearTimeout(timer); timer = null; }
      if (!on) { pan += 1; on = true; }
    },
    settled: () => {
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => { on = false; timer = null; report(); }, 1500);
    },
    frame: () => {
      if (!on || busy) return;
      busy = true;
      void o.query().then((ids) => {
        const want = ids.filter(o.wanted);
        const bad = want.filter((id) => !o.coloured(id)).length;
        frames += 1;
        if (bad) { grey += 1; worst = Math.max(worst, bad / want.length); }
      }).catch(() => {}).finally(() => { busy = false; });
    },
  };
}
