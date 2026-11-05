/**
 * (T282) A PART OF THE APP LOADED LATER: on the web its code is a separate chunk (Metro splits on `import()`), fetched
 * when the part is first drawn or when the browser is idle after the first screen (`preloadParts`), so the map and the
 * Regulations pane — the first screen — arrive first on a slow line (the 4 MB bundle took 43 s on Fast 3G). On a phone
 * the bundle is one file: the part is asked for at once and is in long before anything draws it.
 *
 * No Suspense: until its code is in the part draws nothing (what an unopened sheet draws anyway), then the component
 * itself — no fallback, no look change.
 *
 * A PART WHOSE CODE CANNOT LOAD IS SHOWN AND REPORTED (owner: no silent fallbacks): every failed load goes to
 * `reportFailure`, and a part that is wanted draws the error line with "Try again" in its place — never nothing. (Before,
 * a failed chunk was swallowed by `.catch(() => undefined)` and the part stayed blank for the rest of the run.)
 */
import { createElement, useEffect, useState, type ComponentType } from "react";
import { Platform, View } from "react-native";
import { NET_WORDS } from "@app/core";
import { reportFailure } from "@app/ui";
import { ErrorState } from "./ErrorState";
import { LIGHT, SPACE, type Palette } from "./theme";

const all: (() => Promise<boolean>)[] = [];

/**
 * (T391) Metro's `import()` first asks for the module at once and fetches the chunk only when that throws. With
 * `global.ErrorUtils` installed (the web bundle has it), the miss is not thrown: Metro reports it as a FATAL "Requiring
 * unknown module" and hands back nothing — every web visit filed a fatal crash and the part needed a second try. So the
 * asking moment runs without `ErrorUtils`: the miss throws, the chunk is fetched, and only a real load failure is reported
 * (by `preload`, below). Only the synchronous call: the module's own code runs later, guarded as before.
 */
export function unguarded<T>(load: () => Promise<T>, g: Record<string, unknown> = globalThis as never): Promise<T> {
  if (!("ErrorUtils" in g)) return load();
  const eu = g.ErrorUtils;
  g.ErrorUtils = undefined;
  try { return load(); } finally { g.ErrorUtils = eu; }
}

/** what a part whose code cannot load draws in its place */
export const PART_FAILED = NET_WORDS.part.code;

/**
 * `when`: whether these props draw anything (a sheet's open state). An always-mounted closed sheet must not fetch its
 * code the moment the Shell mounts — on a water's own page that competes with the files the hand-over waits for.
 * Absent: drawn whenever mounted.
 */
export function part<P extends object>(load: () => Promise<ComponentType<P>>, when?: (props: P) => boolean):
    ((props: P) => ReturnType<typeof createElement> | null) & { preload(): Promise<void> } {
  let C: ComponentType<P> | null = null;
  let p: Promise<void> | null = null;
  // a failed load is reported here, once per attempt, and forgotten so the next ask tries again; the rejection still
  // reaches every caller (a drawn part shows it)
  const preload = (): Promise<void> => (p ??= (Platform.OS === "web" ? unguarded(load) : load()).then((c) => {
    // a chunk that hands back nothing (Metro's importAll when the module is still unknown) is a failure
    if (typeof c !== "function" && (typeof c !== "object" || c === null)) throw new Error("a part's code is not in its chunk");
    C = c;
  }).catch((e: unknown) => { p = null; reportFailure(e); throw e; }));
  // reported in preload; a wanted part shows it when drawn
  const settle = () => preload().then(() => true, () => false);
  all.push(settle);
  // a phone: one bundle, asked for at once (in by the first render)
  if (Platform.OS !== "web") void settle();
  function Part(props: P) {
    const [, loaded] = useState(C !== null);
    const [failed, setFailed] = useState(false);
    const [tries, setTries] = useState(0);
    const wanted = !when || when(props);
    useEffect(() => {
      if (C || !wanted) return;
      let live = true;
      preload().then(() => { if (live) loaded(true); }, () => { if (live) setFailed(true); });
      return () => { live = false; };
    }, [wanted, tries]);
    if (C) return createElement(C, props);
    if (!failed || !wanted) return null;
    const palette = (props as { palette?: Palette }).palette ?? LIGHT;
    return (
      <View testID="part-failed" style={{ padding: SPACE.s4, gap: SPACE.s2, backgroundColor: palette.card }}>
        <ErrorState palette={palette} size="body" message={PART_FAILED} retryLabel={NET_WORDS.part.retry} retryTestID="part-retry"
                    onRetry={() => { setFailed(false); setTries((t) => t + 1); }} />
      </View>
    );
  }
  return Object.assign(Part, { preload });
}

/**
 * (T282) Every part's code, fetched one after another (the web, once the first screen is up and the browser idle). Every
 * part is in the one chunk, so the first failure ends the walk (reported once, not once per part); a part drawn later
 * asks again and shows its own failure.
 */
export async function preloadParts(): Promise<void> {
  for (const f of all.slice()) if (!(await f())) return;
}
