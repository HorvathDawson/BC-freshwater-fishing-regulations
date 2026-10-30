/**
 * (T282) A PART OF THE APP LOADED LATER: on the web its code is a separate chunk (Metro splits on `import()`), fetched
 * when the part is first drawn or when the browser is idle after the first screen (`preloadParts`), so the map and the
 * Regulations pane — the first screen — arrive first on a slow line (the 4 MB bundle took 43 s on Fast 3G). On a phone
 * the bundle is one file: the part is asked for at once and is in long before anything draws it.
 *
 * No Suspense: until its code is in the part draws nothing (what an unopened sheet draws anyway), then the component
 * itself — no fallback, no look change.
 */
import { createElement, useEffect, useState, type ComponentType } from "react";
import { Platform } from "react-native";

const all: (() => Promise<void>)[] = [];

/**
 * `when`: whether these props draw anything (a sheet's open state). An always-mounted closed sheet must not fetch its
 * code the moment the Shell mounts — on a water's own page that competes with the files the hand-over waits for.
 * Absent: drawn whenever mounted.
 */
export function part<P extends object>(load: () => Promise<ComponentType<P>>, when?: (props: P) => boolean):
    ((props: P) => ReturnType<typeof createElement> | null) & { preload(): Promise<void> } {
  let C: ComponentType<P> | null = null;
  let p: Promise<void> | null = null;
  const preload = (): Promise<void> => (p ??= load().then((c) => { C = c; }, (e: unknown) => { p = null; throw e; }));
  all.push(() => preload().catch(() => undefined));
  // a phone: one bundle, asked for at once (in by the first render)
  if (Platform.OS !== "web") void preload().catch(() => undefined);
  function Part(props: P) {
    const [, loaded] = useState(C !== null);
    const wanted = !when || when(props);
    useEffect(() => {
      if (C || !wanted) return;
      let live = true;
      preload().then(() => { if (live) loaded(true); }, () => undefined);
      return () => { live = false; };
    }, [wanted]);
    return C ? createElement(C, props) : null;
  }
  return Object.assign(Part, { preload });
}

/** (T282) Every part's code, fetched one after another (the web, once the first screen is up and the browser idle). */
export async function preloadParts(): Promise<void> {
  for (const f of all.slice()) await f();
}
