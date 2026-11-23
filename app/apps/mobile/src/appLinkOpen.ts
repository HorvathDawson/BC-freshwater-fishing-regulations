/**
 * (T1034, T1035) WHAT A TAPPED LINK DOES IN THE APP, split out of App.tsx so it is testable: its theme is applied (session
 * only, as the web start link's `theme=` is: never saved), and its water goes to the Shell as an open request carrying the
 * part and view the link names. A link's `fit` box is a request of its own.
 */
import type { AppLinkOpen } from "@app/ui";

export interface OpenRequest {
  item: string | null; n: number; view?: "regs" | "conditions" | "map"; part?: number | null;
  fit?: readonly [number, number, number, number];
}
export interface AppLinkDeps {
  setTheme: (t: "light" | "dark") => void;
  setOpenRequest: (f: (prev: OpenRequest | null) => OpenRequest) => void;
}

export function applyAppLink(o: AppLinkOpen, d: AppLinkDeps): void {
  if (!("fit" in o) && o.theme) d.setTheme(o.theme);
  d.setOpenRequest((r) => ("fit" in o ? { item: null, fit: o.fit, n: (r?.n ?? 0) + 1 }
                                      : { item: o.item, view: o.view, part: o.part, n: (r?.n ?? 0) + 1 }));
}
