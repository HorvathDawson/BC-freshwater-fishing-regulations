/**
 * (T1034, T1035) WHAT A TAPPED LINK DOES IN THE APP, split out of App.tsx so it is testable: its theme is applied (session
 * only, as the web start link's `theme=` is: never saved), and its water goes to the Shell as an open request carrying the
 * part and view the link names. A link's `fit` box is a request of its own.
 *
 * (T1038) and what the web's start link hands the Shell at a page's start, handed here through the same request: the day
 * (the Shell's one day; null today), the Conditions horizon (absent: Now), the Regulations page's start (tab, sheet,
 * scroll) and `whole`; its angler is the session's profile, set as the web's start sets it (`setAnglerProfile`, D8); a
 * day gone by opens as today and says so with the web's own words.
 */
import type { Horizon, Who } from "@app/core";
import { START_NOTICE_WORDS, type AppLinkOpen, type RegsStart } from "@app/ui";

export interface OpenRequest {
  item: string | null; n: number; view?: "regs" | "conditions" | "map"; part?: number | null;
  fit?: readonly [number, number, number, number];
  /** (T1038) the day it opens on (null: today); absent: the day is left as it is (a notification's tap) */
  day?: Date | null;
  /** (T1038) the Conditions horizon it opens on; absent: left as it is */
  horizon?: Horizon;
  /** (T1038) where the water's Regulations page starts; absent: as any open */
  regs?: RegsStart;
  /** (T1038) the water's Map framed on the whole water */
  whole?: boolean;
}
export interface AppLinkDeps {
  setTheme: (t: "light" | "dark") => void;
  setOpenRequest: (f: (prev: OpenRequest | null) => OpenRequest) => void;
  /** (T1038) the session's angler (`@app/ui` `setAnglerProfile`) */
  setWho: (w: Who) => void;
  /** (T1038) the line the start says (the web's `start-notice`), null: none */
  setNotice: (line: string | null) => void;
}

export function applyAppLink(o: AppLinkOpen, d: AppLinkDeps): void {
  if (!("fit" in o) && o.theme) d.setTheme(o.theme);
  if (!("fit" in o) && o.who) d.setWho(o.who);
  d.setNotice(o.past ? START_NOTICE_WORDS["day-past"] : null);
  const days = { day: o.day, horizon: o.horizon ?? 0 };
  d.setOpenRequest((r) => ("fit" in o ? { item: null, fit: o.fit, ...days, n: (r?.n ?? 0) + 1 }
                                      : { item: o.item, view: o.view, part: o.part, ...days, regs: o.regs,
                                          ...(o.whole ? { whole: true } : {}), n: (r?.n ?? 0) + 1 }));
}

/** (T1038) The line over the app: the start's notice (the web's), else a phone link's; none once the reader shut it. */
export const shownNotice = (start: string | null, link: string | null, shut: boolean): string | null =>
  shut ? null : start ?? link;
