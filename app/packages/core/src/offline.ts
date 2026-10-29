/**
 * (T238) THE OFFLINE MAPS PAGE (More → Offline maps), in words. Owner 2026-10-29: the REGULATIONS ARE NEVER PER REGION
 * — one fixed first row for all of B.C., always kept, nothing to save or remove; only the MAP is a choice by region.
 *
 *   REGULATIONS   "Regulations: all of B.C." with its size (every data file the release names) and "Always available
 *                 offline". On the web, under it, how much this browser holds so far (the background prefetch walks
 *                 every region's over time, the view's first — T253).
 *   MAP           today ONE province-wide file (tiles + basemap): no per-region tile split exists yet (T85/T202), so
 *                 the map is one row and the regions are listed as part of it, no size and no action of their own —
 *                 a size per region would be made up (AGENTS 32). The rows become choices when the split lands.
 *   PHONE         storage used is the installed set; a release download (T231) shows its progress, a paused one Resume;
 *                 "Download large updates on Wi-Fi only" is T231's mobile-data setting, inverted.
 */
import { megabytes } from "./install";

export const OFFLINE_WORDS = {
  title: "Offline maps",
  intro: {
    phone: "The regulations for all of B.C. are always on this phone. The map is kept for the whole province.",
    web: "The regulations for all of B.C. are kept in this browser as you use the app. The map itself needs a connection in a browser.",
  },
  total: "Storage used",
  regs: {
    name: "Regulations: all of B.C.",
    state: "Always available offline",
    /** web: what the release cache holds of them so far */
    webHeld: (held: string, of: string) => `${held} of ${of} kept in this browser so far`,
    webAll: "All kept in this browser",
  },
  map: {
    name: "Map of B.C.",
    phone: "On this phone · one file for the whole province",
    web: "Read online as you move the map",
  },
  regionsHead: "Map by region",
  region: (r: string, name: string | null) => (name ? `Region ${r.toUpperCase()} · ${name}` : `Region ${r.toUpperCase()}`),
  /** a region's map row until the map is split by region (T85/T202) */
  regionPart: { phone: "Part of the province map · on this phone", web: "Part of the province map" },
  /** the phone's release download (T231), shown on the page while one is pending */
  update: {
    downloading: (pct: string) => `Downloading new map data · ${pct}`,
    waitingWifi: "New map data is waiting for Wi-Fi",
    ready: "New map data is downloaded; it is used the next time you open the map",
    resume: "Resume now",
  },
  wifi: { label: "Download large updates on Wi-Fi only", hint: "Map and regulation updates can be hundreds of megabytes",
          on: "On", off: "Off" },
  note: "Choosing map regions one at a time comes when the map is split by region. The regulations stay whole either way.",
} as const;

/** The fishing regions' names as the synopsis prints them (short form); a region not listed is named by number alone. */
export const REGION_NAMES: Readonly<Record<string, string>> = {
  "1": "Vancouver Island", "2": "Lower Mainland", "3": "Thompson-Nicola", "4": "Kootenay", "5": "Cariboo",
  "6": "Skeena", "7a": "Omineca", "7b": "Peace", "8": "Okanagan",
};

export interface OfflineInput {
  readonly platform: "phone" | "web";
  /** the release's regions (its regs files' region words) */
  readonly regions: readonly string[];
  /** every data file the release names, bytes; `held`: what the web's release cache keeps of them (null: not known yet) */
  readonly regs: { readonly bytes: number; readonly held: number | null };
  readonly map: { readonly tiles: number; readonly basemap: number };
  /** phone: the release download (the update service's release phase and progress) */
  readonly update?: { readonly phase: string; readonly bytes: number; readonly of: number } | null;
  /** phone: large downloads wait for Wi-Fi (the inverse of T231's allow-on-mobile-data); null: no setting here */
  readonly wifiOnly?: boolean | null;
}

export interface OfflineRowVM {
  readonly k: string;
  readonly name: string;
  /** "459.8 MB"; null: no size of its own (a region's part of the province map) */
  readonly size: string | null;
  readonly state: string;
  /** on this device whole (drawn with a check) */
  readonly kept: boolean;
  /** the row aloud: name, size, state */
  readonly aloud: string;
}

export interface OfflineVM {
  readonly title: string;
  readonly intro: string;
  readonly total: { readonly label: string; readonly value: string; readonly aloud: string } | null;
  /** the fixed first row: the regulations for all of B.C. — never an action */
  readonly regs: OfflineRowVM;
  readonly map: OfflineRowVM;
  readonly regionsHead: string;
  readonly regions: readonly OfflineRowVM[];
  readonly update: { readonly line: string; readonly progress: number | null; readonly resume: string | null } | null;
  readonly wifi: { readonly label: string; readonly hint: string; readonly on: boolean; readonly value: string } | null;
  readonly note: string;
}

const pct = (bytes: number, of: number) => (of > 0 ? Math.min(100, Math.floor((bytes / of) * 100)) : 0);

export function offlineVM(i: OfflineInput): OfflineVM {
  const W = OFFLINE_WORDS, phone = i.platform === "phone";
  const mapBytes = i.map.tiles + i.map.basemap;
  const row = (k: string, name: string, size: string | null, state: string, kept: boolean): OfflineRowVM =>
    ({ k, name, size, state, kept, aloud: size ? `${name}: ${size}. ${state}.` : `${name}: ${state}.` });
  const held = i.regs.held;
  const regsState = phone || held === null ? W.regs.state
    : held >= i.regs.bytes ? `${W.regs.state} · ${W.regs.webAll}`
    : `${W.regs.state} · ${W.regs.webHeld(megabytes(held), megabytes(i.regs.bytes))}`;
  const regs = row("regs", W.regs.name, megabytes(i.regs.bytes), regsState, phone || (held !== null && held >= i.regs.bytes));
  const map = row("map", W.map.name, megabytes(mapBytes), phone ? W.map.phone : W.map.web, phone);
  const regions = i.regions.map((r) =>
    row(r, W.region(r, REGION_NAMES[r] ?? null), null, phone ? W.regionPart.phone : W.regionPart.web, phone));
  const used = phone ? mapBytes + i.regs.bytes : held;
  const u = phone ? i.update ?? null : null;
  const update = !u || u.phase === "current" || u.phase === "app-too-old" ? null
    : u.phase === "downloading"
      ? { line: W.update.downloading(`${pct(u.bytes, u.of)}%`), progress: u.of > 0 ? u.bytes / u.of : 0, resume: null }
      : u.phase === "waiting-wifi" ? { line: W.update.waitingWifi, progress: null, resume: W.update.resume }
      : u.phase === "ready" ? { line: W.update.ready, progress: null, resume: null } : null;
  const wifi = phone && i.wifiOnly !== undefined && i.wifiOnly !== null
    ? { label: W.wifi.label, hint: W.wifi.hint, on: i.wifiOnly, value: i.wifiOnly ? W.wifi.on : W.wifi.off } : null;
  return {
    title: W.title,
    intro: phone ? W.intro.phone : W.intro.web,
    total: used === null ? null : { label: W.total, value: megabytes(used), aloud: `${W.total}: ${megabytes(used)}` },
    regs, map, regionsHead: W.regionsHead, regions, update, wifi, note: W.note,
  };
}
