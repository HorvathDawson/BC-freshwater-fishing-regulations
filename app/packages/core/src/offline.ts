/**
 * (T238) THE OFFLINE MAPS PAGE (More → Offline maps), in words. One view model for the phone and the web; the facts are
 * handed in (`@app/data` `offlineSizes` / `createOfflineStore`, the update service's release download), nothing is
 * stated here that the data did not say (AGENTS 32).
 *
 *   PHONE   the whole release is installed (map and data, every region): each region is listed with its size and "On this
 *           phone", nothing to toggle; the map is one file for the province (no per-region tile split yet — T85/T202);
 *           storage used is the held set; a release download in progress shows its progress, a paused one (Wi-Fi only,
 *           a failure) offers Resume; the Wi-Fi-only setting is T231's large-updates-on-mobile-data setting, shown here.
 *   WEB     the browser keeps the files it fetched (the release cache): each region "Saved" / "Partly saved" / "Not saved",
 *           with Save (download it now, with progress; Resume after a failure) or Remove; the province's shared data and
 *           the map are listed as what they are — shared data saved with the first region, the map read online.
 */
import { megabytes } from "./install";

export const OFFLINE_WORDS = {
  title: "Offline maps",
  intro: {
    phone: "This phone keeps the whole province: the map and every region's regulations work with no connection.",
    web: "Save a region to open its regulations without downloading them again. The map itself needs a connection in a browser.",
  },
  total: "Storage used",
  /** the province-wide parts */
  map: { name: "Map of BC", phone: "On this phone · one file for the whole province", web: "Read online as you move the map" },
  shared: { name: "Province-wide data", phone: "On this phone", web: "Saved with the first region you save" },
  regionsHead: "Regions",
  region: (r: string, name: string | null) => (name ? `Region ${r.toUpperCase()} · ${name}` : `Region ${r.toUpperCase()}`),
  state: {
    phone: "On this phone",
    kept: "Saved",
    partial: "Partly saved",
    none: "Not saved",
    saving: (pct: number) => `Saving… ${pct}%`,
    removing: "Removing…",
    failed: "Stopped",
  },
  action: {
    save: "Save",
    resume: "Resume",
    remove: "Remove",
    /** said aloud: the action and the region it acts on */
    aloud: (what: string, region: string) => `${what} ${region}`,
  },
  clear: "Remove everything saved",
  /** the phone's release download (T231), shown on the page while one is pending */
  update: {
    downloading: (line: string) => `Downloading new map data · ${line}`,
    waitingWifi: "New map data is waiting for Wi-Fi",
    ready: "New map data is downloaded; it is used the next time you open the map",
    resume: "Resume now",
  },
  wifi: { label: "Download large updates on Wi-Fi only", hint: "Map and regulation updates can be hundreds of megabytes",
          on: "On", off: "Off" },
  failed: (error: string) => `The download stopped: ${error}. What was saved is kept; Resume fetches the rest.`,
  note: {
    phone: "Regions cannot be removed one at a time yet: the map is one file for the whole province.",
    web: "Your browser may also clear saved data when space runs low.",
  },
} as const;

/** The fishing regions' names as the synopsis prints them (short form); a region not listed is named by number alone. */
export const REGION_NAMES: Readonly<Record<string, string>> = {
  "1": "Vancouver Island", "2": "Lower Mainland", "3": "Thompson-Nicola", "4": "Kootenay", "5": "Cariboo",
  "6": "Skeena", "7a": "Omineca", "7b": "Peace", "8": "Okanagan",
};

export interface OfflineRegionInput {
  readonly region: string;
  readonly bytes: number;
  readonly held: number;
  readonly state: "kept" | "partial" | "none";
}

export interface OfflineInput {
  readonly platform: "phone" | "web";
  readonly regions: readonly OfflineRegionInput[];
  readonly shared: { readonly bytes: number; readonly held: number };
  readonly map: { readonly tiles: number; readonly basemap: number };
  /** web: the store's job (a save with progress, a remove; a failed save) */
  readonly job?: { readonly region: string; readonly kind: "save" | "remove"; readonly phase: "running" | "failed";
                   readonly bytes: number; readonly of: number; readonly error: string | null } | null;
  /** web: false when this browser cannot keep files (no Cache Storage): nothing is offered */
  readonly storable?: boolean;
  /** phone: the release download (the update service's release phase and progress) */
  readonly update?: { readonly phase: string; readonly bytes: number; readonly of: number } | null;
  /** phone: large downloads wait for Wi-Fi (the inverse of T231's allow-on-mobile-data); null: no setting here */
  readonly wifiOnly?: boolean | null;
}

export type OfflineActionKind = "save" | "resume" | "remove";

export interface OfflineRowVM {
  readonly k: string;
  readonly name: string;
  readonly size: string;
  readonly state: string;
  /** on this device whole (drawn with a check) */
  readonly kept: boolean;
  /** 0–1 while saving; null otherwise */
  readonly progress: number | null;
  readonly action: { readonly kind: OfflineActionKind; readonly label: string; readonly aloud: string;
                     readonly busy: boolean } | null;
  /** the row aloud: name, size, state */
  readonly aloud: string;
}

export interface OfflineVM {
  readonly title: string;
  readonly intro: string;
  readonly total: { readonly label: string; readonly value: string; readonly aloud: string };
  readonly parts: readonly OfflineRowVM[];
  readonly regionsHead: string;
  readonly regions: readonly OfflineRowVM[];
  readonly update: { readonly line: string; readonly progress: number | null; readonly resume: string | null } | null;
  readonly failed: string | null;
  readonly clear: string | null;
  readonly wifi: { readonly label: string; readonly hint: string; readonly on: boolean; readonly value: string } | null;
  readonly note: string;
}

const pct = (bytes: number, of: number) => (of > 0 ? Math.min(100, Math.floor((bytes / of) * 100)) : 0);

export function offlineVM(i: OfflineInput): OfflineVM {
  const W = OFFLINE_WORDS, phone = i.platform === "phone";
  const storable = phone ? false : i.storable !== false;
  const mapBytes = i.map.tiles + i.map.basemap;
  const regionBytes = i.regions.reduce((n, r) => n + (phone ? r.bytes : r.held), 0);
  const used = phone ? mapBytes + i.shared.bytes + regionBytes : i.shared.held + regionBytes;
  const job = i.job ?? null;
  const busy = job?.phase === "running";
  const row = (k: string, name: string, size: string, state: string, kept: boolean, progress: number | null = null,
               action: OfflineRowVM["action"] = null): OfflineRowVM =>
    ({ k, name, size, state, kept, progress, action, aloud: `${name}: ${size}. ${state}.` });
  const sharedKept = phone || (i.shared.held >= i.shared.bytes && i.shared.bytes > 0);
  const act = (kind: OfflineActionKind, name: string) => ({ kind, label: W.action[kind], aloud: W.action.aloud(W.action[kind], name), busy });
  /** a row's state while the store works on it (a save's progress, a remove, a failed save), else null */
  const working = (k: string, name: string, bytes: number): OfflineRowVM | null => {
    const mine = job?.region === k ? job : null;
    if (mine?.kind === "save" && mine.phase === "running")
      return row(k, name, megabytes(bytes), W.state.saving(pct(mine.bytes, mine.of)), false, mine.of > 0 ? mine.bytes / mine.of : 0);
    if (mine?.kind === "remove" && mine.phase === "running") return row(k, name, megabytes(bytes), W.state.removing, false);
    if (mine?.phase === "failed") return row(k, name, megabytes(bytes), W.state.failed, false, null, storable ? act("resume", name) : null);
    return null;
  };
  const sharedRow = phone ? row("shared", W.shared.name, megabytes(i.shared.bytes), W.shared.phone, true)
    : working("shared", W.shared.name, i.shared.bytes)
      ?? row("shared", W.shared.name, megabytes(i.shared.bytes),
             sharedKept ? W.state.kept : i.shared.held > 0 ? W.state.partial : W.shared.web, sharedKept, null,
             storable && !sharedKept && i.shared.held > 0 ? act("save", W.shared.name) : null);
  const parts: OfflineRowVM[] = [
    row("map", W.map.name, megabytes(mapBytes), phone ? W.map.phone : W.map.web, phone),
    sharedRow,
  ];
  const regions = i.regions.map((r) => {
    const name = W.region(r.region, REGION_NAMES[r.region] ?? null);
    if (phone) return row(r.region, name, megabytes(r.bytes), W.state.phone, true);
    const w = working(r.region, name, r.bytes);
    if (w) return w;
    const state = W.state[r.state];
    const action = !storable ? null : r.state === "kept" ? act("remove", name) : act("save", name);
    return row(r.region, name, megabytes(r.bytes), state, r.state === "kept", null, action);
  });
  const u = phone ? i.update ?? null : null;
  const update = !u || u.phase === "current" || u.phase === "app-too-old" ? null
    : u.phase === "downloading"
      ? { line: W.update.downloading(`${pct(u.bytes, u.of)}%`), progress: u.of > 0 ? u.bytes / u.of : 0, resume: null }
      : u.phase === "waiting-wifi" ? { line: W.update.waitingWifi, progress: null, resume: W.update.resume }
      : u.phase === "ready" ? { line: W.update.ready, progress: null, resume: null } : null;
  const anyKept = i.shared.held > 0 || i.regions.some((r) => r.held > 0);
  const wifi = phone && i.wifiOnly !== undefined && i.wifiOnly !== null
    ? { label: W.wifi.label, hint: W.wifi.hint, on: i.wifiOnly, value: i.wifiOnly ? W.wifi.on : W.wifi.off } : null;
  return {
    title: W.title,
    intro: phone ? W.intro.phone : W.intro.web,
    total: { label: W.total, value: megabytes(used), aloud: `${W.total}: ${megabytes(used)}` },
    parts, regionsHead: W.regionsHead, regions, update,
    failed: job?.phase === "failed" ? W.failed(job.error ?? "no connection") : null,
    clear: !phone && storable && anyKept && !busy ? W.clear : null,
    wifi,
    note: phone ? W.note.phone : W.note.web,
  };
}
