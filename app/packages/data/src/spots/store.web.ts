/**
 * Spots in a browser: local storage, for now.
 *
 * Deliberately the simplest thing that works, and deliberately NOT presented as durable.
 * Browser storage is per-origin, per-device, and a person clearing site data clears their
 * notes with it. That is acceptable while the web app is something you open to look at a
 * map, and it is not acceptable once anyone keeps a season's notes in it — at which point
 * this becomes IndexedDB plus the export/import that `model.ts` calls a requirement.
 *
 * The phone writes a real file (`store.native.ts`). Nothing above this line can tell.
 */
import { environmentError, now, reportOnce } from "@app/core";
import type { HistoryText } from "../feed/history";
import { openSpotsWith, type SpotsFs, type SpotsStore } from "./core";
import { openPhotos } from "./photoStore.web";

const KEY = "canifishthis.spots.v1";

/** The store rules are `core.ts`'s. Here a "file" is a localStorage key. A write that the browser refuses (quota, or
 *  storage blocked in a private window) THROWS, so the save rejects and the reader is told (P01). It used to be
 *  swallowed. */
export function openSpots(): SpotsStore {
  const ls = (): Storage => {
    const s = globalThis.localStorage;
    if (!s) throw new Error("This browser is not letting the app store anything.");
    return s;
  };
  const fs: SpotsFs = {
    // (T368) storage refused is a failure, never "no spots file" (an empty list with no word)
    exists: (n) => ls().getItem(n) !== null,
    read: (n) => { const v = ls().getItem(n); if (v === null) throw new Error(`no ${n}`); return v; },
    write: (n, t) => { ls().setItem(n, t); },
    rename: (a, b) => { const s = ls(), v = s.getItem(a); if (v === null) throw new Error(`no ${a}`); s.setItem(b, v); s.removeItem(a); },
    remove: (n) => { ls().removeItem(n); },
  };
  // a spot deleted takes its photos with it (S1); the photos themselves are in IndexedDB
  return openSpotsWith(fs, KEY, Date.now, openPhotos());
}

/**
 * THE LAYERS SHEET'S CHOICE (owner 2026-10-22: the Spots switch, and every other, kept across a reload): one key of
 * its own beside the spots, never inside their file. A browser that refuses storage reads as nothing stored, and a
 * refused write throws for `@app/ui` `useLayerChoice` to shrug off — it is a preference, not a record.
 */
export function openLayerStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.layers.v1");
}

/** T51: the last fetched "Like your visit?" forecasts, one key of their own — a cache, never inside the spots. */
export function openLikeStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.likeVisit.v1");
}

/** (T211) The last captured DFO salmon feed (owner 2026-10-27: offline, the last feed captured) — a cache of its own. */
export function openSalmonFeedStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.dfoFeed.v1");
}

/**
 * (T315, T633 item 6 / T585 F14) THE GAUGES' KEPT WEEK IN A BROWSER: IndexedDB, as the photos are (a few MB of text
 * would crowd localStorage, which is why the web once kept the week for the session only and a page opened offline had
 * nothing where the phone had its week). One record of whole text, the same text a phone's file holds.
 *
 * IndexedDB is read later than a file, so the store has `load` (the kept text, asked once by `gaugeHistory`) and its
 * `read` answers nothing. A write goes out behind the call; one that fails is reported once (a cache: the next write
 * is the next one's to make). A browser with no IndexedDB at all (storage switched off) keeps the week for the session,
 * as before: the environment's, said by `load` and not reported.
 */
export function openGaugeHistoryStore(idb: IDBFactory | undefined = globalThis.indexedDB): HistoryText {
  const DB = "canifishthis.gaugeHistory", STORE = "kept", KEY = "week";
  const done = <T>(r: IDBRequest<T>): Promise<T> => new Promise((res, rej) => {
    r.onsuccess = () => res(r.result);
    r.onerror = () => rej(r.error ?? new Error("IndexedDB refused the request"));
  });
  let db: Promise<IDBDatabase> | null = null;
  const open = (): Promise<IDBDatabase> => (db ??= (() => {
    if (!idb) return Promise.reject(environmentError(new Error("This browser is not letting the app keep the gauges' week.")));
    const r = idb.open(DB, 1);
    r.onupgradeneeded = () => { if (!r.result.objectStoreNames.contains(STORE)) r.result.createObjectStore(STORE); };
    return done(r);
  })().catch((e) => { db = null; throw e; }));
  return {
    read: () => null,
    load: async () => {
      const text = await done((await open()).transaction(STORE, "readonly").objectStore(STORE).get(KEY) as IDBRequest<unknown>);
      return typeof text === "string" ? text : null;
    },
    write: (text) => {
      void open().then((d) => done(d.transaction(STORE, "readwrite").objectStore(STORE).put(text, KEY)))
        .catch((e: unknown) => reportOnce("gauge-history-write", e));
    },
  };
}

/** (T230) The last captured in-season feed + links, and the in-season ids this device has been shown. */
export function openInSeasonStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.inseasonFeed.v1");
}
export function openInSeasonSeenStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.inseasonSeen.v1");
}
/** (T700) this device's in-season notification history (`@app/core` `inboxText`). */
export function openInSeasonInboxStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.inseasonInbox.v1");
}


/** (T231) The update service's small state (`@app/data` `UpdateStore`): validators, times, the metered setting. */
export function openUpdateStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.updates.v1");
}

/** (T251) The Updates log's ring buffer (development and staging builds only), as whole text. */
export function openUpdateLogStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.updateLog.v1");
}

/** (T237) Problem reports waiting to be sent (offline), as whole text (a JSON list). */
export function openReportOutboxStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.reportOutbox.v1");
}

/** (T242) The "Send crash reports" switch ("on" | "off"; nothing stored = on), one key of its own. */
export function openCrashSettingStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.crashReports.v1");
}

/** (T242) The newest Android exit record already looked at (epoch ms as text), one key of its own. */
export function openCrashExitStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.crashExitSeen.v1");
}

/** (T242) A fatal error kept for the next start (one JSON payload, or empty), one key of its own. */
export function openCrashPendingStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.crashPending.v1");
}

/** (T352) The random install id crash and problem reports carry (16 hex characters, made once), one key of its own. */
export function openInstallIdStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.installId.v1");
}

/** (T236) The first-run notice's accepted version (`FIRST_RUN_VERSION`), one key of its own. */
export function openFirstRunStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.firstRun.v1");
}

/** (T248) The first-run walkthrough's seen version (`WALKTHROUGH_VERSION`), one key of its own. */
export function openWalkthroughStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.walkthrough.v1");
}

/** (T384) Whether the first open has asked for the reader's location ("asked"), one key of its own: asked once. */
export function openLocationAskStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.locationAsk.v1");
}

/** (T390) The once-a-day newer-app check (the web has no channel today; kept for the identical export surface). */
export function openAppUpdateStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.appUpdate.v1");
}

/** (T247) The recent searches (a JSON list of what the reader looked for), one key of its own; a cache, device-only. */
export function openRecentSearchStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.recentSearches.v1");
}

/** (T261) The species the Runs tab paints (a JSON list of names), kept between visits; device-only. */
export function openRunSpeciesStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.runSpecies.v1");
}

/** (T234) The notification settings, alert rules and background-check log (`@app/data` `NotifySettings`), as whole text. */
export function openNotifyStore(): { read(): string | null; write(text: string): void; keepAside(text: string): void } {
  const KEY = "canifishthis.notify.v1";
  // (T554 item 2) an unreadable value is copied to a key of its own before anything writes over it
  return { ...keyStore(KEY), keepAside: (t) => {
    const s = globalThis.localStorage;
    if (!s) throw new Error("This browser is not letting the app store anything.");
    s.setItem(`${KEY}.corrupt-${now()}`, t);
  } };
}

/** (T254) The Data setting for low-bandwidth mode ("auto" | "on" | "off"), one key of its own. */
export function openDataModeStore(): { read(): string | null; write(text: string): void } {
  return keyStore("canifishthis.lowData.v1");
}

function keyStore(LAYERS_KEY: string): { read(): string | null; write(text: string): void } {
  return {
    read: () => { try { return globalThis.localStorage?.getItem(LAYERS_KEY) ?? null; } catch { return null; } },
    write: (t) => {
      const s = globalThis.localStorage;
      if (!s) throw new Error("This browser is not letting the app store anything.");
      s.setItem(LAYERS_KEY, t);
    },
  };
}
