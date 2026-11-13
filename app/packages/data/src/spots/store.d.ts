/**
 * The shared declaration for `./store`.
 *
 * `store.native.ts` writes a `.spots` file; `store.web.ts` uses browser storage. Bundlers
 * pick by platform extension, TypeScript does not — so both are checked against one
 * signature. A store that exports something the other does not is how a feature ships on
 * one platform only.
 */
import type { SpotsStore } from "./core";
import type { HistoryText } from "../feed/history";

export declare function openSpots(): SpotsStore;

/** The Layers sheet's choice, as whole text under one name (`@app/ui` `LayerStore`). */
export declare function openLayerStore(): { read(): string | null; write(text: string): void };

/** T51: the last fetched "Like your visit?" forecasts, as whole text under one name (`./likeVisit` `LikeStore`). */
export declare function openLikeStore(): { read(): string | null; write(text: string): void };

/** (T211) The last captured DFO salmon feed, as whole text under one name (`../feed/salmon` `SalmonFeedStore`). */
export declare function openSalmonFeedStore(): { read(): string | null; write(text: string): void };
/**
 * (T315, T633 item 6) The gauges' kept week (`../feed/history` `gaugeHistory`), as whole text: a file on a phone (read
 * at once), IndexedDB in a browser (read later: `HistoryText.load`). Both keep it across a launch.
 */
export declare function openGaugeHistoryStore(): HistoryText;

/** (T230) The last captured in-season feed + links (`../feed/inseason`), as whole text under one name. */
export declare function openInSeasonStore(): { read(): string | null; write(text: string): void };
/** (T230) The in-season row ids this device has been shown in the notice (a JSON list). */
export declare function openInSeasonSeenStore(): { read(): string | null; write(text: string): void };
/** (T700) This device's in-season notification history (`@app/core` `inboxText`), as whole text under one name. */
export declare function openInSeasonInboxStore(): { read(): string | null; write(text: string): void };
/** (T231) The update service's small state (`@app/data` `UpdateStore`), as whole text under one name. */
export declare function openUpdateStore(): { read(): string | null; write(text: string): void };
/** (T251) The Updates log's ring buffer (development and staging builds only), as whole text. */
export declare function openUpdateLogStore(): { read(): string | null; write(text: string): void };
/** (T236) The first-run notice's accepted version (`@app/core` `FIRST_RUN_VERSION`), as whole text. */
export declare function openFirstRunStore(): { read(): string | null; write(text: string): void };
/** (T248) The first-run walkthrough's seen version (`@app/core` `WALKTHROUGH_VERSION`), as whole text. */
export declare function openWalkthroughStore(): { read(): string | null; write(text: string): void };
/** (T384) Whether the first open has asked for the reader's location ("asked"), as whole text. */
export declare function openLocationAskStore(): { read(): string | null; write(text: string): void };
/** (T390) The once-a-day newer-app check (`@app/core` `AppUpdateSaved`), a small file of its own. */
export declare function openAppUpdateStore(): { read(): string | null; write(text: string): void };
/** (T247) The recent searches, a JSON list as whole text; device-only. */
export declare function openRecentSearchStore(): { read(): string | null; write(text: string): void };
/** (T237) Problem reports waiting to be sent (offline), as whole text (a JSON list). */
export declare function openReportOutboxStore(): { read(): string | null; write(text: string): void };
/** (T242) The "Send crash reports" switch ("on" | "off"; nothing stored = on), as whole text. */
export declare function openCrashSettingStore(): { read(): string | null; write(text: string): void };
/** (T242) The newest Android exit record already looked at (epoch ms as text), as whole text. */
export declare function openCrashExitStore(): { read(): string | null; write(text: string): void };
/** (T242) A fatal error kept for the next start (one JSON payload, or empty), as whole text. */
export declare function openCrashPendingStore(): { read(): string | null; write(text: string): void };
/** (T352) The random install id crash and problem reports carry (16 hex characters, made once), as whole text. */
export declare function openInstallIdStore(): { read(): string | null; write(text: string): void };
/** (T254) The Data setting for low-bandwidth mode ("auto" | "on" | "off"), as whole text. */
export declare function openDataModeStore(): { read(): string | null; write(text: string): void };
/** (T261) The species the Runs tab paints (a JSON list of names), as whole text; device-only. */
export declare function openRunSpeciesStore(): { read(): string | null; write(text: string): void };
/** (T234) The notification settings, alert rules and background-check log, as whole text; device-only. */
export declare function openNotifyStore(): { read(): string | null; write(text: string): void; keepAside(text: string): void };
