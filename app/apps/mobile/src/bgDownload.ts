/**
 * (T298) Android's DownloadManager (modules/bg-download, Kotlin): the first install's big files download in the
 * system's process — on with the app closed, with the system's progress notification. `null` on the web, on iOS and in
 * a development client built before the module (the install then downloads in the app, as before).
 */
import { requireOptionalNativeModule } from "expo";
import type { BackgroundNative, BgJob } from "@app/data/release";

const native = requireOptionalNativeModule<{
  jobs(): Promise<BgJob[]>;
  enqueue(url: string, name: string, title: string, text: string, metered: boolean): Promise<number>;
  remove(id: number): Promise<void>;
  claim(id: number, toUri: string): Promise<void>;
  sha256(uri: string): Promise<string>;
}>("BgDownload");

export const backgroundDownloads: BackgroundNative | null = native ? {
  jobs: () => native.jobs(),
  enqueue: (url, name, title, text, metered) => native.enqueue(url, name, title, text, metered),
  remove: (id) => native.remove(id),
  claim: (id, to) => native.claim(id, to),
  sha256: (uri) => native.sha256(uri),
} : null;
