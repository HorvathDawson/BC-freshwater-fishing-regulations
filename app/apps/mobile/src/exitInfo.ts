/**
 * (T242) Why the last runs ended (modules/exit-info): Android's ApplicationExitInfo (Kotlin); (T362) on iOS the app's own
 * record written at the crash (a fatal signal or an uncaught exception, Swift). `null` on the web.
 */
import { requireOptionalNativeModule } from "expo";
import type { ExitRecord, ExitSource } from "@app/ui";

const native = requireOptionalNativeModule<{ recent(since: number): Promise<ExitRecord[]> }>("ExitInfo");

export const exitSource: ExitSource | null = native ? { recent: (since) => native.recent(since) } : null;
