/** (T242) Android's ApplicationExitInfo for the last runs (modules/exit-info, Kotlin). `null` on the web and iOS. */
import { requireOptionalNativeModule } from "expo";
import type { ExitRecord, ExitSource } from "@app/ui";

const native = requireOptionalNativeModule<{ recent(since: number): Promise<ExitRecord[]> }>("ExitInfo");

export const exitSource: ExitSource | null = native ? { recent: (since) => native.recent(since) } : null;
