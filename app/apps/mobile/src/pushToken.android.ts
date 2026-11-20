/**
 * (T739) Android: NO PUSH. Owner 2026-11-16 12:08, asked about Firebase for Android push: "we dont want this right now".
 * No token is asked, no push task is registered, nothing is sent to our worker (which refuses `platform: "android"`),
 * and the build needs no `google-services.json`; the phone checks on its WorkManager schedule exactly as before.
 * This half exists only because a platform variant comes as a whole ios + android set (`tools/check-platform.mjs`).
 */
import type { PushSeam } from "@app/data";

export const pushSeam: PushSeam | null = null;
