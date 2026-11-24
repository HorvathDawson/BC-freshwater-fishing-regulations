/**
 * (T1049) THE START PROBE: test-bed measurement only, beside the open probe (on while `globalThis.__openProbe` is set:
 * apps/mobile sets it from `EXPO_PUBLIC_PAN_PROBE=1`, the test bed's build); otherwise every call is a no-op.
 *
 * One `[start] {…}` line per event of a start link's landing (`adb logcat -s ReactNativeJS`): each jump the start asks
 * for, each scroll event, each content-size change, each change of the section tops, and when and why the start ends.
 * The test bed's `start-land` scenario reads them.
 */
import { now } from "@app/core";
import { openProbe } from "@app/data";

export function startLog(ev: string, data: Record<string, unknown> = {}): void {
  if (!openProbe.on()) return;
  console.log(`[start] ${JSON.stringify({ ev, t: now(), ...data })}`);
}
