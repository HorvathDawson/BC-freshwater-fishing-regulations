/**
 * (T603 item 5) ON AN ANDROID EMULATOR? Read from the build's own facts (`Build.FINGERPRINT` and `MODEL`, which React
 * Native hands over as `Platform.constants`), the markers every Android emulator image carries ("generic", "sdk_gphone",
 * "Android SDK built for", "emulator"). A real phone carries none. iOS: false (a simulator is built by its own
 * script, which tags the build itself).
 */
import { Platform } from "react-native";

const MARKERS = /generic|sdk_gphone|sdk_google|sdk built for|emulator|emu64|goldfish|ranchu/i;

export function onEmulator(): boolean {
  if (Platform.OS !== "android") return false;
  const c = Platform.constants as unknown as Record<string, unknown>;
  return [c.Fingerprint, c.Model].some((v) => typeof v === "string" && MARKERS.test(v));
}
