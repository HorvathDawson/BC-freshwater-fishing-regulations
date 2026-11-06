import { Platform } from "react-native";

/** (T436) the phone's platform line: its OS and version ("ios 18.1", "android 34"); the web twin is platformLine.web.ts */
export function platformLine(): string {
  return `${Platform.OS} ${Platform.Version}`;
}
