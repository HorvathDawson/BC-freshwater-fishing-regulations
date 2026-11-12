/**
 * (T605 item 3) THE APP CAME BACK TO THE FRONT — the one place the shell hears it (react-native's AppState on a phone;
 * on the web react-native-web answers it from the page's visibility). Tells the shared clock (`appBecameActive`: every
 * "today" is re-read at once, so a phone opened the morning after is on the new day) and counts the returns for what
 * asks again on each one (the app-update check, T390).
 */
import { useEffect, useState } from "react";
import { AppState } from "react-native";
import { appBecameActive } from "@app/ui";

export function useResumed(): number {
  const [resumed, setResumed] = useState(0);
  useEffect(() => {
    let was = AppState.currentState;
    const sub = AppState.addEventListener("change", (st) => {
      if (st === "active" && was !== "active") { appBecameActive(); setResumed((n) => n + 1); }
      was = st;
    });
    return () => sub.remove();
  }, []);
  return resumed;
}
