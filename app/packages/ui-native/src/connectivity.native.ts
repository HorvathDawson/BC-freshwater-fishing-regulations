/**
 * THE DEVICE'S WORD ON BEING ONLINE, on a phone: `@react-native-community/netinfo` (owner 2026-10-21 late: "Add
 * network-status detection on phones (netinfo)"), so satellite leaves the moment the phone goes offline and comes back
 * the moment it returns. How its state is read, and the fallback when its native module is not linked, are
 * `netInfoConnectivity.ts`'s; the one-tile probe stays (`useImageryReach`) for what NetInfo cannot see.
 *
 * Loaded lazily: NetInfo throws on import where its native module is missing (a development client built before it
 * was added), and that must cost satellite's instant switch, not the app.
 */
import type { Connectivity } from "@app/ui";
import { netInfoConnectivity, type NetInfoLike } from "./netInfoConnectivity";

declare const require: (id: string) => { default: NetInfoLike };

export const connectivity: Connectivity = netInfoConnectivity(() => require("@react-native-community/netinfo").default);
