/**
 * THE DEVICE'S WORD ON BEING ONLINE, on a phone: none. React Native has no built-in connectivity API, and the one
 * library that gives it (`@react-native-community/netinfo`) is not a dependency of this app — adding a native module
 * for one switch is the owner's call (deps.md). So a phone never says; `useImageryReach` probes one imagery tile
 * instead, on choosing satellite and then on a timer, which is how satellite leaves and comes back by itself here.
 */
import type { Connectivity } from "@app/ui";

export const connectivity: Connectivity = {
  watches: false,
  online: () => true,
  subscribe: () => () => {},
};
