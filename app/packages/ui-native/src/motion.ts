/**
 * THE NATIVE DRIVER, WHERE THERE IS ONE. iOS and Android run an `Animated` loop on the UI thread when asked
 * (`useNativeDriver: true`); react-native-web has no native animated module, and asking for it there logs
 * "useNativeDriver is not supported … Falling back to JS-based animation" on every spring. So the platform decides:
 * the driver on a phone, plain JS on the web (where the browser composites transforms anyway).
 */
import { Platform } from "react-native";

export const NATIVE_DRIVER: boolean = Platform.OS !== "web";
