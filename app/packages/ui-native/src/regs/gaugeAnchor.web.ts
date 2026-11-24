/**
 * (T1045) A GAUGE LINK'S PLATFORM HALF (web): a real anchor (`href`: react-native-web's Text renders `<a>`), so a crawler and a
 * reader get the same link; a click follows it, so the in-app press is not wired (`onPress: undefined`).
 */
export const gaugeAnchor = (href: string): Record<string, unknown> => ({ href, hrefAttrs: { rel: "noopener" }, onPress: undefined });
