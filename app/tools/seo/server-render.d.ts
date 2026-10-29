/**
 * (T269) The two server-render entry points pane.tsx uses, typed here: the workspace carries react-dom (react-native-web's
 * renderer) and react-native-web without their type packages, and these are all the build reads of them.
 */
declare module "react-dom/server" {
  import type { ReactNode } from "react";
  export function renderToString(element: ReactNode): string;
  export function renderToStaticMarkup(element: ReactNode): string;
}
/** react-native-web's record of the pictures already loaded (an Image is drawn by a server render only when in it). */
declare module "react-native-web/dist/modules/ImageLoader" {
  export const ImageUriCache: { add(uri: string): void; has(uri: string): boolean };
}
