/** The About page: what the app is, the authority disclaimer, the official synopsis, the versions. */
import { MENU_WORDS } from "./words";

export interface AboutInput {
  /** The app build (app config); null when the build states none. */
  appBuild: string | null;
  /** The data release id (the manifest's `data_release`); null on the development bundle. */
  release: string | null;
  /** The tile version the map draws (`<handles16>.<tilebuild12>`); null when none is known. */
  tiles: string | null;
  /** The printed edition the regulations were read from (the manifest's `sources.synopsis.title`). */
  edition: string | null;
}

export interface AboutVM {
  readonly what: string;
  readonly notOfficial: string;
  readonly official: { readonly label: string; readonly url: string };
  readonly versionsTitle: string;
  readonly versions: readonly { readonly label: string; readonly value: string }[];
}

export function aboutVM(a: AboutInput, synopsisUrl: string): AboutVM {
  const w = MENU_WORDS.about;
  return {
    what: w.what, notOfficial: w.notOfficial, official: { label: w.official, url: synopsisUrl },
    versionsTitle: w.versions,
    versions: [
      { label: w.app, value: a.appBuild ?? w.notStated },
      { label: w.release, value: a.release ?? w.devBundle },
      { label: w.tiles, value: a.tiles ?? w.notStated },
      { label: w.regulations, value: a.edition ?? w.notStated },
    ],
  };
}
