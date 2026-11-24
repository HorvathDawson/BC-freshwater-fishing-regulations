import type { Manifest } from "@app/data";
import type { ReplacedState } from "@app/ui";

/**
 * (T1016) Nothing on a phone (the web twin installs the triage, replaced.web.ts): the release is installed whole on the
 * device and every read is from its disk (an update is staged beside it and applied whole), and the code is in the
 * binary, so neither a pruned release's 404 nor a missing chunk can meet the reader here.
 */
export function installReplaced(_o: { state: ReplacedState; manifestUrl: string; releaseBase: string;
                                      opened: () => readonly Manifest[]; fetch: typeof fetch }): () => void { return () => {}; }

/** Never called on a phone (no code notice is raised there): a call is a defect, thrown to be shown and reported. */
export function reloadPage(): void { throw new Error("reloadPage: a phone has no page to reload"); }

/** Never called on a phone (no verdict is raised there: no triage): a call is a defect, thrown to be shown and reported. */
export function autoReload(): string | null { throw new Error("autoReload: a phone has no page to reload"); }
