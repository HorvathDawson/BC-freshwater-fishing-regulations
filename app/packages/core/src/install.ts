/**
 * START-UP AND FIRST-RUN WORDS (P11): what the app says while it reads the manifest, installs a release (native, first
 * run) and decodes paint. Composed here so a test pins the finished string (AGENTS 33) and no screen types its own.
 */

export const INSTALL_WORDS = {
  failed: "The map data could not be loaded.",
  firstRunOffline: "The first start needs a connection: the map and regulations download once, then work offline.",
  wifi: "Wi-Fi recommended.",
} as const;

const STAGES: Readonly<Record<string, string>> = {
  manifest: "Checking for the latest regulations…",
  install: "Downloading the map and regulations for offline use",
  paint: "Opening the map…",
  fonts: "Loading type…",
  // the dev bundle path (until P20)
  bundle: "Opening the map data…",
};

/** The line for a start-up stage; an unknown stage says nothing rather than something made up. */
export function startStage(stage: string): string {
  return STAGES[stage] ?? "";
}

/** Megabytes to one decimal, thousands grouped — by hand: an engine's Intl is not something core may count on. */
const mb = (bytes: number) => {
  const [whole, tenth] = (bytes / 1_000_000).toFixed(1).split(".");
  return `${whole!.replace(/\B(?=(\d{3})+(?!\d))/g, ",")}.${tenth}`;
};

/** "12.5 of 1,461.2 MB downloaded"; nothing when nothing is fetched (an update that only copies held files). */
export function installLine(p: { bytes: number; of: number }): string {
  return p.of > 0 ? `${mb(p.bytes)} of ${mb(p.of)} MB downloaded` : "";
}
