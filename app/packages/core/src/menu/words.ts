/** THE MENU'S WORDS — every word its pages draw, in one table (AGENTS 33: a test pins the finished strings). */
import { IMAGERY_ATTRIBUTION } from "../imagery";

/** The pages that open OVER the More tab. Display and Contact are drawn on the tab itself (owner 2026-10-22). */
export type MenuPageKey = "glossary" | "sources" | "about" | "report";

/** The app's name as the menu writes it (app.json's display name is the slug; the owner names the product). */
export const APP_NAME = "Can I Fish This?";

/**
 * (T167) WHERE THE PROVINCE POSTS ITS IN-SEASON CHANGES — the section of its fishing-regulations page that lists them
 * by region (anchor `#In-Season-changes`, read 2026-10-23). The app does not read them; the More tab links here.
 */
export const INSEASON_URL =
  "https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/fishing/fishing-regulations#In-Season-changes";

/**
 * (T180) WHO PUBLISHES WHAT, named once. The Sources and credits page (`MENU_WORDS.sources.from`) and the map's own
 * credit lines (`MAP_CREDITS`, sources.ts) both read these, so a publisher's name cannot drift between the two.
 */
export const PUBLISHERS = {
  province: "Province of British Columbia",
  eccc: "Environment and Climate Change Canada",
  rfc: "BC River Forecast Centre",
  osm: "© OpenStreetMap contributors",
  protomaps: "© Protomaps",
  openMeteo: "Open-Meteo.com",
  ccBy: "CC BY 4.0",
} as const;
const P = PUBLISHERS;

export const MENU_WORDS = {
  /** The More tab's name and its page's title (T48: the menu lives in the bottom bar). */
  title: "More",
  back: "Back",
  items: {
    glossary: { label: "Glossary", hint: "What the regulations' words mean" },
    // (T235) "Sources and licences": every dataset, its licence and the words it asks for
    sources: { label: "Sources and licences", hint: "Who publishes the data, and under which licence" },
    about: { label: "About", hint: "What this app is, and what it is not" },
    report: { label: "Report a problem", hint: "Something wrong, missing or confusing" },
  } satisfies Record<MenuPageKey, { label: string; hint: string }>,
  /**
   * THE APP'S COLOURS (owner 2026-10-21 late: the palette left the Layers menu for the hamburger) — a whole-app setting,
   * not a map layer: it repaints every screen and every map.
   */
  display: {
    title: "Display",
    palette: "Colours",
    options: [
      { k: "light", t: "Light" },
      { k: "dark", t: "Dark" },
      { k: "cvd", t: "Colour-blind" },
    ],
    cvd: "The colour-blind palette changes the hues and nothing else. It has a light ground only.",
  },
  glossary: {
    search: "Search the glossary",
    loading: "Loading the glossary",
    none: (q: string) => `No term matches “${q}”.`,
    unavailable: "The glossary comes with the regulations data, which this build does not hold.",
    failed: (why: string) => `The glossary could not be read: ${why}`,
  },
  about: {
    what: `${APP_NAME} reads British Columbia's freshwater fishing regulations onto a map: tap a stream or a `
      + "lake and it says what the regulations allow there on the day you choose, alongside what the water is doing.",
    notOfficial: "This app is not the official regulations. It is built from the B.C. Freshwater Fishing Regulations "
      + "Synopsis and other public sources, and it can be wrong or out of date. The Synopsis, the Province's in-season "
      + "changes and federal notices are the law: check them before you fish. Nothing in this app is legal advice.",
    official: "Open the official Synopsis",
    versions: "Versions",
    app: "App build",
    release: "Data release",
    tiles: "Map tiles",
    regulations: "Regulations",
    devBundle: "Development bundle",
    notStated: "Not stated",
    /** (T232) the data source, said only in a development or staging build */
    env: {
      label: "Data source",
      names: { local: "Local", staging: "Staging", production: "Production" },
      choose: "Read data from (development builds only)",
      pending: (to: string) => `${to} is used from the next start. Close and reopen the app.`,
      overridden: (built: string) => `Chosen here; this build was made for ${built}.`,
    },
  },
  /**
   * (T167, owner 2026-10-23: "separate out references to a separate thing") THE SOURCES — one name each, the same on
   * the More tab's freshness list and on the Sources and credits page (the owner's own names) — and who publishes
   * each, on what terms. Dates and freshness are the More tab's alone (more.ts `dataVM`); this page carries none.
   */
  sources: {
    intro: "Who publishes the data this app reads, and on what terms.",
    names: {
      synopsis: "Synopsis", inseason: "In-season changes", dfo: "DFO salmon", fwa: "Freshwater Atlas",
      tiles: "Map tiles", basemap: "Basemap", hydat: "HYDAT", feed: "Live gauge readings",
      forecast: "River forecasts", weather: "Weather", imagery: "Satellite imagery",
    },
    from: {
      synopsis: `${P.province}. The printed regulations this app reads.`,
      inseason: `${P.province}. Not read by this app.`,
      dfo: "Fisheries and Oceans Canada. The federal salmon limits and closures.",
      fwa: `${P.province}. Contains information licensed under the Open Government Licence – `
        + "British Columbia.",
      tiles: "Drawn for this app from the Freshwater Atlas and the regulations.",
      basemap: `${P.osm}. ${P.protomaps}.`,
      // (owner 2026-10-23, decisions.md "data permissions") ECCC's hydrometric data — HYDAT and the real-time
      // readings — are under the Open Government Licence – Canada; the BC River Forecast Centre's forecasts (CLEVER,
      // COFFEE, ELF) are used with its permission and credited by name
      hydat: `${P.eccc}. Historical hydrometric data, under the Open Government Licence – `
        + "Canada.",
      feed: `${P.eccc}. Real-time hydrometric data, under the Open Government Licence – `
        + "Canada.",
      forecast: `${P.rfc}. The CLEVER, COFFEE and ELF river forecasts, used with permission.`,
      weather: `Weather data by ${P.openMeteo}, ${P.ccBy}.`,
      imagery: `${IMAGERY_ATTRIBUTION}. Shown only while online, never stored.`,
    },
    /** the map tiles' build, exact — here and nowhere else (the More tab gives the tiles' state, never the hash) */
    version: (v: string) => `Version ${v}`,
  },
  contact: {
    title: "Contact",
    intro: "Questions, corrections and ideas are welcome.",
    placeholder: "Contact details are coming soon. Until then, use Report a problem to share a note from your device.",
    email: "Email",
    link: "Website",
  },
  report: {
    intro: "Tell us what is wrong. Nothing leaves this device until you send it yourself, and only what you can see below.",
    what: "What's wrong?",
    whatHint: "For example: the closure dates on this river do not match the Synopsis.",
    diagnostics: "Include with your report",
    diagnosticsHint: "Switch off anything you would rather not share.",
    send: "Send by email",
    share: "Share…",
    noShare: "Sharing is not available here. Copy the report below and send it to us.",
    empty: "Write a few words about the problem first.",
    preview: "This is what will be sent",
    subject: `${APP_NAME} problem report`,
    subjectAt: (where: string) => `${APP_NAME} problem report: ${where}`,
    fromRegs: "Report a problem with this page",
    /** the diagnostics' switches, in the order the form lists them */
    diag: {
      app: "App build", platform: "Platform", release: "Data release", tiles: "Map tiles",
      where: "Where you were", errors: "Recent errors",
    },
    /** "Where you were", one line per fact */
    where: {
      screen: (s: string) => `Screen: ${s}`,
      tab: (t: string) => `Tab: ${t}`,
      water: (w: string) => `Water: ${w}`,
      part: (p: string) => `Part: ${p}`,
      section: (n: number) => `Section: ${n}`,
      day: (d: string) => `Day: ${d}`,
      /** the screen a regulations tab's report names */
      regs: "water · regulations",
    },
  },
} as const;
