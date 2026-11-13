/** THE MENU'S WORDS — every word its pages draw, in one table (AGENTS 33: a test pins the finished strings). */
import { IMAGERY_ATTRIBUTION } from "../imagery";

/** The pages that open OVER the More tab. Display and Contact are drawn on the tab itself (owner 2026-10-22). */
export type MenuPageKey = "offline" | "notifications" | "help" | "glossary" | "sources" | "about" | "report"
  | "changelog";

/** The app's name as the menu writes it (app.json's display name is the slug; the owner names the product). */
export const APP_NAME = "Can I Fish This?";

/**
 * (T167) WHERE THE PROVINCE POSTS ITS IN-SEASON CHANGES — the section of its fishing-regulations page that lists them
 * by region (anchor `#In-Season-changes`, read 2026-10-23). The app reads them through the feeds worker (T230) and shows
 * them on the waters they apply to; the More tab links here for the reader to check.
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
    // (T238) each region's size, kept on this device or not, save / remove, storage used
    offline: { label: "Offline maps", hint: "What is kept on this device, region by region" },
    // (T234) a phone only: the switches, the gauge alerts, the last background checks (`@app/core` alerts.ts words)
    notifications: { label: "Notifications", hint: "Gauge alerts and new regulations data" },
    // (T248) what the colours, lines, badges, bell and refresh button mean; re-opens the first-run walkthrough
    help: { label: "Help", hint: "What the colours, lines and symbols mean" },
    glossary: { label: "Glossary", hint: "What the regulations' words mean" },
    // (T235) "Sources and licences": every dataset, its licence and the words it asks for
    sources: { label: "Sources and licences", hint: "Who publishes the data, and under which licence" },
    about: { label: "About", hint: "What this app is, and what it is not" },
    report: { label: "Report a problem", hint: "Something wrong, missing or confusing" },
    // (T251) development and staging builds only: reached from About, never listed on the More tab
    // (T234 v1.1) every version's changes, newest first: reached from About, never listed on the More tab
    changelog: { label: "What's new", hint: "Every version of the app, newest first" },
  } satisfies Record<MenuPageKey, { label: string; hint: string }>,
  /**
   * (T703, owner 2026-11-13: "all the toggles ... should be in a settings menu in here") THE SETTINGS SCREEN, opened from
   * the first row of the More tab: every control the app has, under three headings. Not a `MenuPageKey`: More opens it
   * itself (it holds no state the Shell needs).
   */
  settings: {
    label: "Settings", hint: "Downloads, crash reports and display",
    sections: { data: "Data", privacy: "Privacy", display: "Display" },
  },
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
    /** (T288) the A–Z rail: the strip's name, and each letter's */
    rail: "Jump to a letter",
    clear: "Clear the search",
    jump: (letter: string) => `Jump to ${letter}`,
    /** a term's printed pages, read aloud ("p. 80" on the row) */
    pages: (cite: string) => `Synopsis ${cite}`,
    unavailable: "The glossary comes with the regulations data, which this build does not hold.",
    failed: (why: string) => `The glossary could not be read: ${why}`,
  },
  about: {
    /** (owner 2026-10-29) the link to the privacy policy page */
    privacy: "Privacy policy",
    what: `${APP_NAME} reads British Columbia's freshwater fishing regulations onto a map: tap a stream or a `
      + "lake and it says what the regulations allow there on the day you choose, alongside what the water is doing.",
    notOfficial: "This app is not the official regulations. It is built from the B.C. Freshwater Fishing Regulations "
      + "Synopsis and other public sources, and it can be wrong or out of date. The Synopsis, the Province's in-season "
      + "changes and federal notices are the law: check them before you fish. Nothing in this app is legal advice.",
    official: "Open the official Synopsis",
    versions: "Versions",
    app: "Version",
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
      /** (T423) the choice could not be kept: shown under the switch (and reported) */
      failed: (to: string) => `Could not save ${to}. The data source is unchanged.`,
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
      inseason: `${P.province}. Shown on the waters they apply to, as posted.`,
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
    /** (T569) the same entry on a water's Conditions and Map views */
    fromWater: "Report a problem with this water",
    /** (T237) About's link to the form */
    fromAbout: "Report a problem",
    /** (T237) the reason, the form's first choice (`REPORT_REASONS` order) */
    reason: "What kind of problem?",
    reasons: {
      rule: "Wrong rule", place: "Wrong place on the map", missing: "Missing water", name: "Wrong name",
      bug: "App bug", feature: "Feature request", other: "Something else",
    },
    /** (T407) the question and its example follow the reason: a bug and a request are not "what's wrong" with the data */
    whatFor: {
      bug: { what: "What happened?", hint: "For example: the map went blank after I closed the water page." },
      feature: { what: "What would you like?", hint: "For example: show sunrise and sunset on the water page." },
    } as Partial<Record<"rule" | "place" | "missing" | "name" | "bug" | "feature" | "other", { what: string; hint: string }>>,
    email: "Your email (optional)",
    emailHint: "Only if you would like a reply. It is never shown or shared.",
    emailBad: "That does not look like an email address.",
    /** the one button that sends it to us (when this build has a report address) */
    sendNow: "Send report",
    sending: "Sending…",
    sent: "Thank you — your report was sent.",
    queued: "You are offline. Your report is saved on this device and will be sent when you are back online.",
    queuedRetry: "We could not reach our server. Your report is saved on this device and will be sent automatically.",
    refused: (why: string) => `The report was not accepted: ${why}`,
    another: "Report something else",
    /** the privacy line under the form */
    privacy: "We use your report only to fix the app's data. It holds what you wrote and the details you left switched on; "
      + "your email, if you give one, is used only to reply — and only if you tick ‘You may contact me’.",
    /** (owner 2026-10-29) shown only once an email is given; off by default */
    contact: "You may contact me about this report",
    orEmail: "Or send it yourself:",
    /** "Where you were": the map's colouring when the report was asked */
    envLine: (env: string) => `Data: ${env}`,
    modes: { standard: "Trout & more", salmon: "Salmon", runs: "Run timing" },
    /** the diagnostics' switches, in the order the form lists them */
    diag: {
      app: "App version", platform: "Platform", release: "Data release", tiles: "Map tiles",
      where: "Where you were", map: "Map view", install: "Install id", errors: "Recent errors",
    },
    /** (T352) "Map view", one line per fact: what the map showed, where it looked (not where you were) */
    map: {
      layer: (l: string) => `Layer: ${l}`,
      horizon: (h: number | "change") => `Forecast: ${h === 0 ? "now" : h === "change" ? "expected change" : `+${h} days`}`,
      zoom: (z: number) => `Zoom: ${z}`,
      centre: (lat: number, lon: number) => `Map centre: ${lat}, ${lon}`,
    },
    /** (T352) the random id made once on this device; the same one goes with a crash report */
    installLine: (id: string) => `${id} (random, made on this device)`,
    /** "Where you were", one line per fact */
    where: {
      screen: (s: string) => `Screen: ${s}`,
      tab: (t: string) => `Tab: ${t}`,
      water: (w: string) => `Water: ${w}`,
      part: (p: string) => `Part: ${p}`,
      /** (T352) the tapped section's handle, meaningful only beside the data release */
      section: (n: number) => `Section: ${n}`,
      mode: (m: string) => `Map colours: ${m}`,
      day: (d: string) => `Day: ${d}`,
      /** (T569) a report asked from a water's Conditions: the gauge charted, the quantity and span picked, the feed's version */
      station: (s: string) => `Gauge: ${s}`,
      quantity: (q: string) => `Quantity: ${q}`,
      span: (s: string) => `Chart span: ${s}`,
      feed: (f: string) => `Gauge feed: ${f}`,
      /** the screens a water's views name (the Regulations one is `regs`) */
      conditions: "water · conditions",
      map: "water · map",
      /** the tab a report from these views names */
      tabs: { conditions: "Conditions", map: "Map" },
      /** the screen a regulations tab's report names */
      regs: "water · regulations",
    },
  },
} as const;

/**
 * (T242) More → Data, "Send crash reports": a switch, on by default. The hint says what goes (the privacy page says it in
 * full, and the code in `ui/crashReport.ts` is what it describes): the error and where in the code, the screen and (T352,
 * owner 2026-11-03) the open water's id, and a random id made on this device — never your location, spots or photos.
 */
export const CRASH_WORDS = {
  label: "Send crash reports",
  on: "On",
  off: "Off",
  hint: "When the app hits an error it sends the error, the app version, the kind of screen, the id of the water you had open "
    + "and a random id made on this device to our own server. Never your location, spots or photos. Nothing else.",
} as const;

/**
 * (T476) THE DEVELOPER CARD'S WORDS (development and staging builds only; the map credits' long press): the card, its four
 * sections and the Build info rows. The Updates log's and the data source's own words stay in UPDATE_LOG_WORDS / about.env.
 */
export const DEV_WORDS = {
  title: "Developer (test build)",
  name: "Developer",
  close: "Close",
  closeLabel: "Close the developer card",
  sections: { lab: "Map lab", source: "Data source", updates: "Updates log", build: "Build info" },
  noUpdatesLog: "This build keeps no Updates log.",
  build: { platform: "Platform", release: "Data release", env: "Environment", origin: "Origin", channel: "Channel" },
} as const;
