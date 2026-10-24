/**
 * THE MENU (owner 2026-10-21; T48: the bottom bar's MORE tab — the top-left hamburger and its drawer are gone, moved,
 * never copied). (Owner 2026-10-22, refinement pass; T167 2026-10-23) The More tab itself shows every source's date and
 * freshness, the display options and Contact INLINE (more.ts `menuVM`); what opens as a page over it is Glossary,
 * Sources and credits (who publishes each source, and on what terms), About and Report a problem. Every word they draw
 * and every sentence they compose is here or in more.ts, where a test can pin the finished string (AGENTS 33); the
 * components draw these VMs and decide nothing.
 *
 * ONE FILE PER PAGE (T62): words.ts (MENU_WORDS, the app name), glossary.ts, about.ts, sources.ts (the list and its
 * credits), freshness.ts (each source's state, the lapse rule), contact.ts, report.ts; this barrel is `./menu`.
 *
 *   glossaryRows    the glossary's list, searched — the release's own terms (`answers.glossary`, the same terms the
 *                   regs page's `gl:` sheets open); a term's definition is `glossBlocks` (regs/sheets.ts), never a copy
 *   aboutVM         what the app is, the authority disclaimer, the official synopsis, the versions
 *   sourcesVM       every source the app reads, who publishes it and its licence, the edition / tile build in use —
 *                   references only; each source's date and freshness (`sourceFreshness`) are the More tab's
 *   contactVM       the ONE contact value (app config), or a placeholder until the owner supplies it
 *   diagnosticRows / reportMessage / mailtoUrl   a problem report: what the user wrote plus the diagnostics they can
 *                   SEE and switch off, composed for the platform's share sheet or an email — nothing is ever sent
 *                   without the user pressing send, and nothing is sent in the background
 */
export * from "./words";
export * from "./glossary";
export * from "./about";
export * from "./sources";
export * from "./freshness";
export * from "./contact";
export * from "./report";
