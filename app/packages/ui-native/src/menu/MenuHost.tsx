/**
 * THE MENU'S PAGES, mounted once by the Shell: the page opened from the More tab (`MoreScreen`, T48 — the hamburger's
 * drawer is gone) or from a regulations tab's report link — Glossary, Data sources & dates, About, Report a problem.
 * (Owner 2026-10-22) Display and Contact are no pages: the More tab draws them inline. Everything a page
 * says comes from core/menu.ts over facts handed in here — the release's manifest (its id, `sources`, the edition's
 * valid-until), the tile version the map draws, when the gauge feed was last fetched, the app's own config (build,
 * contact) and the recent error log. Nothing here is fetched for the menu except the glossary, read from the regs
 * store only while its page is open.
 *
 * Each open level is a back layer (G1): back closes a glossary term, then the page.
 */
import { useMemo } from "react";
import { Platform } from "react-native";
import { aboutVM, dayLabel, MENU_WORDS, SYNOPSIS_PDF, sourcesVM } from "@app/core";
import type { RegsLoader } from "@app/data/regs";
import { ERROR_LOG, routeWhere, useGlossary, type MenuControls, type Route } from "@app/ui";
import type { Palette } from "../theme";
import { GlossaryPage } from "./GlossaryPage";
import { AboutPage, SourcesPage } from "./InfoPages";
import { MenuPage } from "./MenuPage";
import { ReportPage } from "./ReportPage";

/** What the app itself knows about its build (apps/mobile config.ts). */
export interface MenuInfo {
  /** the app build, e.g. "1.2.0 (34)"; null when the build states none */
  readonly appBuild: string | null;
  /** THE ONE CONTACT VALUE: an email address or a web link; null until the owner supplies one (a placeholder shows) */
  readonly contact: string | null;
}

/** No build facts: the build states none and the owner has supplied no contact yet. */
export const NO_MENU_INFO: MenuInfo = Object.freeze({ appBuild: null, contact: null });

export function MenuHost({ menu, palette, loader, info = NO_MENU_INFO, tiles, tilesPaired = null, feedFetchedAt, route,
                           day, newRelease = null }: {
  menu: MenuControls; palette: Palette;
  /** the release (null: the dev bundle, which holds no regulations and no manifest) */
  loader: RegsLoader | null;
  info?: MenuInfo;
  /** the tile version the map draws, or null */
  tiles: string | null;
  /** the tiles and the data proven one pair (P11 `usePairing`'s `ok`): the map tiles' freshness */
  tilesPaired?: boolean | null;
  /** when the gauge feed was last fetched (its ISO instant), or null */
  feedFetchedAt: string | null;
  /** a newer data release the server names (the sync icon's re-read), or null: the regulations and map rows go stale */
  newRelease?: string | null;
  /** where the reader is now (the Shell's route) and the map's day (null = today) — a report is prefilled with them */
  route: Route;
  day: Date | null;
}) {
  const s = menu.state;
  const where = routeWhere(route, day ? dayLabel(day) : null);
  const glossary = useGlossary(loader, s.page === "glossary");
  const m = loader?.manifest ?? null;
  const term = s.page === "glossary" && s.term ? glossary.value?.terms.find((t) => t.id === s.term) ?? null : null;

  // the diagnostics as they stood when the report opened (the error log keeps moving underneath)
  const reportOpen = s.page === "report";
  const diagnostics = useMemo(() => ({
    appBuild: info.appBuild,
    // the OS and its version on a phone; on the web react-native-web's Version is a placeholder ("0.0.0"), so the
    // platform alone — the browser is not read (nothing the reader did not choose to share)
    platform: Platform.OS === "web" ? "web" : `${Platform.OS} ${Platform.Version}`,
    release: loader?.release ?? null,
    tiles,
    where: s.report ?? where,
    errors: ERROR_LOG.recent(),
  }), [reportOpen, s.report]);     // frozen at open, deliberately: a report states what was true when it was asked

  const page = (k: "sources" | "about" | "report", node: React.ReactNode) => (
    <MenuPage open={s.page === k} title={MENU_WORDS.items[k].label} onBack={menu.closePage} palette={palette}
              testID={`menu-page-${k}`}>
      {s.page === k ? node : null}
    </MenuPage>
  );

  return (
    <>
      <MenuPage open={s.page === "glossary"} title={term?.term ?? MENU_WORDS.items.glossary.label}
                onBack={term ? menu.closeTerm : menu.closePage} palette={palette} scroll={false}
                backLabel={term ? MENU_WORDS.items.glossary.label : undefined} testID="menu-page-glossary">
        {s.page === "glossary" && (
          <GlossaryPage glossary={glossary} term={s.term} onTerm={menu.openTerm} onCloseTerm={menu.closeTerm}
                        palette={palette} />
        )}
      </MenuPage>
      {page("sources", <SourcesPage palette={palette} vm={sourcesVM({
        sources: m?.sources ?? null, validUntil: m?.regulations.valid_until ?? null, tiles, tilesPaired, feedFetchedAt,
        newRelease, now: Date.now() })} />)}
      {page("about", <AboutPage palette={palette} vm={aboutVM({
        appBuild: info.appBuild, release: loader?.release ?? null, tiles,
        edition: m?.sources?.synopsis?.title ?? null }, SYNOPSIS_PDF)} />)}
      {page("report", <ReportPage palette={palette} contact={info.contact} diagnostics={diagnostics} />)}
    </>
  );
}
