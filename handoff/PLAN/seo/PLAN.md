# SEO plan — get found by anglers (T188 plan; build is T192-T198)

Domain (owner 2026-10-24): **https://canifishthis.ca** is canonical; canifishthis.com redirects to it. Water pages
show the FULL rules in plain words (decisions.md, "water pages").

## Hard rules
- No spam bots, link schemes, purchased links, comment/forum auto-posting, fake reviews or doorway pages. Search engines
  penalise them and they would put the site at risk. Automation is for OUR pages and for monitoring only.
- The release bundle is the only source for page content. A page never says anything the app would not.
- Every page carries "not a legal document — check the synopsis" and the book page cited.

## 1. Per-water static pages
**Why:** the web app is a client-rendered map; crawlers read it poorly. One pre-rendered, crawlable page per water
answers "can I fish the Vedder River today".

**URLs** (clean, lowercase, stable, no query strings):
- `/water/<slug>` for each named water, e.g. `/water/vedder-river`. Slug = ASCII-folded name; collisions get the region
  suffix (`/water/mill-creek-r3`); a slug is assigned once and kept in a committed slug map so URLs never change between
  releases (a renamed water keeps its old slug; a removed one 301s to its region page, never a soft 404).
- `/region/<n>` (the 8 regions) and `/region/<n>/open-this-weekend` hub pages that link to every water in the region
  (internal links are how crawlers reach thousands of pages; each water page links back to its region and neighbours).
- `/` stays the app; also `/about`, `/sitemap.xml`, `/robots.txt`.
- Unit: one page per named water (the unit the app's panel uses). Parts with distinct rules are sections of that page,
  not separate URLs (avoids thin near-duplicates). Waters with no rules get no page.

**Page content** (plain HTML, readable without JS):
1. H1 water name, region/management unit; a one-sentence answer ("Open to fishing today with limits" / "Closed: ..."),
   dated "as of <release date>".
2. The full rules for that water in plain words, by part, in the app presenter's wording (species, season/when, quota,
   size, gear, conduct, closures, licensing note), with the synopsis book page cited per rule.
3. Today's line: computed at build for the build date and stated with its date; an optional tiny script refreshes it
   on the client (the static text is already correct for the release day).
4. Nearby gauges (name, link into the app), neighbouring / tributary / parent waters as links.
5. Prominent "Open in the app" link at this water; the "not a legal document" line; link to the official synopsis.
No maps, forecasts or interactive features on the page.

**Built from the bundle:** a Node build step (`app/tools/build-seo.mjs`, no new runtime deps) reads the release bundle
(the files the app loads; never data/curated or pipeline files) and runs the app's own presenter/decided code from
`app/packages/core`, so wording and open/closed logic are shared and page and app cannot disagree. It writes
`dist-seo/` HTML, `sitemap.xml`, `robots.txt`, `slug-map.json`. A test asserts for every water that the page's verdict and
rule list equal the presenter output on the same bundle (mutation-pinned) and that nothing is read outside the bundle.
The step runs in the release/publish job after the bundle is cut, so pages refresh with every release; the release diff
also feeds the "changed this year" page (section 5).

## 2. Sitemap, canonicals, redirect
- `sitemap.xml` (index + chunks of <= 50,000 URLs): every water, region, hub and static page; `<lastmod>` changes only
  when the page's content hash changed (so crawlers trust it). Listed in `robots.txt`
  (`Sitemap: https://canifishthis.ca/sitemap.xml`); robots allows all except data/API paths.
- Canonical: every page has a self-referential `<link rel="canonical">` to the https .ca URL (one trailing-slash form);
  the app shell at `/` canonicals to `/`. Title "<Water> fishing regulations (BC) — Can I Fish This?" (<= 60 chars),
  a unique meta description (<= 155 chars) built from the verdict and top rule, Open Graph / Twitter card with a
  generated share image (T195).
- Redirect: canifishthis.com and www.* on both -> **301** to the same path on https://canifishthis.ca (host-level, path
  preserved; HSTS on .ca). Done at the registrar/DNS or CDN edge (owner access needed, T198).
- Removed slugs 301; unknown paths get a real 404 with links to the regions.

## 3. Structured data (JSON-LD, generated; must match visible text)
- `BreadcrumbList` (Home > Region > Water) on every water page.
- `WebPage` with `dateModified` = release date, `isPartOf` the `WebSite` (declared on `/`).
- The water as a `Place` (name, `containedInPlace` the region, `geo` centroid), typed `RiverBodyOfWater` /
  `LakeBodyOfWater` only when the type is certain.
- `FAQPage` only where a real question is answered in visible text ("Can I fish X today?", "What is the trout limit on
  X?"). Google limits FAQ rich results, so this is low-value insurance; drop it if it ever mismatches the page.
- `Organization` on About (name, url, contact).
- A build-time check: JSON parses, required fields present, every JSON-LD fact appears in the HTML; spot-check with
  Google's Rich Results Test and the schema.org validator.

## 4. Search Console and Bing setup (the OWNER does these)
1. After the domain is live on hosting (T86): in Google Search Console add a **Domain property** `canifishthis.ca`
   (DNS TXT verification); add `canifishthis.com` as a second Domain property so the redirect is visible.
2. Bing Webmaster Tools: sign in and **Import from Google Search Console** (or add the site with DNS/meta verification).
3. Submit `https://canifishthis.ca/sitemap.xml` in both; confirm "Success" and that the discovered-URL count matches the
   build's page count.
4. URL Inspection on `/`, one water page and one region hub: "URL is on Google", canonical = the .ca URL, mobile
   usability clean. Request indexing for those three only; do not mass-request.
5. Optional (T198): IndexNow pings for OUR changed URLs after each release. It is an official protocol, not a link scheme.
6. Add the owner's email as a notification recipient; share read access with whoever reads the numbers.
Verification tokens are pasted to Claude by the owner; T198 wires them in.

## 5. Content that earns links (all generated from the release)
- Region "what's open this weekend", season-start calendar by species and region, "closures and rule changes this year"
  (release diff). Dated, accurate, shareable; each links to the water pages it mentions. These are what clubs and forums
  link to; nothing is bought or seeded.
- About page (T193): who made it (free, non-profit), data sources (provincial synopsis, ECCC/WSC gauges, DFO, PSF with
  their terms respected), the not-a-legal-document line, a contact address.

## 6. Performance targets (a static page must beat the app)
Lighthouse CI, mobile profile, 4G throttled, against built pages:
- LCP <= 1.8 s, CLS <= 0.05, INP <= 200 ms (Core Web Vitals "good" at p75); Lighthouse mobile Performance >= 95,
  SEO = 100, Accessibility >= 95.
- HTML <= 30 KB gzip per water page; one shared CSS (<= 10 KB gzip, critical part inlined), no blocking JS, system font
  stack; share image off the critical path.
- CDN-cached: hashed assets cache long, HTML short with revalidation; HTTP/2+, Brotli, HTTPS only.
- The release step fails when a page, the CSS or the p95 page size exceeds the target (beside T129's WEB_BUDGET, T196).

## 7. Legitimate outreach (humans, by hand, honest)
One honest message each, written by a person, saying it is a free non-profit app they made and asking for feedback, not
links. No scripts, no repeated posting, no sockpuppets, no incentivised votes.
- **BC angling clubs and conservation groups** (fly-fishing federation chapters, regional rod-and-gun and angling clubs,
  stewardship societies): offer the app and a short blurb; clubs may link from their site or newsletter if it helps
  their members. Keep a tracker of contact and date (T197 provides the template).
- **Forums and Facebook groups** about BC fishing: introduce once, in the place where self-promotion is allowed, after
  reading the group's rules; answer questions afterwards.
- **Freshwater Fisheries Society of BC:** ask whether they would mention or list a free regulations companion, stating
  that it cites the provincial synopsis and is not a legal source. Same for fish-and-game associations.
- **Local tackle shops and guides:** a counter card with a QR code and a short note; shops answer "is X open?" all day.
  Give them their region's page URL to put on their own site.
- **Reddit r/bcfishing, done honestly:** one post as the maker ("I built a free app that tells you whether a water is
  open; feedback welcome"), follow the sub's self-promotion rules, be transparent, take criticism, do not cross-post
  across many subs, never ask for upvotes. Take part in the sub genuinely afterwards.
- App-store listings (T86/T88): accurate keywords, the .ca URL as the developer site.
- Corrections: anyone who reports a wrong rule gets a reply and a fix; that is the best word of mouth.

## 8. What to measure
- Search Console: impressions, clicks, CTR, average position by page type (water / region / hub) and top queries; pages
  indexed vs submitted; "Crawled - not indexed" and duplicate-canonical counts; the Core Web Vitals report.
- Bing Webmaster: the same, plus crawl errors.
- Cookie-less analytics (choice made in T198): organic sessions, landing pages, "Open in the app" click-through from
  water pages, app opens from search.
- Build gates: page count == sitemap count == bundle waters with rules; Lighthouse numbers; JSON-LD valid.
- Quality: does the page answer each of the top 20 queries; wrong-rule reports per month.
- Review monthly for six months. Guidance, not guarantees: >= 90% of submitted URLs indexed within 8 weeks; top 3 for
  "<water> fishing regulations" on the most-searched 20 waters within 6 months; organic sessions rising month on month.

## 9. Tickets
T192 slug map and page model from the bundle; T193 static water pages and About; T194 sitemap, robots, canonicals,
structured data and gates; T195 region hubs, weekend/changes pages, share images; T196 performance budget and
Lighthouse CI; T197 outreach kit (drafts only); T198 .com -> .ca redirect, HSTS, Search Console/Bing wiring and
measurement. Order: T192 -> T193 -> T194 (T196 alongside) -> T195; T198 with T86 hosting; T197 once pages are live.
