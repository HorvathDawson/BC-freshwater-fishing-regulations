# T1011 — Website: big waters have no page (Columbia, Kootenay, Similkameen, Kettle, Peace rivers; Chilko Lake); the home page has no crawlable links

Status: PARTIAL — 54778147e: parts 1 and 2 built (lane L2, 2026-11-22). left (Claude): part 3 (rebuild index-requests-top-100.md from Search Console once it has weeks of data; move the Columbia, Kootenay, Similkameen, Kettle and Peace in) and the former name under the name in the app's pane (needs `alias` in the core region slices: a pipeline change).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1011 | Website: 156 waters partly outside B.C. get pages (`outside_bc` is a count); renamed waters' former names in title / description / JSON-LD / lists; static home links; top-100 indexing list | PARTIAL | 54778147e | left (Claude): part 3 (top-100 from Search Console) and the former name in the app pane (needs `alias` in core slices) |

Source: owner 2026-11-22 asked for the top 100 pages to submit for indexing, ranked by how much BC searches for fishing
each water (`handoff/PLAN/seo/index-requests-top-100.md`, written that day). The research found these two gaps.

## Done 2026-11-22 (lane L2, 54778147e)
- **(a) waters partly outside B.C.:** `item.outside_bc` is a COUNT of sections outside B.C., not a flag; `PAGE_ORDS_SQL`
  (`app/packages/data/src/regs/pageable.ts:15`) is now `i.outside_bc < i.sections` (`pageable.test.ts`, mutation-checked
  with `<=`; gates the gauge Share too, `waterHasPage`, T530). Bare build on r-741815c6090d: 19,610 -> 19,766 waters
  (+156), 20,338 -> 20,498 pages; no clash. Columbia, Kootenay, Similkameen, Kettle, Peace, Sumas, Flathead now have
  pages. The parts outside B.C. are left off (`waterModel`); NOT checked in a rendered water page (bare build).
- **(b) former names:** 9 waters carry a core `alias` "<name> (formerly)" (Chilko, Tatlayoko, Eagle, Stum, Anah, Alexis,
  Yohetta, Puntzi, Chilcotin lakes): title ("Tŝilhqox Biny (formerly Chilko Lake) fishing regulations"), meta
  description, JSON-LD `alternateName` and list lines say it. Additive only (owner rule: never rename or drop names
  without asking). Alces Lake's alias ("ALCES LAKE") is not a former name and is not printed.
- **2. home links:** built, then dropped 2026-11-23 (below).

## Work (left)
1. **Part 3:** once Search Console has a few weeks of impressions, rebuild `index-requests-top-100.md` from its
   Performance report (queries and pages), not Google Trends; move the Columbia, Kootenay, Similkameen, Kettle and
   Peace in.
2. **Former name under the name in the app's pane:** the core region slices the app reads on a tap carry no `alias`
   table (a read there would fetch the whole 1.5 MB core), so the slices need to carry it (pipeline change).

Acceptance (part 1): each listed water has a page in the sitemap, or this ticket says why it must not have one.

Risk: low (part 2), unknown until the cause is found (part 1). Size: S–M. Owner: none.

## Dropped 2026-11-23 (T1045, owner: "Drop the hidden row (Recommended)")
The home page's crawlable link row (`#site-links`: `withHomeLinks`, `homeLinkProblems`, and their tests) is REMOVED. It sat clipped under
the app's root and was `inert` with script: links meant only for crawlers, which Google's spam policies ("Hidden text and links",
developers.google.com/search/docs/essentials/spam-policies#hidden-text-and-links) name. The sitemap already lists the gauges, regions,
About and Privacy; the pages stay reachable that way.
