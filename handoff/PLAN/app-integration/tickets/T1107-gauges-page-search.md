# T1107 — The web gauges page gets a search: by place, river or water, giving gauges only (nearest first)

Status: PARTIAL — built in a draft PR (code 4816a87d4); left (Claude): the owner looks at T1107-shots/ (questions in the PR), then merge. Core's main-search index (`buildSearchIndex` / `queryIndex`) over a gauge-cut file
(`tools/seo/gaugeSearch.ts`); place -> nearest gauges with km, river -> its gauges then nearest others, station number or name -> that gauge; no
match said; a failed file or live index said and reported. Current data = core `donorState` on the live index (words: "Reporting now" / "Not
reporting today" / "Not a real-time gauge"). Conditions link = `routeSearch` of the Conditions tab. Pictures: `T1107-shots/`. Owner picks open: see the PR.

Source: owner 2026-11-25 ~00:20 (planning chat), verbatim: "in 0.9.1 we should make this page
(https://canifishthis.ca/gauges) have a search option that allows search to nearest place etc like the main search bar
it should be able to search by place or river etc to get gauge only" and "someone found the page by search so we want
it usable". The page is reached from web search engines, so a visitor may land on it first, with no idea of the app.

Owner addition 2026-11-25 (verbatim): "it should also indicate gauges without current data and ones with". The /gauges list itself
(static, build-time) did not say it; the search results do, from the live feed index.
Owner addition 2026-11-25, typed in the planning chat (verbatim): "it should also link to the condition map page on the webapp and say
something like to view the conditions visually on a map go here". Built as one line with a link to `/?browse=conditions` (the address the app
writes for the Conditions tab); the words are a proposal.

Owner answer 2026-11-25 on the pictures (verbatim): "should use mute color and text. also make the button for going to map look better and more asthetic. this page should still look professional".
Done in v2: the current-data words are the muted `.tag` text, no dot; the Conditions line is a `.card` with the site's primary `.app` button (the gauge
page's own "Open on the website" pattern); spacing, list borders, focus ring and hover from existing tokens. Pictures `T1107-shots/v2-*` (button option B: `v2-button-option-B-*`).

## Work
1. A search field at the top of `/gauges`, the same component and index as the main search bar (one search, not a
   second one): typing a place, a river, a lake or a station name or number.
2. Results are gauges only:
   - a river or water: the gauges on it (the frozen match file's gauges for that water), then the nearest others;
   - a place (town, park, landmark): the gauges nearest to it, with the distance, nearest first;
   - a station name or WSC number: that gauge.
   Each result says the gauge's name, its water, the distance where a place was searched, and opens the gauge's page.
3. No silent empty: no match says so and offers the nearest gauges to the centre of what was typed, or the full list.
   A failed index load is shown and reported (`no-silent-fallbacks`).
4. Landing from a search engine: the page says in one line what it is and links to the water's rules; title and
   description fit "river levels / flow near <place>" searches (with the SEO pages' generator, not by hand).
5. Tests: place → nearest gauges in distance order; river → its own gauges first; station number → that gauge; no
   water or place results leak into the list. A picture of the page with results, light and dark, to the owner.

## Acceptance
On production `/gauges`, typing a town gives the nearest gauges with distances, typing a river gives its gauges,
and every result opens a gauge. The tests fail without the change.

Severity: medium (a public entry page that is hard to use). Release: 0.9.1 (owner: "in 0.9.1"); web only, ships with
the 0.9.1 site deploy. If the 0.9.1 chat has no lane before its cut, it says so and this goes first in 0.9.2.
Model: Sonnet (UI on the existing search), Opus only if the place-to-gauge distance needs new data. Size: S–M.
Owner: yes (looks at the picture).
