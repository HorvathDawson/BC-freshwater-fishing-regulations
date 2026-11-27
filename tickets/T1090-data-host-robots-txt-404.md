# T1090 — `data.canifishthis.ca/robots.txt` answers 404 (Search Console)

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1090 | `data.canifishthis.ca/robots.txt` answers 404 (Search Console) | OPEN — 0.10.1 | | owner 2026-11-24 |

Source: owner 2026-11-24 (planning chat), pasted from Google Search Console: "https://data.canifishthis.ca/robots.txt
11/19/26, 12:11 PM — Not Fetched - Not found (404)". "another ticket too".

## Work
1. Decide what the data host should tell crawlers, and write it down. Note the trap: the web app loads its data
   from this host, so a `Disallow: /` here would stop Google's renderer from fetching the data and the pages would
   render empty (compare T1015, the renderer running the web app). The likely right answer: a `robots.txt` that
   allows fetching, and `X-Robots-Tag: noindex` on the data files so the JSON, tiles and PDFs themselves are not
   listed in search.
2. Serve it from the data worker / bucket on production and staging (staging stays `noindex` everywhere), and add
   it to the release smoke check so a deploy cannot drop it.
3. Check the other hosts the same way (`build.canifishthis.ca`, the feeds worker host, staging): each answers
   `robots.txt` with a deliberate 200.
4. After the production deploy (the owner's), the owner re-tests in Search Console.

## Acceptance
`robots.txt` is 200 on every host with the intended rules; a data file carries the intended header; the smoke check
fails without it; Search Console shows it fetched; the site's pages still render with data in the URL inspection.

Depends: none. Severity: low (a 404 here means "no restrictions" to Google; nothing is broken, but nothing is
deliberate either). Release: 0.9.2 (a worker / bucket deploy, production is the owner's). Model: Sonnet. Size: S.
Owner: yes (the production deploy and the Search Console re-test).
