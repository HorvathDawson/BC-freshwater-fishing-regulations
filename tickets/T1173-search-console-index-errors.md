# T1173 — Google Search Console: duplicate, redirect and redirected pages not indexed

Status: OPEN — future (owner 2026-11-26: "add as a ticket for future"); the first step is the owner's export

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1173 | Google Search Console: duplicate, redirect and redirected pages not indexed | OPEN — future (owner 2026-11-26) | — | owner: export the affected URL lists from Search Console; then a web lane: canonical tags, redirect rules, sitemap |

Source: Google Search Console emails of the night of 2026-11-25, three new reasons pages are not indexed:
"Duplicate without user-selected canonical", "Redirect error", "Page with redirect". The emails give no URLs.
Owner, 2026-11-26: "add as a ticket for future". Related: T1152 (what the site tells search engines).

## Work
1. Export the affected URL lists from Search Console (Pages report, each reason, Export). Only the owner can log in.
2. Canonical tags: every water page and part page names itself, in one form, as its canonical address.
3. The site's redirect rules: trailing slash, www against the bare domain, http against https. Each old form goes in
   one hop to the final address; no loops, no chains.
4. The sitemap lists only final addresses (no address that redirects).
5. Ask Search Console to validate the fix; record the counts before and after here.

## Acceptance
The three reasons' counts fall to zero (or each left address has a reason here) within one Search Console
validation; a test checks that every sitemap address answers 200 with itself as canonical. Severity: low (search
reach). Model: Sonnet. Size: S–M.

## Owner

- [x] 2026-11-26 — Google's three new index errors → "add as a ticket for future" (2026-11-26)
- [ ] 2026-11-26 — When this starts: export the three URL lists from Search Console and send them. Default: the ticket waits for them.
