# T197 — SEO outreach kit (drafts only, no sending)

Status: PARTIAL - committed 1c4dd582, 435ce2d8; first real-release build, v1 redirects and owner steps left (see Outcome).

Source: T188 plan, handoff/PLAN/seo/PLAN.md section 7. Owner rule: no spam bots, link schemes or auto-posting.
## Work
Honest copy and templates for BC angling clubs, forums, the Freshwater Fisheries Society, tackle shops (QR counter card)
and one r/bcfishing maker post; a club-list tracker. No scripts, no automated posting, no link schemes.
## Acceptance
The owner reads and sends by hand; every template says the app is a free non-profit and asks for feedback, not links.

Depends: T193 live. Size: S. Owner: yes (sends the messages). Default: drafts only.

## Outcome (committed 1c4dd582, 435ce2d8)
- `handoff/PLAN/seo/outreach/`: README (the rules), club email, FFSBC/fish-and-game email, forum post, one r/bcfishing maker post, tackle
  shop counter card (QR to the region page), and an empty `tracker.csv`. Nothing sent or scheduled; no scripts. `outreach.test.ts` holds
  every template to free + non-profit + feedback + not a legal source, and to never ask for a link, share, vote or review.
- Owner: fill the brackets and send each by hand once the pages are live.

## Staging 2026-10-28
Live on https://staging.canifishthis.ca with release r-46be3c135e68 (19,774 pages, budget clean; v1 redirects 19,147 water /
162 region / 22 home, each one 301, spot-checked). Lighthouse mobile: A11y 100, CLS 0, Performance 69-76 from Cloudflare-injected
bot/analytics script (zone setting, owner). Still PARTIAL: production deploy and Search Console are the owner's. Record, findings
and redo steps: handoff/PLAN/seo/staging.md; deploy scripts: deploy/staging/.
