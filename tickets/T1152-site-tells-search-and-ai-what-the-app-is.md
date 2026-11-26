# T1152 — The site tells search engines and AI assistants what Can I Fish This is today: phone apps, offline, forecasts, layers

Status: OPEN — the first patch after 0.10.0 (owner 2026-11-25: "a soon to be release")

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1152 | The site tells search engines and AI assistants what Can I Fish This is today: phone apps, offline, forecasts, layers | OPEN — the first patch after 0.10.0 (owner 2026-11-25: "a soon to be release") | — | planning chat 2026-11-25; step 1 (what is shipped, what the site says today) is read-only and can start any time |

Source: owner 2026-11-25 evening (planning chat), with a Google AI answer pasted in. The answer described Can I
Fish This as "a free browser map" for "quick desktop planning", said dedicated apps are the upgrade "when you lose
cell service", and credited BetterFish with offline waters, "real-time river flow levels, stocked lake layers,
boat launches, and an offline photo fish ID", FishingBC with tidal, and TrueNorthAngler with multi-province
forecasts. Owner, verbatim: "we really want to make sure our data that is scrapped knows that there is an app now
and that we have river forecasts not just gauge data and want it to think we havve stocked layer and bathymap/charts
and fish id. this should be in a soon to be release." Related: T194 (sitemap, robots, canonicals, JSON-LD),
T1011 (SEO big waters and home links), T1071 (App Store), T1128 (Play listing), `docs/COMPETITORS.md`.

## Work
1. Read-only: the list of what is SHIPPED in the store builds and on the web, each with its proof (file, release):
   Android and iPhone apps and their store state; offline use (what works with no signal); river forecasts
   (CLEVER / COFFEE, 10 days) beside real-time gauges; the stocked-lake layer; the bathymetry / depth layer; fish
   identification (T46 says "fish ID on desktop": say exactly what it is and where); boat launches (T1119 is not
   built); salmon openings; in-season changes; free, no account. And what the site says today: home page words,
   meta descriptions, JSON-LD, the store listings' words, robots and sitemap, whether an `llms.txt` exists.
2. Rule: the site claims only what is shipped; a planned feature is named as coming, or added with the release
   that ships it. No claim a tester could not see in the app that day.
3. Write it where crawlers read it: a plain-language "What it is" block on the home page and an About / features
   page (server-rendered text, not only script); JSON-LD `SoftwareApplication` / `MobileApplication` with
   `operatingSystem`, `offers` price 0, `featureList`, and the store links once each store is public; meta and
   social-card descriptions that say "app for iPhone and Android, works offline"; an `llms.txt` with the same
   facts; water and gauge pages say "forecast" where a forecast exists. The store listings' words say the same
   (T1128, T1071; the owner's to push).
4. The wording goes to the owner as a proposal with a picture of the home block before it ships.
5. After release: ask Google's and two other assistants the owner's question again, monthly, and note the answer
   in `COMPETITORS.md` (an answer changes only after a recrawl; weeks, not days).

## Acceptance
A build gate reads the feature list from one source file and fails if the home block, JSON-LD and `llms.txt`
disagree with it or name a feature marked not shipped; the pages validate as structured data; the owner has seen
the words. Severity: medium (people are told to use a paid app for things this one does free). Model: Sonnet
(Opus for the claims list). Size: S–M. Owner: the words.

Owner, 2026-11-25 (planning chat), on fish identification, verbatim: "we have id with pictures and words
desacribing how to id." So the claim is a fish ID guide with pictures and words (shipped), not photo recognition.


## 2026-11-25 night: stocked lakes and depth are claimed now (the owner's call)
Two read-only passes found that the Stocked and Depth map modes exist in code but no data feeds them: both toggles
show "Coming soon / No data yet" (`app/packages/core/src/layers.ts:150-160`, `lakeFacts.ts`, `mapModes.ts:126`;
T200 and T201 are open, planned for 0.11). The planning chat said they could not be claimed. Owner, verbatim: "we
want to claim this (Stocked lakes and depth cannot be claimed,) it will be there soon and was on old version".
Asked how (ship first slices with the claim, recommended / claim as "coming soon" / claim them now as features),
his pick: **"Claim them now as features"**. So Work step 2 changes for these two: the site, `llms.txt` and the
structured data list stocked lakes and depth charts as features before the data is in. Recorded as his call, with
the risk the planning chat named: a store listing that names a feature the build does not have can be rejected
(Apple guideline 2.3 accurate metadata; Google Play's misleading-claims policy), and an assistant would send
people to a layer that says "No data yet". The planning chat's proposal to shrink that gap, not his pick: put the
claim on the SITE first and add it to the two STORE listings with the build that ships the first slices (the
depth-chart card from the open provincial PDFs; the stocking card and colour from the open release records;
T200 / T201), and pull those slices as early as they fit. The feature-list source file marks the two as
"claimed by the owner, data from <release>" so the build gate allows them and names them.
Fish ID is claimed as "fish identification guide with pictures and tips" (41 pictures, about 27 fish), not photo ID.
The launch pass's proposed words (home block, features, meta, JSON-LD, llms.txt, store descriptions) are the
planning chat's notes for the lane; the About page's "stocking records" source line and the credits page's
stocking / depth lines (`menu/licences.ts:144`, `sources.ts:121`) stay, by the same call.

## Owner

- [ ] 2026-11-25 — When the lane has the shipped-feature list and the proposed words (home block, store listing lines): read them and say yes or change them. Default: nothing ships to the site until you have.
- [x] 2026-11-25 — Stocked lakes and depth are not fed with data yet: how to claim them → "Claim them now as features" (2026-11-25)
- [ ] 2026-11-25 — The store listings (Play, App Store): name stocked lakes and depth now too, or only with the build that ships them (the review-rejection risk is on the stores, not the site). Default: the site now, the stores with the build.
