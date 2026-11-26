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
T1011 (SEO big waters and home links), T1071 (App Store), T1128 (Play listing), `handoff/PLAN/COMPETITORS.md`.

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

## Owner

- [ ] 2026-11-25 — When the lane has the shipped-feature list and the proposed words (home block, store listing lines): read them and say yes or change them. Default: nothing ships to the site until you have.
