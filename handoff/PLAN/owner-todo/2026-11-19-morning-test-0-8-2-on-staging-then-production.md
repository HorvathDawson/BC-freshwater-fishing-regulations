order: 190

## 2026-11-19 morning: test 0.8.2 on staging, then production

**Test 0.8.2 on staging (2026-11-19 morning).** Everything from last night is on staging: data release r-379e72fc9524,
the staging website (https://staging.canifishthis.ca, app commit 3ab2b1b24) and Play INTERNAL 0.8.2 (build 37, staging
data, patched MapLibre). Nothing went to production. What to look at:
1. Salmon, Mamquam River (https://staging.canifishthis.ca/water/mamquam-river): Source (DFO) lists "DFO's rows behind
   these answers"; the bait ban is one line, "Bait ban: no bait of any kind — DFO (all year) and B.C. rules" [T870].
2. Coquitlam / Vedder: "10 adult chinook a year from all fresh waters"; the Vedder adds "(here an adult is over 62 cm)" [T869].
3. Glenn Creek / Littlejohn Creek: a trout row says "Duncan River’s rule for its tributaries" (or similar) under the fish [T892].
4. Fraser River, Gear and Methods: each source said once per section; the sturgeon exception once [T900].
5. The map at the Lillooet braids (head of Harrison Lake): side channels half width [T891].

**Questions from the night (each built with the default named):**
- Q1 [T869] The Vedder: DFO's own note says an adult is over 62 cm with NO dates; the rule book (p.77) says Jul 1–Dec 31.
  Built: follow DFO (the card says "over 62 cm" all year). Or use the book's dates?
  ANSWERED 2026-11-19 ~06:40: "good" — keep following DFO.
- Q2 [T891] About 9,800 small lines that are not braids (FWA types 1400/1410/2000/1425/6010, mostly small unnamed lines)
  pass the existing side-channel test and also draw thinner. Built: keep. Or narrow to braids only (1100/1350/1150)?
  ANSWERED 2026-11-19 ~06:40: "probably okay" — keep.
- Q3 [T870] The live salmon feed's size check was raised from 112 kB to 135 kB (the DFO rows add ~34 kB raw, 15.7 kB
  gzipped). Built: raised. OK?
  ANSWERED 2026-11-19 ~06:40: "okay we will work to shrink it down later" -> T905.
- Q4 [T892] The trout rows use the same words as the Other fish rows ("X’s rule for its tributaries"). OK on the table?
  2026-11-19 ~06:40: "i dont see this anywhere give example" — example sent (Glenn Creek, Littlejohn Creek); still open.
- Q5 [T903] The map speed-up (paint only what differs) did NOT land: under a stressed phone, mode switches were 11–22 %
  faster but panning on Salmon got worse (p95 116 → 154 ms). Numbers in the ticket; next chat retries the safer half.
- Q6 [T319] Store screenshots: nothing was uploaded (a permission block). Run the two commands in the T319 ticket, or
  approve them when Claude runs them.

**Production, after you test (the order matters):** the FEEDS worker first — the new app shows no DFO rows for a place
the old feed carries (T870) — in the usual order (feeds deploy → `deploy/prod/sync-inputs.sh` → /run/dfo → data upload →
/run/gauges; `handoff/PLAN/release/PRODUCTION.md`), the data step being
`CONFIRM_PROD=r-379e72fc9524 python -m pipeline release --prod r-379e72fc9524`,
then the phone builds (`CONFIRM_PROD=canifishthis.ca deploy/prod/build-apk.sh`;
`CONFIRM_PROD=canifishthis.ca deploy/staging/build-ios-testflight.sh --production`), then say go and Claude deploys the
production website from the same commit.
