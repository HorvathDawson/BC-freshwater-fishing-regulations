# T254 — bad connections: measure, then a low-bandwidth mode and resilient fetches

Status: DONE (886a926e2, 2696f6609, 72920f8de, 7de67f607, 6248996c7, 51cf4a832, 30ddaa454)

Source: owner 2026-10-29 — make the app work well on BAD connections (rural BC, 1–2 bars, flaky): simulate bad
connections, and use a different loading strategy when the connection is poor.

## The harness — `app/tools/bad-net.mjs`
Headless Chrome over CDP, a cold profile per step, phone screen (390 × 844), the throttle in force from the first byte
(`cdp.mjs` gained `setup` / `onEvent`, and request start/finish times).

    node app/tools/bad-net.mjs --url http://localhost:39260 --serve app/dist/<export>     # a local production build
    node app/tools/bad-net.mjs --url https://staging.canifishthis.ca                       # staging (never prod)
      [--profiles slow-3g,2g,flaky,offline-back] [--steps paint,named,unnamed,link,conditions,pdf]
      [--json f] [--md f] [--shots dir] [--desktop] [--low on|off|auto] [--seed 254]

| profile | down / up / RTT | extra |
|---|---|---|
| slow-3g | 400 / 400 kbit/s / 400 ms | |
| 2g | 100 / 50 kbit/s / 800 ms | |
| flaky | 1 Mbit/s / 384 kbit/s / 300 ms | seeded: 8 % of data requests reset, 5 % stall 45 s then time out, a 4 s blackout every 30 s |
| offline-back | slow-3g | the map paints, the line drops, a named tap (what shows?), the line returns (recovers by itself / after a re-tap / stuck) |

Steps: map chrome + map painted; tap → Regulations for a named water (Gravel Slough) and an unnamed stream (R2), the tap
repeated until the water opens; a start link (`?regs=gnis:3062`) → Regulations; Conditions → horizons and its data; "Rule
book page N" → PDF opened / offered / cap. Each records failed requests by kind, console errors, bytes by kind, the
release files fetched, and which slow states were shown. `--serve` gzips text and fonts as the CDN does.

## Built
**Policy, pure — `@app/data` `net/`.** `netMode(setting, hints, wasLow)`: the reader's Data setting first (On / Off),
then Automatic: Save-Data → low; a MEASURED rate below 1 Mbit/s → low (above 2 Mbit/s to leave: hysteresis); nothing
measured yet → the platform's word (browser `effectiveType` slow-2g/2g/3g, `downlink` < 1 Mbit/s, `rtt` ≥ 900 ms; a phone's
NetInfo cellular generation 2g/3g). A measured rate beats the platform (Chrome said "4g" under throttling). Offline is said
beside the mode. `DEFERRAL_ORDER` / `priorityOf`: the answer files (paint, core, water, regs-<r>, regs-sid, down), then
the extras a page adds (salmon slices, access note, bait & fly, Conditions, climate, geo, near), then anything ahead of
need. `timeoutsFor(bytes)`: a first byte within 20 s, never 20 s without a byte, the whole file within 20 s + bytes at
32 kbit/s — a moving download is never cut. `backoffMs`: 1, 2, 4, 8 … 15 s, ± 20 %, 4 tries.

**Mechanism.** `createFetchGate` (keyed; in low mode a download waits for everything before it in the order; asking
again joins and RAISES it — a prefetch the reader then taps for jumps the queue; `ahead` one at a time). `fetchResilient`
(per-size timeouts, retries, Range resume when the body is the stored file — gzip magic or `%PDF` — else from 0; a 200 to a
Range drops the kept bytes; while the device is offline the next try WAITS for it instead of spending tries; the caller's
sha check refuses any bad join, then one whole refetch). `createNetMeter` (wire bytes over busy time in an 8 s window: idle
time is not slowness, two downloads are one link, a stall reads ≈ 0). `createNetActivity` (what is downloading: waiting /
arriving, bytes of bytes, the try). One `Net` per app (`createNet`). The web release source, the web manifest and the
gauge feed go through it (the feed: 45 s cap, one retry, and the LAST GOOD copy this session read on a dead line, kept
across a refresh).

**Low-bandwidth mode does:** the background tier holds (`prefetchHold` → `low-data`); the idle tier fetches only the
region's file, behind anything asked (no core / sql.js unasked — tried core-ahead and measured Conditions 20 s slower);
the page's extras wait behind its answer (gate); the map's bait & fly marks wait for a quiet line (15 s, nothing
downloading); the rule book is OFFERED with its size and time before a byte ("The rule book is 25.0 MB — about 13 min on
this connection." Download it / Not now; not when already on the device); a large update waits as on mobile data.

**Screens.** A banner over the app: "Slow connection — showing what we have" (auto-low) / "Offline — showing what's saved
on this device" (press: More). Under a Regulations spinner: "Loading the regulations… 1.2 MB of 3.4 MB" / "Connection
dropped — trying again (1)" / "Offline — this will load when you're back online" (at once on a slow line, after 2.5 s
otherwise). The Salmon card's place while it is not there: "Salmon rules: still loading (slow connection)" / "…: not
available offline" / "…: couldn't load" + Try again — never a silent gap. A failed page or water sheet: Try again, and it
asks again BY ITSELF when the device comes back online. The start screen: the banner and "405.3 kB of 2.2 MB". More → Data:
"Low-bandwidth mode" Automatic / On / Off with "On now — slow connection (400 kbit/s)". Words: `@app/core` `net.ts`.

**Found by the harness, fixed:** on 2G-ish the web app NEVER STARTED — expo-font's 12 s font check fails, `useFonts`
reports an error, never ready, and the app sat on "Loading type…" for good (staging too). The app now draws in the system
face on a failed check or 4 s after its data is open; the faces swap in when they land.

**Native.** The same `netMode`, fed by NetInfo (`hints`: cellular generation; online), the same More → Data setting
(kept in `low-data.txt`), the same banner, waiting lines, offer and feed timeouts. The installed release is local, so the
gate has nothing to order there; the synopsis download is resilient (no stream on RN: no resume); a large update waits on
a slow line. NOT VERIFIED ON A DEVICE (T255's Android test bed is the place).

## Measured (2026-10-29)
A local production build (`expo export`, served gzipped by `--serve`) over the local release r-0fa7927b16e5 on :39217:
BEFORE = 38c34587a (the commit before T254), AFTER = this ticket's commits. One run each (flaky is seeded); times are from
the navigation (paint, link) or from the opening tap (taps), "data" = the Conditions files landed. Raw JSON / logs in the
lane's scratchpad; reproduce with the commands above.

| line | step | BEFORE | AFTER |
|---|---|---|---|
| Slow 3G | map chrome / painted | 45 s / 57 s | 45 s / 56 s |
| Slow 3G | named tap → regs | 51 s (fetched method + access too) | **46 s** (core, water, regs-2 only) |
| Slow 3G | unnamed tap → regs | 97 s | 97 s |
| Slow 3G | link → regs | 136 s | 130 s |
| Slow 3G | Conditions shown / data | 0 s / 38 s | 0 s / **32 s** (no method) |
| Slow 3G | rule book | 25 MB download, 32 % after 180 s | **offered first**: "25.0 MB — about 11 min on this connection" |
| 2G-ish | map | **never: stuck on "Loading type…"** (font check gave up) | chrome 179 s, painted 221 s |
| 2G-ish | named / unnamed tap → regs | never (no app) | 181 s / 385 s |
| 2G-ish | link → regs | never | 516 s |
| 2G-ish | Conditions data | never | 235 s (6 stalled tries retried, all landed) |
| 2G-ish | rule book | never | offered: "about 45 min" |
| flaky | map chrome / painted | 19 s / 24 s | 19 s / 24–75 s (a blackout mid-paint is waited out) |
| flaky | named tap → regs | **"Regulations unavailable: Failed to fetch"** | **10–23 s** |
| flaky | unnamed tap → regs | **"Regulations unavailable: Failed to fetch"** | **30–31 s** |
| flaky | link → regs | **"Regulations unavailable"** | **49–67 s** |
| flaky | Conditions data | 3 s, a file FAILED (wasm + release) | 11–25 s, all landed |
| flaky | rule book | no page to press | offered |
| offline-back | tap while offline | nothing (no message), every fetch failed | the banner: "Offline — showing what's saved on this device" |
| offline-back | line back | **stuck** (a re-tap failed too: 11 console errors) | **opens** (84 s after the line returned on Slow 3G; a probe saw it open with no re-tap) |

Staging (https://staging.canifishthis.ca, BEFORE = the site as it was; AFTER = deployed from 30ddaa454, which also carries
other lanes' commits):

| line | step | BEFORE | AFTER |
|---|---|---|---|
| Slow 3G | map chrome / painted | 47 s / 58 s | 47 s / 58 s |
| Slow 3G | named / unnamed tap → regs | 51 s / 97 s | 46 s / 97 s |
| Slow 3G | link → regs | 138 s | 132 s |
| Slow 3G | Conditions data | 38 s | 32 s |
| Slow 3G | rule book | 32 % after 180 s | offered, "about 10 min" |
| 2G-ish | everything | **never: stuck on "Loading type…"** | painted 228 s; named 180 s; link 524 s; Conditions 288 s; unnamed still loading at the 600 s cap, saying "Loading the regulations… 5.6 MB of 6.4 MB"; rule book offered "about 45 min" |
| flaky | named / unnamed / link → regs | **"Regulations unavailable: Failed to fetch"** / the tap opened nothing / unavailable | **24 s / 44 s / 47 s** |
| flaky | Conditions data | 2.5 s with a file FAILED | 23 s, landed |
| offline-back | line back | **stuck** (12 console errors) | opens (78 s after the line returned) |

Every AFTER step shows its slow state (the harness records it): the banner on every slow line, the waiting line on every
regs wait, the start line on boot, the offer on the PDF. Release files a slow-line named tap fetched: before paint,
regs-2, method, core, water, access; after paint, regs-2, core, water (the extras after the page).

**What did not move, and why.** On one bar the answer itself is the cost: core 1.27 MB + water 2.22 MB + regs-2 0.26 MB +
the wasm 0.33 MB ≈ 4 MB gz is ≥ 80 s at 400 kbit/s whatever the order, and the app bundle (1.5 MB gz) is 45 s before the
map chrome. Ordering removed what competed (≈ 0.5 MB on a named tap); the rest is bytes → T256.

## Screenshots — `../T254-shots/`
Phone (390 × 844) and desktop (1280 × 800), the AFTER build on Slow 3G: `*-boot-slow` (start line), `*-regs-waiting`
("Loading the regulations… 265.7 kB of 9.2 MB" — decoded bytes), `*-regs-slow-banner`, `*-map-slow-banner`,
`*-more-data-low` (the setting), `*-pdf-offer`, `*-map-offline`, `*-regs-offline` ("Offline — this will load when you're
back online").

## Left
- T256 (filed): the bytes an answer costs (water / core by region), a start link before paint, the Conditions chart before
  its climate envelope (1.4 MB), the phone's first install on `fetchResilient`, the bundle, the request walk's first run.
- Release files on the web are sent `Content-Encoding: gzip`, so the browser hands over decoded bytes and a cut release
  file restarts from 0 (it is ≤ 2.2 MB gz); RESUME works for the 25 MB rule book (raw, Range on both servers).
- On 2G-ish the map's bait & fly file still came before a tap in the harness (its 15 s quiet line passed while the
  reader looked at the map); a longer wait or on-demand like Salmon is a one-line change if the owner wants it.
- The Updates log could show the gate's retries (`onRetry` is there); not wired.
- Native: not run on a device (T255's Android test bed with adb network control is the place to measure it).
- The staging smoke test (T241) now fails 3 checks that are not T254's: another lane's six-step walkthrough covers the map
  taps and Run timing moved to its own tab; map, Conditions, the gauge reading and the PDF pass.

## Tests
`packages/data/src/net/net.test.ts` (mode table incl. hysteresis and measured-beats-browser; deferral order; timeouts by
size; backoff; the gate: off-low all at once, low answer → extras → one ahead, raise on re-ask, failure / mode change
release; the meter: busy time, two downloads one link, stall; `fetchResilient`: whole, stall → Range resume, not resumable
→ from 0, 200 to a Range, no first byte / 5xx retried, 404 not, gives up with why, offline waits without spending a try,
abort, wire bytes to the meter; `fetchVerifiedVia`: resumed gzip verifies, a bad sha refetched whole then refused, extras
behind the answer; the web source raises a prefetched file). `packages/ui/src/net.render.test.tsx` (setting stored / read
back, browser hints, `useNetState` mode + hysteresis + offline + override + handed to `Net`, `useNetQuiet`).
`packages/core/src/net.test.ts` (every line). `packages/ui-native/src/NetBanner.test.tsx` (banner, More → Data row,
waiting line, part lines, the rule book offer: nothing fetched until Download, NetInfo generation).
`release/prefetch.test.ts`, `release/background.test.ts`, `feed/http.test.ts` (low-data plan and hold; the feed's last
good copy on a dead line).

Depends: T231, T251, T253. Size: L. Owner: none.
