# T256 — bad connections, part 2: fewer bytes per answer, charts before their envelope, the phone's installer

Status: OPEN

Source: T254's measurements (owner 2026-10-29, "make the app work well on BAD connections"). T254 made the app order,
survive and say what it is doing on a poor line; what is left is the BYTES an answer costs, which no ordering can hide.
Numbers: `node app/tools/bad-net.mjs` (T254 ticket, "Measured").

## Items
1. **The answer's bytes.** A named water's Regulations need core (1.27 MB gz) + water (2.22 MB) + regs-<region> (0.26 MB)
   + the sql.js wasm (0.33 MB): ≈ 4 MB, ≥ 80 s on Slow 3G and ≥ 5 min on 2G-ish however they are ordered. Split
   `water` / `core` by region (or a per-region "answer" slice), so a tap in Region 2 reads Region 2. Weigh against the
   request walk's pins (T129) and the release total gate.
2. **A start link waits for paint.** `bootRelease` decodes paint (0.68 MB gz) before the Shell draws, so a `?regs=` link
   (an SEO page's "Open in the app") waits ≈ 14 s (Slow 3G) / 55 s (2G) for a file its page does not need first.
   Let a regs-first start draw the page before paint, the map after.
3. **Conditions chart before its envelope.** `gaugeSeries` waits for `climate` (1.4 MB gz) before the chart draws a
   point; on a poor line draw the observations and forecast first and add the normal-range band when it lands (the
   answer store, `askOnce`, must not keep the envelope-less series as the answer).
4. **The phone's first install** (`install.ts`, `toDisk`) is not on `fetchResilient`: no per-size timeouts, retries or
   resume on a 1–2 bar first run. Wire it (expo-file-system's resumable download) and test on a device.
5. **The app bundle** is 1.5 MB gz: 45 s of Slow 3G before the map chrome (and the fonts). Look at code-splitting the
   menu pages / charts out of the first bundle.
6. **The request walk** (`app/tools/request-walk.mjs`) fails at the named tap on an exported build (before T254 too):
   it does not accept the first-run notice / walkthrough the way `bad-net.mjs` does. Make it.

Depends: T254. Size: L. Owner: none.
