# T279 — Water pages stored pre-compressed; SEO build on several cores

Status: OPEN (owner 2026-10-29: "can we just store all the layouts like we have now but have them be pre compressed?").

Source: T269 made each /water page a snapshot of the app's pane in all three layouts (~75 KB raw, ~10 KB gzip; 19,936 pages
= 1.5 GB raw, 26.5 min build on one core).

## Work
1. Keep the three layouts. The build writes each page compressed (brotli + gzip), and the site serves the stored bytes with the
   right `Content-Encoding` by the request's Accept-Encoding (staging site worker; same design for production hosting).
   Uncompressed copies are not uploaded.
2. Run the page renders across worker threads (≤4, per heavy-jobs rule) so the build takes minutes, not half an hour.
3. Check the host's per-deploy file count and size limits with the pre-compressed files; report the headroom.

## Acceptance
Upload size and build time before/after; a staging page loads with `content-encoding: br` and renders identically; the T196
budgets still pass.

Depends: T269. Size: S. Owner: no (production hosting is the owner's).
