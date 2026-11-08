# T513 — Gauges frozen on old readings: Cloudflare's edge cache served stale ECCC CSVs to the feeds worker

Status: DONE on staging (2026-11-08, worker 336fd079); production needs the owner's `deploy/prod/feeds.sh`. Part of T495 gate 1.

Owner 2026-11-08 (screenshot): Capilano River at Canyon (08GA031) showed 0.742 m³/s "observed 2026-11-06 08:45 PT ·
checked 22 min ago", while ECCC read 32.6 m³/s and rising.

## Found
- Every gauge run fetched all CSVs "ok" (status: eccc-hourly 432 ok), yet 162 of 434 stations held old readings, some
  since 2026-10-28 (staging index.json 20:05Z; each checked against the live datamart, which had newer rows).
- 08GA031's station file had `recent` from 11-04 to 11-06 07:30 although the live hourly file starts 11-06: the worker was
  reading an old copy of the file.
- ECCC's datamart sends no Cache-Control, and `.csv` is one of Cloudflare's default cached extensions, so a Worker
  subrequest was answered from the edge cache. Nothing counted it as a failure.

## Fix
- `src/index.ts` `sourceFetch`: every source request of a job goes out with `cache: "no-store"`.
- Staging after the deploy: 431 of 434 stations observed 2026-11-08 (08GA031 at 11:45, 70th percentile); run wall time
  86 s (was 764 s).

## Left
- Production: owner runs `deploy/prod/feeds.sh` (carries the T511 cards too).
- No guard yet that would catch an old copy. Idea: compare a station's newest row with the run time, and count stations
  more than 1 day behind as a held part on /status.
