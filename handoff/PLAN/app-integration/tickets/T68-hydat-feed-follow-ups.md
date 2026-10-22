# T68 — HYDAT / feed follow-ups

Status: OPEN.
- A "Forecasts" line on the More Data block: the feed index needs a feed-wide `forecastIssuedAt` (publish.py), the
  reader, a line. Feed republish only.
- The roster-drift notice's command uses the default roster path even with `--stations`.
- scripts/update-hydat.sh stops before upload; document/automate the R2 upload step (human-run with credentials).
