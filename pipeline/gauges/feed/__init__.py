"""The live half: ECCC observations, the BC River Forecast Centre, and the HYDAT envelope.

KNOWS STATION IDS AND NOTHING ELSE. No atlas, no graph, no bundle, no matching — which is
what lets the same code run as a 30-minute cron against R2 and as a one-shot against a dev
directory. See `pipeline/docs/15-live-data-flow.md`."""
