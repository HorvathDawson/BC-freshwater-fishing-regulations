"""in_season — scrape + resolve in-season regulation changes.

    scraper  — fetch the in-season changes page → structured rows
    resolver — resolve those rows to reach IDs → cron/in-season/in_season.json

Re-run on a schedule by .github/workflows/update-in-season.yml via
scripts/update-in-season.sh.
"""
