"""recurring — jobs re-run on a schedule against a live site, not a one-shot static build.

Currently: in-season regulation change scraping + resolution
(in_season_scraper.py / in_season_resolver.py), re-run every 6 hours by
.github/workflows/update-in-season.yml via scripts/update-in-season.sh.
"""
