#!/usr/bin/env bash
# Render build script
pip install -r requirements.txt
python -c "from scraper import setup_database; setup_database()"
python -c "from scraper import scrape_all_feeds; scrape_all_feeds()"
