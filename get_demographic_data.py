#!/usr/bin/env python


import pandas as pd
import sys
import os

from lib.quickfacts import QuickFactsScraper

DEFAULT_DEMOGRAPHIC_DATA_PATH = os.path.join("data", "demographics.csv")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: {} <cleaned data path>".format(sys.argv[0]))
        sys.exit(1)

    data = pd.read_csv(sys.argv[1])

    scraper = QuickFactsScraper(use_cache=True)

    counties = list(zip(data["state"], data["county"]))

    print("[*] Scraping QuickFacts pages...")
    print()

    scraper.get_bulk_county_data(counties, show_progress=True)
    scraper.close()

    print()
    print("[*] Obtained data for {}/{} counties".format(len(scraper.quickfacts_data), len(counties)))

    scraper.export_data(DEFAULT_DEMOGRAPHIC_DATA_PATH)

    print("[*] Wrote demographic data to {}".format(DEFAULT_DEMOGRAPHIC_DATA_PATH))

