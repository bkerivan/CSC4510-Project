#!/usr/bin/env python


import sys

from lib.quickfacts import build_quickfacts_county_map, QuickFactsScraper


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: {} <cleaned data path>".format(sys.argv[0]))
        sys.exit(1)

    county_map = build_quickfacts_county_map(sys.argv[1])
    scraper = QuickFactsScraper(county_map)

    # Seems to work for a few different tested counties
    scraper.scrape_county_data("Wisconsin", "Oconto")
    print(scraper.quickfacts_data)
    scraper.close()

