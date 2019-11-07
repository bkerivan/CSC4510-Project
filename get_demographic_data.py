#!/usr/bin/env python


import pandas as pd
import sys

from lib.quickfacts import QuickFactsScraper


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: {} <cleaned data path>".format(sys.argv[0]))
        sys.exit(1)

    data = pd.read_csv(sys.argv[1])
    data = data[data["state"] != "Alaska"]

    scraper = QuickFactsScraper()
    scraper.get_bulk_county_data(list(zip(data["state"], data["county"]))[:60])

    print(scraper.quickfacts_data)

