#!/usr/bin/env python


import pandas as pd
import sys
import time

from lib.quickfacts import QuickFactsScraper


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: {} <cleaned data path>".format(sys.argv[0]))
        sys.exit(1)

    data = pd.read_csv(sys.argv[1])
    data = data[data["state"] != "Alaska"]

    scraper = QuickFactsScraper()

    s = time.process_time()
    scraper.get_bulk_county_data(zip(data["state"], data["county"]))
    e = time.process_time()

    print(scraper.quickfacts_data)
    print("\n\n{} secs\n\n".format(e - s))

    scraper.close()

