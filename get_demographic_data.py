#!/usr/bin/env python


import pandas as pd

import requests
import sys

from bs4 import BeautifulSoup


QUICK_FACTS_URL = "https://census.gov/quickfacts"


def get_county_names(data_path):
    election_data = pd.read_csv(data_path)

    # Don't know how to handle Alaska "districts" right now
    election_data = election_data[election_data["state"] != "Alaska"]

    # Replace any spaces and periods per QuickFacts URL format
    counties = [county.lower().replace(' ', '').replace('.', '') + "county" + state.lower().replace(' ', '') for county, state in zip(election_data["county"], election_data["state"])]

    # Some misc. replacements
    return [county if county != "districtofcolumbiacountydistrictofcolumbia" else "DC" for county in counties]


class QuickFactsScraper:
    def __init__(self, counties=[]):
        self.session = requests.Session()
        self.demographic_data = pd.DataFrame(columns=["county", "race_wht_pct",
        "race_blkafr_pct", "race_amrindalsk_pct", "race_asn_pct", "race_hwnpcf_pct",
        "race_multi_pct", "race_hspltn_pct", "race_whtnothsp_pct"])
        self.counties = counties

    def scrape_county_data(self, county):
        r = self.session.get("{}/{}".format(QUICK_FACTS_URL, county))

        if r.status_code != 200:
            r.raise_for_status()

        html = BeautifulSoup(r.text, "lxml")

        race_wht_pct_field = html.find("tr", {"data-mnemonic": "RHI125218"})
        race_wht_pct_stat = race_wht_pct_field.findChild(attrs={"data-value": True})
        race_wht_pct = float(race_wht_pct_stat["data-value"])
        print(race_wht_pct)

    def close():
        self.session.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: {} <cleaned data path>".format(sys.argv[0]))
        sys.exit(1)

    counties = get_county_names(sys.argv[1])
    
    scraper = QuickFactsScraper()
    scraper.scrape_county_data("autaugacountyalabama")

