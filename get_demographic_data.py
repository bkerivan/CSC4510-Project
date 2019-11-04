#!/usr/bin/env python

#TODO: Detect invalid county name by checking for "United States" title of
#      QuickFacts page


import pandas as pd

import requests
import sys

from bs4 import BeautifulSoup


QUICK_FACTS_URL = "https://census.gov/quickfacts"

ATTRIBUTE_MAP = {
    "race_wht_pct": "RHI125218",
    "race_blkafr_pct": "RHI225218",
    "race_amrindalsk_pct": "RHI325218",
    "race_asn_pct": "RHI425218",
    "race_hwnpcf_pct": "RHI525218",
    "race_multi_pct": "RHI625218",
    "race_hspltn_pct": "RHI725218",
    "race_whtnothsp_pct": "RHI825218"
}


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

        attributes = {"county": county}

        for attribute, tag in ATTRIBUTE_MAP.items():
            attribute_field = html.find("tr", {"data-mnemonic": tag})
            attribute_element = attribute_field.findChild(attrs={"data-value": True})
            attribute_value = float(attribute_element["data-value"])
            attributes[attribute] = attribute_value

        record = pd.DataFrame([attributes], columns=attributes.keys())
        self.demographic_data = self.demographic_data.append(record)

    def close(self):
        self.session.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: {} <cleaned data path>".format(sys.argv[0]))
        sys.exit(1)

    counties = get_county_names(sys.argv[1])
    
    scraper = QuickFactsScraper()
    scraper.scrape_county_data("autaugacountyalabama")
    print(scraper.demographic_data)
    scraper.close()

