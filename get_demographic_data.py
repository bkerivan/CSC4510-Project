#!/usr/bin/env python

#TODO: Detect invalid county name by checking for "United States" title of
#      QuickFacts page


import pandas as pd

import requests
import sys

from bs4 import BeautifulSoup


QUICKFACTS_URL = "https://census.gov/quickfacts"
QUICKFACTS_ATTRIBUTE_MAP = {
    "race_wht_pct": "RHI125218",
    "race_blkafr_pct": "RHI225218",
    "race_amrindalsk_pct": "RHI325218",
    "race_asn_pct": "RHI425218",
    "race_hwnpcf_pct": "RHI525218",
    "race_multi_pct": "RHI625218",
    "race_hspltn_pct": "RHI725218",
    "race_whtnothsp_pct": "RHI825218"
}


def build_quickfacts_county_map(data_path):
    election_data = pd.read_csv(data_path)

    # Don't know how to handle Alaska "districts" right now
    election_data = election_data[election_data["state"] != "Alaska"]

    # Each state is mapped to a map of its consitituent counties
    # Each county is mapped to its representation in QuickFacts URL format
    county_map = {
        state: {
            # Remove spaces and periods per QuickFacts URL format
            county: "{}county{}".format(county.lower().replace(' ', '').replace('.', ''), state.lower().replace(' ', ''))
            for county in election_data[election_data["state"] == state]["county"]
        }
        for state in election_data["state"]
    }

    return county_map


# Raised if county not found in data, or county QuickFacts parameter doesn't
# return a QuickFacts page for that county
class InvalidCountyException(Exception):
    pass


class QuickFactsScraper:
    # Needs to be passed the county map, so that each scraper doesn't have to
    # build from data, which is slow
    def __init__(self, county_map):
        self.session = requests.Session()
        self.quickfacts_data = pd.DataFrame(columns=["state", "county"] + list(QUICKFACTS_ATTRIBUTE_MAP.keys()))
        self.county_map = county_map

    # Both state and county must be provided, so both can be dataframe
    # attributes
    def scrape_county_data(self, state, county):
        try:
            quickfacts_county = self.county_map[state][county]
        except KeyError:
            raise InvalidCountyException("County {}, {} not found in election data".format(county, state))

        r = self.session.get("{}/{}".format(QUICKFACTS_URL, quickfacts_county))

        if r.status_code != 200:
            r.raise_for_status()

        html = BeautifulSoup(r.text, "lxml")

        # Can append dict to dataframe as a row
        attributes = {"state": state, "county": county}

        # data-mnemonic identifies the attribute type in the QuickFacts table,
        # as represented by QUICKFACTS_ATTRIBUTE_MAP
        for attribute, tag in QUICKFACTS_ATTRIBUTE_MAP.items():
            attribute_field = html.find("tr", {"data-mnemonic": tag})
            attribute_element = attribute_field.findChild(attrs={"data-value": True})
            attribute_value = float(attribute_element["data-value"])
            attributes[attribute] = attribute_value

        record = pd.DataFrame([attributes], columns=attributes.keys())
        self.quickfacts_data = self.quickfacts_data.append(record)

    # Not sure if this is necessary but I suppose it doesn't hurt
    def close(self):
        self.session.close()


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
    
