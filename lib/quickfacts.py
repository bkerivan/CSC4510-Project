import pandas as pd
import requests

from bs4 import BeautifulSoup


_QUICKFACTS_URL = "https://census.gov/quickfacts"
_QUICKFACTS_ATTRIBUTE_MAP = {
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


# Raised if imported QuickFacts data has wrong format
class QuickFactsImportError(Exception):
    pass


# Raised if county not found in data
class InvalidCountyException(Exception):
    pass


class QuickFactsScraper:
    # Needs to be passed the county map, so that each scraper doesn't have to
    # build from data, which is slow
    def __init__(self, county_map, saved_data_path=None):
        self.session = requests.Session()
        self.county_map = county_map
        self.attributes = ["state", "county"] + list(_QUICKFACTS_ATTRIBUTE_MAP.keys())

        if saved_data_path:
            self.import_data(saved_data_path)
        else:
            self.quickfacts_data = pd.DataFrame(columns=self.attributes)


    def import_data(path):
        data = pd.read_csv(path)

        if list(data.columns.values) != self.attributes:
            raise QuickFactsImportError("Incorrect columns in {}".format(path))

        self.quickfacts_data = data


    def export_data(path):
        self.quickfacts_data.to_csv(path, index=False)


    # Both state and county must be provided, so both can be dataframe
    # attributes
    def scrape_county_data(self, state, county):
        try:
            quickfacts_county = self.county_map[state][county]
        except KeyError:
            raise InvalidCountyException("County {}, {} not found in election data".format(county, state))

        r = self.session.get("{}/{}".format(_QUICKFACTS_URL, quickfacts_county))

        if r.status_code != 200:
            r.raise_for_status()

        html = BeautifulSoup(r.text, "lxml")

        # Can append dict to dataframe as a row
        attributes = {"state": state, "county": county}

        # data-mnemonic identifies the attribute type in the QuickFacts table,
        # as represented by _QUICKFACTS_ATTRIBUTE_MAP
        for attribute, tag in _QUICKFACTS_ATTRIBUTE_MAP.items():
            attribute_field = html.find("tr", {"data-mnemonic": tag})
            attribute_element = attribute_field.findChild(attrs={"data-value": True})
            attribute_value = float(attribute_element["data-value"])
            attributes[attribute] = attribute_value

        record = pd.DataFrame([attributes], columns=attributes.keys())

        # Update record if already in dataframe, otherwise append
        if self.quickfacts_data.loc[(self.quickfacts_data["state"] == state) &
                                    (self.quickfacts_data["county"] == county)].empty:
            self.quickfacts_data = self.quickfacts_data.append(record)
        else:
            self.quickfacts_data.loc[(self.quickfacts_data["state"] == state) &
                                     (self.quickfacts_data["county"] == county)] = record


    # Not sure if this is necessary but I suppose it doesn't hurt
    def close(self):
        self.session.close()

