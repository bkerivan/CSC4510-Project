import pandas as pd
import requests
import time

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

# Raised if imported QuickFacts data has wrong format
class QuickFactsImportError(Exception):
    pass


class QuickFactsScraper:
    def __init__(self, saved_data_path=None):
        self.attributes = ["state", "county"] + list(_QUICKFACTS_ATTRIBUTE_MAP.keys())
        self.session = requests.Session()

        if saved_data_path:
            self.import_data(saved_data_path)
        else:
            self.quickfacts_data = pd.DataFrame(columns=self.attributes)


    def build_quickfacts_url(self, state, county):
        if state == "District of Columbia" and county == "District of Columbia":
            quickfacts_county_param = "DC"
        elif state == "Alaska":
            # Haven't figured out how to handle Alaska's "districts" yet
            raise Exception("Counties in Alaska are currently unsupported")
        else:
            # Remove ' ', '.', '-', and "'" per QuickFacts URL format
            quickfacts_county_param = \
            "{}county{}".format(county.lower().replace(' ', '').replace('.', '').replace('-', '').replace('\'', ''),
                                state.lower().replace(' ', ''))

        return "{}/{}".format(_QUICKFACTS_URL, quickfacts_county_param)


    # Get data for a single county
    def get_county_data(self, state, county):
        r = self.session.get(self.build_quickfacts_url(state, county))

        if r.status_code != 200:
            r.raise_for_status()

        self.scrape_quickfacts_page(state, county, r.content)


    # Counties must be a list of two-tuples: (state name, county name)
    def get_bulk_county_data(self, counties):
        for state, county in counties:
            print("[DEBUG]: {} County, {}".format(county, state))
            self.get_county_data(state, county)


    def scrape_quickfacts_page(self, state, county, webpage):
        html = BeautifulSoup(webpage, "lxml")

        # Can append dict to dataframe as a row
        df_attributes = dict.fromkeys(self.attributes)
        df_attributes["state"] = state
        df_attributes["county"] = county

        # data-mnemonic identifies the attribute type in the QuickFacts table,
        # as represented by _QUICKFACTS_ATTRIBUTE_MAP
        for attribute, tag in _QUICKFACTS_ATTRIBUTE_MAP.items():
            attribute_field = html.find("tr", {"data-mnemonic": tag})
            attribute_element = attribute_field.findChild(attrs={"data-value": True})
            attribute_value = float(attribute_element["data-value"])
            df_attributes[attribute] = attribute_value

        df = pd.DataFrame([df_attributes], columns=self.attributes)

        # Update record if already in dataframe, otherwise append
        if self.quickfacts_data.loc[(self.quickfacts_data["state"] == state) &
                                    (self.quickfacts_data["county"] == county)].empty:
            self.quickfacts_data = self.quickfacts_data.append(df)
        else:
            self.quickfacts_data.loc[(self.quickfacts_data["state"] == state) &
                                     (self.quickfacts_data["county"] == county)] = df


    def import_data(path):
        data = pd.read_csv(path)

        if list(data.columns.values) != self.attributes:
            raise QuickFactsImportError("Incorrect columns in {}".format(path))

        self.quickfacts_data = data


    def export_data(path):
        self.quickfacts_data.to_csv(path, index=False)


    def close(self):
        self.session.close()

