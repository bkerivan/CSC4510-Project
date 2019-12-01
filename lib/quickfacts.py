import pandas as pd
import requests
import requests_cache
import time

from bs4 import BeautifulSoup
from tqdm import tqdm


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

# Raised if scraping an individual page fails
class QuickFactsScraperError(Exception):
    pass


class QuickFactsScraper:
    def __init__(self, saved_data_path=None, use_cache=False):
        self.attributes = ["state", "county"] + list(_QUICKFACTS_ATTRIBUTE_MAP.keys())
        self.session = requests_cache.CachedSession() if use_cache else requests.Session()

        if saved_data_path:
            self.import_data(saved_data_path)
        else:
            self.quickfacts_data = pd.DataFrame(columns=self.attributes)


    def build_quickfacts_url(self, state, county):
        # For Virginia, these need to be named cities, not counties
        va_cities = (
            "Alexandria",
            "Bristol",
            "Buena Vista",
            "Charlottesville",
            "Chesapeake",
            "Colonial Heights",
            "Covington",
            "Danville",
            "Emporia",
            "Falls Church",
            "Fredericksburg",
            "Galax",
            "Hampton",
            "Harrisonburg",
            "Hopewell",
            "Lexington",
            "Lynchburg",
            "Manassas",
            "Manassas Park",
            "Martinsville",
            "Newport News",
            "Norfolk",
            "Petersburg",
            "Poquoson" 
        )

        # These have their own independent formatting for some reason
        va_city_counties = (
            "Norton",
            "Portsmouth",
            "Radford",
            "Salem",
            "Staunton",
            "Suffolk",
            "Virginia Beach",  
            "Waynesboro",
            "Williamsburg",
            "Winchester"
        )

        if county in va_cities and state == "Virginia":
            district_term = "city"
        elif county in va_city_counties and state == "Virginia":
            district_term = "cityvirginiacounty"
            # Don't include state at the end for these
            state = ""
        elif state == "Louisiana":
            # Louisiana has "parishes", not counties
            district_term = "parish"
        else:
            district_term = "county"

        if state == "District of Columbia" and county == "District of Columbia":
            quickfacts_county_param = "DC"
        # Misc. replacements... unfortunately these will need to be hard-coded in
        elif county == "Baltimore City" and state == "Maryland":
            quickfacts_county_param = "baltimorecountymaryland"
        elif county == "Saint Louis" and state == "Minnesota":
            quickfacts_county_param = "stlouiscountyminnesota"
        elif county == "St. Louis County" and state == "Missouri":
            # This one already has "county" in the name...
            quickfacts_county_param = "stlouiscountymissouri"
        elif county == "St. Louis City" and state == "Missouri":
            quickfacts_county_param = "stlouiscitymissouri"
        elif county == "Carson City" and state == "Nevada":
            quickfacts_county_param = "carsoncitynevada"
        elif county == "Kansas City" and state == "Missouri":
            # Ok then...
            quickfacts_county_param = "kansascitycitymissouri"
        elif county == "Bronx" and state == "New York":
            quickfacts_county_param = "bronxcountybronxboroughnewyork"
        elif county == "Kings" and state == "New York":
            quickfacts_county_param = "kingscountybrooklynboroughnewyork" 
        elif county == "New York" and state == "New York":
            quickfacts_county_param = "newyorkcountymanhattanboroughnewyork" 
        elif county == "Queens" and state == "New York":
            quickfacts_county_param = "queenscountyqueensboroughnewyork"
        elif county == "Richmond" and state == "New York":
            quickfacts_county_param = "richmondcountystatenislandboroughnewyork"
        else:
            # Remove ' ', '.', '-', and "'" per QuickFacts URL format
            quickfacts_county_param = \
            "{}{}{}".format(county.lower().replace(' ', '').replace('.', '').replace('-', '').replace('\'', ''),
                            district_term, state.lower().replace(' ', ''))

        return "{}/{}".format(_QUICKFACTS_URL, quickfacts_county_param)


    # Get data for a single county
    def get_county_data(self, state, county):
        r = self.session.get(self.build_quickfacts_url(state, county))

        if r.status_code != 200:
            if r.status_code == 404:
                print("[!] Page not found: {}".format(r.url))
            else:
                r.raise_for_status()

        try:
            self.scrape_quickfacts_page(state, county, r.content)
        except QuickFactsScraperError as e:
            print("[!] {}".format(e))


    # Counties must be a list of two-tuples: (state name, county name)
    def get_bulk_county_data(self, counties, show_progress=False):
        pbar = tqdm(counties, total=len(counties), desc="Scraping...") if show_progress else counties

        for state, county in pbar:
            if show_progress:
                pbar.set_description("{}, {}".format(county, state))
                pbar.refresh() 
            self.get_county_data(state, county)
        
        if show_progress:
            pbar.close()


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

            if attribute_field is None:
                raise QuickFactsScraperError("Could not find attribute \"{}\" for {}, {}".format(attribute, county, state))

            attribute_element = attribute_field.findChild(attrs={"data-value": True})

            if attribute_element is None:
                raise QuickFactsScraperError("Could not find value of \"{}\" for {}, {}".format(attribute, county, state))

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


    def import_data(self, path):
        data = pd.read_csv(path)

        if list(data.columns.values) != self.attributes:
            raise QuickFactsImportError("Incorrect columns in {}".format(path))

        self.quickfacts_data = data


    def export_data(self, path):
        self.quickfacts_data.to_csv(path, index=False)


    def close(self):
        self.session.close()

