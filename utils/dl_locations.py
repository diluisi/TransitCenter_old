import pandas as pd
import numpy as np
import urllib.request

class download_destinations:

    def __init__(self,region):
        self.region = region

        # grab info needed by county for downloading data
        pdc = pd.read_csv("./data/county_ids.csv") # change later
        pdc = pdc[pdc["region_name"] == region]
        self.region_info = pdc

    # function for downloading employment data from LEHD
    def get_employment(self):

        # get state IDs
        self.region_info["state"] = (self.region_info["county_id"] / 1000).astype(int)

        # our counties have numeric FIP codes, while the LEHD data has alpha codes, so we need to make a link to download data just four our states
        state_FIP_codes = pd.read_csv("./data/all_regions/state_FIP_codes.csv")
        state_FIP_codes = state_FIP_codes[["Numeric code", "Alpha code"]]
        self.region_info = self.region_info.merge(state_FIP_codes, how = 'inner', left_on = "state", right_on = "Numeric code")

        # download LEHD workplace data for each state.
        for state in self.region_info["Alpha code"].unique():

            # download the workplace data for each state
            file_name = state.lower() + "_wac_S000_JT00_2017.csv.gz"
            url_name = "https://lehd.ces.census.gov/data/lodes/LODES7/" + state.lower() + "/wac/" + file_name

            urllib.request.urlretrieve(url_name, "./data/" + self.region + "/" + file_name)




        # IDs
        # download LEHD data by IDs

        # save somewhere?
        # data/destination_data/Region

        None

    def get_healthcare(self):

        # place

        None


    def get_groceries(self):

        None


    def get_parks(self):

        None




test = download_destinations("Boston")
test.get_employment()
test.get_healthcare()
test.get_groceries()
test.get_parks()
