import pandas as pd
import numpy as np
import urllib.request
import gzip
import shutil
import os

class get_destination_data:

    # initializing
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


        # output dataframe
        dfo = None

        # download LEHD workplace data for each state.
        for state in self.region_info["Alpha code"].unique():

            # setting up the paths
            zip_file_name = state.lower() + "_wac_S000_JT00_2017.csv.gz"
            csv_file_name = state.lower() + "_wac_S000_JT00_2017.csv"
            url_name = "https://lehd.ces.census.gov/data/lodes/LODES7/" + state.lower() + "/wac/" + zip_file_name
            file_path = "./data/" + self.region + "/destination_data/"

            # downloading
            urllib.request.urlretrieve(url_name, file_path + zip_file_name)

            # unziping
            with gzip.open(file_path + zip_file_name, 'rb') as f_in:
                with open(file_path + csv_file_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            # remove the downloaded zip file
            os.remove(file_path + zip_file_name)

            # create a field for the county ID for subsetting
            lehd_wac = pd.read_csv(file_path + csv_file_name)
            lehd_wac["county_id"] = (lehd_wac["w_geocode"] / 10000000000).astype(int)

            # subset just for our counties
            lehd_wac = lehd_wac[lehd_wac['county_id'].isin(self.region_info["county_id"])]

            # create a field for block group
            lehd_wac["block_group_id"] = (lehd_wac["w_geocode"] / 1000).astype(int)

            # delete columns we do not want to tabulate
            del lehd_wac['w_geocode'], lehd_wac['createdate'], lehd_wac['county_id']

            # group by block_group_id and count the number of jobs
            lehd_wac = lehd_wac.groupby(['block_group_id']).sum()

            # append into the output
            if dfo is None:
                dfo = lehd_wac
            else:
                dfo = pd.concat([dfo, lehd_wac])

            # remove the downlaoed unzipped csv file
            os.remove(file_path + csv_file_name)

        # save the output
        dfo.to_csv(file_path + "lehd_employment_data.csv")




    def get_healthcare(self):

        # place

        None


    def get_groceries(self):

        None


    def get_parks(self):

        None




test = get_destination_data("Boston")
test.get_employment()
test.get_healthcare()
test.get_groceries()
test.get_parks()
