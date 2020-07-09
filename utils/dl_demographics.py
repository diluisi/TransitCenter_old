import pandas as pd
import cenpy
import lehd

def dl(region, variables):
    """
    Downloads data from the ACS and LEHD
    """

    county_id_path = "data/nationwide_data/county_ids.csv"

    out_data_path = "data/" + region + "/input/population_data/demographics.csv"

    # getting the counties to DL data for
    county_ids = pd.read_csv(county_id_path, dtype={'county_id': 'str'})
    county_ids = county_ids[county_ids["region_name"] == region]

    # setting up the connection to the census API
    conn = cenpy.products.APIConnection("ACSDT5Y2018")


    # create a unique list of states
    counties = []

    # loop over each county, downloading data from census
    dfs = []
    for county in county_ids['county_id'].to_list():
        data = conn.query(variables, geo_unit = 'block group', geo_filter = {"state": county[:2],"county": county[2:]})
        dfs.append(data)

    df = pd.concat(dfs)

    # creating a combined block group csv
    df["geoid"] = df["state"] + df["county"] + df["tract"] + df["block group"]
    del df["state"], df["county"], df["tract"], df["block group"]


    # downloading LEHD data
    dfl = lehd.dl_lodes.rac(
        locations = county_ids['county_id'].to_list(),
        year = 2017,
        geography = "BG",
        )

    df = pd.merge(df,dfl,left_on ="geoid", right_on ="h_geoid_BG", how = "outer")

    del df["h_geoid_BG"]

    df.to_csv(out_data_path, index = False)
