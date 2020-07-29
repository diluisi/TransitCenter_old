import pandas as pd
import cenpy
import lehd

def dl(region):
    """
    Downloads data from the ACS and LEHD from
    https://www.census.gov/data/developers/data-sets/acs-5year.html
    """

    county_id_path = "data/nationwide_data/county_ids.csv"

    esential_worker_path = "data/nationwide_data/essential_share_LEHD.csv"

    out_data_path = "data/" + region + "/input/population_data/demographics.csv"

    variables = [
    'B03002_001E', # total population
    'B03002_003E', # non-hispanic white
    'B03002_004E', # non-hispanic black
    'B03002_006E', # non-hispanic asian
    'B03002_007E', # non-hispanic pacific
    'B03002_005E', # non-hispanic American Indian and Alaska Native
    'B03002_012E', # hispanic
    'B11001_001E', # total households,
    'B11005_002E', # total households with children (child<18)
    'B11005_007E', # total single mother households (child<18)
    'C17002_002E', # under poverty line by less than 0.5 income/poverty ratio
    'C17002_003E' # under poverty line by less of 0.5 to 0.99 income/poverty ratio,
    ]

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
        # block group or tract for geo_unit

    df = pd.concat(dfs)

    # creating a combined block group csv
    df["geoid"] = df["state"] + df["county"] + df["tract"] + df["block group"]
    del df["state"], df["county"], df["tract"], df["block group"]

    # coding variables
    df["pop_total"] = df['B03002_001E'].astype(int) # total population
    df["pop_white"] = df['B03002_003E'].astype(int) # non-hispanic white
    df["pop_black"] = df['B03002_004E'].astype(int) # non-hispanic black
    df["pop_asiapacific"] = df['B03002_006E'].astype(int) + df['B03002_007E'].astype(int) # non-hispanic asian or pacific islander
    df["pop_hispanic"] = df["B03002_012E"].astype(int) # hipsanic
    df["pop_indig"] = df["B03002_005E"].astype(int) # non-hipsanic native amereican
    df["pop_otherrace"] = df["pop_total"].astype(int) - df["pop_white"].astype(int) - df["pop_black"].astype(int) - df["pop_asiapacific"].astype(int) - df["pop_hispanic"].astype(int) - df["pop_indig"].astype(int)  # other race

    df["pop_poverty"] = df["C17002_002E"].astype(int) + df["C17002_003E"].astype(int) # pop under poverty line
    df["hhld_total"] = df["B11001_001E"].astype(int) # total households
    df["hhld_total_w_chld"] = df['B11005_002E'].astype(int) # total households with children (<18 years old)
    df["hhld_single_mother"] = df['B11005_007E'].astype(int) # total single mother households (<18 years old)
    df.drop(variables, inplace=True, axis=1) # dropping unneeded variables


    # bringing in shares for essential worker by LEHD NAICS category
    ess = pd.read_csv(esential_worker_path)
    ess = ess[ess["name"] == region]

    # downloading LEHD data
    dfl = lehd.dl_lodes.rac(
        locations = county_ids['county_id'].to_list(),
        year = 2017,
        geography = "BG",
        )

    dfl["workers_all"] = dfl["C000"]

    # computing total essential workers
    dfl["workers_essential"] = 0
    for index, row in ess.iterrows():
        LEHDcat = row['LEHD']
        e_share = row['e_share']
        dfl["workers_essential"] = dfl["workers_essential"] + dfl[LEHDcat] * e_share

    dfl = dfl[["h_geoid_BG","workers_all","workers_essential"]]

    # merge and save the output
    df = pd.merge(df,dfl,left_on ="geoid", right_on ="h_geoid_BG", how = "outer")
    del df["h_geoid_BG"]
    df.to_csv(out_data_path, index = False)
