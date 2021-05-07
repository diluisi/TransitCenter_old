'''
Functions for 1) downloading demographic data into a csv file
and 2) generate a dot density layer for this demographic data

run via...

from utils import dl_demographics
dl_demographics.dl("Boston")
dl_demographics.dots("Boston")

'''

import pandas as pd
import geopandas as gpd
import shapely
import random
import csv
import cenpy
import lehd

def dl(region):
    """
    Downloads data from the ACS from
    https://www.census.gov/data/developers/data-sets/acs-5year.html
    and LEHD data from
    https://lehd.ces.census.gov/data/
    """

    county_id_path = "data/nationwide_data/county_ids.csv"

    block_group_ids_path = "data/" + region + "/input/boundary_data/" + "block_group_pts.csv"

    esential_worker_path = "data/nationwide_data/essential_share_LEHD.csv"

    out_data_path = "data/" + region + "/input/population_data/demographics.csv"

    # variables to download from the ACS
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

    # variables which are only available at the CT level
    variables_ct = [
        'B08201_001E', # total hhlds
        'B08201_002E' # hhlds without vehicles
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

    # bringing in the data that are only at CT

    dfid = pd.read_csv(block_group_ids_path, dtype=str)



    dfid = dfid[["GEOID"]]
    dfid["GEOID"] = dfid["GEOID"].astype(str)
    dfid["CT"] = dfid["GEOID"].str[:-1]

    df = pd.merge(df,dfid,left_on ="geoid", right_on ="GEOID", how = "left")




    # loop over each county, downloading data from census
    dfs = []
    for county in county_ids['county_id'].to_list():
        data = conn.query(variables_ct, geo_unit = 'tract', geo_filter = {"state": county[:2],"county": county[2:]})
        dfs.append(data)

    dfs = pd.concat(dfs)
    dfs["CT"] = dfs["state"] + dfs["county"] + dfs["tract"]

    # recoding variables
    dfs["hhld_total_ct"] = dfs["B08201_001E"]
    dfs["hhld_nocar"] = dfs["B08201_002E"]

    dfs = dfs[["CT","hhld_total_ct","hhld_nocar"]]

    df = pd.merge(df, dfs, left_on = "CT", right_on = "CT")

    df["hhld_nocar"] = df["hhld_nocar"].astype(int) * df["hhld_total"].astype(int) / df["hhld_total_ct"].astype(int)

    df["hhld_nocar"] = df["hhld_nocar"].fillna(0)

    df["hhld_nocar"] = df["hhld_nocar"].astype(int)

    df.drop(["hhld_total_ct","CT","GEOID"], inplace=True, axis=1)




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
    dfl["workers_essential"] = dfl["workers_essential"].round(0)

    dfl = dfl[["h_geoid_BG","workers_all","workers_essential"]]


    # merge and save the output
    df = pd.merge(df,dfl,left_on ="geoid", right_on ="h_geoid_BG", how = "left")
    del df["h_geoid_BG"]




    df.to_csv(out_data_path, index = False)


def dots(region):

    # variables for generating dots
    demo_vars = ['pop_total','pop_white','pop_black','pop_asiapacific','pop_hispanic','pop_indig','pop_otherrace','pop_poverty','hhld_total','hhld_total_w_chld','hhld_single_mother','hhld_nocar','workers_all','workers_essential']
    # number of dots to generate per count in each variable
    demo_dot_counts = [100,100,100,100,100,100,100,100,50,50,50,50,50,50]

    # paths
    block_group_path = "data/" + region + "/input/boundary_data/block_group_poly.geojson"
    demo_data_path = "data/" + region + "/input/population_data/"
    demo_file_name = "demographics.csv"
    dots_file_name = "demo_dots.csv"

    # function for generating random dot in polygon (from StackExchange)
    def generate_random(number, polygon):
        points = []
        minx, miny, maxx, maxy = polygon.bounds
        while len(points) < number:
            pnt = shapely.geometry.Point(random.uniform(minx, maxx), random.uniform(miny, maxy))
            if polygon.contains(pnt):
                points.append(pnt)
        return points

    # reading in demographic and polygon data
    gdf = gpd.read_file(block_group_path)
    gdf["GEOID"] = gdf["GEOID"].astype(str)
    dfd = pd.read_csv(demo_data_path + demo_file_name, dtype=str)
    dfd["geoid"] = dfd["geoid"].astype(str)
    gdf = pd.merge(gdf,dfd, left_on = "GEOID", right_on = "geoid")

    # loop over each row in the demographic file
    output = [["geoid","x","y","var","N","dots_per_N","n_dots"]]
    for index, row in gdf.iterrows():
        i = 0
        # loop over each variable, getting number of dots to generate
        while i < len(demo_vars):
            var = demo_vars[i]
            N = float(row[demo_vars[i]])
            dots_per_N = float(demo_dot_counts[i])
            try:
                n_dots = round(N/dots_per_N)
            except:
                n_dots = 0
            n = 0
            # generate dots
            while n < n_dots:
                pts = generate_random(1,row["geometry"])
                x = round(pts[0].x,5)
                y = round(pts[0].y,5)
                output.append([row["geoid"],x,y,var,N,dots_per_N,n_dots])
                n += 1
            i += 1

    # write the output to CSV
    with open(demo_data_path + dots_file_name, 'w') as csvfile:
        writer = csv.writer(csvfile)
        for row in output:
            writer.writerow(row)
