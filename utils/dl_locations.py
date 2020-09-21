"""

(still in progress)

The functions in this class downloads destination data for different study regions,
based on their county IDs. It then links these data to census block groups based
on their GEOID or based on their spatial location. Requiremets are the data_util.cfg
file noting the where data will be downloaded to, and a of course a working internet
connection. Example for running is as follows

# setup, init
regionDL = get_destination_data("Boston")

# download LEHD employment data
regionDL.get_employment()

# join healthcare data to blocks groups
regionDL.get_healthcare()

# join SNAP grocery store data to block groups
regionDL.get_groceries()

# download and join greenspace and parks from OSM to block groups
regionDL.get_parks()

"""


import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import urllib.request
import gzip
import shutil
import os
import json
import requests
import sys
import overpy
import shapely.geometry as geometry
from shapely.ops import linemerge, unary_union, polygonize
import geopandas as gpd
import utils


class get_destination_data:

    # initializing
    def __init__(self,region):
        self.region = region

        # load in file paths
        with open('data_folder.cfg', 'r') as d:
            d = d.read()
        self.data_paths = json.loads(d)

        # file paths for nationwide_data
        self.data_folder_path = self.data_paths["folder_path"] + self.data_paths["folder_name"]
        self.region_folder_path = self.data_folder_path + self.data_paths["contents"]["region_data"]["folder_name"][self.region]
        self.input_data_path = self.region_folder_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["folder_name"]
        print("We will be saving data to ", self.input_data_path)


        # paths for nationwide_data thats needed
        state_FIP_codes_path = self.data_folder_path + self.data_paths["contents"]["nationwide_data"]["folder_name"] + self.data_paths["contents"]["nationwide_data"]["contents"]["state_FIP_codes"]
        county_ids_path = self.data_folder_path + self.data_paths["contents"]["nationwide_data"]["folder_name"] + self.data_paths["contents"]["nationwide_data"]["contents"]["county_ids"]

        # grab info needed by county for downloading data
        pdc = pd.read_csv(county_ids_path)
        pdc = pdc[pdc["region_name"] == region]
        self.region_info = pdc

        # get state IDs
        self.region_info["state"] = (self.region_info["county_id"] / 1000).astype(int)

        # our counties have numeric FIP codes, while the LEHD data has alpha codes, so we need to make a link to download data just four our states
        state_FIP_codes = pd.read_csv(state_FIP_codes_path)
        state_FIP_codes = state_FIP_codes[["Numeric code", "Alpha code"]]
        self.region_info = self.region_info.merge(state_FIP_codes, how = 'inner', left_on = "state", right_on = "Numeric code")


    # function for downloading employment data from LEHD and summarizing by block group
    def get_employment(self):

        print("Downloading LEHD employment data and joining to block groups")

        # output dataframe
        dfo = None

        # set up path for where to download and store the data
        file_path = self.region_folder_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["folder_name"] + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["destination_data"]["folder_name"]
        output_file_path = file_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["destination_data"]["contents"]["employment_lehd"]

        # download LEHD workplace data for each state.
        for state in self.region_info["Alpha code"].unique():

            # setting up the file naming needed for downloading
            zip_file_name = state.lower() + "_wac_S000_JT00_2017.csv.gz"
            csv_file_name = state.lower() + "_wac_S000_JT00_2017.csv"
            url_name = "https://lehd.ces.census.gov/data/lodes/LODES7/" + state.lower() + "/wac/" + zip_file_name

            # print(file_path + zip_file_name)

            # downloading
            urllib.request.urlretrieve(url_name, file_path + zip_file_name)

            # unzipings
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
        dfo.to_csv(output_file_path)



    def get_healthcare(self):

        # set up path for where to download and store the data
        file_path = self.region_folder_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["folder_name"] + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["destination_data"]["folder_name"]
        output_file_path = file_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["destination_data"]["contents"]["healthcare"]
        block_group_poly_path = self.input_data_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["boundary_data"]["folder_name"] + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["boundary_data"]["contents"]["block_group_polygons"]


        # download the data

        url_hospital = 'https://opendata.arcgis.com/datasets/6ac5e325468c4cb9b905f1728d6fbf0f_0.geojson'
        urllib.request.urlretrieve(url_hospital, '../data/General/hospital.geojson')

        url_urgent_care = 'https://opendata.arcgis.com/datasets/335ccc7c0684453fad69d8a64bc89192_0.geojson'
        urllib.request.urlretrieve(url_urgent_care, '../data/General/urgent_care.geojson')

        url_rx = 'https://rxopen.org/api/v1/map/download/facility'
        urllib.request.urlretrieve(url_rx, '../data/General/pharmacy.csv')

        # load the geom data

        block_geom = gpd.read_file(block_group_poly_path)

        hospital_geom = gpd.read_file('../data/General/hospital.geojson')
        urgent_care_geom = gpd.read_file('../data/General/urgent_care.geojson')
        rx = pd.read_csv('../data/General/pharmacy.csv')


        hospital_geom = gpd.sjoin(hospital_geom, block_geom, how="inner", op='intersects')
        urgent_care_geom = gpd.sjoin(urgent_care_geom, block_geom, how="inner", op='intersects')

        # count facilities
        hospital = hospital_geom.groupby(['GEOID']).count()[['FID']]
        hospital.columns = ['hospitals']
        urgent_care = urgent_care_geom.groupby(['GEOID']).count()[['FID']]
        urgent_care.columns = ['urgent_care_facilities']


        # converting lat lon to geom
        rx = pd.read_csv('pharmacy.csv')
        rx = pd.concat([rx, rx['CalcLocation'].str.split(',', expand=True)], axis=1)
        rx['lon'] = rx[0].astype(float)
        rx['lat'] = rx[1].astype(float)

        rx_geom = gpd.GeoDataFrame(
            rx, geometry=gpd.points_from_xy(rx['lat'], rx['lon']))

        # count facilities
        rx_geom = gpd.sjoin(rx_geom, block_geom, how="inner", op='intersects')
        rx_block = rx_geom.groupby(['GEOID']).count()[['Name']]
        rx_block.columns = ['pharmacies']
    

        healthcare = hospital.join(rx_block, how = 'outer')
        healthcare = healthcare.join(urgent_care, how = 'outer')
        healthcare = healthcare.fillna(0)

        healthcare.to_csv(output_file_path)
    
    def get_education(self):
        # set up path for where to download and store the data
        file_path = self.region_folder_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["folder_name"] + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["destination_data"]["folder_name"]
        output_file_path = file_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["destination_data"]["contents"]["education"]
        block_group_poly_path = self.input_data_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["boundary_data"]["folder_name"] + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["boundary_data"]["contents"]["block_group_polygons"]

        url_university = 'https://opendata.arcgis.com/datasets/0d7bedf9d582472e9ff7a6874589b545_0.geojson'
        urllib.request.urlretrieve(url_university, '../data/General/university.geojson')

        url_supp_college = 'https://opendata.arcgis.com/datasets/284d5c00b0d046e18eddff4017927dd1_0.geojson'
        urllib.request.urlretrieve(url_supp_college, '../data/General/supp_college.geojson')

        # load the geom data

        block_geom = gpd.read_file(block_group_poly_path)


        university_geom = gpd.read_file('../data/General/university.geojson')
        supp_college_geom = gpd.read_file('../data/General/supp_college.geojson')

        university_geom = gpd.sjoin(university_geom, block_geom, how="inner", op='intersects')
        supp_college_geom = gpd.sjoin(supp_college_geom, block_geom, how="inner", op='intersects')

        # count facilities
        university = university_geom.groupby(['GEOID']).count()[['OBJECTID']]
        university.columns = ['university']
        supp_college = supp_college_geom.groupby(['GEOID']).count()[['OBJECTID']]
        supp_college.columns = ['supp_college']


        edu_temp = university.join(supp_college, how = 'outer')
        edu_temp = edu_temp.fillna(0)
        edu_temp['count'] = edu_temp['university'] + edu_temp['supp_college']
        education = edu_temp['count']

        education.to_csv(output_file_path)



    # joining SNAP grocery stores by block groups and counting occurances
    def get_groceries(self):

        print("Joining SNAP grocery stores by block groups and counting the number of stores per block group")

        # set paths for input data
        all_snap_file_path = self.data_folder_path + self.data_paths["contents"]["nationwide_data"]["folder_name"] + self.data_paths["contents"]["nationwide_data"]["contents"]["snap_retailers_usda"]
        block_group_poly_path = self.input_data_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["boundary_data"]["folder_name"] + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["boundary_data"]["contents"]["block_group_polygons"]

        # folder to where we are going to save the output data
        output_file_path = self.input_data_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["destination_data"]["folder_name"] + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["destination_data"]["contents"]["groceries_snap"]

        # load in the SNAP data
        snap = pd.read_csv(all_snap_file_path)

        # subset just for our counties
        snap = snap[snap['state'].isin(self.region_info["Alpha code"])]

        # subset just for 2019
        snap = snap[(snap['Y2019'] == 1)]

        # reduce size of dataframe
        snap = snap[["store_id","Y2019","X","Y"]]

        # turn into a geodataframe
        snap_geometry = [Point(xy) for xy in zip(snap.X, snap.Y)]
        snap = snap.drop(['X', 'Y'], axis=1)
        crs = 'epsg:4269'
        snap = gpd.GeoDataFrame(snap, crs=crs, geometry=snap_geometry)

        # load in block group polygons
        bgpoly = gpd.read_file(block_group_poly_path)

        # spatial join the block groups to the points
        snap = gpd.sjoin(snap, bgpoly, how="inner", op='intersects')

        # group by block_group_id and count the number of jobs
        snap = snap.groupby(['GEOID']).sum()[["Y2019"]]

        # update the column names
        snap.columns = ["snap"]

        # write to file
        snap.to_csv(output_file_path)


    def get_parks(self):

        file_path = self.region_folder_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["folder_name"] + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["destination_data"]["folder_name"]
        output_file_path = file_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["destination_data"]["contents"]["greenspace"]
        
        block_group_poly_path = self.input_data_path + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["boundary_data"]["folder_name"] + self.data_paths["contents"]["region_data"]["contents"]["input"]["contents"]["boundary_data"]["contents"]["block_group_polygons"]


        region = self.region
        county_ids = utils.county_ids.get_county_ids(region)
        xmin, xmax, ymin, ymax = utils.geometry.osm_bounds(region, county_ids, file = False, raw = True)

        bbox = (ymin, xmin, ymax, xmax)

        api = overpy.Overpass()

        query = """ 
        [out:json];
        (

        way["leisure"="park"]{0};
        way["leisure"="nature_reserve"]{0};
        way["leisure"="playground"]{0};
        way["leisure"="garden"]{0};
        way["landuse"="grass"]{0};
        way["leisure"="pitch"]{0};
        way["leisure"="dogpark"]{0};
        way["leisure"="common"]{0};
        way["natural"="wood"]{0};
        way["natural"="beach"]{0};
        way["natural"="scrub"]{0};
        way["natural"="fell"]{0};
        way["natural"="heath"]{0};
        way["natural"="moor"]{0};
        way["natural"="grassland"]{0};
        way["landuse"="recreation_ground"]{0};
        way["landuse"="allotments"]{0};
        way["landuse"="cemetery"]{0};
        way["landuse"="meadow"]{0};
        way["landuse"="orchard"]{0};
        way["landuse"="greenfield"]{0};
        way["landuse"="vineyard"]{0};
        way["landuse"="village_green"]{0};
        way["landuse"="forest"]{0};
        
        relation["leisure"="park"]{0};
        relation["leisure"="nature_reserve"]{0};
        relation["leisure"="playground"]{0};
        relation["leisure"="garden"]{0};
        relation["landuse"="grass"]{0};
        relation["leisure"="pitch"]{0};
        relation["leisure"="dogpark"]{0};
        relation["leisure"="common"]{0};
        relation["natural"="wood"]{0};
        relation["natural"="beach"]{0};
        relation["natural"="scrub"]{0};
        relation["natural"="fell"]{0};
        relation["natural"="heath"]{0};
        relation["natural"="moor"]{0};
        relation["natural"="grassland"]{0};
        relation["landuse"="recreation_ground"]{0};
        relation["landuse"="allotments"]{0};
        relation["landuse"="cemetery"]{0};
        relation["landuse"="meadow"]{0};
        relation["landuse"="orchard"]{0};
        relation["landuse"="greenfield"]{0};
        relation["landuse"="vineyard"]{0};
        relation["landuse"="village_green"]{0};
        relation["landuse"="forest"]{0};
        );
        (._;>;);
        out;
        """.format(bbox)
        response = api.query(query)

        line = []

        for way in response.ways:
            coords = []
            for node in way.nodes:
                coords.append((node.lon, node.lat))
                
            line.append(geometry.LineString(coords))

        merged = linemerge([*line]) 
        borders = unary_union(merged) 
        polygons = list(polygonize(borders))

        parks = gpd.GeoDataFrame(geometry=gpd.GeoSeries(polygons))

        bgpoly = gpd.read_file(block_group_poly_path)

        parks_block =  gpd.overlay(parks, bgpoly, how='intersection')

        parks_block.crs = {'init' :'epsg:4326'}
        parks_block = parks_block.to_crs({'init': 'epsg:3857'})
        parks_block['area'] = parks_block.area/ 10**6 # conversion to square kilometers
        parks_block = parks_block.groupby(['GEOID']).sum()[['area']]

        parks_block.to_csv(output_file_path, index = True)
