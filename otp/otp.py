#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 29 15:15:31 2020

@author: Rick
"""

import geopandas as gpd
import pandas as pd
import requests
import json
import csv
from datetime import datetime 
from time import sleep
import os
import urllib
from urllib.request import urlopen
import subprocess
from shutil import copyfile
from shutil import move

class study_area:
    class boston:
        county_ids = ["25017", "25025", "25009", "25021", "25023", "25005", "25027", "33015", "33017"]
        region = 'Boston'
        
class geometry:
    def boundaries(region, county_ids, **kwargs):
        
        # flag to indicate whether the data should be returned in memory
        in_memory = kwargs.get('in_memory', False)
        
        # get the geometry for counties
        gurl = " https://gist.githubusercontent.com/jamaps/6d94faec77dde5df0f4cdf8d5018ca9d/raw/7d65b613cee762f7224baf60db33b00e903aee1c/US_counties_wgs84.geojson"
        gdf_counties = gpd.read_file(urlopen(gurl))
        gdf_counties = gdf_counties[gdf_counties.GEOID.isin(county_ids)]
        gdf_counties.to_file("../spatial/" + region + "_counties.geojson", driver='GeoJSON')
    
        # define the region boundaries
        gdf_region = gdf_counties.dissolve(by="LSAD")
        gdf_region["name"] = region 
        gdf_region = gdf_region[["geometry","name"]]
        gdf_region.to_file("../spatial/" + region + "_boundary.geojson", driver='GeoJSON')
        
        if in_memory == True:
            return gdf_region
        else:
            gdf_region.to_file("../spatial/" + region + "_boundary.geojson", driver='GeoJSON')
        
        
        
    def block_groups(region, county_ids):
        
  
        
        # download and into a pandas dataframe
        burl = "https://www2.census.gov/geo/docs/reference/cenpop2010/blkgrp/CenPop2010_Mean_BG.txt"
        pd_block_groups = pd.read_csv(urlopen(burl))
        
        # string for the full county GEO ID
        pd_block_groups["county_geoid_temp"] = 1000 * pd_block_groups["STATEFP"] + pd_block_groups["COUNTYFP"]
        pd_block_groups["CGEOID"] = pd_block_groups["county_geoid_temp"].astype(str)
        pd_block_groups["CGEOID"][pd_block_groups["county_geoid_temp"] < 10000] = "0" + pd_block_groups["CGEOID"]
        
        # subset to just our counties
        pd_block_groups = pd_block_groups[pd_block_groups.CGEOID.isin(county_ids)]
        
        # string for the full block group GEO ID
        pd_block_groups["block_geoid_temp"] = 1000 * 10000000 * pd_block_groups["STATEFP"] + 10000000 * pd_block_groups["COUNTYFP"] + 10 * pd_block_groups["TRACTCE"] + pd_block_groups["BLKGRPCE"] 
        pd_block_groups["GEOID"] = pd_block_groups["block_geoid_temp"].astype(str)
        pd_block_groups["GEOID"][pd_block_groups["block_geoid_temp"] < 100000000000] = "0" + pd_block_groups["GEOID"]
        
        # delete the temp columns
        del pd_block_groups["block_geoid_temp"]
        del pd_block_groups["county_geoid_temp"]
        
        # remove the block groups with an ID of 0, these pertain to those in non-tracted area (usually a waterbody)
        pd_block_groups = pd_block_groups[pd_block_groups["BLKGRPCE"] > 0]

        
        burl = "https://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_bg_500k.zip"
        gdf_block_groups_poly = gpd.read_file(burl)
        
        # add a field for the county
        gdf_block_groups_poly["CGEOID"] = gdf_block_groups_poly["STATEFP"] + gdf_block_groups_poly["COUNTYFP"]
        
        # subset to just our counties
        gdf_block_groups_poly = gdf_block_groups_poly[gdf_block_groups_poly.CGEOID.isin(county_ids)]
        
        # remove the block groups with an ID of 0, these pertain to those in non-tracted area (usually a waterbody)
        gdf_block_groups_poly = gdf_block_groups_poly[gdf_block_groups_poly["BLKGRPCE"] != "0"]
    
        
        
    def osm_bounds(region, county_ids, **kwargs):

        file = kwargs.get('file', True)
        raw = kwargs.get('raw', False)
        
        if file == True:
            gdf_boundary = gpd.read_file("../spatial/" + region + "_boundary.geojson")
        else:
            gdf_boundary = geometry.boundaries(region, county_ids, in_memory = True)
        
        xmin = float(gdf_boundary.bounds.minx) - 0.05
        xmax = float(gdf_boundary.bounds.maxx) + 0.05
        ymin = float(gdf_boundary.bounds.miny) - 0.05
        ymax = float(gdf_boundary.bounds.maxy) + 0.05

        command_for_clip = "osmconvert us-latest.osm.pbf -b=" + str(xmin) + "," + str(ymin) + "," + str(xmax) + "," + str(ymax) + " -o=" + region + ".osm.pbf"
        
        if raw == True:
            return [xmin, xmax, ymin, ymax]
        else:
            return (command_for_clip)

class build:
    
    def build_otp(input_date):
        
        result = subprocess.run(['java', '-Xmx8G', '-jar', 'otp-1.4.0-shaded.jar', '--build', '../otp/otp_input',
                        '--analyst'], stdout=subprocess.PIPE)

        with open('build_otp_log.txt', 'w') as f:
            f.truncate()
            f.write(result.stdout.decode())
            f.close()
        
        with open('build_otp_log.txt', 'r') as f:     
            last_line = f.readlines()[-1]
            f.close()
            
        # make sub directory for the Graphs
        os.makedirs("graphs/graph-" + input_date, exist_ok=True)
        
        # move it!
        move("otp_input/Graph.obj", "graphs/graph-" + input_date + "/Graph.obj")
        
        os.makedirs('../gtfs/gtfs-'+input_date, exist_ok=True)
        
        # Move GTFS to archive
        gtfs_files = os.listdir("otp_input")
        for file in gtfs_files:
            if ".zip" in file:
                move('otp_input/' + file, '../gtfs/gtfs-'+input_date+'/' + file)
        
            
        return last_line


