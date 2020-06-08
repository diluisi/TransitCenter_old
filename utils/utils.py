#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 10:36:52 2020

@author: Rick
"""

import geopandas as gpd
import pandas as pd
import urllib
from urllib.request import urlopen

class county_ids:
    def get_county_ids(region, extent, **kwargs):
        
        data = pd.read_csv('data/county_ids.csv')
        names = kwargs.get('names', False)
        data['county_id'] = data['county_id'].astype(str)
        
        
        if extent == 'core':
            df = data[(data['region_name'] == region) & (data['core'] == True)]
        elif extent == 'msa':
            df = data[(data['region_name'] == region) & (data['msa'] == True)]
        elif extent == 'region':
            df = data[(data['region_name'] == region) & (data['region'] == True)]
        elif extent == 'all':
            df = data[(data['region_name'] == region)]
        else:
            return 'Not Valid Extent'
        
        if names == True:
            temp = df[['county_id', 'county_name']]
            lst = []
            for index, rows in temp.iterrows():
                lst.append(list(rows))
            return lst
        else: 
            return list(df['county_id'])
    

class geometry:
    def boundaries(region, county_ids, extent, **kwargs):
        
        # flag to indicate whether the data should be returned in memory
        in_memory = kwargs.get('in_memory', False)
        
        # get the geometry for counties
        gurl = "https://gist.githubusercontent.com/jamaps/6d94faec77dde5df0f4cdf8d5018ca9d/raw/7d65b613cee762f7224baf60db33b00e903aee1c/US_counties_wgs84.geojson"
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
            gdf_region.to_file("../spatial/" + region + '_' + extent + "_boundary.geojson", driver='GeoJSON')
        
        
        
    def block_groups(region, county_ids, extent):
        
  
        
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
        
        pd_block_groups.to_csv("../spatial/" + region + '_' + extent + "_block_group_pts.csv")
        
        gdf_block_groups_poly.to_file("../spatial/" + region + '_' + extent +  "_block_group_poly.geojson", driver='GeoJSON')
    
        
        
    def osm_bounds(region, county_ids, extent, **kwargs):

        file = kwargs.get('file', True)
        raw = kwargs.get('raw', False)
        
        if file == True:
            gdf_boundary = gpd.read_file("../spatial/" + region + '_' + extent +  "_boundary.geojson")
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
