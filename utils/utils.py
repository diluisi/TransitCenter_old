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
import configparser
import os, os.path


if os.path.isfile('config.cfg'):
    config = configparser.ConfigParser()
    config.read('config.cfg')
else:
    config = configparser.ConfigParser()
    config.read('../config.cfg')
    
class county_ids:


    def get_county_ids(region,  **kwargs):
        '''
        
        Reads the county ids csv and returns ids for inputted region.

        Parameters
        ----------
        region : string
            name of region
        names : boolean
            returns names of counties if specified

        Returns
        -------
        data : list
            list containing county ids
            If names is specified, format will be a nested list.

        '''

        
        # reads file
        data = pd.read_csv(config['General']['county_ids'], converters={'county_id': lambda x: str(x)})
        names = kwargs.get('names', False)

        df = data[(data['region_name'] == region)]

        
        if names == True:
            temp = df[['county_id', 'county_name']]
            lst = []
            for index, rows in temp.iterrows():
                lst.append(list(rows))
            return lst
        else: 
            return list(df['county_id'])
    

class geometry:
    def boundaries(region, county_ids, **kwargs):
        '''
        

        Parameters
        ----------
        region : string
            region name
        county_ids : string
            list of county ids
        in_memory : boolean
            True if the output should be returned instead of being written as a file

        Returns
        -------
        gdf_region : is in_memory is True, will return the results as a GeoDataframe

        '''
        
        # flag to indicate whether the data should be returned in memory
        in_memory = kwargs.get('in_memory', False)
        
        # get the geometry for counties
        gurl = "https://gist.githubusercontent.com/jamaps/6d94faec77dde5df0f4cdf8d5018ca9d/raw/7d65b613cee762f7224baf60db33b00e903aee1c/US_counties_wgs84.geojson"
        gdf_counties = gpd.read_file(urlopen(gurl))
        gdf_counties = gdf_counties[gdf_counties.GEOID.isin(county_ids)]
        gdf_counties.to_file(config[region]['county_boundaries'], driver='GeoJSON')
    
        # define the region boundaries
        gdf_region = gdf_counties.dissolve(by="LSAD")
        gdf_region["name"] = region 
        gdf_region = gdf_region[["geometry","name"]]
        
        # dissolving for Washington DC bug
        gdf_region = gdf_region.dissolve(by= 'name', aggfunc='sum')
        
        
    
        
        if in_memory == True:
            return gdf_region
        else:
            gdf_region.to_file(config[region]['region_boundary'], driver='GeoJSON')
        
        
        
    def block_groups(region, county_ids):
        '''
        

        Parameters
        ----------
        region : string
            region name
        county_ids : string
            list of county ids

        Returns
        -------
        None.
            Will write outputs to a file

        '''
  
        
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
        
        pd_block_groups.to_csv(config[region]['block_group_points'])
        
        gdf_block_groups_poly.to_file(config[region]['block_group_polygons'], driver='GeoJSON')
    
    def tracts(region, county_ids):
        '''
        

        Parameters
        ----------
        region : string
            region name
        county_ids : string
            list of county ids

        Returns
        -------
        None.
            Will write outputs to a file

        '''
  
        
        # download and into a pandas dataframe
        burl = "https://www2.census.gov/geo/docs/reference/cenpop2010/tract/CenPop2010_Mean_TR.txt"
        pd_tracts = pd.read_csv(urlopen(burl))
        
        # string for the full county GEO ID
        pd_tracts["county_geoid_temp"] = 1000 * pd_tracts["STATEFP"] + pd_tracts["COUNTYFP"]
        pd_tracts["CGEOID"] = pd_tracts["county_geoid_temp"].astype(str)
        pd_tracts["CGEOID"][pd_tracts["county_geoid_temp"] < 10000] = "0" + pd_tracts["CGEOID"]
        
        # subset to just our counties
        pd_tracts = pd_tracts[pd_tracts.CGEOID.isin(county_ids)]
        
        # string for the full tract group GEO ID
        pd_tracts["tract_geoid_temp"] = 1000 * 1000000 * pd_tracts["STATEFP"] + 1000000 * pd_tracts["COUNTYFP"] + pd_tracts["TRACTCE"] 
        pd_tracts["GEOID"] = pd_tracts["tract_geoid_temp"].astype(str)
        pd_tracts["GEOID"][pd_tracts["tract_geoid_temp"] < 10000000000] = "0" + pd_tracts["GEOID"]
        
        # delete the temp columns
        del pd_tracts["tract_geoid_temp"]
        del pd_tracts["county_geoid_temp"]
        
        # remove the tracts with an ID of 0, these pertain to those in non-tracted area (usually a waterbody)
        pd_tracts = pd_tracts[pd_tracts["TRACTCE"] > 0]

        
        burl = "https://www2.census.gov/geo/tiger/GENZ2019/shp/cb_2019_us_tract_500k.zip"
        gdf_tracts_poly = gpd.read_file(burl)
        
        # add a field for the county
        gdf_tracts_poly["CGEOID"] = gdf_tracts_poly["STATEFP"] + gdf_tracts_poly["COUNTYFP"]
        
        # subset to just our counties
        gdf_tracts_poly = gdf_tracts_poly[gdf_tracts_poly.CGEOID.isin(county_ids)]
        
        # remove the block groups with an ID of 0, these pertain to those in non-tracted area (usually a waterbody)
        gdf_tracts_poly = gdf_tracts_poly[gdf_tracts_poly["TRACTCE"] != "0"]
        
        pd_tracts.to_csv(config[region]['tract_points'])
        
        gdf_tracts_poly.to_file(config[region]['tract_polygons'], driver='GeoJSON')
        
        
    def osm_bounds(region, county_ids, **kwargs):
        '''
        

        Parameters
        ----------
        region : string
            region name
        county_ids : string
            list of county ids
        file : boolean
            False to call the boundaries function instead of reading the file from the disk.
        raw : boolean
            True if only the coordinates of the bounding box is wanted instead of the osmconvert command

        Returns
        -------
        command : string
            command for the osmconvert shell command
        coordinate : list
            lat/lon coordinates in the form of xmin, xmax, ymin, ymax

        '''

        file = kwargs.get('file', True)
        raw = kwargs.get('raw', False)
        
        if file == True:
            gdf_boundary = gpd.read_file(config[region]['region_boundary'])
        else:
            gdf_boundary = geometry.boundaries(region, county_ids, in_memory = True)
        
        # adds buffer to the bounds
        xmin = float(gdf_boundary.bounds.minx) - 0.05
        xmax = float(gdf_boundary.bounds.maxx) + 0.05
        ymin = float(gdf_boundary.bounds.miny) - 0.05
        ymax = float(gdf_boundary.bounds.maxy) + 0.05

        command_for_clip = 'osmconvert '+ '"' + config['General']['us_osm'] +'"' +' -b=' + str(xmin) + "," + str(ymin) + "," + str(xmax) + "," + str(ymax) + " -o=" + '"'+config[region]['osm'] + region + ".osm.pbf"+'"'
        
        if raw == True:
            return [xmin, xmax, ymin, ymax]
        else:
            os.system(command_for_clip)

            return (command_for_clip)
