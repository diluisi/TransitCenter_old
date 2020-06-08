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
        
    class ny:
        county_ids = ['34003','36005','36031','34017','36047','34023','34025','34027','36059','36061','34031','36081','36085','36103','34039','36119']
        region = 'New York'
        

class build:
    
    def build_otp(input_date):
        
        
        # Move gtfs files
        gtfs_files = os.listdir('../gtfs/feeds_'+input_date)
        for file in gtfs_files:
            if ".zip" in file:
                move('../gtfs/feeds_'+input_date+'/' + file, 'otp_input/' + file, )
        
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
        
        os.makedirs('../gtfs/feeds_'+input_date, exist_ok=True)
        
        # Move GTFS to archive
        gtfs_files = os.listdir("otp_input")
        for file in gtfs_files:
            if ".zip" in file:
                move('otp_input/' + file, '../gtfs/feeds_'+input_date+'/' + file)
        
            
        return last_line
    def get_gtfs(region, county_ids, input_date):
    
    # date into an in order to properly check dates
        xmin, xmax, ymin, ymax = geometry.osm_bounds(region, county_ids, file = False, raw = True)
        
        input_date_int = 10000 * int(input_date[0:4]) + 100 * int(input_date[5:7]) + int(input_date[8:10])
        
        # query to find all locations within the bounding box of our region
        
        response = requests.get(
            "https://transit.land/api/v1/operators",
            params = {
                "bbox": str(xmin) + "," + str(ymin) + "," + str(xmax) + "," + str(ymax) # from the previous section
            }
        )
        all_operators_json = response.json()
        all_operators_json = all_operators_json["operators"]
            
        # loop over operators, adding unique feed info (based on onestop_id) to a list
        
        feed_base_info = []
        for operator in all_operators_json:
            for onestop_id in operator["represented_in_feed_onestop_ids"]:
                feed_base_info.append([onestop_id, operator["name"],operator["website"],operator["state"],operator["metro"],operator["timezone"]])
            
        
        # loop over feed info, getting info for each feed, and saving to an output array
        
        output_feed_info = [["operator_name", "operator_website", "operator_state", "operator_metro", "operator_timezone", "transitland_feed_id", "date_fetched", "earliest_calendar_date", "latest_calendar_date", "transitland_historical_url"]]
        for feed_info in feed_base_info:
            
            # base info
            operator_name = feed_info[1]
            operator_website = feed_info[2]
            operator_state = feed_info[3]
            operator_metro = feed_info[4]
            operator_timezone = feed_info[5]
            
            # get feed versions
            response = requests.get(
            "https://transit.land/api/v1/feed_versions",
            params = {
                "feed_onestop_id": feed_info[0],
                "per_page": 999
                }
            )        
            feeds = response.json()
            sleep(1) # to avoid API timeout
            
            # if there are feeds, find the feed that is the most recent to the input date
            try:
                nfeeds = (len(feeds["feed_versions"]))
                if nfeeds > 0:
    
                    # looping over feed versions
                    i = nfeeds - 1
                    while i >= 0: 
    
                        # grabbing date info
                        date_fetched_iso8601 = feeds["feed_versions"][i]["fetched_at"]
                        date_fetched = str(date_fetched_iso8601)[0:10]
                        date_fetched_int = 10000 * int(date_fetched[0:4]) + 100 * int(date_fetched[5:7]) + int(date_fetched[8:10])
    
                        # checking if before the input date
                        if date_fetched_int < input_date_int:
    
                            # output info
                            date_fetched = date_fetched
                            earliest_calendar_date = feeds["feed_versions"][i]["earliest_calendar_date"]
                            latest_calendar_date = feeds["feed_versions"][i]["latest_calendar_date"]
                            transitland_historical_url = feeds["feed_versions"][i]["download_url"]
                            feed_id = feeds["feed_versions"][i]["feed"]                    
                            output_feed_info.append([operator_name, operator_website, operator_state, operator_metro, operator_timezone, feed_id, date_fetched, earliest_calendar_date, latest_calendar_date, transitland_historical_url])
                            
                            break # break since this should be the most recent
    
                        else:
                            None
    
                        i = i - 1
            
            except:
                None
        
        # make sub directory for the GTFS
        os.makedirs("../gtfs/feeds_" + input_date, exist_ok=True)
        
        # write this info to a csv file, downloading the GTFS at the same time
        gtfs_zips_to_dl = []
        with open("../gtfs/feeds_" + input_date + "/" + region + "_feed_info_" + input_date + ".csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            for row in output_feed_info:
                writer.writerow(row)
                gtfs_zip = [row[5],row[9]]
                if gtfs_zip not in gtfs_zips_to_dl:
                    gtfs_zips_to_dl.append(gtfs_zip)
            
        for gtfs_zip in gtfs_zips_to_dl:
            print(gtfs_zip)
            try:
                urllib.request.urlretrieve(gtfs_zip[1], "../gtfs/feeds_" + input_date + "/" + input_date + "_" + gtfs_zip[0] + ".zip")
                sleep(1)
            except:
                None



