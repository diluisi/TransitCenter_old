#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 16:53:41 2020

@author: Rick
"""

import requests
import csv
from time import sleep
import os
import urllib
import pandas as pd
from shapely.geometry import Point, Polygon
import time
import datetime
from datetime import datetime
from datetime import timedelta  
import geopandas 

class get:
    def transit_land(region, county_ids, input_date, xmin, xmax, ymin, ymax):
        
        
        
        # date into an in order to properly check dates
        
        
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
                
    def transit_feeds(region, county_ids, input_date, xmin, xmax, ymin, ymax):
        
        #TODO config file
        key = 'f2a91a7e-154d-434a-8083-2cd18e25f3d2'

        coords = [(xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin)]
        poly = Polygon(coords)
        
        s = requests.Session()
        
        url = 'https://api.transitfeeds.com'
        gtfs_url = 'https://transitfeeds.com/p/'
        
        response = s.get(
            url+'/v1/getLocations',
            params = {'key': key
               
            }
        )
        locations = response.json()['results']['locations']
        sleep(15)
        
        # filters locations to only those within bounding box

        location_ids = []
        for operator in locations:
            pt = Point(operator['lng'], operator['lat'])
            if pt.within(poly) is True: 
                location_ids.append(operator['id'])
        
        os.makedirs("../gtfs/feeds_" + input_date, exist_ok=True)
        
        # loops through all locations

        id_lst = []
        for ids in location_ids:
            for attempt in range(4):
                try: 
                    response = s.get(
                        url+'/v1/getFeeds',
                        params = {'key': key, 'location' : ids, 'type': 'gtfs', 'limit': 200
        
                        }
                    )
                    feeds = response.json()['results']['feeds']
                    for agencies in feeds:
                        ts = agencies['latest']['ts']
                        dt_str = str(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d'))
                        name = agencies['t']
                        loc = agencies['l']['t']
                        dt_fetched = str(datetime.utcfromtimestamp(ts).strftime('%Y%m%d'))
                        id_lst.append(list([agencies['id'], dt_fetched, name, loc, dt_str]))
                    sleep(1)
                    break
                except:
                    print(attempt)
                    sleep(10)
                    
        for agency in id_lst:
            print(agency[0])
            
            # to account for timezone issues
            for attempt in range(3):
                try:
                    if attempt == 0:
                        url = gtfs_url + agency[0] + '/' + agency[1] + '/download'
                        urllib.request.urlretrieve(url, "../gtfs/feeds_" + input_date + 
                                               "/" + agency[2] + '-' + input_date + ".zip")
                        break
                    elif attempt == 1:
                        bkwd_dt = str((datetime.strptime(id_lst[0][1], '%Y%m%d') + timedelta(days=1)).date().strftime('%Y%m%d'))   
                        url = gtfs_url + agency[0] + '/' + bkwd_dt + '/download'
                        urllib.request.urlretrieve(url, "../gtfs/feeds_" + input_date + 
                                               "/" + agency[2] + '-' + input_date + ".zip")
                        break
                    elif attempt == 2:
                        fwd_dt = str((datetime.strptime(id_lst[0][1], '%Y%m%d') - timedelta(days=1)).date().strftime('%Y%m%d'))   
                        url = gtfs_url + agency[0] + '/' + fwd_dt + '/download'
                        urllib.request.urlretrieve(url, "../gtfs/feeds_" + input_date + 
                                               "/" + agency[2] + '-' + input_date + ".zip")
                        break
                    else:
                        print('Skipping due to error')
                except:
                    pass
        
        #TODO reformat output csv
        with open("../gtfs/feeds_" + input_date + "/" + region + "_feed_info_" + input_date + ".csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            for row in id_lst:
                writer.writerow(row)
    