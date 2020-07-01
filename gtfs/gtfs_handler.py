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
import configparser
from gtfslite import GTFS
import shutil

<<<<<<< Updated upstream
class get:

    config = configparser.ConfigParser()
    config.read('../config.cfg')
=======
config = configparser.ConfigParser()
config.read('../config.cfg')

class get:
>>>>>>> Stashed changes

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
        
        output_feed_info = [["operator_name", "operator_website", "operator_metro", "feed_id", "date_fetched", "earliest_calendar_date", "latest_calendar_date", "url"]]
        for feed_info in feed_base_info:
            
            # base info
            operator_name = feed_info[1]
            operator_website = feed_info[2]
            operator_metro = feed_info[4]
            
            
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
                            output_feed_info.append([operator_name, operator_website, operator_metro, feed_id, date_fetched, earliest_calendar_date, latest_calendar_date, transitland_historical_url])
                            
                            break # break since this should be the most recent
        
                        else:
                            None
        
                        i = i - 1
            
            except:
                None
        
        # make sub directory for the GTFS
<<<<<<< Updated upstream
        os.makedirs("../gtfs/feeds_" + input_date, exist_ok=True)
        
        # write this info to a csv file, downloading the GTFS at the same time
        gtfs_zips_to_dl = []
        with open("../gtfs/feeds_" + input_date + "/" + region + "_feed_info_" + input_date + ".csv", "w") as csvfile:
=======
        os.makedirs(config[region]['gtfs_static'] + "/feeds_" + input_date, exist_ok=True)
        
        # write this info to a csv file, downloading the GTFS at the same time
        gtfs_zips_to_dl = []
        with open(config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + region + "_feed_info_" + input_date + ".csv", "w") as csvfile:
>>>>>>> Stashed changes
            writer = csv.writer(csvfile)
            for row in output_feed_info:
                writer.writerow(row)
                gtfs_zip = [row[0],row[7]]
                if gtfs_zip not in gtfs_zips_to_dl:
                    gtfs_zips_to_dl.append(gtfs_zip)
        counter = 0    
        for gtfs_zip in gtfs_zips_to_dl:
            print(gtfs_zip)
            try:
                counter = counter + 1
<<<<<<< Updated upstream
                urllib.request.urlretrieve(gtfs_zip[1], "../gtfs/feeds_" + input_date + "/" + input_date + "_" + gtfs_zip[0] + '_' + str(counter) + ".zip")
=======
                urllib.request.urlretrieve(gtfs_zip[1], config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + input_date + "_" + gtfs_zip[0] + '_' + str(counter) + ".zip")
>>>>>>> Stashed changes
                sleep(1)
            except:
                None
                
    def transit_feeds(region, county_ids, input_date, xmin, xmax, ymin, ymax):
        
<<<<<<< Updated upstream
        key = get.config['GTFS']['key']
=======
        key = config['API']['key']
>>>>>>> Stashed changes
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
        
<<<<<<< Updated upstream
        os.makedirs("../gtfs/feeds_" + input_date, exist_ok=True)
=======
        os.makedirs(config[region]['gtfs_static'] + "/feeds_" + input_date, exist_ok=True)
>>>>>>> Stashed changes
        
        # loops through all locations
        feed_info_lst = []
        
        for ids in location_ids:

                
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

                for attempt in range(3):
                    try:
                        if attempt == 0:
                            
                            api_url = gtfs_url + agencies['id'] + '/' + dt_fetched + '/download'
<<<<<<< Updated upstream
                            dir = "../gtfs/feeds_" + input_date + "/" + name + '-' + input_date + ".zip"
=======
                            dir = config[region]['gtfs_static'] + "/feeds_"+ input_date + "/" + name + '-' + input_date + ".zip"
>>>>>>> Stashed changes
                            urllib.request.urlretrieve(api_url, dir)
                            break
                        elif attempt == 1:
                            bkwd_dt = str((datetime.strptime(dt_fetched, '%Y%m%d') + timedelta(days=1)).date().strftime('%Y%m%d'))   
                            api_url = gtfs_url + agencies['id'] + '/' + bkwd_dt+ '/download'
<<<<<<< Updated upstream
                            dir = "../gtfs/feeds_" + input_date + "/" + name + '-' + input_date + ".zip"
=======
                            dir = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + name + '-' + input_date + ".zip"
>>>>>>> Stashed changes
                            urllib.request.urlretrieve(api_url, dir)
                            dt_fetched = str((datetime.strptime(dt_fetched, '%Y%m%d') + timedelta(days=1)).date().strftime('%Y-%m-%d')) 
                            
                            break
                        elif attempt == 2:
                            fwd_dt = str((datetime.strptime(dt_fetched, '%Y%m%d') - timedelta(days=1)).date().strftime('%Y%m%d'))   
                            api_url = gtfs_url + agencies['id'] + '/' + fwd_dt + '/download'
<<<<<<< Updated upstream
                            dir = "../gtfs/feeds_" + input_date + "/" + name + '-' + input_date + ".zip"
=======
                            dir = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + name + '-' + input_date + ".zip"
>>>>>>> Stashed changes
                            urllib.request.urlretrieve(api_url, dir)
                            dt_fetched = str((datetime.strptime(dt_fetched, '%Y%m%d') - timedelta(days=1)).date().strftime('%Y-%m-%d'))   
                            break
                        else:
                            print('Skipping due to error')
                            break
                    except:
                        sleep(1)
<<<<<<< Updated upstream
                dir_name = "../gtfs/feeds_" + input_date + "/" + name + '-' + input_date 
=======
                dir_name = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + name + '-' + input_date 
>>>>>>> Stashed changes
                try:
                    shutil.unpack_archive(dir_name + '.zip', dir_name)

                    stops = pd.read_csv(dir_name + '/stops.txt')
                    try:
                        os.remove(dir_name + '/pathways.txt')
                    except:
                        pass
                    try:          
                        gtfs_file = GTFS.load_zip(dir)
                        dt_st = str(gtfs_file.summary().first_date.date())
                        dt_end = str(gtfs_file.summary().last_date.date())
                        op_url = gtfs_file.agency['agency_url'][0]
                    except:
                        dt_st = None
                        dt_end = None
                        op_url = None

                    for index, row in stops.iterrows():
                        if(pd.isnull(row['stop_lat'])):
                            stops.at[index, 'stop_lat'] = 0
                            stops.at[index, 'stop_lon'] = 0


                    stops.to_csv(dir_name+'/stops.txt', index = False)

                    shutil.make_archive(dir_name, 'zip', dir_name)

                    shutil.rmtree(dir_name)
                except:
                    dt_st = None
                    dt_end = None
                    op_url = None
<<<<<<< Updated upstream
=======
                    shutil.rmtree(dir_name)
>>>>>>> Stashed changes

                feed_info_lst.append(list([name, op_url, loc, agencies['id'], dt_str, dt_st, dt_end, api_url]))

                
                
        feed_info = pd.DataFrame(feed_info_lst, columns = ['operator_name' , 'operator_url', 'operator_region', 'transit_feeds_id', 'date_fetched', 'earliest_calendar_date', 'latest_calendar_date', 'transitfeeds_url']) 
<<<<<<< Updated upstream
        feed_info.to_csv("../gtfs/feeds_" + input_date + "/" + region + "_feed_info_" + input_date + ".csv", index = False)
=======
        feed_info.to_csv(config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + region + "_feed_info_" + input_date + ".csv", index = False)
>>>>>>> Stashed changes


    