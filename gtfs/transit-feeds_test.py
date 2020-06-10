#!/usr/bin/env python
# coding: utf-8


import requests
import csv
import os
import urllib
import sys
import importlib
import pandas as pd
from shapely.geometry import Point, Polygon
import time
import datetime
from datetime import datetime
from time import sleep
import geopandas 

sys.path.insert(1, '../utils')

import utils



s = requests.Session()




region = 'New York'
extent = 'core'
county_ids = utils.county_ids.get_county_ids(region, extent)
input_date = '2020-05-01'
input_ts = time.mktime(datetime.strptime(input_date, "%Y-%m-%d").timetuple())
xmin, xmax, ymin, ymax = utils.geometry.osm_bounds(region, county_ids, extent, file = False, raw = True)

url = 'https://api.transitfeeds.com'



# bounding box

coords = [(xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin)]
poly = Polygon(coords)



# api key

#key = '9efe3e4d-9b04-467c-9c72-0cc3cf666fd0'
key = 'f2a91a7e-154d-434a-8083-2cd18e25f3d2'



output_feed_info = [list(['operator_name', 'operator_website', 'operator_location', 'operator_metro', 'feed_id', 'date_query', 
                         'date_fetched', 'earliest_calendar_date', 'latest_calendar_date', 'transit_feeds_url'])]



# gets all available locations on transit feeds

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



# loops through all locations
error_count = 0
id_lst = []
for ids in location_ids:
    for attempt in range(4):
        try: 
            response = s.get(
                url+'/v1/getFeeds',
                params = {'key': key, 'location' : ids, 'type': 'gtfs', 'limit': 100

                }
            )
            feeds = response.json()['results']['feeds']
            for agencies in feeds:
                id_lst.append(agencies['id'])
            sleep(1)
            break
        except:
            print(attempt)
            sleep(60)

# check for duplicates
id_lst = list(dict.fromkeys(id_lst))


import test

for row in id_lst:
    print(row)
    for attempt in range(10):

        try:
            
            with requests.Session() as s:
                response = s.get(
                    url+'/v1/getFeedVersions',
                    params = {'key': key, 'feed' : row, 'limit' : 10
    
                    }
                )
                sleep(1)
            feed_info = response.json()['results']['versions']
            
            
            break
        except:
            print(attempt)
            sleep(1)
            
            
    for version in feed_info:

    # checks latest version to input date

        if input_ts > version['ts']:

            # writes table for metadata

            op_name = version['f']['t']
            op_url = version['f']['u']['i']
            op_loc = version['f']['l']['t']
            op_id = version['f']['id']
            date_fetched = str(datetime.utcfromtimestamp(version['ts']).strftime('%Y-%m-%d'))
            earliest_calendar_date = str(datetime.strptime(version['d']['s'], '%Y%m%d').date())
            latest_calendar_date =  str(datetime.strptime(version['d']['f'], '%Y%m%d').date()) 
            url = version['url']
            temp = list([op_name, op_url, op_loc, region, op_id, input_date, date_fetched, earliest_calendar_date,
                       latest_calendar_date, url])
            output_feed_info.append(temp)
            

            # downloads gtfs

            urllib.request.urlretrieve(feed_info[1]['url'], "../gtfs/feeds_" + input_date + "/" + op_name + '-' + input_date + ".zip")
            break


   





