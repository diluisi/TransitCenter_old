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

if os.path.isfile('config.cfg'):
    config = configparser.ConfigParser()
    config.read('config.cfg')
else:
    config = configparser.ConfigParser()
    config.read('../config.cfg')
    
feed_id_lookup = pd.read_csv(config['General']['gen'] + '/feed_id_lookup.csv')

class utility:
    
    def feed_id_func(transit_land_id):
        
        feed_id = str(feed_id_lookup[feed_id_lookup['transit_land'] == transit_land_id]['feed_id'].iloc[0])
        return feed_id

class APITimeoutException(Exception):
     pass
class InvalidFeed(Exception):
     pass

class get:

    def transit_land(region, input_date, xmin, xmax, ymin, ymax):
        
        banned = pd.read_csv(config['General']['gen'] + '/banned_agencies.csv')
        
        # date into an in order to properly check dates
        
        
        input_date_int = 10000 * int(input_date[0:4]) + 100 * int(input_date[5:7]) + int(input_date[8:10])
        
        # query to find all locations within the bounding box of our region
        
        response = requests.get(
            "https://transit.land/api/v1/operators",
            params = {
                "bbox": str(xmin) + "," + str(ymin) + "," + str(xmax) + "," + str(ymax) # from the previous section
            }
        )
        
        try:
            all_operators_json = response.json()
            all_operators_json = all_operators_json["operators"]
        except:
            raise APITimeoutException('API Timed Out')
            
        # loop over operators, adding unique feed info (based on onestop_id) to a list
        
        feed_base_info = []
        for operator in all_operators_json:
            for onestop_id in operator["represented_in_feed_onestop_ids"]:
                if onestop_id in list(banned['transit_land_id']):
                    continue
                feed_base_info.append([onestop_id, operator["name"],operator["website"],operator["state"],operator["metro"],operator["timezone"]])
        
        os.makedirs(config[region]['gtfs_static'] + "/feeds_" + input_date, exist_ok=True)
        
        # loop over feed info, getting info for each feed, and saving to an output array
        counter = 0
        output_feed_info = []
        for feed_info in feed_base_info:

            # base info
            onestop_id = feed_info[0]
            operator_name = feed_info[1]
            operator_website = feed_info[2]
            operator_metro = feed_info[4]

            for attempt in range(3):
                try:
                    # get feed versions
                    response = requests.get(
                    "https://transit.land/api/v1/feed_versions",
                    params = {
                        "feed_onestop_id": feed_info[0],
                        "per_page": 1000
                        }
                    )        
                    feeds = response.json()
                    sleep(1) # to avoid API timeout

                except:
                    print('Attempt ' + str(attempt))
                    if attempt == 3:
                        raise APITimeoutException('API Timed Out after 4 Attempts')
                    else:
                    
                        sleep(7**attempt)
                    
            
            # if there are feeds, find the feed that is the most recent to the input date
            try:
                
                nfeeds = (len(feeds["feed_versions"]))
                if nfeeds > 0:
                    print(operator_name)
                    # looping over feed versions
                    i = nfeeds - 1
                    while i >=0: 
                        # grabbing date info
                        date_fetched_iso8601 = feeds["feed_versions"][i]["fetched_at"]
                        earliest_calendar_date = feeds["feed_versions"][i]["earliest_calendar_date"]
                        
                        earliest_date_str = str(earliest_calendar_date)[0:10]
                        earliest_date_int = 10000 * int(earliest_date_str[0:4]) + 100 * int(earliest_date_str[5:7]) + int(earliest_date_str[8:10])
                        date_fetched = str(date_fetched_iso8601)[0:10]
                        #date_fetched_int = 10000 * int(date_fetched[0:4]) + 100 * int(date_fetched[5:7]) + int(date_fetched[8:10])
        
                        # checking if before the input date
                        print(earliest_date_int, input_date_int)
                        if earliest_date_int < input_date_int:
                            

                            # output info
                            date_fetched = date_fetched
                            earliest_calendar_date = feeds["feed_versions"][i]["earliest_calendar_date"]
                            latest_calendar_date = feeds["feed_versions"][i]["latest_calendar_date"]
                            transitland_historical_url = feeds["feed_versions"][i]["download_url"]
                            transit_land_id = feeds["feed_versions"][i]["feed"]       
                            feed_id = utility.feed_id_func(transit_land_id)             
                            
                            
                            
                            
                            try:
                                counter = counter + 1
                                name = operator_name
                                feed_publisher_name = operator_name

                                filename = feed_id
                                filename = filename.replace('/', '_')

                                dir_name = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + filename

                                try:
                                    urllib.request.urlretrieve(transitland_historical_url, dir_name + ".zip")
                                except:
                                    raise APITimeoutException('API Timed Out')

                                try:
                                    shutil.unpack_archive(dir_name + '.zip', dir_name)
                
                                    stops = pd.read_csv(dir_name + '/stops.txt', dtype={'stop_id': str})
                                    try:
                                        os.remove(dir_name + '/pathways.txt')
                                    except:
                                        pass
                
                                    try:
                                        gtfs_file = GTFS.load_zip(dir_name + '.zip')
                                        num_stops = gtfs_file.route_stops_inside(config[region]['region_boundary']).sum()[0]
                                    except: 
                                        num_stops = 2
                                        

                                    if num_stops < 2:
                                        os.remove(dir_name + '.zip')
                                        os.remove(dir_name)
                                        continue
                
                                    for index, row in stops.iterrows():
                                        if(pd.isnull(row['stop_lat'])):
                                            stops.at[index, 'stop_lat'] = 0
                                            stops.at[index, 'stop_lon'] = 0
                                            
                                    try:
                                        os.remove(dir_name + '/feed_info.txt')
                                    except:
                                        pass
                
                                    feed_txt_lst = []
                                    feed_publisher_name = name
                                    feed_publisher_url = name
                                    feed_lang = 'EN'
                                    feed_startdate = '20200101'
                                    feed_enddate = '20220101'
                                    feed_version = '1'
                                    
                                    feed_txt_lst.append(list([feed_publisher_name, feed_publisher_url, feed_lang, feed_startdate, feed_enddate, feed_version, feed_id]))
                                    feed_txt = pd.DataFrame(feed_txt_lst, columns = ['feed_publisher_name' , 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version','feed_id']) 
                                    feed_txt.to_csv(dir_name+'/feed_info.txt', index = False)
                                    stops.to_csv(dir_name+'/stops.txt', index = False)
                
                                    shutil.make_archive(dir_name, 'zip', dir_name)
                
                                    shutil.rmtree(dir_name)
                                except:
                                    feed_id = None
                                    shutil.rmtree(dir_name)
                                sleep(1)
                                
                            except:
                                
                                feed_id = None
                                pass
                            
                            output_feed_info.append([operator_name, operator_website, operator_metro, feed_id, onestop_id, date_fetched, earliest_calendar_date, latest_calendar_date, transitland_historical_url])

                            break # break since this should be the most recent
        
                        else:
                            None
        
                        i = i - 1
            except:
                None
                
        if region == 'New York':
            
            dt_fetched = date_fetched
            name = 'NICE'
            loc = 'Nassau County'
            nice_url =  'https://www.nicebus.com/NICE/media/nicebus-gtfs/NICE_GTFS.zip'
            print(name)
                        
            api_url = nice_url 
            dir = config[region]['gtfs_static'] + "/feeds_"+ input_date + "/" +  'nassau-inter-county-express_268' + ".zip"
            urllib.request.urlretrieve(nice_url, dir)
            dir_name = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + 'nassau-inter-county-express_268'



            try:          
                gtfs_file = GTFS.load_zip(dir_name)
                dt_st = str(gtfs_file.summary().first_date.date())
                dt_end = str(gtfs_file.summary().last_date.date())
                op_url = gtfs_file.agency['agency_url'][0]
            except:
                dt_st = None
                dt_end = None
                op_url = None

            shutil.unpack_archive(dir_name + '.zip', dir_name)

            feed_txt_lst = []
            feed_publisher_name = name
            feed_publisher_url = name
            feed_lang = 'EN'
            feed_startdate = '20200101'
            feed_enddate = '20220101'
            feed_version = '1'
            feed_id = 'nassau-inter-county-express/268'
            feed_txt_lst.append(list([feed_publisher_name, feed_publisher_url, feed_lang, feed_startdate, feed_enddate, feed_version, feed_id]))
            feed_txt = pd.DataFrame(feed_txt_lst, columns = ['feed_publisher_name' , 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version','feed_id']) 
            feed_txt.to_csv(dir_name+'/feed_info.txt', index = False)
            shutil.make_archive(dir_name, 'zip', dir_name)
    
            shutil.rmtree(dir_name)


            output_feed_info.append(list([name, op_url, loc, '', feed_id, dt_fetched, dt_st, dt_end, api_url]))
    
        feed_info = pd.DataFrame(output_feed_info, columns = ['operator_name' , 'operator_url', 'operator_region', 'transit_feeds_id', 'transit_land_id', 'date_fetched', 'earliest_calendar_date', 'latest_calendar_date', 'transitland_url']) 
        feed_info.to_csv(config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + region + "_feed_info_" + input_date + ".csv", index = False)

    def transit_feeds(region, input_date, xmin, xmax, ymin, ymax):
        banned = pd.read_csv(config['General']['gen'] + '/banned_agencies.csv')

        key = config['API']['key']
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
        
        os.makedirs(config[region]['gtfs_static'] + "/feeds_" + input_date, exist_ok=True)
        
        # loops through all locations
        feed_info_lst = []
        
        for attempt in range(4):    
            response = s.get(
                url+'/v1/getFeeds',
                params = {'key': key, 'location' : ids, 'type': 'gtfs', 'limit': 200

                }
            )
            
            try:
                feeds = response.json()['results']['feeds']
                break
            except:
                print('Attempt ' + attempt)
                if attempt == 3:
                    raise APITimeoutException('API Timed Out after 4 Attempts')
                else:
                
                    time.sleep(7**attempt)

            for agencies in feeds:
                ts = agencies['latest']['ts']
                dt_str = str(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d'))
                name = agencies['t']
                loc = agencies['l']['t']
                dt_fetched = str(datetime.utcfromtimestamp(ts).strftime('%Y%m%d'))
                if agencies['id'] in list(banned['transit_feeds_id']):
                    continue
                if agencies['id'] in ['nj-transit/408', 'nj-transit/409']:
                    continue

                filename = agencies['id']
                filename = filename.replace('/', '_')

                for attempt in range(3):
                    try:
                        if attempt == 0:
                            
                            api_url = gtfs_url + agencies['id'] + '/' + dt_fetched + '/download'
                            dir = config[region]['gtfs_static'] + "/feeds_"+ input_date + "/" + filename + ".zip"
                            urllib.request.urlretrieve(api_url, dir)
                            break
                        elif attempt == 1:
                            bkwd_dt = str((datetime.strptime(dt_fetched, '%Y%m%d') + timedelta(days=1)).date().strftime('%Y%m%d'))   
                            api_url = gtfs_url + agencies['id'] + '/' + bkwd_dt+ '/download'
                            dir = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + filename + ".zip"
                            urllib.request.urlretrieve(api_url, dir)
                            dt_fetched = str((datetime.strptime(dt_fetched, '%Y%m%d') + timedelta(days=1)).date().strftime('%Y-%m-%d')) 
                            
                            break
                        elif attempt == 2:
                            fwd_dt = str((datetime.strptime(dt_fetched, '%Y%m%d') - timedelta(days=1)).date().strftime('%Y%m%d'))   
                            api_url = gtfs_url + agencies['id'] + '/' + fwd_dt + '/download'
                            dir = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + filename + ".zip"
                            urllib.request.urlretrieve(api_url, dir)
                            dt_fetched = str((datetime.strptime(dt_fetched, '%Y%m%d') - timedelta(days=1)).date().strftime('%Y-%m-%d'))   
                            break
                        else:
                            print('Skipping due to error')
                            break
                    except:
                        sleep(1)
                dir_name = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + filname 
                
                try:
                    try:
                        shutil.unpack_archive(dir_name + '.zip', dir_name)
    
                        stops = pd.read_csv(dir_name + '/stops.txt', dtype={'stop_id': str})
                        stop_times = pd.read_csv(dir_name + '/stop_times.txt')
                        
                        try:
                            os.remove(dir_name + '/pathways.txt')
                        except:
                            pass
                        try:          
                            gtfs_file = GTFS.load_zip(dir)
                            num_stops = gtfs_file.route_stops_inside(config[region]['region_boundary']).sum()[0]
                            dt_st = str(gtfs_file.summary().first_date.date())
                            dt_end = str(gtfs_file.summary().last_date.date())
                            op_url = gtfs_file.agency['agency_url'][0]
                            
                        except:
                            dt_st = None
                            dt_end = None
                            op_url = None
                            num_stops = 2 # this means gtfs-lite wasn't able to open the file, so we should keep the feed to be safe
    
                        for index, row in stops.iterrows():
                            if(pd.isnull(row['stop_lat'])):
                                stops.at[index, 'stop_lat'] = 0
                                stops.at[index, 'stop_lon'] = 0
                        
                        try:
                            os.remove(dir_name + '/feed_info.txt')
                        except:
                            pass
                        feed_txt_lst = []
                        feed_publisher_name = name
                        feed_publisher_url = name
                        feed_lang = 'EN'
                        feed_startdate = '20200101'
                        feed_enddate = '20220101'
                        feed_version = '1'
                        feed_id = agencies['id']
                        feed_txt_lst.append(list([feed_publisher_name, feed_publisher_url, feed_lang, feed_startdate, feed_enddate, feed_version, feed_id]))
                        feed_txt = pd.DataFrame(feed_txt_lst, columns = ['feed_publisher_name' , 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version','feed_id']) 
                        feed_txt.to_csv(dir_name+'/feed_info.txt', index = False)
                        
                        stops.to_csv(dir_name+'/stops.txt', index = False)
                        
                        # re-save to remove leading 0s from stop_id
                        stop_times.to_csv(dir_name+'/stop_times.txt', index = False)
                        
                        shutil.make_archive(dir_name, 'zip', dir_name)
    
                        shutil.rmtree(dir_name)
                    except:
                        dt_st = None
                        dt_end = None
                        op_url = None
                        num_stops = 2
                        shutil.rmtree(dir_name)
                except:
                    dt_st = None
                    dt_end = None
                    op_url = None
                    num_stops = 2
                
                if num_stops < 2:
                    os.remove(dir_name + '.zip')
                    continue
                print(name)
                
                feed_info_lst.append(list([name, op_url, loc, agencies['id'], dt_str, dt_st, dt_end, api_url]))
        
        if region == 'District of Columbia':
            ts = agencies['latest']['ts']
            dt_str = str(datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d'))
            name = 'Virginia Rail Express'
            loc = 'Virginia'
            dt_fetched = str(datetime.utcfromtimestamp(ts).strftime('%Y%m%d'))
            vre_url =  'https://transitfeeds.com/p/virginia-railway-express/250/latest/download'
            print(name)
                        
            api_url = vre_url 
            dir = config[region]['gtfs_static'] + "/feeds_"+ input_date + "/" + 'virginia-railway-express_250' + ".zip"
            urllib.request.urlretrieve(api_url, dir)
            dir_name = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + 'virginia-railway-express_250'



            try:          
                gtfs_file = GTFS.load_zip(dir)
                dt_st = str(gtfs_file.summary().first_date.date())
                dt_end = str(gtfs_file.summary().last_date.date())
                op_url = gtfs_file.agency['agency_url'][0]
            except:
                dt_st = None
                dt_end = None
                op_url = None

            shutil.unpack_archive(dir_name + '.zip', dir_name)

            feed_txt_lst = []
            feed_publisher_name = name
            feed_publisher_url = 'https://www.google.com'
            feed_lang = 'EN'
            feed_startdate = '20200101'
            feed_enddate = '20220101'
            feed_version = '1'
            feed_id = 'virginia-railway-express/250'
            feed_txt_lst.append(list([feed_publisher_name, feed_publisher_url, feed_lang, feed_startdate, feed_enddate, feed_version, feed_id]))
            feed_txt = pd.DataFrame(feed_txt_lst, columns = ['feed_publisher_name' , 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version','feed_id']) 
            feed_txt.to_csv(dir_name+'/feed_info.txt', index = False)
            shutil.make_archive(dir_name, 'zip', dir_name)
    
            shutil.rmtree(dir_name)

            feed_info_lst.append(list([name, op_url, loc, feed_id, dt_str, dt_st, dt_end, api_url]))
            
        if region == 'New York':
            
            dt_fetched = str(datetime.utcfromtimestamp(ts).strftime('%Y%m%d'))
            name = 'NICE'
            loc = 'Nassau County'
            dt_fetched = str(datetime.utcfromtimestamp(ts).strftime('%Y%m%d'))
            nice_url =  'https://www.nicebus.com/NICE/media/nicebus-gtfs/NICE_GTFS.zip'
            print(name)
                        
            api_url = nice_url 
            dir = config[region]['gtfs_static'] + "/feeds_"+ input_date + "/" + 'nassau-inter-county-express_268' + ".zip"
            urllib.request.urlretrieve(nice_url, dir)
            dir_name = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + 'nassau-inter-county-express_268' 


            try:          
                gtfs_file = GTFS.load_zip(dir_name)
                dt_st = str(gtfs_file.summary().first_date.date())
                dt_end = str(gtfs_file.summary().last_date.date())
                op_url = gtfs_file.agency['agency_url'][0]
            except:
                dt_st = None
                dt_end = None
                op_url = None

            shutil.unpack_archive(dir_name + '.zip', dir_name)

            feed_txt_lst = []
            feed_publisher_name = name
            feed_publisher_url = name
            feed_lang = 'EN'
            feed_startdate = '20200101'
            feed_enddate = '20220101'
            feed_version = '1'
            feed_id = 'nassau-inter-county-express/268'
            feed_txt_lst.append(list([feed_publisher_name, feed_publisher_url, feed_lang, feed_startdate, feed_enddate, feed_version, feed_id]))
            feed_txt = pd.DataFrame(feed_txt_lst, columns = ['feed_publisher_name' , 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version','feed_id']) 
            feed_txt.to_csv(dir_name+'/feed_info.txt', index = False)
            shutil.make_archive(dir_name, 'zip', dir_name)
    
            shutil.rmtree(dir_name)


            feed_info_lst.append(list([name, op_url, loc, feed_id, dt_str, dt_st, dt_end, api_url]))
                
        if region in ['New York', 'Philadelphia']:
            
            dt_fetched = str(datetime.utcfromtimestamp(ts).strftime('%Y%m%d'))
            name = 'NJ Transit Rail GTFS'
            loc = 'New Jersey'
            dt_fetched = str(datetime.utcfromtimestamp(ts).strftime('%Y%m%d'))
            nj_rail_url =  'https://www.njtransit.com/rail_data.zip'
            print(name)            
            api_url = nj_rail_url 
            dir = config[region]['gtfs_static'] + "/feeds_"+ input_date + "/" +'nj-transit_408' + ".zip"
            urllib.request.urlretrieve(api_url, dir)
            dir_name = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" +'nj-transit_408' 
            # try:
            #     shutil.unpack_archive(dir_name + '.zip', dir_name)
            #     shutil.make_archive(dir_name , 'zip', dir_name +'/rail_data')
            # except:
            #     pass
            try:          
                gtfs_file = GTFS.load_zip(dir)
                dt_st = str(gtfs_file.summary().first_date.date())
                dt_end = str(gtfs_file.summary().last_date.date())
                op_url = gtfs_file.agency['agency_url'][0]
            except:
                dt_st = None
                dt_end = None
                op_url = None

            shutil.unpack_archive(dir_name + '.zip', dir_name)


            feed_txt_lst = []
            feed_publisher_name = name
            feed_publisher_url = 'https://www.google.com'
            feed_lang = 'EN'
            feed_startdate = '20200101'
            feed_enddate = '20220101'
            feed_version = '1'
            feed_id = 'nj-transit/408'
            feed_txt_lst.append(list([feed_publisher_name, feed_publisher_url, feed_lang, feed_startdate, feed_enddate, feed_version, feed_id]))
            feed_txt = pd.DataFrame(feed_txt_lst, columns = ['feed_publisher_name' , 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version','feed_id']) 
            feed_txt.to_csv(dir_name+'/feed_info.txt', index = False)
            shutil.make_archive(dir_name, 'zip', dir_name)
            shutil.rmtree(dir_name)
            feed_info_lst.append(list([name, op_url, loc, feed_id, dt_str, dt_st, dt_end, api_url]))
            
            
            
            name = 'NJ Transit Bus GTFS'
            loc = 'New Jersey'
            dt_fetched = str(datetime.utcfromtimestamp(ts).strftime('%Y%m%d'))
            nj_bus_url =  'https://www.njtransit.com/bus_data.zip'
            print(name)
            api_url = nj_bus_url 
            dir = config[region]['gtfs_static'] + "/feeds_"+ input_date + "/" + 'nj-transit_409' + ".zip"
            urllib.request.urlretrieve(api_url, dir)
            dir_name = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + 'nj-transit_409' 
            # try:
            #     shutil.unpack_archive(dir_name + '.zip', dir_name)
            #     shutil.make_archive(dir_name , 'zip', dir_name +'/rail_data')
            # except:
            #     pass
            try:          
                gtfs_file = GTFS.load_zip(dir)
                dt_st = str(gtfs_file.summary().first_date.date())
                dt_end = str(gtfs_file.summary().last_date.date())
                op_url = gtfs_file.agency['agency_url'][0]
            except:
                dt_st = None
                dt_end = None
                op_url = None

            shutil.unpack_archive(dir_name + '.zip', dir_name)


            feed_txt_lst = []
            feed_publisher_name = name
            feed_publisher_url = 'https://www.google.com'
            feed_lang = 'EN'
            feed_startdate = '20200101'
            feed_enddate = '20220101'
            feed_version = '1'
            feed_id = 'nj-transit/409'
            feed_txt_lst.append(list([feed_publisher_name, feed_publisher_url, feed_lang, feed_startdate, feed_enddate, feed_version, feed_id]))
            feed_txt = pd.DataFrame(feed_txt_lst, columns = ['feed_publisher_name' , 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version','feed_id']) 
            feed_txt.to_csv(dir_name+'/feed_info.txt', index = False)
            shutil.make_archive(dir_name, 'zip', dir_name)
            shutil.rmtree(dir_name)
            feed_info_lst.append(list([name, op_url, loc, feed_id, dt_str, dt_st, dt_end, api_url]))
                
        feed_info = pd.DataFrame(feed_info_lst, columns = ['operator_name' , 'operator_url', 'operator_region', 'transit_feeds_id', 'date_fetched', 'earliest_calendar_date', 'latest_calendar_date', 'transitfeeds_url']) 
        feed_info.to_csv(config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + region + "_feed_info_" + input_date + ".csv", index = False)

    
    def transit_feeds_historical(region, input_date, xmin, xmax, ymin, ymax):
        
        banned = pd.read_csv(config['General']['gen'] + '/banned_agencies.csv')        
        key = config['API']['key']
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
        
        os.makedirs(config[region]['gtfs_static'] + "/feeds_" + input_date, exist_ok=True)
        
        # loops through all locations
        feed_info_lst = []
        
        for ids in location_ids:


            for attempt in range(4):    
                response = s.get(
                    url+'/v1/getFeeds',
                    params = {'key': key, 'location' : ids, 'type': 'gtfs', 'limit': 200

                    }
                )
                
                try:
                    feeds = response.json()['results']['feeds']
                    break
                except:
                    print('Attempt ' + attempt)
                    if attempt == 3:
                        raise APITimeoutException('API Timed Out after 4 Attempts')
                    else:
                    
                        time.sleep(7**attempt)

            for agencies in feeds:
                if agencies['id'] in list(banned['transit_feeds_id']):
                    continue

                filename = agencies['id']
                filename = filename.replace('/', '_')
                name = agencies['t']
                loc = agencies['l']['t']                
                dt_time = datetime.strptime(input_date, '%Y-%m-%d')
                
                for j in range(365):
                    dt_fetched = str(dt_time.strftime('%Y%m%d'))
                    dt_str = str(dt_time.strftime('%Y-%m-%d'))
                    api_url = gtfs_url + agencies['id'] + '/' + dt_fetched + '/download'
                    request = requests.get(api_url)
                    if request.status_code == 200:
                        update = True
                        break
                    else:
                        dt_time = dt_time - timedelta(days = 1)
                        
                    if j == 364:
                        update = False
                        print('No updated feed for:' + str(name))
                
                                     
                
                if update == True:
                    api_url = gtfs_url + agencies['id'] + '/' + dt_fetched + '/download'
                    dir = config[region]['gtfs_static'] + "/feeds_"+ input_date + "/" + filename + ".zip"
                    urllib.request.urlretrieve(api_url, dir)
    
    
                    dir_name = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + filename
                    try:
                        try:
                            shutil.unpack_archive(dir_name + '.zip', dir_name)
        
                            stops = pd.read_csv(dir_name + '/stops.txt', dtype={'stop_id': str})
                            try:
                                os.remove(dir_name + '/pathways.txt')
                            except:
                                pass
                            try:          
                                gtfs_file = GTFS.load_zip(dir)
                                num_stops = gtfs_file.route_stops_inside(config[region]['region_boundary']).sum()[0]
                                dt_st = str(gtfs_file.summary().first_date.date())
                                dt_end = str(gtfs_file.summary().last_date.date())
                                op_url = gtfs_file.agency['agency_url'][0]
                            except:
                                dt_st = None
                                dt_end = None
                                op_url = None
                                num_stops = 2
        
                            for index, row in stops.iterrows():
                                if(pd.isnull(row['stop_lat'])):
                                    stops.at[index, 'stop_lat'] = 0
                                    stops.at[index, 'stop_lon'] = 0
        
                            feed_txt_lst = []
                            feed_publisher_name = name
                            feed_publisher_url = 'https://www.google.com'
                            feed_lang = 'EN'
                            feed_startdate = '20200101'
                            feed_enddate = '20220101'
                            feed_version = '1'
                            feed_id = agencies['id']
                            feed_txt_lst.append(list([feed_publisher_name, feed_publisher_url, feed_lang, feed_startdate, feed_enddate, feed_version, feed_id]))
                            feed_txt = pd.DataFrame(feed_txt_lst, columns = ['feed_publisher_name' , 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version','feed_id']) 
                            feed_txt.to_csv(dir_name+'/feed_info.txt', index = False)
                            
                            stops.to_csv(dir_name+'/stops.txt', index = False)
        
                            shutil.make_archive(dir_name, 'zip', dir_name)
        
                            shutil.rmtree(dir_name)
                        except:
                            dt_st = None
                            dt_end = None
                            op_url = None
                            num_stops = 2
                            shutil.rmtree(dir_name)
                    except:
                        dt_st = None
                        dt_end = None
                        op_url = None
                        num_stops = 2
                else:
                    continue
        
                
                if num_stops < 2:
                    os.remove(dir_name + '.zip')
                    continue
                
                feed_info_lst.append(list([name, op_url, loc, agencies['id'], dt_str, dt_st, dt_end, api_url]))

        
        if region == 'District of Columbia':


            name = 'Virginia Rail Express'
            loc = 'Virginia'

            vre_url =  'https://transitfeeds.com/p/virginia-railway-express/250/'
            
            while True:
                dt_fetched = str(dt_time.strftime('%Y%m%d'))
                dt_str = dt_fetched
                api_url = vre_url + '/' + dt_fetched + '/download'
                request = requests.get(api_url)
                if request.status_code == 200:
                    break
                else:
                    dt_time - dt_time - timedelta(days = 1)
                        

            dir = config[region]['gtfs_static'] + "/feeds_"+ input_date + "/" + 'virginia-railway-express_250' + ".zip"
            urllib.request.urlretrieve(api_url, dir)
            dir_name = config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + 'virginia-railway-express_250' 



            try:          
                gtfs_file = GTFS.load_zip(dir)
                dt_st = str(gtfs_file.summary().first_date.date())
                dt_end = str(gtfs_file.summary().last_date.date())
                op_url = gtfs_file.agency['agency_url'][0]
            except:
                dt_st = None
                dt_end = None
                op_url = None
            
            shutil.unpack_archive(dir_name + '.zip', dir_name)

            feed_txt_lst = []
            feed_publisher_name = name
            feed_publisher_url = 'https://www.google.com'
            feed_lang = 'EN'
            feed_startdate = '20200101'
            feed_enddate = '20220101'
            feed_version = '1'
            feed_id = 'virginia-railway-express/250'
            feed_txt_lst.append(list([feed_publisher_name, feed_publisher_url, feed_lang, feed_startdate, feed_enddate, feed_version, feed_id]))
            feed_txt = pd.DataFrame(feed_txt_lst, columns = ['feed_publisher_name' , 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version','feed_id']) 
            feed_txt.to_csv(dir_name+'/feed_info.txt', index = False)
            shutil.make_archive(dir_name, 'zip', dir_name)
    
            shutil.rmtree(dir_name)
        

            feed_info_lst.append(list([name, op_url, loc, feed_id, dt_str, dt_st, dt_end, api_url]))

        feed_info = pd.DataFrame(feed_info_lst, columns = ['operator_name' , 'operator_url', 'operator_region', 'transit_feeds_id', 'date_fetched', 'earliest_calendar_date', 'latest_calendar_date', 'transitfeeds_url']) 
        feed_info.to_csv(config[region]['gtfs_static'] + "/feeds_" + input_date + "/" + region + "_feed_info_" + input_date + ".csv", index = False)
        

    def process_feeds(region, dir_name, name, feed_id):        

        shutil.unpack_archive(dir_name + '.zip', dir_name)

        stops = pd.read_csv(dir_name + '/stops.txt', dtype={'stop_id': str})
        try:
            os.remove(dir_name + '/pathways.txt')
        except:
            pass

        try:
            gtfs_file = GTFS.load_zip(dir_name + '.zip')
            num_stops = gtfs_file.route_stops_inside(config[region]['region_boundary']).sum()[0]
        except: 
            num_stops = 2


        if num_stops < 2:
            os.remove(dir_name + '.zip')
            os.remove(dir_name)
            raise InvalidFeed('Invalid Feed')

        for index, row in stops.iterrows():
            if(pd.isnull(row['stop_lat'])):
                stops.at[index, 'stop_lat'] = 0
                stops.at[index, 'stop_lon'] = 0

        try:
            os.remove(dir_name + '/feed_info.txt')
        except:
            pass

        feed_txt_lst = []
        feed_publisher_name = name
        feed_publisher_url = 'https://www.google.com'
        feed_lang = 'EN'
        feed_startdate = '20200101'
        feed_enddate = '20220101'
        feed_version = '1'

        feed_txt_lst.append(list([feed_publisher_name, feed_publisher_url, feed_lang, feed_startdate, feed_enddate, feed_version, feed_id]))
        feed_txt = pd.DataFrame(feed_txt_lst, columns = ['feed_publisher_name' , 'feed_publisher_url', 'feed_lang', 'feed_start_date', 'feed_end_date', 'feed_version','feed_id']) 
        feed_txt.to_csv(dir_name+'/feed_info.txt', index = False)
        stops.to_csv(dir_name+'/stops.txt', index = False)

        shutil.make_archive(dir_name, 'zip', dir_name)

        shutil.rmtree(dir_name)
    