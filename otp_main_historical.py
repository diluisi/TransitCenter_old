#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 29 12:09:03 2020

@author: Rick

This function presents a more automated way to call opentripplanner rather than relying on multiple cron job commands.

This function will download historical gtfs feeds, build otp graphs, then call the otp_travel_times function for all regions in the config file.
"""
import gtfs
import utils
import otp
import configparser
import datetime
from subprocess import call
import time
import argparse
from datetime import timedelta

config = configparser.ConfigParser()
config.read('config.cfg')

parser = argparse.ArgumentParser(description='arguments for dates')

parser.add_argument("-s", '--start_date', help="start date for historical feeds, YYYY-MM-DD")
parser.add_argument("-e", '--end_date', help="end date for historical feeds, YYYY-MM-DD")

args = parser.parse_args()


start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')

if __name__ == '__main__':

    
    start_time = time.time()
    
    
    threads = 4
    
    region_lst = []
    
    # finding regions
    for i in config:
        if i in ['DEFAULT', 'API', 'General']:
            continue
        region_lst.append(i)
    
    dt_str = datetime.datetime.now().strftime('%Y-%m-%d')
    
    for region in region_lst:
    
    #for region in ['Boston', 'New York']: # if evaluating specific regions
        print(region)
        
        mode = config[region]['low_cost_modes']
        
        county_ids = utils.county_ids.get_county_ids(region)
    
        
        xmin, xmax, ymin, ymax = utils.geometry.osm_bounds(region, county_ids, raw = True)
        
        
        #looping through weeks to download historical feeds
        
        while True:
            start_date = start_date + timedelta(days = 7)
            dt_str = start_date.strftime('%Y-%m-%d')
            
            if start_date > end_date:
                break
            #depending on the region, certain gtfs feeds may only be available for one website
            if region in config['General']['transit_feeds']:
        
                gtfs.get.transit_feeds(region, dt_str, xmin, xmax, ymin, ymax)
            
            else:
                gtfs.get.transit_land_historical(region, dt_str, xmin, xmax, ymin, ymax)
                
            otp.build.build_otp(region, dt_str)
                
            for period in ['AM', 'EVE', 'MID']:
                call(["python", config['General']['otp'] + '/otp_handler_rand8.py', '-d', dt_str, '-m', 
                      config[region]['low_cost_modes'], '-r', region,
                      '-p', str(threads), '-z', period])
                call(["python", config['General']['otp'] + '/otp_handler_rand8.py', '-d', dt_str, '-m', 
                      'TRANSIT', '-r', region,
                      '-p', str(threads), '-z', period])
        
        # break
    time.time() - start_time
