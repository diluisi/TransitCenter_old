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
import sys

config = configparser.ConfigParser()
config.read('config.cfg')

parser = argparse.ArgumentParser(description='arguments for dates and region')

parser.add_argument("-r", '--region', help="region to evaluate")
parser.add_argument("-p", '--threads', help="number of threads")
parser.add_argument("-z", '--period', help="if ALL, then evaluate all periods, else do a single")
parser.add_argument("-b", '--build_only', default = False, action='store_true')
parser.add_argument("-o", '--run_only', default = False, action='store_true', help = 'if true, will run on existing graph')
parser.add_argument("-s", '--dst', default = False, action='store_true', help = 'if true, will use daylight savings time')


args = parser.parse_args()

region = args.region
threads = int(args.threads)
period = args.period.upper()
build_only = args.build_only
run_only = args.run_only
dst = args.dst

if __name__ == '__main__':

    start_time = time.time()    
    
    county_ids = utils.county_ids.get_county_ids(region)

    print(county_ids)
    
    print(region)
    
    xmin, xmax, ymin, ymax = utils.geometry.osm_bounds(region, county_ids, raw = True)
    
    
    print(xmin, xmax, ymin, ymax)

    date_list = ['2020-09-20', '2020-10-18']    
    # date_list = ['2020-11-15', '2020-12-20', '2021-01-17', '2021-02-21']    
    # date_list = ['2020-02-23', '2020-11-15', '2020-12-20', '2021-01-17', '2021-02-21']
    # date_list = ['2020-03-15', '2020-04-19', '2020-05-10', '2020-06-21', '2020-07-19', '2020-08-16', '2020-09-20', '2020-10-18'] 
    # date_list = ['2020-03-1', '2020-04-19', '2020-06-21', '2020-07-19', '2020-08-16'] 

    for dt_str in date_list:
        
        
        #depending on the region, certain gtfs feeds may only be available for one website
        
        if (build_only == False):

            if run_only == False:
                if region in config['General']['transit_feeds']:
                    print('Transit Feeds')
                    gtfs.get.transit_feeds_historical(region, dt_str, xmin, xmax, ymin, ymax)
                
                else:
                    
                    print('Transit Land')
                    for i in range(2):
                    
                        gtfs.get.transit_land(region, dt_str, xmin, xmax, ymin, ymax)
                        time.sleep(90)
                
                otp.build.build_otp(region, dt_str)
            
            if period == 'ALL':

                if region in ['New York', 'Philadelphia', 'District of Columbia', 'Boston']:
                    if dst == True:
                        period_list = ['EDT_MP', 'EDT_PM', 'EDT_WE']
                    else:
                        period_list = ['EST_MP', 'EST_PM', 'EST_WE']

                elif region in ['Los Angeles', 'San Francisco-Oakland']:
                    if dst == True:
                        period_list = ['PDT_MP', 'PDT_PM', 'PDT_WE']
                    else:
                        period_list = ['PST_MP', 'PST_PM', 'PST_WE']

                elif region in ['Chicago']:
                    if dst == True:
                        period_list = ['CDT_MP', 'CDT_PM', 'CDT_WE']
                    else:
                        period_list = ['CST_MP', 'CST_PM', 'CST_WE']     

                else:

                    print('Not Valid Region')
                    sys.exit()

                for periods in period_list:
                    call(["python3", config['General']['otp'] + '/otp_handler_rand2_azure.py', '-d', dt_str,
                        '-r', region, '-p', str(threads), '-z', periods])
            else:
                call(["python3", config['General']['otp'] + '/otp_handler_rand2_azure.py', '-d', dt_str,
                    '-r', region, '-p', str(threads), '-z', period])

        else:
            if region in config['General']['transit_feeds']:
        
                gtfs.get.transit_feeds_historical(region, dt_str, xmin, xmax, ymin, ymax)
            
            else:
                gtfs.get.transit_land(region, dt_str, xmin, xmax, ymin, ymax)
            
            otp.build.build_otp(region, dt_str)
        
        # break
    time.time() - start_time
