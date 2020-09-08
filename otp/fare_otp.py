#!/usr/bin/env python
# coding: utf-8



import requests, json, time
import pandas as pd
import geopandas as gpd
import concurrent.futures
import configparser
import argparse
import datetime
from datetime import timedelta
from subprocess import Popen, call
import subprocess
import sys
import time
import os
import traceback

from pathlib import Path
root_path = Path(os.getcwd())

sys.path.insert(1, str(root_path.parent)+'/fare')
import fare



parser = argparse.ArgumentParser(description='Process region')
parser.add_argument("-d", '--date', default = datetime.datetime.now().strftime('%Y-%m-%d'), help="input date for otp, YYYY-MM-DD")
parser.add_argument("-r", '--region',  help="Region to evaluate")
parser.add_argument("-p", '--threads', default = 5, help="number of threads")
parser.add_argument("-z", '--period', help="time period, AM, EVE, MID")
parser.add_argument("-b", '--lowcost', default=False, action='store_true', help="if evaluating a premium network")

args = parser.parse_args()

date = args.date
region = args.region
threads = int(args.threads)
lowcost = args.lowcost
period = args.period


config = configparser.ConfigParser()

if os.path.isfile('config.cfg'):
    config = configparser.ConfigParser()
    config.read('config.cfg')
else:
    config = configparser.ConfigParser()
    config.read('../config.cfg')
    
pts_path = config[region]['tract_points'] 
outpath = config[region]['itinerary']
graph_path = config[region]['graphs']
otp_path = config['General']['otp']

if lowcost == True:
    premium_routes = config[region]['premium_routes']
    mode = config[region]['low_cost_modes']
else:
    mode = 'TRANSIT'
    premium_routes = None

# shell command to start up otp server
def call_otp():
    
    command = ['java', '-Xmx4G', '-jar', otp_path+'/otp-1.4.0-shaded.jar', '--router', 'graphs-'+date,
          '--graphs', graph_path, '--server', '--enableScriptingWebService']
    p = Popen(command)
    
    time.sleep(60) #time needed to ensure the otp server starts up
    return p

# function to return the itineraries
def return_itineraries(ox,oy,dx,dy,date_us,hr,minute):

	# parameters
    options = {
   		'fromPlace': str(oy) + ", " + str(ox),
   		'toPlace': str(dy) + ", " + str(dx),
   		'time': str(hr)+':' + minute + 'am',
   		'date': date_us,
   		'mode': mode+',WALK',
   		'maxWalkDistance':5000,
   		'clampInitialWait':0,
   		'wheelchair':False,
   		#'batch': True,
        'BannedRoutes':premium_routes,
   		'numItineraries': 1
   	}
   
   	# send to server and get data
    response = requests.get(
   		"http://localhost:8080/otp/routers/default/plan",
   		params = options
           )
    # return as json
    data = json.loads(response.text)
    return data

# function to create the isochrones
def analyst(x, y):

    # calling isochrones to filter out tracts
    bins = [7200]
    cutoff = f'&cutoffSec={bins[0]}'
    tm = str(hr)+':%20' + minute + 'am'
    walk_dist = 5000
    otp_mode = 'WALK,'+mode
    params = f'?toPlace={y},{x}&fromPlace={y},{x}&arriveBy=TRUE&mode={otp_mode}&date={date_us}&time={tm}&maxWalkDistance={walk_dist}'
    url = f'http://localhost:8080/otp/routers/graphs-'+date+'/isochrone'+params+cutoff
    response = requests.get(url)

    return(response.json())

if __name__ == '__main__':
    
    # ensuring parrallel processing works on mac os
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    start_time = time.time()
    
    p = call_otp()

    # load in the point file
    df_pts = pd.read_csv(pts_path)
    output_lst = []
    
    columns = ['origin_tract', 'destination_tract', 'departure_datetime', 'period', 'low_cost', 'fare', 'travel_time']
    
    # configuring the period for the time bins
    if period == 'AM':
        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        while True:
            if dt.weekday() > 4:
                dt = dt + timedelta(days = 1)
            else:
                o_date = dt.strftime('%Y-%m-%d')
                break
        hr_lst = [7,8]
        
    elif period == 'MID':
        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        while True:
            if dt.weekday() < 5:
                dt = dt + timedelta(days = 1)
            else:
                o_date = dt.strftime('%Y-%m-%d')
                break
        hr_lst = [10,11]
        
    elif period == 'EVE':
        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        while True:
            if dt.weekday() > 4:
                dt = dt + timedelta(days = 1)
            else:
                o_date = dt.strftime('%Y-%m-%d')
                break
        hr_lst = [22,23]
    else:
        pass
    date_us = dt.strftime('%m-%d-%Y')
    hr = hr_lst[1]
    minute = '00' #right now, we're not randomly sampling time bins for fares
    tm = datetime.datetime.strptime(o_date+' '+str(hr) + ':' + minute, '%Y-%m-%d %H:%M')
    
    i = 0
    
    # reading the points 
    gdf_pts = gpd.GeoDataFrame(df_pts, geometry=gpd.points_from_xy(df_pts.LONGITUDE, df_pts.LATITUDE))

    #setting up the structure for the error json
    error_json = []
    error_dict = {
    	'origin_tract': "",
    	'destination_tract': "",
    	'departure_datetime': "",
    	'fare_dict': "",
    	'traceback': ""
        }
    
    
    # loop over every possible OD pair in our input point dataset
    output = []
    for index, row in df_pts.iterrows():
        x1 = row['LONGITUDE']
        y1 = row['LATITUDE']
        origin = int(row['GEOID'])
        
        # calling isochrones
        try:
            data = analyst(x1, y1)
        except:
            # usually because point is on a body of water
            # print(x1, y1)
            continue
        
        # filters out blocks with travel time over 120 minutes
        iso_120 = gpd.GeoDataFrame.from_features(data["features"])
        gdf_120 = gpd.sjoin(iso_120, gdf_pts, how="left")
        df_120 = pd.DataFrame(gdf_120).reset_index()

        df_pts = pd.read_csv(pts_path)
        
        dest_lst = []
        start = 0
        for index2, row2 in df_120.groupby(df_120.index // threads):
            #looping origins
            param = []
            dest_lst = []
            
            # defining params for otp
            for j in range(start,start+threads):
                
                try:
                    x2 = row2['LONGITUDE'][j]
                    y2 = row2['LATITUDE'][j]
                    dest = int(row2['GEOID'][j])
                except:
                    x2 = 0
                    y2 = 0
                    dest = 0
                    # to solve an issue where the number of tracts is not divisible by number of threads
                
                # list of destination tracts
                dest_lst.append(dest)        
                tup = (x1,y1,x2,y2,date_us,hr,minute)
                param.append(tup) # origin and destination to send to otp
            
            
            thread_lst = []
            result = []

            # parralelizing the otp plan call
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for i in range(threads):
                    thread_lst.append(executor.submit(return_itineraries, param[i][0],  param[i][1],  param[i][2],  param[i][3], 
                                                      param[i][4], param[i][5], param[i][6]))
                    
                for f in concurrent.futures.as_completed(thread_lst):
                    # retrieving the information from each thread for the otp call
                    result.append(f.result())
                    
            start = start + threads
            
            
            for j in range(0, threads):
                # storing the output information into output
                
                # checks if otp was able to calculate the itinerary
                if 'plan' in result[j].keys():
                    
                    fare_dict = {
                      "OTP_itinerary_all": result[j]
                    }
                    
                    try:
                        #calling the fare values, otherwise it will append to traceback
                        fare_cost = fare.fare(fare_dict, region)
                        if dest_lst[j] == 0:
                            continue
                        
                        output_lst.append([origin, dest_lst[j], str(tm), period,lowcost, fare_cost, result[j]['plan']['itineraries'][0]['duration']])
                    
                    except:
                        error_data = error_dict 
                        error_data['origin_tract'] = origin
                        error_data['destination_tract'] = dest_lst[j]
                        error_data['departure_datetime'] = str(tm)
                        error_data['fare_dict'] = result[j]['plan']
                        error_data['traceback'] = traceback.format_exc()
                        
                        error_json.append(error_data)
                
            
        # print(index)
        # if index == 5:
        #     break


    end_time = time.time()

    # write to csv
    output = pd.DataFrame(output_lst, columns = columns)
    output.to_csv(outpath+'/'+region+'_'+period+'_'+mode+'_'+date+'test.csv', index = False)

    #writing the error json
    if len(error_json) >= 1:
        print('Some fares could not be calculated. See the error file.')
        error_path = outpath+'/MissingFare_'+region+'_'+period+'_'+mode+'_'+date + '.json'
        with open(error_path, 'w') as f:
            json.dump(error_data, f)


    print(len(output))
    
    print(end_time - start_time)
    
    p.kill() # making sure the server shuts down
    sys.exit()




    




