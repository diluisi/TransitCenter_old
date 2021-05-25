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
import os
import traceback
import csv
import sqlite3

from pathlib import Path
root_path = Path(os.getcwd())
fare_path = str(Path(os.getcwd()).parent) + '/fare'


sys.path.insert(1, str(root_path.parent)+'/fare')
import fare


parser = argparse.ArgumentParser(description='Process region')
parser.add_argument("-d", '--date', default = datetime.datetime.now().strftime('%Y-%m-%d'), help="input date for otp, YYYY-MM-DD")
parser.add_argument("-r", '--region',  help="Region to evaluate")
parser.add_argument("-p", '--threads', default = 5, help="number of threads")
parser.add_argument("-z", '--period', help="time period, AM, EVE, MID")

args = parser.parse_args()

date = args.date
region = args.region
threads = int(args.threads)
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
block_path = config[region]['block_group_points'] 


#reads the list of premium routes to ban
results = []
with open(config[region]['gtfs_static'] + '/premium_routes.csv' , newline='') as f:
    for row in csv.reader(f):
        results.append(row[0])
premium_routes = ','.join(results)
# mode = 'RAIL'
mode = 'TRANSIT'


# shell command to start up otp server
def call_otp():
    
    command = ['java', '-Xmx208G', '-jar', otp_path+'/otp-1.4.0-shaded.jar', '--router', 'graphs-'+date,
          '--graphs', graph_path, '--server', '--enableScriptingWebService']
    
    print('\njava -Xmx4G -jar ' + otp_path+'/otp-1.4.0-shaded.jar'+ ' --router '+ 'graphs-'+date+
          ' --graphs '+graph_path+' --server'+' --enableScriptingWebService\n')
    p = Popen(command)
    
    time.sleep(90) #time needed to ensure the otp server starts up
    return p

# function to return the itineraries
def return_itineraries(param):

    ox = param[0]
    oy = param[1]
    dx = param[2]
    dy = param[3]
    date_us = param[4]
    hr = param[5]
    minute = param[6]

	# parameters
    options = {
   		'fromPlace': str(oy) + ", " + str(ox),
   		'toPlace': str(dy) + ", " + str(dx),
   		'time': str(hr)+':' + minute,
   		'date': date_us,
   		'mode': mode+',WALK',
   		'maxWalkDistance':5000,
   		'clampInitialWait':0,
   		'wheelchair':False,
   		#'batch': True,
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

# function to return the lowcost itineraries
def return_lowcost(param:

    ox = param[0]
    oy = param[1]
    dx = param[2]
    dy = param[3]
    date_us = param[4]
    hr = param[5]
    minute = param[6]

	# parameters
    options = {
   		'fromPlace': str(oy) + ", " + str(ox),
   		'toPlace': str(dy) + ", " + str(dx),
   		'time': str(hr)+':' + minute,
   		'date': date_us,
   		'mode': mode+',WALK',
   		'maxWalkDistance':5000,
   		'clampInitialWait':0,
   		'wheelchair':False,
   		#'batch': True,
        'bannedRoutes':premium_routes,
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
    bins = [5400] #90 minute transit travel time
    cutoff = f'&cutoffSec={bins[0]}'
    tm = str(hr)+':%20' + minute + 'am'
    walk_dist = 5000
    otp_mode = 'WALK,'+mode
    params = f'?toPlace={y},{x}&fromPlace={y},{x}&arriveBy=TRUE&mode={otp_mode}&date={date_us}&time={tm}&maxWalkDistance={walk_dist}'
    url = f'http://localhost:8080/otp/routers/graphs-'+date+'/isochrone'+params+cutoff
    response = requests.get(url)
    print(url)
    return(response.json())

if __name__ == '__main__':
    
    # ensuring parrallel processing works on mac os
    # __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    start_time = time.time()
    
    os.makedirs(outpath + '/' + 'fares', exist_ok=True)
    os.makedirs(outpath + '/fares' + '/' + str(date), exist_ok=True)
    
    df_path = outpath + '/fares' + '/' + str(date) 
    
    # connect to database
    DB_NAME = fare_path + '/FareDB.db'
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    p = call_otp()

    # load in the point file
    df_pts = pd.read_csv(pts_path)
    df_block = pd.read_csv(block_path)
    block_poly = gpd.read_file(config[region]['block_group_polygons'])
    gdf_pts = gpd.GeoDataFrame(df_pts, geometry=gpd.points_from_xy(df_pts.LONGITUDE, df_pts.LATITUDE))


    
    #declaring variables
    output_lst = []
    fare_lst = []
    fare_error = []
    error_json = []
    full_json = []
    output_lst_lowcost = []
    
    columns = ['origin_tract', 'destination_tract', 'fare_all', 'travel_time_all']
    columns_lowcost = ['origin_tract', 'destination_tract', 'fare_lowcost', 'travel_time_lowcost']

    
    # configuring the period for the time bins
    if period == 'MP':
        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        while True:
            if dt.weekday() > 4:
                dt = dt + timedelta(days = 1)
            else:
                o_date = dt.strftime('%Y-%m-%d')
                break
        hr_lst = [7,8]
        
    elif period == 'WE':
        dt = datetime.datetime.strptime(date, '%Y-%m-%d')
        while True:
            if dt.weekday() < 5:
                dt = dt + timedelta(days = 1)
            else:
                o_date = dt.strftime('%Y-%m-%d')
                break
        hr_lst = [10,11]
        
    elif period == 'PM':
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


        try:
            data = analyst(x1, y1)
        except:
            # usually because point is on a body of water
            # print(x1, y1)
            continue
        
        # filters out blocks with travel time over 90 minutes
        iso = gpd.GeoDataFrame.from_features(data["features"])
        gdf_blocks = gpd.sjoin(iso, block_poly, how="left", op = 'intersects')
        tract_list = list(gdf_blocks['GEOID'].astype(str).str[:-1].astype(int).drop_duplicates())
        df_120 = df_pts[df_pts['GEOID'].isin(tract_list)]
        df_120 = pd.DataFrame(df_120).reset_index()
        df_120 = df_120.sample(frac=1).reset_index(drop=True)
        
        dest_lst = []
        start = 0

        for index2, row2 in df_120.groupby(df_120.index // threads):
            
            print(index, index2)
            # for testing purposes, running a limited set
            # if index2 == 50:
            #     break
            
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
            thread_lst_lowcost = []
            result_lowcost = []
            
            # parralelizing the otp plan call
            with concurrent.futures.ThreadPoolExecutor() as executor:
                thread_list = executor.map(return_itineraries, param)
                result = list(thread_lst)

                    

            
            #lowcost network calculations
            with concurrent.futures.ThreadPoolExecutor() as executor:
                thread_list_lowcost = executor.map(return_lowcost, param)
                result_lowcost = list(thread_lst_lowcost)
                    

                    
            start = start + threads
            
            
            for j in range(0, threads):
                # storing the output information into output
                try:
                # checks if otp was able to calculate the itinerary
                    if 'plan' in result[j].keys():
                        
                        fare_dict = {
                          "OTP_itinerary_all": result[j]
                        }
                        
                        try:
                            #calling the fare values, otherwise it will append to traceback
                            fare_cost = fare.fare(fare_dict, region, c)
                            if dest_lst[j] == 0:
                                continue
                            
                            output_lst.append([origin, dest_lst[j], fare_cost, result[j]['plan']['itineraries'][0]['duration']])

    
    
                        except:
                            error_data = error_dict 
                            error_data['origin_tract'] = origin
                            error_data['destination_tract'] = dest_lst[j]
                            error_data['departure_datetime'] = str(tm)
                            error_data['fare_dict'] = result[j]
                            error_data['traceback'] = traceback.format_exc()
                            
                            error_json.append(dict(error_data))
                            error_data = ""
                            fare_error.append(fare_dict)
                    else: 
                        if dest_lst[j] == 0:
                            pass
                        else:
                            error_data = error_dict 
                            error_data['origin_tract'] = origin
                            error_data['destination_tract'] = dest_lst[j]
                            error_data['departure_datetime'] = str(tm)
                            error_data['fare_dict'] = result[j]
                            error_data['traceback'] = traceback.format_exc()
                            
                            error_json.append(dict(error_data))
                            error_data = ""
                            fare_error.append(fare_dict)
                        
                except:
                    pass
                
                # try:
                #     full = error_dict 
                #     full['origin_tract'] = origin
                #     full['destination_tract'] = dest_lst[j]
                #     full['departure_datetime'] = str(tm)
                #     full['fare_dict'] = fare_dict
                #     full['traceback'] = traceback.format_exc()
                    
                #     full_json.append(dict(full))
                #     full = ""
                #     fare_lst.append(fare_dict)
                # except:
                #     pass
                
                try:
                
                # low cost network 
                    if 'plan' in result_lowcost[j].keys():
                        
                        fare_dict = {
                          "OTP_itinerary_all": result_lowcost[j]
                        }
                        
                        try:
                            #calling the fare values, otherwise it will append to traceback
                            fare_cost = fare.fare(fare_dict, region, c)
                            if dest_lst[j] == 0:
                                continue
                            
                            output_lst_lowcost.append([origin, dest_lst[j], fare_cost, result_lowcost[j]['plan']['itineraries'][0]['duration']])

    
                        except:
                            error_data = error_dict 
                            error_data['origin_tract'] = origin
                            error_data['destination_tract'] = dest_lst[j]
                            error_data['departure_datetime'] = str(tm)
                            error_data['fare_dict'] = result_lowcost[j]
                            error_data['traceback'] = traceback.format_exc()
                            
                            error_json.append(dict(error_data))
                            error_data = ""
                            fare_error.append(fare_dict)
                    else:
                        
                        if dest_lst[j] == 0:
                            pass
                        else:
                            error_data = error_dict 
                            error_data['origin_tract'] = origin
                            error_data['destination_tract'] = dest_lst[j]
                            error_data['departure_datetime'] = str(tm)
                            error_data['fare_dict'] = result_lowcost[j]
                            error_data['traceback'] = traceback.format_exc()
                            
                            error_json.append(dict(error_data))
                            error_data = ""
                            fare_error.append(fare_dict)
                except:
                    pass
                # try:
                #     full = error_dict 
                #     full['origin_tract'] = origin
                #     full['destination_tract'] = dest_lst[j]
                #     full['departure_datetime'] = str(tm)
                #     full['fare_dict'] = fare_dict
                #     full['traceback'] = traceback.format_exc()
                    
                #     full_json.append(dict(full))
                #     full = ""
                #     fare_lst.append(fare_dict)
    
                # except:
                #     pass
            
            if index%100 == 0:
                output = pd.DataFrame(output_lst, columns = columns)
                output_lowcost = pd.DataFrame(output_lst_lowcost, columns = columns_lowcost)
                fare_df = pd.merge(output, output_lowcost, on = ['origin_tract', 'destination_tract'], how = 'outer')
                fare_df.to_csv(df_path + '/' + 'Ver1period_' + period + '.csv', index = False)
            
    
    end_time = time.time()

    # write to csv
    output = pd.DataFrame(output_lst, columns = columns)
    output_lowcost = pd.DataFrame(output_lst_lowcost, columns = columns_lowcost)

    
    fare_df = pd.merge(output, output_lowcost, on = ['origin_tract', 'destination_tract'], how = 'outer')
    fare_df.to_csv(df_path + '/' + 'Ver1period_' + period + '.csv', index = False)
    
    #writing the error json
    if len(error_json) >= 1:
        print('Some fares could not be calculated. See the error file.')
        error_path = df_path+'/missing_fare' + '.json'
        with open(error_path, 'w') as f:
            json.dump(error_json, f)

    #outputing the entire json of the itineraries for testing purposes
    # full_path = df_path+'/full' + '.json'
    # with open(full_path, 'w') as f:
    #     json.dump(full_json, f)
    print(len(output))
    
    print(end_time - start_time)
    
    conn.close()
    
    p.kill() # making sure the server shuts down
    sys.exit()




    




