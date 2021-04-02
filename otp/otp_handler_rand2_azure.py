#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 21:19:01 2020

@author: Rick


This function sets up the parameters/files/directories to be used in the otp jython call. 
It will also call the command to run otp.

This script can either be run directly from the command line, or be used in conjuction with otp_main.py.
"""

import multiprocessing
from subprocess import call
import pandas as pd
import numpy as np
import time
import configparser
import argparse
import os
import glob
import datetime
from datetime import timedelta
import random
from contextlib import contextmanager
import csv
import sys


#command line arguments
parser = argparse.ArgumentParser(description='Process region')
parser.add_argument("-d", '--date', default = datetime.datetime.now().strftime('%Y-%m-%d'), help="input date for otp in YYYY-MM-DD")
parser.add_argument("-r", '--region',  help="region")
parser.add_argument("-p", '--threads', default = 4, help="number of threads")
parser.add_argument("-z", '--period', help="time period, AM, EVE , MID")


#reading the arguments
args = parser.parse_args()
date = args.date
mode = 'TRANSIT'
region = args.region
threads = int(args.threads)
period = args.period



#finds the config file depending if the python script is called directly, or through otp_main.py
if os.path.isfile('config.cfg'):
    config = configparser.ConfigParser() 
    config.read('config.cfg')
else:
    config = configparser.ConfigParser()
    config.read('../config.cfg')

#reading paths from the config
pts_path = config[region]['block_group_points'] 
#pts_path = config[region]['tract_points'] 
graph_path = config[region]['graphs'] 
otp_path = config['General']['otp'] 
outpath = config[region]['itinerary']


#reads the list of premium routes to ban
results = []
with open(config[region]['gtfs_static'] + '/premium_routes.csv') as f:
    for row in csv.reader(f):
        results.append(row[0])
banned_routes = ','.join(results)



def run_rabbit_run(num, o_path, path, hr, minute, o_date, suffix):
    print ("running", o_path)
    
    minute = ("{:02d}".format(int(minute)))
    
    #shell command to call otp
    #see otp_travel_times.py 
    call(["/home/transitcenter/jython2.7.2/bin/jython", '-J-Xmx7500m', '-Dpython.path='+otp_path+'/otp-1.4.0-shaded.jar', 
          config['General']['otp'] + '/otp_travel-times.py', 
          '--date', date, '--hour', str(hr), '--minute', minute, '--mode', mode, '--o_path', o_path, '--d_path', 
          pts_path, '--num', num
          , '--out', path, '--region', region, '--graph', graph_path, '--o_date', o_date, '--lowcost', banned_routes, 
          '--suffix', suffix])

# temporarily changes working directory
@contextmanager
def cwd(path):
    oldpwd=os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpwd)

if __name__ == '__main__':
    
    # to ensure multithreading works on mac os
    # multiprocessing.set_start_method('spawn', force=True)
    # __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    
    start_time = time.time()
    
    # converting the periods into dates and times
    # if run date falls on weekday/weekend, then it will look for the next weekend/weekday
    
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    while True:
        if dt.weekday() != 3:
            dt = dt + timedelta(days = 1)
        else:

            wk_date = dt.strftime('%Y-%m-%d')

            pm_dt = dt + timedelta(days = 1)
            pm_date = pm_dt.strftime('%Y-%m-%d')
            break
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    while True:
        if dt.weekday() != 5:
            dt = dt + timedelta(days = 1)
        else:
            we_date = dt.strftime('%Y-%m-%d')
            break

    # Legacy Periods
    if period == 'MP':
        o_date = wk_date
        hr_lst = [7,8]
    
    elif period == 'WE':
        o_date = we_date
        hr_lst = [10,11]
        
    elif period == 'PM':
        o_date = wk_date
        hr_lst = [22,23]

    # These new periods are to account for the server being on UTC time
    elif period == 'EST_MP':
        o_date = wk_date
        hr_lst = [12,13]
    elif period == 'EDT_MP':
        o_date = wk_date
        hr_lst = [11,12]
    elif period == 'CST_MP':
        o_date = wk_date
        hr_lst = [13,14]
    elif period == 'CDT_MP':
        o_date = wk_date
        hr_lst = [12,13]
    elif period == 'PST_MP':
        o_date = wk_date
        hr_lst = [15,16]
    elif period == 'PDT_MP':
        o_date = wk_date
        hr_lst = [14,15]

    elif period == 'EST_PM':
        o_date = pm_date
        hr_lst = [3,4]
    elif period == 'EDT_PM':
        o_date = pm_date
        hr_lst = [2,3]
    elif period == 'CST_PM':
        o_date = pm_date
        hr_lst = [4,5]
    elif period == 'CDT_PM':
        o_date = pm_date
        hr_lst = [3,4]
    elif period == 'PST_PM':
        o_date = pm_date
        hr_lst = [6,7]
    elif period == 'PDT_PM':
        o_date = pm_date
        hr_lst = [5,6]

    elif period == 'EST_WE':
        o_date = we_date
        hr_lst = [15,16]
    elif period == 'EDT_WE':
        o_date = we_date
        hr_lst = [14,15]
    elif period == 'CST_WE':
        o_date = we_date
        hr_lst = [16,17]
    elif period == 'CDT_WE':
        o_date = we_date
        hr_lst = [15,16]
    elif period == 'PST_WE':
        o_date = we_date
        hr_lst = [18,19]
    elif period == 'PDT_WE':
        o_date = we_date
        hr_lst = [17,18]
    else:
        print('Invalid Period:' + period)

    period_notz = period[-2:]
    
    # adding the directories to save the file into
    os.makedirs(outpath + '/' + 'travel_times', exist_ok=True)
    os.makedirs(outpath + '/' + 'travel_times/' + str(date), exist_ok=True)
    os.makedirs(outpath + '/' + 'travel_times/' + str(date) + '/period' + period_notz , exist_ok=True)
    
    # directory for the output path
    json_path = outpath + '/' + 'travel_times/' + str(date) + '/period' + period_notz 
    
    # setting up the list of arguments for the otp function
    period_param = []
    j = 0
    for i in hr_lst:
        # randomly finding the 2 time bins per 2 hour period
        rand = random.randint(59 - j*30 - 29, 59 - j*30)
        temp = (i, rand)
        period_param.append(temp)
        os.makedirs(json_path + '/' + str(temp[0]) + "{:02d}".format(temp[1]) + '_all')
        os.makedirs(json_path + '/' + str(temp[0]) + "{:02d}".format(temp[1]) + '_lowcost')
        j = j + 1
    
    #reading the block pts
    df_pts = pd.read_csv(pts_path)
    
    #randomly reordering the block points for performance
    df_pts = df_pts.sample(frac=1).reset_index(drop=True)
    
    #partitioning the block pts for faster performance by multithreading the different partition
    #only the origin is partitioned, destinations will use the original unpartitioned file
    df_parts = np.array_split(df_pts, threads)
    
    #adding the different partitioned files path to a list
    path_parts = []
    for i in range(threads):
        path_parts.append(pts_path[:-4]+'_partition'+str(i) + '.csv')
        df_parts[i].to_csv(path_parts[i]) 
    
    #setting up multiprocessing
    pool = multiprocessing.Pool(processes = threads)
    
    
    c = 0
    for j in range(2):
        for k in ['lowcost', 'all']:
            param = []
            for i in range(threads):
                #tupple containing the path to the partitioned block group pts file, output directory, time bin, and date
                tup = (str(c), path_parts[i], json_path, period_param[j][0], period_param[j][1], o_date, k)
                param.append(tup)
                c = c + 1
            
            # running the otp function
            pool.starmap(run_rabbit_run, param)
        
        with cwd(json_path + '/' + str(period_param[j][0]) + "{:02d}".format(period_param[j][1]) + '_all'):        
            # combining the results of each of the threaded otp output into 1 csv file
            extension = 'csv'
            all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
            tt_all = pd.concat([pd.read_csv(f) for f in all_filenames ])
            for f in all_filenames:
                os.remove(f)
                
        with cwd(json_path +  '/' + str(period_param[j][0]) + "{:02d}".format(period_param[j][1]) + '_lowcost' ):        
            # combining the results of each of the threaded otp output into 1 csv file
            extension = 'csv'
            all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
            tt_lowcost = pd.concat([pd.read_csv(f) for f in all_filenames ])
            for f in all_filenames:
                os.remove(f)
                
    
        tt = pd.merge(tt_all, tt_lowcost, on = ['o_block', 'd_block'], how = 'outer')
        
        #export to csv
        tt.to_csv(json_path + '/' + str(period_param[j][0]) + "{:02d}".format(period_param[j][1]) + ".csv", index=False)
        
        # tt_all refers to the df for the 'regular' network, while tt_lowcost is the travel time matrix for the 'lowcost' network
        # tt_all.to_csv(json_path + '/' + str(period_param[j][0]) + "{:02d}".format(period_param[j][1]) + "_all.csv", index=False)
        # tt_lowcost.to_csv(json_path + '/' + str(period_param[j][0]) + "{:02d}".format(period_param[j][1]) + "_lowcost.csv", index=False)
        
        os.rmdir(json_path + '/' + str(period_param[j][0]) + "{:02d}".format(period_param[j][1]) + '_all')
        os.rmdir(json_path + '/' + str(period_param[j][0]) + "{:02d}".format(period_param[j][1]) + '_lowcost')
        
    pool.close()
    
    # # removing all the partitioned files
    for f in path_parts:
        os.remove(f)
        
    date_row = [date, wk_date, we_date]

    with open(outpath + '/otp_run_dates.csv','a') as fd:
        wr = csv.writer(fd, quoting=csv.QUOTE_ALL)
        wr.writerow(date_row)
        
    print (time.time() - start_time)
    sys.exit()