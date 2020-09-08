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


#command line arguments
parser = argparse.ArgumentParser(description='Process region')
parser.add_argument("-d", '--date', default = datetime.datetime.now().strftime('%Y-%m-%d'), help="input date for otp in YYYY-MM-DD")
parser.add_argument("-m", '--mode', default = 'TRANSIT', help="mode to check, must be supported by GTFS standards")
parser.add_argument("-r", '--region',  help="region")
parser.add_argument("-p", '--threads', default = 4, help="number of threads")
parser.add_argument("-z", '--period', help="time period, AM, EVE , MID")
parser.add_argument("-b", '--lowcost', default=False, action='store_true', help="boolean, if evaluating a non-premium network")

#reading the arguments
args = parser.parse_args()
date = args.date
mode = args.mode
region = args.region
threads = int(args.threads)
period = args.period
lowcost = args.lowcost


#finds the config file depending if the python script is called directly, or through otp_main.py
if os.path.isfile('config.cfg'):
    config = configparser.ConfigParser()
    config.read('config.cfg')
else:
    config = configparser.ConfigParser()
    config.read('../config.cfg')

#reading paths from the config
pts_path = config[region]['block_group_points'] 
graph_path = config[region]['graphs'] 
otp_path = config['General']['otp'] 
outpath = config[region]['itinerary']

if lowcost == True:
    # bans premium routes if any are set
    lowcost_flag = config[region]['premium_routes']
    lowcost_str = 'Lowcost'
else:
    lowcost_flag = None
    lowcost_str = 'All'

def run_rabbit_run(num, i, path, hr, minute, o_date):
    print ("running", i)
    
    minute = ("{:02d}".format(int(minute)))
    
    #shell command to call otp
    #see otp_travel_times.py 
    call(["/Users/Rick/jython2.7.2/bin/jython", '-Dpython.path='+otp_path+'/otp-1.4.0-shaded.jar', 
          config['General']['otp'] + '/otp_travel-times.py', 
          '--date', date, '--hour', str(hr), '--minute', minute, '--mode', mode, '--o_path', i, '--d_path', 
          pts_path, '--num', num
          , '-x', path, '--region', region, '--graph', graph_path, '-a', o_date, '-b', lowcost_flag])

if __name__ == '__main__':
    
    # to ensure multithreading works on mac os
    multiprocessing.set_start_method('spawn', force=True)
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    
    start_time = time.time()
    
    # converting the periods into dates and times
    # if run date falls on weekday/weekend, then it will look for the next weekend/weekday
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
    
    # setting up the list of arguments for the otp function
    period_param = []
    for i in hr_lst:
        # randomly finding the 8 time bins per 2 hour period
        for j in range(4):
            rand = random.randint(j*15,(j+1)*15-1)
            temp = (i, rand)
            period_param.append(temp)
    
    # adding the directories to save the file into
    os.makedirs(outpath + '/' + 'matrix_' + str(date), exist_ok=True)
    os.makedirs(outpath + '/' + 'matrix_' + str(date) + '/period' + period, exist_ok=True)
    os.makedirs(outpath + '/' + 'matrix_' + str(date) + '/period' + period + '/mode' + lowcost_str, exist_ok=True)
    
    # directory for the output path
    json_path = outpath + '/' + 'matrix_' + str(date) + '/period' + period + '/mode' + lowcost_str
    
    
    #reading the block pts
    df_pts = pd.read_csv(pts_path)
    
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
    for j in range(8):
        param = []
        for i in range(threads):
            
            #tupple containing the path to the partitioned block group pts file, output directory, time bin, and date
            tup = (str(c), path_parts[i], json_path, period_param[j][0], period_param[j][1], o_date)
            param.append(tup)
            c = c + 1
            
        # running the otp function
        pool.starmap(run_rabbit_run, param)
        
    pool.close()
    
    # removing all the partitioned files
    for f in path_parts:
        os.remove(f)

    #combining the results of each of the threaded otp output into 1 csv file
    os.chdir(json_path)
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    od_matrix = pd.concat([pd.read_csv(f) for f in all_filenames ])
    for f in all_filenames:
        os.remove(f)
    #export to csv
    od_matrix.to_csv("otp_matrix" + '-' + str(date) + '_period' + period + '_' + mode + ".csv", index=False)




    print (time.time() - start_time)