#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 21:19:01 2020

@author: Rick
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




parser = argparse.ArgumentParser(description='Process region')
parser.add_argument("-d", '--date', default = datetime.datetime.now().strftime('%Y-%m-%d'), help="input date for otp")
parser.add_argument("-t", '--hour', default = 8, help="hour")
parser.add_argument("-m", '--mode', default = 'TRANSIT', help="mode to check")
parser.add_argument("-r", '--region',  help="region")
parser.add_argument("-p", '--threads', default = 4, help="number of threads")
parser.add_argument("-y", '--minute', default = '00', help="minute")

args = parser.parse_args()

date = args.date
hr = int(args.hour)
mode = args.mode
region = args.region
threads = int(args.threads)
minute = ("{:02d}".format(int(args.minute)))



config = configparser.ConfigParser()
config.read('../config.cfg')
pts_path = config[region]['block_group_points'] 
graph_path = config[region]['graphs'] 

outpath = config[region]['itinerary']

def run_rabbit_run(num, i, path):
    
    call(["/Users/Rick/jython2.7.2/bin/jython", "-Dpython.path=otp-1.4.0-shaded.jar", 'otp_travel-times.py', 
          '--date', date, '--hour', str(hr), '--minute', minute, '--mode', mode, '--o_path', i, '--d_path', pts_path, '--num', num
          , '-x', path, '--region', region, '--graph', graph_path])

if __name__ == '__main__':
    multiprocessing.set_start_method('spawn', force=True)
    __spec__ = "ModuleSpec(name='builtins', loader=<class '_frozen_importlib.BuiltinImporter'>)"
    
    start_time = time.time()
    

    
    
    
    os.makedirs(outpath + '/' + 'matrix_' + str(date), exist_ok=True)
    os.makedirs(outpath + '/' + 'matrix_' + str(date) + '/hr' + str(hr), exist_ok=True)
    os.makedirs(outpath + '/' + 'matrix_' + str(date) + '/hr' + str(hr) + '/mode' + mode, exist_ok=True)
    
    json_path = outpath + '/' + 'matrix_' + str(date) + '/hr' + str(hr) + '/mode' + mode
    
    df_pts = pd.read_csv(pts_path)
    
    
    df_parts = np.array_split(df_pts, threads)
    
    param = []
    path_parts = []
    for i in range(threads):
        path_parts.append(pts_path[:-4]+'_partition'+str(i) + '.csv')
        df_parts[i].to_csv(path_parts[i])
        tup = (str(i), path_parts[i], json_path)
        param.append(tup)
    
    pool = multiprocessing.Pool()
    

    pool.starmap(run_rabbit_run, param)
    pool.close()
    
    for f in param:
        os.remove(f[1])

    os.chdir(json_path)
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    od_matrix = pd.concat([pd.read_csv(f) for f in all_filenames ])
    for f in all_filenames:
        os.remove(f)
    #export to csv
    od_matrix.to_csv("otp_matrix" + '-' + str(date) + '_hr' + str(hr) + '_' + mode + ".csv", index=False)

    
    print (time.time() - start_time)