#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 29 15:15:31 2020

@author: Rick
"""

import os
import subprocess
from shutil import move
from datetime import datetime
import configparser

if os.path.isfile('config.cfg'):
    config = configparser.ConfigParser()
    config.read('config.cfg')
else:
    config = configparser.ConfigParser()
    config.read('../config.cfg')
    
class gtfsException(Exception):
     pass

class build:
    
    def build_otp(region, input_date):
        '''
        
        Calls a shell command to build the OTP graph
        Will run if the GTFS data, OSM data, and .jar file are in their associated directories.

        Parameters
        ----------
        input_date : string
            date when the GTFS files were pulled.

        Returns
        -------
        last_line : string
            time it took to run the command, or if the command failed

        '''

        # Move gtfs files
        gtfs_files = os.listdir(config[region]['gtfs_static'] + '/feeds_'+input_date)
        for file in gtfs_files:
            if ".zip" in file:
                move(config[region]['gtfs_static'] + "/feeds_" + input_date+'/' + file, config['General']['otp_input'] + '/' + file, )
        move(config[region]['osm'] + region + '.osm.pbf', config['General']['otp_input'] + '/' + region + '.osm.pbf', )
        
        result = subprocess.run(['java', '-Xmx8G', '-jar', config['General']['otp'] + '/otp-1.4.0-shaded.jar', 
                                 '--build', config['General']['otp_input'], '--analyst'], stdout=subprocess.PIPE)
        
        # writes a log of the shell output
        with open(config['General']['otp'] + '/build_otp_log.txt', 'w') as f:
            f.truncate()
            f.write(result.stdout.decode())
            f.close()
        
        with open(config['General']['otp'] + '/build_otp_log.txt', 'r') as f:     
            last_line = f.readlines()[-1]
            f.close()
            
        # make sub directory for the Graphs
        os.makedirs(config[region]['graphs'] + '/graphs-' + input_date, exist_ok=True)
        
        # move it!
        move(config['General']['otp_input'] + "/Graph.obj", config[region]['graphs'] + '/graphs-'+ input_date + "/Graph.obj")
        
        if os.path.getsize(config[region]['graphs'] + '/graphs-'+ input_date + "/Graph.obj") < 50000000:
            print('Error')
        
        move(config['General']['otp_input'] + '/' + region + '.osm.pbf', config[region]['osm'] + region + '.osm.pbf' )
        
        # Move GTFS to archive
        gtfs_files = os.listdir(config['General']['otp_input'])
        for file in gtfs_files:
            if ".zip" in file:
                move(config['General']['otp_input'] + '/' + file, config[region]['gtfs_static'] + "/feeds_" + input_date+'/' + file)
        
        size = os.stat(config[region]['gtfs_static'] + "/feeds_" + input_date+'/' + file).st_size
        
        if size < 100000000: # graph files are always larger than 100
            
            with open(config['General']['otp'] + '/build_otp_log.txt', 'r') as f:
                raw_data = f.read().split('\n')
            raw_data.reverse()
            
            
            i = 0

            while True:
                
                i = i + 1
                
                if raw_data[i].split()[2] == '(GtfsModule.java:163)':
                    row = raw_data[i]
                    break
            error_feed = row.split('/')[-1]
            
            raise gtfsException(error_feed + ' is not a valid feed.\n Consider removing the feed or choose a new date with a valid feed')
            
        return last_line

if __name__ == '__main__':
    build.build_otp(str(datetime.date(datetime.now())))



