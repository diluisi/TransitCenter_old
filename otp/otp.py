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

config = configparser.ConfigParser()
config.read('../config.cfg')

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
                move(config[region]['gtfs_static'] + "/feeds_" + input_date+'/' + file, 'otp_input/' + file, )
        move(config[region]['osm'] + '/' + region + '.osm.pbf', 'otp_input/' + region + 'osm.pbf', )
        
        result = subprocess.run(['java', '-Xmx8G', '-jar', 'otp-1.4.0-shaded.jar', '--build', '../otp/otp_input',
                        '--analyst'], stdout=subprocess.PIPE)
        
        # writes a log of the shell output
        with open('../otp/build_otp_log.txt', 'w') as f:
            f.truncate()
            f.write(result.stdout.decode())
            f.close()
        
        with open('../otp/build_otp_log.txt', 'r') as f:     
            last_line = f.readlines()[-1]
            f.close()
            
        # make sub directory for the Graphs
        os.makedirs(config[region]['graphs'] + 'graphs-' + input_date, exist_ok=True)
        
        # move it!
        move("../otp/otp_input/Graph.obj", config[region]['graphs'] + 'graphs-'+ input_date + "/Graph.obj")
        move('otp_input/' + region + 'osm.pbf', config[region]['osm'] + '/' + region + '.osm.pbf' )
        
        # Move GTFS to archive
        gtfs_files = os.listdir("otp_input")
        for file in gtfs_files:
            if ".zip" in file:
                move('../otp/otp_input/' + file, config[region]['gtfs_static'] + "/feeds_" + input_date+'/' + file)
        
            
        return last_line

if __name__ == '__main__':
    build.build_otp(str(datetime.date(datetime.now())))



