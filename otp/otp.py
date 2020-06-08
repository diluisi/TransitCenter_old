#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 29 15:15:31 2020

@author: Rick
"""

import os
import subprocess
from shutil import move


class build:
    
    def build_otp(input_date):
        
        
        # Move gtfs files
        gtfs_files = os.listdir('../gtfs/feeds_'+input_date)
        for file in gtfs_files:
            if ".zip" in file:
                move('../gtfs/feeds_'+input_date+'/' + file, 'otp_input/' + file, )
        
        result = subprocess.run(['java', '-Xmx8G', '-jar', 'otp-1.4.0-shaded.jar', '--build', '../otp/otp_input',
                        '--analyst'], stdout=subprocess.PIPE)

        with open('build_otp_log.txt', 'w') as f:
            f.truncate()
            f.write(result.stdout.decode())
            f.close()
        
        with open('build_otp_log.txt', 'r') as f:     
            last_line = f.readlines()[-1]
            f.close()
            
        # make sub directory for the Graphs
        os.makedirs("graphs/graph-" + input_date, exist_ok=True)
        
        # move it!
        move("otp_input/Graph.obj", "graphs/graph-" + input_date + "/Graph.obj")
        
        os.makedirs('../gtfs/feeds_'+input_date, exist_ok=True)
        
        # Move GTFS to archive
        gtfs_files = os.listdir("otp_input")
        for file in gtfs_files:
            if ".zip" in file:
                move('otp_input/' + file, '../gtfs/feeds_'+input_date+'/' + file)
        
            
        return last_line




