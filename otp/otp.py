#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 29 15:15:31 2020

@author: Rick
"""

import geopandas as gpd
import pandas as pd
import requests
import json
import csv
from datetime import datetime 
from time import sleep
import os
import urllib
from urllib.request import urlopen
import subprocess
from shutil import copyfile
from shutil import move

class study_area:
    class boston:
        county_ids = ["25017", "25025", "25009", "25021", "25023", "25005", "25027", "33015", "33017"]
        region = 'Boston'
        