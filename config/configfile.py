#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 13 00:41:46 2020

This class contains all the configurations necessary to run the dashboard for 
one region.
Each field represents the path to the file.

Python Standards:
PEP 8 -- Style Guide for Python Code
PEP 256 -- Docstring Processing System Framework
PEP 257 -- Docstring Conventions
PEP 258 -- Docutils Design Specification

@author: Diego Silva
@institution: University of Toronto
"""

from configparser import ConfigParser
from collections import namedtuple
import os
import pandas as pd
import syslog
import requests, json
from pathlib import Path

path = str(Path(os.getcwd()).parent)


CONFIG_FILE_NAME='config.cfg' #example
REGIONS_DEFAULT = ['Boston', 'New York', 'Philadelphia', 'District of Columbia', 'Chicago','Los Angeles','San Francisco-Oakland'] #example
OTP_CONFIG_FILE = 'otp_config.ini'

class FileConfig():
    """
    A class used to provide configuration file and manipulates data objects
    for each region.
    
    Attributes
    ----------
    config_file : ConfigParser object
        a structure similar to what’s found in Microsoft Windows INI files
        
    Methods
    -------
    write_default_config(region_list)
        Generates configuration file template for each region
        
    export_config(config_file=CONFIG_FILE_NAME)
        Writes configuration file    
    
    import_config(config_file=CONFIG_FILE_NAME)
        Imports configuration file        
    
    to_namedtuple(region)
        Creates namedtuple data structure for each region and extents functions
        from reader function packages
    
    get_regions()
        Gets a list of regions from ConfigParser Object
    """
    
    def __init__(self, config_file=CONFIG_FILE_NAME):
        """
        Parameters
        ----------
        config_file : Configuration file
            Receives a configuration *.ini file
        """
        self.config_parser = ConfigParser()        
        if os.path.isfile(config_file):
            self.import_config()
        else:
            print('Config file not found! Creating a new one')
            self.write_default_config(REGIONS_DEFAULT)
            self.export_config()

#*****************************************************************************
# WRITE config file
#*****************************************************************************
    # Template config.ini
    def write_default_config(self, region_list=REGIONS_DEFAULT):
        """Writes a template for configuration file.

        It needs a region list.

        Parameters
        ----------
        region_list : list, required
            List of regions.
        """
        # Object to store configurations
        config = self.config_parser

        # Transit Feeds Key
        config['API'] = {}
        config['API']['key'] = 'f2a91a7e-154d-434a-8083-2cd18e25f3d2'

        config['General'] = {}
        config['General']['county_ids'] = path + '/data/General/county_ids.csv' 
        config['General']['us_osm'] = path + '/data/General/us-latest.osm.pbf' 
        config['General']['transit_feeds'] = 'Boston, Philadelphia, District of Columbia, Chicago, Los Angeles'
        config['General']['transit_land'] = 'San Francisco-Oakland, New York'
        config['General']['gen'] = path + '/data/General' 
        config['General']['otp_input'] = path + '/otp/otp_input'
        config['General']['otp'] = path + '/otp'

        # Paths
        for REGION in region_list:
            # Boston region config
            config[REGION] = {}   
            # Boundary data
            config[REGION]['block_group_polygons'] = path + '/data/' + REGION + '/input/boundary_data/block_group_poly.geojson'
            config[REGION]['block_group_points'] = path + '/data/' + REGION + '/input/boundary_data/block_group_pts.csv'
            config[REGION]['tract_polygons'] = path + '/data/' + REGION + '/input/boundary_data/tract_poly.geojson'
            config[REGION]['tract_points'] = path + '/data/' + REGION + '/input/boundary_data/tract_pts.csv'
            config[REGION]['county_boundaries'] = path + '/data/' + REGION + '/input/boundary_data/country_boundaries.geojson'
            config[REGION]['region_boundary'] = path + '/data/' + REGION + '/input/boundary_data/region_boundary.geojson'   
            # Open Street Map
            config[REGION]['osm'] = path + '/data/' + REGION + '/input/osm_data/'   
            # Population data
            config[REGION]['population_data'] = path + '/data/' + REGION + '/input/population_data/'   
            # Destination data
            config[REGION]['destination_data'] = path + '/data/' + REGION + '/input/destination_data/destination_employment_lehd.csv'   
            # GTFS
            config[REGION]['gtfs_static'] = path + '/data/' + REGION + '/input/gtfs/gtfs_static'
            config[REGION]['gtfs_rt'] = path + '/data/' + REGION + '/input/gtfs/gtfs_realtimes'   
            # Output
            config[REGION]['accessibility'] = path + '/data/' + REGION + '/input/output/accessibility_calc_output'
            config[REGION]['equity'] = path + '/data/' + REGION + '/input/output/equity_calc_output'
            config[REGION]['fare'] = path + '/data/' + REGION + '/input/output/fare_calc_output'
            config[REGION]['reliability'] = path + '/data/' + REGION + '/input/output/fare_calc_output'
            config[REGION]['service'] = path + '/data/' + REGION + '/input/output/servicehours_calc_output'   
            # OTP
            config[REGION]['graphs'] = path + '/data/' + REGION + '/otp/graphs'  
            config[REGION]['itinerary'] = path + '/data/' + REGION + '/otp/itinerary' 

          

    def export_config(self, config_file=CONFIG_FILE_NAME):
        """Exports configuration file as *.cfg

        Parameters
        ----------
        config_file : Configuration file, required
            The config_file default is None CONFIG_FILE_NAME global variable
        """
        # export config.ini
        with open(config_file, 'w') as cfg:
            self.config_parser.write(cfg)
            
    def import_config(self, config_file=CONFIG_FILE_NAME):
        """Imports configuration file as *.ini and allocates to Object parameter
        FileConfig() class.
        
        Parameters
        ----------
        config_file : Configuration file, required
            The config_file default is None CONFIG_FILE_NAME global variable
        """
        self.config_parser.read(config_file)
                   
#*****************************************************************************
# READ config file
#*****************************************************************************
    def to_namedtuple(self, region):
        """Creates named tuple data sctructure.
        Named tuples assign meaning to each position in a tuple and allow for 
        more readable, self-documenting code.

        Parameters
        ----------
        region : list, required
            List of regions.
        """
        config = self.config_parser       
        if region in config.sections():
            # template namedtuple
            Region = namedtuple('Region', ['name',
                                           'block_group_polygons',
                                           'block_group_points',
                                           'country_boundaries',
                                           'region_boundary',
                                           'osm',
                                           'population_data',
                                           'destination_data',
                                           'gtfs_static',
                                           'gtfs_rt',
                                           'accessibility',
                                           'equity',
                                           'fare',
                                           'reliability',
                                           'service',
                                           'otp',
                                           'points'])
            try:    
                return Region(
                            region,
                            config[region]['block_group_polygons'],
                            config[region]['block_group_points'],
                            config[region]['country_boundaries'],
                            config[region]['region_boundary'],
                            config[region]['osm'],
                            config[region]['population_data'],
                            config[region]['destination_data'],
                            config[region]['gtfs_static'],
                            config[region]['gtfs_rt'],
                            config[region]['accessibility'],
                            config[region]['equity'],
                            config[region]['fare'],
                            config[region]['reliability'],
                            config[region]['reliability'],
                            config[region]['service'],
                            pd.read_csv(config[region]['points'])
                            )
            except Exception as e:
                # if dashboard is installed on Linux/AWS we can check syslog file
                syslog.syslog(syslog.LOG_ERR, 'Failed to create namedtuple: %s' % e)              
        else:
            syslog.syslog(syslog.LOG_ERR, 'Region not found') #log Unix
            #journalctl -f -u syslog verificar dinamicamente 
            # cd /var/log > tail syslog
            print('Region not found')
            
    def get_regions(self):
        """Access sections inside configuration file and returns a list containing
        each region header.
        """
        return self.config_parser.sections()
    

class OTPConfig():
    """
    A class used to provide OTP configuration file.
    
    Attributes
    ----------
    config_file : ConfigParser object
        a structure similar to what’s found in Microsoft Windows INI files
        
    Methods
    -------
    write_default_config(region_list)
        Generates configuration file template.
        
    export_config(config_file=CONFIG_FILE_NAME)
        Writes configuration file    
    
    import_config(config_file=CONFIG_FILE_NAME)
        Imports configuration file        
    """
    
    def __init__(self,config_file=OTP_CONFIG_FILE):
        """
        Parameters
        ----------
        config_file : Configuration file
            Receives a configuration *.ini file
        """
        self.config_parser = ConfigParser()
        if os.path.isfile(config_file):
            self.import_otp_config()
        else: # if config is not created
            print('OTP config file not found! Creating a new one')
            self.write_otp_config()
            self.export_otp_config()

#*****************************************************************************
# WRITE config file
#*****************************************************************************        
    def write_otp_config(self):
        """Writes a template for configuration file.
        """
        
        config = self.config_parser
        config['NETWORK1']   =     {
                                    'time':			'7:30am',
                                    'date':			'03-05-2020',
                                    'mode':			'TRANSIT,WALK',
                                    'maxWalkDistance':	10000,
                                    'clampInitialWait':	0,
                                    'wheelchair':		False, 
                                    'numItineraries': 	1
	                               }
        
        config['NETWORK2']   =     {              
                                    'time':			'7:30am',
                                    'date':			'03-05-2020',
                                    'mode':			'TRANSIT,WALK',
                                    'maxWalkDistance':	10000,
                                    'clampInitialWait':	0,
                                    'wheelchair':		False, 
                                    'numItineraries': 	1
	                               }
        
        config['NETWORK3']   =     {              
                                    'time':			'7:30am',
                                    'date':			'03-05-2020',
                                    'mode':			'TRANSIT,WALK',
                                    'maxWalkDistance':	10000,
                                    'clampInitialWait':	0,
                                    'wheelchair':		False, 
                                    'numItineraries': 	1
	                               }
        config['OTP'] = {}
        config['OTP']['api'] = 'http://localhost:8080/otp/routers/default/plan'

    def export_otp_config(self, config_file=OTP_CONFIG_FILE):
        """Exports configuration file as *.ini

        Parameters
        ----------
        config_file : Configuration file, required
            The config_file default is None CONFIG_FILE_NAME global variable
        """
        # export config.ini
        with open(config_file, 'w') as cfg:
            self.config_parser.write(cfg)

    def import_otp_config(self, config_file=OTP_CONFIG_FILE):
        """Imports configuration file as *.ini and allocates to Object parameter
        OTPConfig() class.
        
        Parameters
        ----------
        config_file : Configuration file, required
            The config_file default is None CONFIG_FILE_NAME global variable
        """
        self.config_parser.read(config_file)

class OTPParser():
    """
    A classtto connect to OTP graph and send requests of trip.
    
    Attributes
    ----------
    origin_destination : File with all block Ids
    network: network to calculate trips
        
    Methods
    -------
    request_data(origin_longitude,origin_latitude,destination_longitude,destination_latitude)
        Request trip from OTP.
        
    make_all_requests()
        Iteration over points file to generate trips from OTP
    """        
    
    def __init__(self, origin_destination, otp_config, network):
        """
        Parameters
        ----------
        origin_destination: .*csv file with block Ids latitude and longitude
        otp_config: OTP config file 
        network: type of Network provided to OTP parameters
        """
        self.origin_destination = origin_destination
        self.otp_config = otp_config
        self.network = network
        
    def request_data(self,origin_longitude,origin_latitude,destination_longitude,destination_latitude):
        """Requests trip from OTP

        Parameters
        ----------
        origin_longitude: origin block id longitude
        origin_latitude: origin block id latitude
        destination_longitude: destination block id latitude
        destination_latitude: destination block id latitude
        """     
        parameters =  {
            		'fromPlace': str(origin_latitude) + ", " + str(origin_longitude),
            		'toPlace': str(destination_latitude) + ", " + str(destination_longitude),
            		'time': self.otp_config[self.network]['time'], #dynamic parameters
            		'date':self.otp_config[self.network]['date'], #dynamic parameters
            		'mode':self.otp_config[self.network]['mode'], # static
            		'maxWalkDistance':self.otp_config[self.network]['maxWalkDistance'], #static
            		'clampInitialWait':self.otp_config[self.network]['clampInitialWait'], #static
            		'wheelchair':self.otp_config[self.network]['wheelchair'], #static
            		'numItineraries': self.otp_config[self.network]['numItineraries'] #static
                    #agencies to ban
                    }                           
        response = requests.get(self.otp_config['OTP']['api'], params = parameters)
        return json.loads(response.text)
    
    def make_all_requests(self):
        """Iteration over points file to generate trips from OTP
        """     
        output = []
        for index_origin, row_origin in self.origin_destination.iterrows():
        	origin_longitude = row_origin['LONGITUDE']
        	origin_latitude = row_origin['LATITUDE']
        	block_origin = row_origin['BLK_ID']
        	for index_destination, row_destination in self.origin_destination.iterrows():
        		destination_longitude = row_destination['LONGITUDE']
        		destination_latitude = row_destination['LATITUDE']
        		block_destination = row_destination['BLK_ID']
        		if block_origin != block_destination:
        			trips = self.request_data(origin_longitude,origin_latitude,
                                     destination_longitude,destination_latitude)
        			output_data = {}
        			output_data["origin_block"] = block_origin
        			output_data["destination_block"] = block_destination
        			output_data["OTP_itinerary_all"] = trips
        			output.append(output_data)
        # write to a json file
        with open('test.json', 'w') as f:
            json.dump(output, f)
