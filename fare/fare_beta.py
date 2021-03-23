#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 14:20:27 2021
@author: diluisi
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 22:53:37 2020
Calculate and search fares for the regions defined by Transit Center.
This function return a float number as a sum of partial costs for each leg of 
a valid trip generated by Open Trip Planner.
To define rules we search on websites of each Agency and mapped fare cost, transfer
rules, exceptions rules, and agreements between different agencies.
In cases where mapping is not possible due to lack of information or a complex 
map we apply National Transit Database (https://www.transit.dot.gov/ntd/data-product/2018-metrics)
cost per mile as a reference to calculate 
fare for those mode/agencies. 
This functionality returns a float number represented as total fare cost for a 
valid trip as the sum of partial costs.
Data collected from agencies is provided by GTFS static downloaded at Transit Feed
and Transit Land websites.
Fare process: https://docs.google.com/drawings/d/1n0UFw7Nfc0xPNZlFgpaee483qyM69SDMAMyWkPveG6c/edit
@author: Diego Silva
@Project: Transit Center
"""
import sqlite3
#import json

# Database
DB_NAME = 'FareDB.db'
# List of agencies using NTD database
EXCP = ["nj transit bus","nj transit rail", "long island rail road", "metro-north railroad", "shore line east",
        "mnr hudson rail link","ny waterway"]
# Some agencies have no agency id, we are replicating agency name as agency id for those cases
MISSING_AGENCY_ID = ["santa cruz metro","county connection","foothill transit","chicago transit authority","cecil transit","prince george’s county thebus"]


# Exclude WALK mode and keep only legs with a valid mode
def list_mode(json_trip):
    '''
    Create a list of valid modes, except WALK.
    json_trip: JSON file generated by OTP
    '''
    list_modes = []
    inter_leg_walk = [] 
    try:
        # iterate over a json file generated by OTP
        for i in range(len(json_trip['OTP_itinerary_all']['plan']['itineraries'][0]['legs'])):
            if json_trip['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][i]['mode'] != "WALK":
                list_modes.append(i)
            if 0 < i < max(list_modes):
                inter_leg_walk.append(i)
    except:
        # when there is not a valid path connecting origin destination pair
        print('There is no path connecting origin destination.')
    return list_modes, inter_leg_walk
        
def query_exceptions(agency_id,region_id,route_type,route_id, c):
    '''
    Search routes classified as excptions on table: EXCEPTIONS.
    agency_id: GTFS id agency
    route_type: GTFS mode
    0 - 'TRAM'
    1 - 'SUBWAY'
    2 - 'RAIL'
    3 - 'BUS'
    4 - 'FERRY'
    5 - 'CABLE TRAM'
    6 - 'AERIAL LIFT'
    7 - 'FUNICULAR'
    11 - 'TROLLEYBUS'
    12 - 'MONORAIL'
    route_id: id of the route
    '''    
    # conn = sqlite3.connect(DB_NAME)
    # c = conn.cursor()
    # Query to find fare exceptions
    c.execute("SELECT * FROM Exceptions WHERE agency_id=:ag_id AND region_id=:reg_id AND route_type=:r_tp AND route_id=:r_id",
              {'ag_id':agency_id, 'reg_id':region_id, 'r_tp':route_type, 'r_id':route_id})
    lst_qry = c.fetchall()
    # conn.close()
    if not lst_qry:
        # in case there is no eceptions return None
        return None
    else:
        # return integer fare cost
        return lst_qry[0][4]

def query_agency(agency_id,agency_name,region_id,route_type, c):
    '''
    Search agency and return fare type.
    agency_id: GTFS id agency
    agency_name: GTFS agency name
    region_id: region of study (Boston, NY...)
    route_type: GTFS mode
    0 - 'TRAM'
    1 - 'SUBWAY'
    2 - 'RAIL'
    3 - 'BUS'
    4 - 'FERRY'
    5 - 'CABLE TRAM'
    6 - 'AERIAL LIFT'
    7 - 'FUNICULAR'
    11 - 'TROLLEYBUS'
    12 - 'MONORAIL'
    '''    
    # conn = sqlite3.connect(DB_NAME)
    # c = conn.cursor()
    # query
    c.execute("SELECT * FROM Agency WHERE agency_id=:ag_id AND agency_name=:ag_name AND region_id=:reg_id AND route_type=:r_tp AND fare_type_id <> 5",
              {'ag_id':agency_id, 'ag_name':agency_name,'reg_id':region_id, 'r_tp':route_type})
    lst_qry = c.fetchall()
    # conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][4]

def query_zone_fare(agency_id,agency_name,region_id,route_type,origin_zone,destination_zone, c):
    '''
    Search zone fare based on Zonal GTFS classification. Return fare.
    agency_id: GTFS id agency
    agency_name: GTFS agency name
    region_id: region of study (Boston, NY...)
    route_type: GTFS mode
    0 - 'TRAM'
    1 - 'SUBWAY'
    2 - 'RAIL'
    3 - 'BUS'
    4 - 'FERRY'
    5 - 'CABLE TRAM'
    6 - 'AERIAL LIFT'
    7 - 'FUNICULAR'
    11 - 'TROLLEYBUS'
    12 - 'MONORAIL'
    origin_zone: initial leg zone 
    destination_zone: final leg zone
    '''    
    # conn = sqlite3.connect(DB_NAME)
    # c = conn.cursor()
    #query
    c.execute("SELECT * FROM ZoneFare WHERE agency_id=:ag_id AND agency_name=:ag_name AND region_id=:reg_id AND route_type=:r_tp AND origin_zone=:orig AND destination_zone=:dest",
              {'ag_id':agency_id, 'ag_name':agency_name,'reg_id':region_id,'r_tp':route_type,'orig':origin_zone,'dest': destination_zone})
    lst_qry = c.fetchall()
    # conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][6]
    
def query_zone(agency_id,agency_name,region_id,route_type,stop_id, c):
    '''
    Search zone based on Zonal GTFS classification. Return zone.
    agency_id: GTFS id agency
    agency_name: GTFS agency name
    region_id: region of study (Boston, NY...)
    route_type: GTFS mode
    0 - 'TRAM'
    1 - 'SUBWAY'
    2 - 'RAIL'
    3 - 'BUS'
    4 - 'FERRY'
    5 - 'CABLE TRAM'
    6 - 'AERIAL LIFT'
    7 - 'FUNICULAR'
    11 - 'TROLLEYBUS'
    12 - 'MONORAIL'
    stop_id: GTFS stop identification
    '''    
    # conn = sqlite3.connect(DB_NAME)
    # c = conn.cursor()
    # query
    c.execute("SELECT * FROM Zone WHERE agency_id=:ag_id AND agency_name=:ag_name AND region_id=:reg_id AND route_type=:r_tp AND stop_id=:stp_id",
              {'ag_id':agency_id,'ag_name':agency_name,'reg_id':region_id,'r_tp':route_type,'stp_id':stop_id})
    lst_qry = c.fetchall()
    # conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][5]
    
def query_region(region, c):
    '''
    Search valid region. 
    region_id: region of study (Boston, NY...)
    '''    
    # conn = sqlite3.connect(DB_NAME)
    # c = conn.cursor()
    # query
    c.execute("SELECT * FROM Regions WHERE region=:rg",{'rg':region})
    lst_qry = c.fetchall()
    # conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][0]

def query_static_fare(agency_id,route_type,region_id, agency_name,c):
    '''
    Search flat fares.
    Flat fares are cost defined by agency on their websites. No variations per day, distance or zone.
    Return fare cost.
    agency_id: GTFS id agency
    agency_name: GTFS agency name
    route_type: GTFS mode
    0 - 'TRAM'
    1 - 'SUBWAY'
    2 - 'RAIL'
    3 - 'BUS'
    4 - 'FERRY'
    5 - 'CABLE TRAM'
    6 - 'AERIAL LIFT'
    7 - 'FUNICULAR'
    11 - 'TROLLEYBUS'
    12 - 'MONORAIL'
    region_id: region of study (Boston, NY...)
    '''        
    # conn = sqlite3.connect(DB_NAME)
    # c = conn.cursor()
    # query
    c.execute("SELECT * FROM StaticFare WHERE agency_id=:ag_id AND route_type=:r_tp AND region_id=:rg_id AND agency_name=:ag_name",
              {'ag_id':agency_id,'r_tp':route_type,'rg_id':region_id, 'ag_name': agency_name})
    lst_qry = c.fetchall()
    # conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][3]

def query_transfer_rules(current_agency_id,current_route_type,current_route_id,current_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id, c):
    '''
    Search for transfer rules
    Return rule if exists.
    agency_id: GTFS id agency at the origin
    (current/ previous) route_type: GTFS mode
    0 - 'TRAM'
    1 - 'SUBWAY'
    2 - 'RAIL'
    3 - 'BUS'
    4 - 'FERRY'
    5 - 'CABLE TRAM'
    6 - 'AERIAL LIFT'
    7 - 'FUNICULAR'
    11 - 'TROLLEYBUS'
    12 - 'MONORAIL'
    (current/ previous) route_id: GTFS route identification
    (current/ previous) stop_id: GTFS stop identification
    '''    
    # conn = sqlite3.connect(DB_NAME)
    # c = conn.cursor()
    # query
    c.execute("SELECT * FROM TransferRules WHERE current_agency_id=:current_ag_id AND current_route_type=:current_rt_tp AND current_route_id=:current_rt_id AND current_stop_id=:current_st_id AND previous_agency_id=:previous_ag_id AND previous_route_type=:previous_rt_tp AND previous_route_id=:previous_rt_id AND previous_stop_id=:previous_st_id AND region_id=:rg_id",
              {'current_ag_id':current_agency_id,'current_rt_tp':current_route_type,'current_rt_id':current_route_id,'current_st_id':current_stop_id,'previous_ag_id':previous_agency_id,'previous_rt_tp':previous_route_type,'previous_rt_id':previous_route_id,'previous_st_id':previous_stop_id,'rg_id':region_id})
    lst_qry = c.fetchall()
    # conn.close()
    if not lst_qry:
        return []
    else:
        return lst_qry
    
def lookup_transfer(current_agency_id,current_route_type,current_route_id,current_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id, c):
    '''
    Search for transfer rules between agencies in four distinct levels:
        - agency level
        - mode level
        - route level
        - stop level
    Return all rules in a single list to be iterated by transfer_rules query.
    It is called separately from query_transfer_rules to pass the right parameters
    for each level.
    (current/ previous) agency_id: GTFS id agency at the origin
    (current/ previous) route_type: GTFS mode
    0 - 'TRAM'
    1 - 'SUBWAY'
    2 - 'RAIL'
    3 - 'BUS'
    4 - 'FERRY'
    5 - 'CABLE TRAM'
    6 - 'AERIAL LIFT'
    7 - 'FUNICULAR'
    11 - 'TROLLEYBUS'
    12 - 'MONORAIL'
    (current/ previous) route_id: GTFS route identification
    (current/ previous) stop_id: GTFS stop identification
    '''    
    # stop level    
    stop_level = query_transfer_rules(current_agency_id,current_route_type,current_route_id,current_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id, c)
    # route level
    route_level = query_transfer_rules(current_agency_id,current_route_type,current_route_id,'.',previous_agency_id,previous_route_type,previous_route_id,'.',region_id, c)
    # mode level
    mode_level = query_transfer_rules(current_agency_id,current_route_type,'.','.',previous_agency_id,previous_route_type,'.','.',region_id, c)
    # agency level
    agency_level = query_transfer_rules(current_agency_id,'.','.','.',previous_agency_id,'.','.','.',region_id, c)
    # list of all rules found
    transfer_control = [stop_level, route_level, mode_level, agency_level]
    
    return transfer_control

def rule_beautifier(leg_item,transfer_rules, transfer_list, rule_id_true):
    '''
    Rule beautifier adjusts rules for posterior updates.
    A list is generated with all rules for each level, if exists.
    This function helps transfer_update function to keep tracking on rules.
    Return transfer_list, rule_id_true, rules choosed
    leg_item: leg index
    transfer_rules: list of rules
    rule_id_true: once a rule is found we flag it as True status in order to used by any other leg
    '''      
    temp_lst = [] # temporary list of rules
    rules_choosed = [] # list of rules searched for current leg index
    
    # exclude from the list all levels None returned by the query
    for item in transfer_rules:
        if item:
            temp_lst.append(item)        
    # If there is a item to be adjusted, a list is created for posterior analysis
    if temp_lst:
        for i in temp_lst:
            # iteration over tuples returned by the query
            for j in i:
                # verify is rule already exists in the list, if not append
                if j[0] not in rule_id_true:
                    rule_id = j[0]
                    max_duration = j[9]
                    max_transfer = j[10]
                    fare_cost = j[11]
                    rule_level = j[12]
                    status = True # rule status
                    transfer_list.append([rule_id, max_duration, max_transfer, fare_cost,leg_item,rule_level,status])
                    rule_id_true.append(j[0])
                    rules_choosed.append(transfer_list.index([j[0], j[9], j[10], j[11],leg_item,j[12],status]))
                else:
                    print('status is false')
                    rule_id = j[0]
                    max_duration = j[9]
                    max_transfer = j[10]
                    fare_cost = j[11]
                    rule_level = j[12]
                    status = False # status da regra
                    rules_choosed.append(rule_id_true.index(j[0]))
                # valid rules
                #rules_choosed.append(transfer_list.index([j[0], j[9], j[10], j[11],leg_item,j[12],status]))
    return transfer_list, rule_id_true, rules_choosed 

def transfer_update(rules_id_true,leg_item,leg_duration,flag,transfer_list,current_agency_id,current_route_type,current_route_id,current_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id, c):   
    '''
    Update fare rules.
    Return tnsfr_lst, flag, cost, rl_id_true
    tnsfr_lst: list of valid transfers
    flag: it is a sentinel to inform whether a rule can be used for the next leg.
        - 0: next leg index will not use the rule found
        - 1: next leg index will not use the rule found
    cost: cost for transferring
    rl_id_true: rule status
    Parameters:
        rules_id_true: rule identification
        leg_item: leg index
        leg_duration: leg duration
        flag: flag status
        transfer_list: list of transfers rules
        current_agency_id: current GTFS agency identification
        current_route_type: current GTFS route type 
        current_route_id: current GTFS route identification
        current_stop_id: current GTFS stop identification
        previous_agency_id: previous GTFS agency identification
        previous_route_type: previous GTFS route type 
        previous_route_id: previous route identification
        previous_stop_id: previous GTFS stop identification
        region_id: region of study (Boston, NY...)
    '''      
    # search for rules on TABLE: TRANSFER
    transfer_rules = lookup_transfer(current_agency_id,current_route_type,current_route_id,current_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id, c)
    # update list of rules
    tnsfr_lst, rl_id_true, rls_choosed = rule_beautifier(leg_item,transfer_rules, transfer_list,rules_id_true)
    # verify if list of rules is empty
    if rls_choosed:
        cost = tnsfr_lst[rls_choosed[0]][3]
        flag = 1 #rule used
    else:
        flag = 0 # if rule is not used flag and cost equal to 0
        cost = None
    #update duration and transfer
    if rls_choosed:
        for i in range(len(tnsfr_lst)):
            if tnsfr_lst[i][0] != tnsfr_lst[rls_choosed[0]][0]:
                tnsfr_lst[i][1] -= leg_duration
            else:    
            #if (tnsfr_lst[i][6]==True) and (tnsfr_lst[i][0] == rl_id_true[rls_choosed[0]]):
                tnsfr_lst[i][1] -= leg_duration
                tnsfr_lst[i][2] -= 1
    #update valid rules        
    for i in range(len(tnsfr_lst)):
        if ((tnsfr_lst[i][1] <= 0) or (tnsfr_lst[i][2] <= 0)):
            tnsfr_lst[i][6] = False
            flag = 0
    
    # list update
    tnsfr_lst = [s for s in tnsfr_lst if s[6]==True]
    updt = []
    for i in tnsfr_lst:
        updt.append(i[0])
    rl_id_true = updt
            
    return tnsfr_lst, flag, cost, rl_id_true

def fare(jsn, region, c):
    '''
    Fare search and calculates cost for each leg index/mode. It also identified
    transfer rules applicable for each leg index and keep tracking on rules.
    jsn: valid JSON file trip generated by OTP
    region: region of study (Boston, NY...)
    '''  
    # Region validation
    valid_regions = {'Boston', 'New York', 'Chicago', 'District of Columbia', 'San Francisco-Oakland', 'Philadelphia','Los Angeles'}
    if region not in valid_regions:
        # if region is not valid
        raise ValueError("results: Region must be one of %r." % valid_regions)
    region_id = query_region(region, c) # search region id
    control_lst, inter_leg_walk = list_mode(jsn) # list of leg index to iterate over
    transfer_list = [] # transfer list
    rule_true = [] # rules id
    
    fare = 0
    # if there is no item within control list return fare
    if not control_lst:
        return fare 
    else:
        transfers = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['transfers']
        # Case: no transfer
        if transfers == 0:
            # Variables setup 
            previous_agency_name    = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['agencyName'].lower() #ok
            # Verify agencies with Null agency Id
            if previous_agency_name in MISSING_AGENCY_ID:
                previous_agency_id  = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['agencyName'].lower() #ok
            else:
                previous_agency_id  = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['agencyId'].lower() #ok
            previous_route_type     = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['routeType'] #ok
            previous_route_id       = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['routeId'].split(':')[1].lower() #ok
            previous_stop_id        = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['from']['stopId'].split(':')[1].lower() #ok            
            next_stop_id            = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['to']['stopId'].split(':')[1].lower()
            distance                = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['distance'] #ok
            
            # verify if fare is within Exception table
            exc_fare = query_exceptions(previous_agency_id,region_id,previous_route_type,previous_route_id, c)
            
            # if exception search returns None
            if exc_fare == None: 
                # Query Agency and verify fare_type_id
                fare_type_id = query_agency(previous_agency_id,previous_agency_name,region_id,previous_route_type, c)
                # Query for each type of fare
                if fare_type_id == 1: #flat fare
                    fare = query_static_fare(previous_agency_id,previous_route_type,region_id, previous_agency_name,c)
                elif fare_type_id == 2: # zonal fare
                    fare = query_zone_fare(previous_agency_id,previous_agency_name,region_id,previous_route_type,
                                    query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,previous_stop_id, c),
                                    query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,next_stop_id, c), c)    
                else:
                    fare = 0
            else:
                # if agency is within EXCP list this means we need to calculate fare based on NTDD dataset
                if previous_agency_name in EXCP:
                    return (exc_fare * distance/1000) / 100 # fare is identified as a cost/mile * distance (meters)
                else:
                    return exc_fare/100
        # Case: transfer != 0
        else:
            # partial fare cost list
            partial_cost=[]
            flag=0 # initial status
            # iteration over the list for each leg index
            for leg_index, leg_item in enumerate(control_lst):
                # setup variables for the first leg, for next looping variables will be update by swapping values
                if leg_index < (len(control_lst) - 1): # while is not the last leg index
                    # Variables setup 
                    previous_agency_name    = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyName'].lower() #ok
                    # Verify agencies with Null agency Id
                    if previous_agency_name in MISSING_AGENCY_ID:
                        previous_agency_id  = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyName'].lower() #ok
                    else:
                        previous_agency_id  = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyId'].lower() #ok
                    previous_route_type     = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeType'] #ok
                    previous_route_id       = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeId'].split(':')[1].lower() #ok
                    previous_stop_id        = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['from']['stopId'].split(':')[1].lower() #ok
                    
                    
                    fare_type_id = query_agency(previous_agency_id,previous_agency_name,region_id,previous_route_type, c)
                    
                    if fare_type_id == 2:
                        next_agency_name    = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyName'].lower() #ok
                        # Verify agencies with Null agency Id
                        if next_agency_name in MISSING_AGENCY_ID:
                            next_agency_id = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyName'].lower() #ok
                        else:
                            next_agency_id      = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyId'].lower() #ok
                        next_route_type         = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeType'] #ok
                        next_route_id           = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeId'].split(':')[1].lower() #ok
                        next_stop_id            = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['to']['stopId'].split(':')[1].lower() #ok
                        distance                = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['distance']
                    else:
                        next_agency_name        = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index+1]]['agencyName'].lower() #ok
                        # Verify agencies with Null agency Id
                        if next_agency_name in MISSING_AGENCY_ID:
                            next_agency_id      = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index+1]]['agencyName'].lower() #ok
                        else:
                            next_agency_id          = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index+1]]['agencyId'].lower() #ok
                        next_route_type         = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index+1]]['routeType'] #ok
                        next_route_id           = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index+1]]['routeId'].split(':')[1].lower() #ok
                        next_stop_id            = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['to']['stopId'].split(':')[1].lower() #ok
                    leg_duration            = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['duration']
                    distance                = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['distance']

                else: # last leg index
                    previous_agency_name    = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyName'].lower() #ok
                    # Verify agencies with Null agency Id
                    if previous_agency_name in MISSING_AGENCY_ID:
                        previous_agency_id  = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyName'].lower() #ok
                    else:
                        previous_agency_id      = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyId'].lower() #ok
                    previous_route_type     = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeType'] #ok
                    previous_route_id       = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeId'].split(':')[1].lower() #ok
                    previous_stop_id        = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['from']['stopId'].split(':')[1].lower() #ok
                    distance                = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['distance']
                    
                    fare_type_id = query_agency(previous_agency_id,previous_agency_name,region_id,previous_route_type,c)
                    
                    if fare_type_id == 2: #if last leg is rail we need destination data
                        next_agency_name    = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyName'].lower() #ok
                        # Verify agencies with Null agency Id
                        if next_agency_name in MISSING_AGENCY_ID:
                            next_agency_id = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyName'].lower() #ok
                        else:
                            next_agency_id      = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyId'].lower() #ok
                        next_route_type         = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeType'] #ok
                        next_route_id           = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeId'].split(':')[1].lower() #ok
                        next_stop_id            = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['to']['stopId'].split(':')[1].lower() #ok
                        distance                = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['distance']
                # if flag is 0 means that we didn't use rules and we can go on to calculate fare for current leg index
                if flag == 0:
                    # verify if is the last item in the list
                    if leg_index < (len(control_lst) - 1):
                        # verify if fare is within Exception table
                        exc_fare = query_exceptions(previous_agency_id,region_id,previous_route_type,previous_route_id, c)                                         
                        if exc_fare == None:
                            # Query Agency and verify fare_type_id                           
                            fare_type_id = query_agency(previous_agency_id,previous_agency_name,region_id,previous_route_type, c)
                            # Query for each type of fare
                            if fare_type_id == 1: # flat fare
                                fare = query_static_fare(previous_agency_id,previous_route_type,region_id, previous_agency_name,c)
                                partial_cost.append(fare)
                                transfer_list, flag, cost,rule_true = transfer_update(rule_true,leg_item,leg_duration,flag,transfer_list,next_agency_id,next_route_type,next_route_id,next_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id, c)
                                partial_cost.append(cost)
                            elif fare_type_id == 2: # zonal fares
                                fare = query_zone_fare(previous_agency_id,previous_agency_name,region_id,previous_route_type,
                                                query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,previous_stop_id, c),
                                                query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,next_stop_id, c), c)
                                partial_cost.append(fare)
                                transfer_list, flag, cost,rule_true = transfer_update(rule_true,leg_item,leg_duration,flag,transfer_list,next_agency_id,next_route_type,next_route_id,next_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id, c)
                                partial_cost.append(cost)
                            else:
                                fare = 0
                                partial_cost.append(fare)
                        else:
                            # verify EXCP list to apply NTD cost per mile
                            if previous_agency_name in EXCP:
                                partial_cost.append(exc_fare * distance/1000)
                            else:
                                partial_cost.append(exc_fare)
                   # if is the last leg in the trip
                    else:
                        # verify if fare is within Exception table
                        exc_fare = query_exceptions(previous_agency_id,region_id,previous_route_type,previous_route_id, c) 
                        if exc_fare == None: 
                            # Query Agency and verify fare_type_id
                            fare_type_id = query_agency(previous_agency_id,previous_agency_name,region_id,previous_route_type, c)
                            # Query for each type of fare
                            if fare_type_id == 1:
                                fare = query_static_fare(previous_agency_id,previous_route_type,region_id, previous_agency_name,c)
                                partial_cost.append(fare)
                            elif fare_type_id == 2:
                                
                                fare = query_zone_fare(previous_agency_id,previous_agency_name,region_id,previous_route_type,
                                                query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,previous_stop_id, c),
                                                query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,next_stop_id, c), c)
                                partial_cost.append(fare)
                            else:
                                fare = 0
                                partial_cost.append(fare)
                        else:
                            # verify EXCP list to apply NTD cost per mile
                            if previous_agency_name in EXCP:
                                partial_cost.append(exc_fare * distance/1000)
                            else:
                                partial_cost.append(exc_fare)
                # se a flag for 1
                else:
                    #se não for o último elemento da lista
                    if leg_index < (len(control_lst) - 1):
                        transfer_list, flag, cost, rule_true = transfer_update(rule_true,leg_item,leg_duration,flag,transfer_list,next_agency_id,next_route_type,next_route_id,next_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id, c)
                        partial_cost.append(cost) # append valor que retorna do transfer dict
                    # se for o último elemento da lista
                    else:
                        break
            # sum up partial fares
            lista_nova = []
            for tst in partial_cost:
                if tst != None:
                    lista_nova.append(tst)
            
            subtotal=0
            for soma in range(len(control_lst)):
                subtotal += lista_nova[soma]
            fare = subtotal
            
            #fare =  sum(partial_cost)
    # return total fare
    return fare/100
