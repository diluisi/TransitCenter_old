#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 22:53:37 2020

@author: diluisi
"""
import sqlite3
import json
import os
import sys

from pathlib import Path
fare_path = str(Path(os.getcwd()).parent) + '/fare'



DB_NAME = fare_path + '/FareDB.db'
print(DB_NAME)

# recebe somente trips que possuem um caminho alcançável
def list_mode(json_trip):
    list_modes = []
    inter_leg_walk = [] 
    try:    
        for i in range(len(json_trip['OTP_itinerary_all']['plan']['itineraries'][0]['legs'])):
            if json_trip['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][i]['mode'] != "WALK":
                list_modes.append(i)
            if 0 < i < max(list_modes):
                inter_leg_walk.append(i)
    except:
        print('There is no path connecting origin destination.')
    return list_modes, inter_leg_walk
        
def query_exceptions(agency_id,region_id,route_type,route_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT * FROM Exceptions WHERE agency_id=:ag_id AND region_id=:reg_id AND route_type=:r_tp AND route_id=:r_id",
              {'ag_id':agency_id, 'reg_id':region_id, 'r_tp':route_type, 'r_id':route_id})
    lst_qry = c.fetchall()
    conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][4]

def query_agency(agency_id,agency_name,region_id,route_type):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT * FROM Agency WHERE agency_id=:ag_id AND agency_name=:ag_name AND region_id=:reg_id AND route_type=:r_tp AND fare_type_id <> 5",
              {'ag_id':agency_id, 'ag_name':agency_name,'reg_id':region_id, 'r_tp':route_type})
    lst_qry = c.fetchall()
    conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][4]

def query_zone_fare(agency_id,agency_name,region_id,route_type,origin_zone,destination_zone):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT * FROM ZoneFare WHERE agency_id=:ag_id AND agency_name=:ag_name AND region_id=:reg_id AND route_type=:r_tp AND origin_zone=:orig AND destination_zone=:dest",
              {'ag_id':agency_id, 'ag_name':agency_name,'reg_id':region_id,'r_tp':route_type,'orig':origin_zone,'dest': destination_zone})
    lst_qry = c.fetchall()
    conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][6]
    
def query_zone(agency_id,agency_name,region_id,route_type,stop_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT * FROM Zone WHERE agency_id=:ag_id AND agency_name=:ag_name AND region_id=:reg_id AND route_type=:r_tp AND stop_id=:stp_id",
              {'ag_id':agency_id,'ag_name':agency_name,'reg_id':region_id,'r_tp':route_type,'stp_id':stop_id})
    lst_qry = c.fetchall()
    conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][5]
    
def query_region(region):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT * FROM Regions WHERE region=:rg",{'rg':region})
    lst_qry = c.fetchall()
    conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][0]

def query_static_fare(agency_id,route_type,region_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT * FROM StaticFare WHERE agency_id=:ag_id AND route_type=:r_tp AND region_id=:rg_id",
              {'ag_id':agency_id,'r_tp':route_type,'rg_id':region_id})
    lst_qry = c.fetchall()
    conn.close()
    if not lst_qry:
        return None
    else:
        return lst_qry[0][3]

def query_transfer_rules(current_agency_id,current_route_type,current_route_id,current_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    # generic
    c.execute("SELECT * FROM TransferRules WHERE current_agency_id=:current_ag_id AND current_route_type=:current_rt_tp AND current_route_id=:current_rt_id AND current_stop_id=:current_st_id AND previous_agency_id=:previous_ag_id AND previous_route_type=:previous_rt_tp AND previous_route_id=:previous_rt_id AND previous_stop_id=:previous_st_id AND region_id=:rg_id",
              {'current_ag_id':current_agency_id,'current_rt_tp':current_route_type,'current_rt_id':current_route_id,'current_st_id':current_stop_id,'previous_ag_id':previous_agency_id,'previous_rt_tp':previous_route_type,'previous_rt_id':previous_route_id,'previous_st_id':previous_stop_id,'rg_id':region_id})
    lst_qry = c.fetchall()
    
    # stop level
    #c.execute("SELECT * FROM TransferRules WHERE current_agency_id=:current_ag_id AND current_route_type=:current_rt_tp AND current_route_id=:current_rt_id AND current_stop_id=:current_st_id AND previous_agency_id=:previous_ag_id AND previous_route_type=:previous_rt_tp AND previous_route_id=:previous_rt_id AND previous_stop_id=:previous_st_id AND region_id=:rg_id",
    #          {'current_ag_id':current_agency_id,'current_rt_tp':current_route_type,'current_rt_id':current_route_id,'current_st_id':current_stop_id,'previous_ag_id':previous_agency_id,'previous_rt_tp':previous_route_type,'previous_rt_id':previous_route_id,'previous_st_id':previous_stop_id,'rg_id':region_id})
    #stop_level = c.fetchall()
    # route level
    #c.execute("SELECT * FROM TransferRules WHERE current_agency_id=:current_ag_id AND current_route_type=:current_rt_tp AND current_route_id=:current_rt_id AND current_stop_id='.' AND previous_agency_id=:previous_ag_id AND previous_route_type=:previous_rt_tp AND previous_route_id=:previous_rt_id AND previous_stop_id='.' AND region_id=:rg_id",
    #          {'current_ag_id':current_agency_id,'current_rt_tp':current_route_type,'current_rt_id':current_route_id,'previous_ag_id':previous_agency_id,'previous_rt_tp':previous_route_type,'previous_rt_id':previous_route_id,'rg_id':region_id})
    #route_level = c.fetchall()
    # mode level
    #c.execute("SELECT * FROM TransferRules WHERE current_agency_id=:current_ag_id AND current_route_type=:current_rt_tp AND current_route_id='.' AND current_stop_id='.' AND previous_agency_id=:previous_ag_id AND previous_route_type=:previous_rt_tp AND previous_route_id=:'.' AND previous_stop_id='.' AND region_id=:rg_id",
    #          {'current_ag_id':current_agency_id,'current_rt_tp':current_route_type,'previous_ag_id':previous_agency_id,'previous_rt_tp':previous_route_type,'rg_id':region_id})
    #mode_level = c.fetchall()
    # agency level
    #c.execute("SELECT * FROM TransferRules WHERE current_agency_id=:current_ag_id AND current_route_type='.' AND current_route_id='.' AND current_stop_id='.' AND previous_agency_id=:previous_ag_id AND previous_route_type='.' AND previous_route_id=:'.' AND previous_stop_id='.' AND region_id=:rg_id",
    #          {'current_ag_id':current_agency_id,'previous_ag_id':previous_agency_id,'rg_id':region_id})
    #agency_level = c.fetchall()
    conn.close()
    if not lst_qry:
        return []
    else:
        return lst_qry
    
def lookup_transfer(current_agency_id,current_route_type,current_route_id,current_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id):
    
    # stop level    
    stop_level = query_transfer_rules(current_agency_id,current_route_type,current_route_id,current_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id)
    # route level
    route_level = query_transfer_rules(current_agency_id,current_route_type,current_route_id,'.',previous_agency_id,previous_route_type,previous_route_id,'.',region_id)
    # mode level
    mode_level = query_transfer_rules(current_agency_id,current_route_type,'.','.',previous_agency_id,previous_route_type,'.','.',region_id)
    # agency level
    agency_level = query_transfer_rules(current_agency_id,'.','.','.',previous_agency_id,'.','.','.',region_id)
    # lista com todas as regras mapeadas das queries
    transfer_control = [stop_level, route_level, mode_level, agency_level]
    
    return transfer_control

def rule_beautifier(leg_item,transfer_rules, transfer_list, rule_id_true):    
    temp_lst = [] #lista de regras
    rules_choosed = [] # lista das regras escolhidas na leg atual
    
    # exclui da lista os níveis que não retornaram valor da query
    for item in transfer_rules:
        if item:
            temp_lst.append(item)        
    # Se houver itens para serem ajustados, cria uma lista de listas para posterior manipulação
    if temp_lst:
        for i in temp_lst:
            # iterando nas tuplas retornadas da query
            for j in i:
                # se a regra não estiver contida na lista de regras válidas, incluir
                if j[0] not in rule_id_true:
                    rule_id = j[0]
                    max_duration = j[9]
                    max_transfer = j[10]
                    fare_cost = j[11]
                    rule_level = j[12]
                    status = True # status da regra
                    transfer_list.append([rule_id, max_duration, max_transfer, fare_cost,leg_item,rule_level,status])
                    rule_id_true.append(j[0])
                # regras que serão utilizadas para atualização
                rules_choosed.append(transfer_list.index([j[0], j[9], j[10], j[11],leg_item,j[12],status]))

    return transfer_list, rule_id_true, rules_choosed 

#****************************************************************************************************** AJUSTAR
def transfer_update(rules_id_true,leg_item,leg_duration,flag,transfer_list,current_agency_id,current_route_type,current_route_id,current_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id):   
    # busca as regras cadastradas na tabela de transferências
    transfer_rules = lookup_transfer(current_agency_id,current_route_type,current_route_id,current_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id)
    # ajusta a lista de regras
    tnsfr_lst, rl_id_true, rls_choosed = rule_beautifier(leg_item,transfer_rules, transfer_list,rules_id_true)
    # verifica se a lista de regras escolhidas de transferência está vazia
    if rls_choosed:
        cost = tnsfr_lst[rls_choosed[0]][3]
        flag = 1
    else:
        flag = 0
        cost = 0
    #atualiza a duração e o transfer
    for i in range(len(tnsfr_lst)):
        if (tnsfr_lst[i][6]==True) and (tnsfr_lst[i][0] in rls_choosed):
            tnsfr_lst[i][1] -= leg_duration
            tnsfr_lst[i][2] -= 1
        else:
            tnsfr_lst[i][1] -= leg_duration          
    #atualiza as regras que ainda estão valendo
    for i in range(len(tnsfr_lst)):
        if ((tnsfr_lst[i][1] <= 0) or (tnsfr_lst[i][2] <= 0)) and (tnsfr_lst[i][6]):
            tnsfr_lst[i][6] = False
            rl_id_true.remove(tnsfr_lst[i][0])    
    
    return tnsfr_lst, flag, cost, rl_id_true
#********************************************************************************************* AJUSTAR

def fare(jsn, region):
    # Region validation
    valid_regions = {'Boston', 'New York', 'Chicago', 'Washington DC', 'San Francisco', 'Philadelphia'}
    if region not in valid_regions:
        raise ValueError("results: Region must be one of %r." % valid_regions)
    
    region_id = query_region(region)
    control_lst, inter_leg_walk = list_mode(jsn)
    transfer_list = []
    rule_true = []
    
    fare = 0
    if not control_lst:
        return fare 
    else:
        transfers = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['transfers']
        # Case: no transfer
        if transfers == 0:
            # Variables setup 
            previous_agency_id      = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['agencyId'] #ok
            previous_agency_name    = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['agencyName'] #ok
            previous_route_type     = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['routeType'] #ok
            previous_route_id       = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['routeId'].split(':')[1] #ok
            previous_stop_id        = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['from']['stopId'].split(':')[1] #ok            
            next_stop_id            = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[0]]['to']['stopId'].split(':')[1] #ok            

            exc_fare = query_exceptions(previous_agency_id,region_id,previous_route_type,previous_route_id)
            
            if exc_fare == None: 
                # Query Agency and verify fare_type_id
                fare_type_id = query_agency(previous_agency_id,previous_agency_name,region_id,previous_route_type)
                # Query for each type of fare
                if fare_type_id == 1:
                    fare = query_static_fare(previous_agency_id,previous_route_type,region_id)
                elif fare_type_id == 2:
                    fare = query_zone_fare(previous_agency_id,previous_agency_name,region_id,previous_route_type,
                                    query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,previous_stop_id),
                                    query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,next_stop_id))
                    
                else:
                    fare = 0
            else:
                return exc_fare/100
        else:
            
            partial_cost=[]
            flag=0
            
            for leg_index, leg_item in enumerate(control_lst):
                # setup variables for the first leg, for next looping variables will be update by swapping values
                if leg_index < (len(control_lst) - 1):
                    # Variables setup 
                    previous_agency_id      = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyId'] #ok
                    previous_agency_name    = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyName'] #ok
                    previous_route_type     = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeType'] #ok
                    previous_route_id       = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeId'].split(':')[1] #ok
                    previous_stop_id        = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['from']['stopId'].split(':')[1] #ok
                    next_agency_id          = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index+1]]['agencyId'] #ok
                    #next_agency_name        = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][leg_item]['agencyName'] #ok
                    next_route_type         = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index+1]]['routeType'] #ok
                    next_route_id           = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index+1]]['routeId'].split(':')[1] #ok
                    next_stop_id            = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['to']['stopId'].split(':')[1] #ok
                    leg_duration            = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['duration']
                    #start_time              =
                    #end_time                =
                else:
                    previous_agency_id      = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyId'] #ok
                    previous_agency_name    = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyName'] #ok
                    previous_route_type     = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeType'] #ok
                    previous_route_id       = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeId'].split(':')[1] #ok
                    previous_stop_id        = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['from']['stopId'].split(':')[1] #ok
                    if previous_route_type == 2:
                        next_agency_id          = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['agencyId'] #ok
                        next_route_type         = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeType'] #ok
                        next_route_id           = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['routeId'].split(':')[1] #ok
                        next_stop_id            = jsn['OTP_itinerary_all']['plan']['itineraries'][0]['legs'][control_lst[leg_index]]['to']['stopId'].split(':')[1] #ok

                if flag == 0:
                    # se não for o último elemento e a flag for zero
                    if leg_index < (len(control_lst) - 1):
                        
                        exc_fare = query_exceptions(previous_agency_id,region_id,previous_route_type,previous_route_id)
                        
                        if exc_fare == None: 
                            # Query Agency and verify fare_type_id
                            fare_type_id = query_agency(previous_agency_id,previous_agency_name,region_id,previous_route_type)
                            print(fare_type_id)
                            # Query for each type of fare
                            if fare_type_id == 1:
                                fare = query_static_fare(previous_agency_id,previous_route_type,region_id)
                                partial_cost.append(fare)
                                
                            elif fare_type_id == 2:
                                fare = query_zone_fare(previous_agency_id,previous_agency_name,region_id,previous_route_type,
                                                query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,previous_stop_id),
                                                query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,next_stop_id))
                                partial_cost.append(fare)
                            else:
                                fare = 0
                                partial_cost.append(fare)
                        else:
                            
                            partial_cost.append(exc_fare)
                        
                        transfer_list, flag, cost,rule_true = transfer_update(rule_true,leg_item,leg_duration,flag,transfer_list,next_agency_id,next_route_type,next_route_id,next_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id)
                        partial_cost.append(cost) # append valor que retorna do transfer dict
                   # se for o último elemento da lista e a flag for zero
                    else:
                        exc_fare = query_exceptions(previous_agency_id,region_id,previous_route_type,previous_route_id) 
                        if exc_fare == None: 
                            # Query Agency and verify fare_type_id
                            fare_type_id = query_agency(previous_agency_id,previous_agency_name,region_id,previous_route_type)
                            # Query for each type of fare
                            if fare_type_id == 1:
                                fare = query_static_fare(previous_agency_id,previous_route_type,region_id)
                                partial_cost.append(fare)
                            elif fare_type_id == 2:
                                
                                fare = query_zone_fare(previous_agency_id,previous_agency_name,region_id,previous_route_type,
                                                query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,previous_stop_id),
                                                query_zone(previous_agency_id,previous_agency_name,region_id,previous_route_type,next_stop_id))
                                partial_cost.append(fare)
                            else:
                                fare = 0
                                partial_cost.append(fare)
                        else:
                            partial_cost.append(exc_fare)
                # se a flag for 1
                else:
                    #se não for o último elemento da lista
                    if leg_index < (len(control_lst) - 1):
                        transfer_list, flag, cost, rule_true = transfer_update(rule_true,leg_item,leg_duration,flag,transfer_list,next_agency_id,next_route_type,next_route_id,next_stop_id,previous_agency_id,previous_route_type,previous_route_id,previous_stop_id,region_id)
                        partial_cost.append(cost) # append valor que retorna do transfer dict
                    # se for o último elemento da lista
                    else:
                        break
            # total fare for each leg
            print(partial_cost)
            fare =  sum(partial_cost)    
    return fare/100
# =============================================================================
# 
#     
# if __name__ == '__main__':
#     
#     # walk trip
#     test_case_1 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/test_case_trip_walk.json' #0
#     # missing trip
#     test_case_2 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/test_case_trip_null.json' #0
#     # two legs: WALK + BUS
#     test_case_3 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/test_case_trip_walk_bus.json' #2.00
#     # two legs: WALK + BUS EXCEPTIONS
#     test_case_4 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/test_case_trip_walk_bus_exception.json'#5.25
#     # two legs: WALK + BUS - TRANSFER==1
#     test_case_5 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/test_case_trip_walk_bus_t1.json' #2.00
#     # Testes Rick
#     test_case_6 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK.json' #0
#     test_case_7 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_BUS_WALK.json' #2.00
#     test_case_8 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_EXPRESS_WALK.json' #5.25
#     test_case_9 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_RAIL_WALK_1.json' #2.75
#     test_case_10 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_RAIL_WALK_2.json' #4.75
#     test_case_11 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_RAIL_WALK_3.json' #2.75
#     test_case_12 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_RAIL_WALK_4.json' #2.75
#     test_case_13 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_SUBWAY_WALK.json' #2.90
#     test_case_14 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_BUS_BUS_WALK_Boston.json' #2.00
#     test_case_15 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_BUS_WALK_BUS_WALK_OutsideBoston.json'#2.00
#     test_case_16 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_BUS_WALK_RAIL_WALK.json' #8.50
#     test_case_17 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_BUS_WALK_SUBWAY_WALK.json' #4.90
#     test_case_18 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_EXPRESS_WALK_BUS_WALK.json' #5.25
#     test_case_19 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_EXPRESS_WALK_SUBWAY_WALK.json' #8.15
#     test_case_20 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_RAIL_WALK_BUS_WALK.json' #2.40
#     test_case_21 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_RAIL_WALK_SUBWAY_WALK.json' #9.90
#     test_case_22 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_SUBWAY_WALK_BUS_WALK.json' #4.90
#     test_case_23 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_SUBWAY_WALK_RAIL_WALK.json' #10.90
#     test_case_24 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/2_WALK_BUS_WALK_RAIL_WALK_BUS_WALK.json' #11.00
#     test_case_25 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/2_WALK_BUS_WALK_RAIL_WALK_SUBWAY_WALK.json' #14.90
#     test_case_26 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/2_WALK_TRAM_WALK_SUBWAY_WALK_BUS_WALK.json' #7.80
#     test_case_27 = '/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/3_WALK_BUS_BUS_WALK_SUBWAY_WALK_BUS_WALK.json'
# 
# 
# 
#     with open(test_case_27) as t:
#         data = json.load(t)
#     print(list_mode(data))
#     #print(exc_fare)
#     fare = fare(data,'Boston')
#     print(fare)
#     #transfers = data['OTP_itinerary_all']['plan']['itineraries'][0]['transfers']
#     #if transfers == 0:
#     #    print(list_mode(data))
#         #a = query_exceptions(843,1,3,10739)
#         #query_zone(1,'MBTA','Boston',2,'Readville')[0][5]
#     #    a = query_zone_fare(1,'MBTA',2,query_zone(1,'MBTA','Boston',2,'Readville')[0][5],query_zone(1,'MBTA','Boston',2,'Lincoln')[0][5])[0][5]/100
#     #    print(a)
# 
# 
#  
# 
# =============================================================================
