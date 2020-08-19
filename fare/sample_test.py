#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug 18 00:26:56 2020

@author: diluisi
"""
from fare import fare
import json

# walk trip
test_case_1 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/test_case_trip_walk.json',0] #0
# missing trip
test_case_2 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/test_case_trip_null.json',0] #0
# two legs: WALK + BUS
test_case_3 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/test_case_trip_walk_bus.json',2] #2.00
# two legs: WALK + BUS EXCEPTIONS
test_case_4 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/test_case_trip_walk_bus_exception.json',5.25] #5.25
# two legs: WALK + BUS - TRANSFER==1
test_case_5 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/test_case_trip_walk_bus_t1.json',2] #2.00
# Testes Rick
test_case_6 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK.json',0] #0
test_case_7 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_BUS_WALK.json',2] #2.00
test_case_8 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_EXPRESS_WALK.json',5.25] #5.25
test_case_9 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_RAIL_WALK_1.json',2.75] #2.75
test_case_10 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_RAIL_WALK_2.json',4.75] #4.75
test_case_11 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_RAIL_WALK_3.json',2.75] #2.75
test_case_12 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_RAIL_WALK_4.json',2.75] #2.75
test_case_13 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/0_WALK_SUBWAY_WALK.json',2.90] #2.90
test_case_14 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_BUS_BUS_WALK_Boston.json',2] #2.00
test_case_15 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_BUS_WALK_BUS_WALK_OutsideBoston.json',2]#2.00
test_case_16 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_BUS_WALK_RAIL_WALK.json',8.50] #8.50
test_case_17 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_BUS_WALK_SUBWAY_WALK.json',4.90] #4.90
test_case_18 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_EXPRESS_WALK_BUS_WALK.json',5.25] #5.25
test_case_19 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_EXPRESS_WALK_SUBWAY_WALK.json',8.15] #8.15
test_case_20 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_RAIL_WALK_BUS_WALK.json',2.4] #2.40
test_case_21 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_RAIL_WALK_SUBWAY_WALK.json',9.9] #9.90
test_case_22 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_SUBWAY_WALK_BUS_WALK.json',4.9] #4.90
test_case_23 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/1_WALK_SUBWAY_WALK_RAIL_WALK.json',10.9] #10.90
test_case_24 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/2_WALK_BUS_WALK_RAIL_WALK_BUS_WALK.json',11] #11.00
test_case_25 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/2_WALK_BUS_WALK_RAIL_WALK_SUBWAY_WALK.json',14.9] #14.90
test_case_26 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/2_WALK_TRAM_WALK_SUBWAY_WALK_BUS_WALK.json',7.8] #7.80
test_case_27 = ['/home/diluisi/Documentos/Doutorado/Projeto US/Fare_DB/Test_Set/3_WALK_BUS_BUS_WALK_SUBWAY_WALK_BUS_WALK.json',6.9]


with open(test_case_1[0]) as t:
    data = json.load(t)

def test_fare():
    assert fare(data,"Boston") == 0

