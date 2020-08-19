#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 29 17:49:02 2020
https://www.excelcise.org/python-sqlite-insert-data-pandas-data-frame/
@author: diluisi
"""
import sqlite3
from subprocess import call
import pandas as pd
import os

def db_connect(db_file):
    """
    Connect to an SQlite database, if db file does not exist it will be created
    param db_file: absolute or relative path of db file
    return: sqlite3 connection
    """
    if os.path.isfile(db_file):
        sqlite3_conn = sqlite3.connect(db_file)
    else:
        call(['python', 'FareDB.py'])
        sqlite3_conn = sqlite3.connect(db_file)
    return sqlite3_conn

def insert_to_table(table_name, df,conn):
    """
    Insert data to tables
    """
    df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
    print('SQL insert process on {table} table finished'.format(table=table_name))

if __name__ == '__main__':
    tables = {'Agency': pd.read_csv("agency.txt"),
              'Exceptions': pd.read_csv("exceptions.txt"),
              'Fare': pd.read_csv("fare_type.txt"),
              'Media': pd.read_csv("media.txt"),
              'Mode': pd.read_csv("mode.txt"),
              'Regions': pd.read_csv("regions.txt"),
              'StaticFare': pd.read_csv("static_fare.txt"),
              'TransferRules': pd.read_csv("transfer.txt"),
              'TransferType': pd.read_csv("transfer_type.txt"),
              'Zone': pd.read_csv("zone.txt"),
              'ZoneFare': pd.read_csv("zone_fare.txt")}  
    
    db_file_path = "FareDB.db"
    conn = sqlite3.connect(db_file_path)
    #print('ok')
    
    for key, value in tables.items():
        insert_to_table(key, value, conn)
    conn.close()