#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 27 01:44:37 2020

Create a base-class, which defines database model we will be using
in Transit Center Project.
Most of fields are based on GTFS.
Fare costs are based on web information.
@author: diluisi
"""
from peewee import Model, SqliteDatabase, TextField, ForeignKeyField

# Ensure foreign-key constraints are enforced.
# Create a database instance that will manage the connection and
# execute queries
database = SqliteDatabase('Fare.db', pragmas={'foreign_keys': 1})

class BaseModel(Model):
    '''
    Create a base-class all our models will inherit, which defines
    the database we'll be using.
    '''
    class Meta:
        database = database
        legacy_table_names = False

class Regions(BaseModel):
    '''
    region_id = id for each region
    region = Region name
    '''
    region_id = TextField()
    region = TextField()
    class Meta:
        table_name = 'regions'

class Mode(BaseModel):
    '''
    Route Type is based on GTFS standard id reference.
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
    route_type = TextField()
    mode = TextField()
    class Meta:
        table_name = 'mode'
        
class Media(BaseModel):
    '''
    0 - cash
    1 - ticket (a paper ticket that can be loaded)
    2 - card (no pass)
    3 - pass card (benefit for high frequency users)
    '''
    media_id = TextField()
    media_type = TextField()
    class Meta:
        table_name = 'media'

class Fare(BaseModel):
    '''
    fare_type_id = 1,'STATIC'; 2,'ZONAL'; 3,'DISTANCE'; 4,'DYNAMIC'; 5,'SPECIFIC'
    '''
    fare_type_id = TextField()
    fare_type = TextField()   
    class Meta:
        table_name = 'fare'

class Agency(BaseModel):
    '''
    Agencies for each region, based on GTFS file.
    agency_id = Agency id in agency.txt
    agency_name = Agency name in agency.txt
    route_type =  0,'TRAM'; 1,'SUBWAY'; 2,'RAIL'; 3,'BUS'; 4,'FERRY'; 5,'CABLE TRAM'
                  6,'AERIAL LIFT'; 7,'FUNICULAR'; 11,'TROLLEYBUS'; 12,'MONORAIL'
    fare_type_id = 1,'STATIC'; 2,'ZONAL'; 3,'DISTANCE'; 4,'DYNAMIC'; 5,'SPECIFIC'
    '''
    agency_id =  TextField()
    agency_name = TextField() 
    region_id = ForeignKeyField(Regions, backref='agency')
    route_type = ForeignKeyField(Mode, backref='agency')
    fare_type_id = ForeignKeyField(Fare, backref='agency')   
    class Meta:
        table_name = 'agency'

class ZoneFare(BaseModel):
    '''
    agency_id = Agency id in agency.txt
    agency_name = Agency name in agency.txt
    route_type =  0,'TRAM'; 1,'SUBWAY'; 2,'RAIL'; 3,'BUS'; 4,'FERRY'; 5,'CABLE TRAM'
                  6,'AERIAL LIFT'; 7,'FUNICULAR'; 11,'TROLLEYBUS'; 12,'MONORAIL'
    origin_zone = zone name
    destination_zone =  zone name
    fare_cost = cost based on web information
    '''
    agency_id = ForeignKeyField(Agency, backref='zone_fare')
    agency_name = ForeignKeyField(Agency, backref='zone_fare')
    route_type = ForeignKeyField(Mode, backref='zone_fare')
    origin_zone = TextField()
    destination_zone = TextField()
    fare_cost = TextField()
    media_id = ForeignKeyField(Media, backref='zone_fare')
    class Meta:
        table_name = 'zone_fare'

class StaticFare(BaseModel):
    '''
    Cost of static fare type based on agency web page 
    '''
    agency_id = ForeignKeyField(Agency, backref='static_fare')
    route_type = ForeignKeyField(Mode, backref='static_fare')
    fare_cost = TextField()
    class Meta:
        table_name = 'static_fare'

class TransferType(BaseModel):
    '''
    Transfers can be applied in four distinct ways:
    0 - Stop transfer
    1 - Route transfer
    2 - Mode transfer
    3 - Agency transfer
    '''
    transfer_type_id = TextField()
    transfer_type = TextField()
    class Meta:
        table_name = 'transfer_type'
        
class TransferRules(BaseModel):
    '''
    Rules for transferring is based on agreements between agencies and/or
    rules within a region transit authority.
    Most of these rules are available on internet
    '''
    current_agency_id = TextField()
    current_route_type = TextField() 
    current_route_id = TextField()
    current_stop_id = TextField()
    previous_agency_id = TextField()
    previous_route_type = TextField() 
    previous_route_id = TextField()
    previous_stop_id = TextField()
    region_id = ForeignKeyField(Regions, backref='transfer')
    transfer_type_id = ForeignKeyField(TransferType, backref='transfer')
    max_duration = TextField()
    max_transfer = TextField()
    fare_cost_transfer = TextField()
    transfer_type = ForeignKeyField(Mode, backref='transfer')
    media_id = ForeignKeyField(Media, backref='transfer')
    class Meta:
        table_name = 'transfer'

class Exceptions(BaseModel):
    '''
    Exceptions are appplied when agencies have more than one mode with different 
    fare structures.
    '''
    agency_id = ForeignKeyField(Agency, backref='exceptions')
    agency_name = ForeignKeyField(Agency, backref='exceptions')
    region_id = ForeignKeyField(Regions, backref='exceptions')
    route_type = ForeignKeyField(Mode, backref='exceptions')
    route_id = TextField()
    fare_cost = TextField()
    media_id = ForeignKeyField(Media, backref='exceptions')
    class Meta:
        table_name = 'exceptions'
        
class Zone(BaseModel):
    '''
    Name of stations and associated zones
    '''
    agency_id = ForeignKeyField(Agency, backref='zone')
    agency_name = ForeignKeyField(Agency, backref='zone')
    region_id = ForeignKeyField(Regions, backref='zone')
    route_type = ForeignKeyField(Mode, backref='zone') 
    stop_id = TextField()
    stop_code = TextField()
    stop_name = TextField()
    fare_zone = TextField()
    media_id = ForeignKeyField(Media, backref='zone')
    class Meta:
        table_name = 'zone'

if __name__ == "__main__":
    #DB creation
    database.connect()
    database.create_tables([Agency,Fare,Mode,ZoneFare,StaticFare,TransferRules,Exceptions,Regions,Zone,TransferType,Media]) 