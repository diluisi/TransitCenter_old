import os
import datetime
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import swifter
from gtfslite import GTFS

def levelofservice(region, gtfs_date):

    '''
    Computes weekly transit level of service for block groups

    gtfs_date: as string in "YYYY-MM-DD" format
    region: string of region name, e.g. "Boston"
    '''

    # distance to buffer each zone (could be a function parameter)
    distance = 200

    # input paths
    block_group_poly = "data/" + region + "/input/boundary_data/block_group_poly.geojson"

    # static GTFS folder
    gtfs_folder = "data/" + region + "/input/gtfs/gtfs_static/feeds_" + gtfs_date

    # output_file_path
    output_file_path = "data/" + region + "/output/LOS_output/LOS_" + gtfs_date + ".csv"


    # get a list the 7 dates for analysis
    gtfs_date = datetime.datetime.strptime(gtfs_date, '%Y-%m-%d')
    dates = []
    i = 0
    while i < 7:
        date = gtfs_date.date() - datetime.timedelta(days=i)
        dates.append(date)
        i += 1

    # crs dictionary needed for polygon buffering
    crs = {
        "Boston": 'epsg:32619',
        "New York": 'epsg:32618',
        "District of Columbia": 'epsg:32618',
        "Philadelphia": 'epsg:32618',
        "Chicago": 'epsg:32616',
        "Los Angeles": 'epsg:32611',
        "San Francisco-Oakland": 'epsg:32610',
    }

    # buffering the polygon file
    gdf = gpd.read_file(block_group_poly)
    gdf.crs = {'init' :'epsg:4326'}
    gdf = gdf.to_crs({'init': crs[region]})
    gdf["geometry"] = gdf["geometry"].buffer(distance, resolution=16)
    gdf = gdf.to_crs({'init': 'epsg:4326'})

    # getting a simple dataframe of all census block groups, this is for merging data to at the end
    blocks = gdf[["GEOID"]]

    # function for getting the number of trips for a block group
    def trips_by_block(geoid,gtfs_in,stops_geoid_in,date_in):
        stops_geoid_in = stops_geoid_in[stops_geoid_in["GEOID"] == geoid]
        stop_list = stops_geoid_in["stop_id"].to_list()
        trips_at_stops_temp = gtfs_in.trips_at_stops(stop_list, date_in)
        n_trips = len(trips_at_stops_temp.index)
        return n_trips

    # empty output list
    output = []

    # loop over GTFS files
    for filename in os.listdir(gtfs_folder):
        if filename.endswith(".zip"):
            gtfs_path = os.path.join(gtfs_folder, filename)
            print(gtfs_path)

            try:
                # loading in the GTFS data
                gtfs = GTFS.load_zip(gtfs_path)

                # converting the stops file into a geopandas point dataframe
                stops_geometry = [Point(xy) for xy in zip(gtfs.stops.stop_lon, gtfs.stops.stop_lat)]
                stops_gdf = gtfs.stops.drop(['stop_lon', 'stop_lat'], axis=1)
                stops_gdf = gpd.GeoDataFrame(stops_gdf, crs="EPSG:4326", geometry=stops_geometry)

                # spatial join block group to stop points
                stops_geoid = gpd.sjoin(stops_gdf, gdf, op="within")[["stop_id","GEOID"]]

                # getting a unique list of block groups that have stops
                unique_geoid = pd.DataFrame(stops_geoid.GEOID.unique(), columns = ["GEOID"])

                # looping over the 7 dates
                for date in dates:

                    # compute number of trips per block group, usinig the above function, and applied for each block group
                    unique_geoid["n_trips"] = unique_geoid["GEOID"].swifter.apply(trips_by_block, args=(gtfs,stops_geoid,date,))

                    output.append(unique_geoid)

                print("SUCCESS")

            except:
                print("FAILED")


    # creating single dataframe for all outupts
    output = pd.concat(output)

    # group by block group, summing total trips
    output = output.groupby(['GEOID']).sum()

    # merging data to the total set of block groups
    output = blocks.merge(output,how="outer",on="GEOID")

    # filling in 0 to block groups without transit
    output = output.fillna(0)

    # updating column names
    output.columns = ["block_group","service"]

    # saving output
    output.to_csv(output_file_path, index = False)
