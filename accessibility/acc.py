

# these are the libraries needed to run, all can be installed at there latest versions, either pip or conda, unless noteed otherwise

import os
import datetime
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import swifter # install via pip
from gtfslite import GTFS # install via pip install gtfs-lite
import tracc # install via pip
import time


# note - check Ids for SF and LA, since they have leading 0s


def get_nexp_beta(in_param, in_region):
    '''
    function for generating the value for beta for a negative exponential decay function based on median travel time
    '''


    acc_config_region = pd.read_csv('accessibility/acc_config_regional.csv')

    acc_config_region = acc_config_region[acc_config_region["region"] == in_region]

    median_time = float(acc_config_region[in_param])

    return np.log(0.5) / median_time




def levelofservice(region, gtfs_date):

    '''
    Computes weekly transit level of service for block groups

    gtfs_date: as string in "YYYY-MM-DD" format
    region: string of region name, e.g. "Boston"
    '''

    start_time = time.time()

    # distance to buffer each zone (could be a function parameter)
    distance = 200

    # input paths
    block_group_poly = "data/" + region + "/input/boundary_data/block_group_poly.geojson"

    # static GTFS folder
    gtfs_folder = "data/" + region + "/input/gtfs/gtfs_static/feeds_" + gtfs_date

    # file for dates when OTP was run
    otp_run_path = "data/" + region + "/otp/itinerary/otp_run_dates.csv"

    # output_file_path
    output_file_path = "data/" + region + "/output/" + "measures_" + gtfs_date + "_" + "LOS" + ".csv"

    # output measure name
    output_measure_name = "los_trips"
    print(output_measure_name)


    # get the dates to compute the metric
    otp_run = pd.read_csv(otp_run_path, names = ["folder_date","run_date_weekday","run_date_saturday"])
    otp_run = otp_run.drop_duplicates()
    otp_run = otp_run[otp_run["folder_date"] == gtfs_date]

    print(otp_run)

    # the two dates for running the metric
    gtfs_date_wkd = datetime.datetime.strptime(otp_run["run_date_weekday"].item(), '%Y-%m-%d')
    gtfs_date_sat = datetime.datetime.strptime(otp_run["run_date_saturday"].item(), '%Y-%m-%d')

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
    output_sat = []
    output_wkd = []

    # loop over GTFS files
    for filename in os.listdir(gtfs_folder):
        if filename.endswith(".zip"):
            gtfs_path = os.path.join(gtfs_folder, filename)

            print("-----------------------------")

            print(gtfs_path)

            try:
                gtfs = GTFS.load_zip(gtfs_path)

                # converting the stops file into a geopandas point dataframe
                stops_geometry = [Point(xy) for xy in zip(gtfs.stops.stop_lon, gtfs.stops.stop_lat)]
                stops_gdf = gtfs.stops.drop(['stop_lon', 'stop_lat'], axis=1)
                stops_gdf = gpd.GeoDataFrame(stops_gdf, crs="EPSG:4326", geometry=stops_geometry)

                # spatial join block group to stop points
                stops_geoid = gpd.sjoin(stops_gdf, gdf, op="within")[["stop_id","GEOID"]]

                # getting a unique list of block groups that have stops
                unique_geoid = pd.DataFrame(stops_geoid.GEOID.unique(), columns = ["GEOID"])
                unique_geoid_2 = pd.DataFrame(stops_geoid.GEOID.unique(), columns = ["GEOID"])

                # compute number of trips per block group, usinig the above function, and applied for each block group

                # for saturday
                date = gtfs_date_sat
                print(date)
                try:
                    unique_geoid_2["n_trips"] = unique_geoid_2["GEOID"].swifter.apply(trips_by_block, args=(gtfs,stops_geoid,date,))
                    print("Success")
                except:
                    print("Failed")
                output_sat.append(unique_geoid_2)

                # for a weekday
                date = gtfs_date_wkd
                print(date)
                try:
                    unique_geoid["n_trips"] = unique_geoid["GEOID"].swifter.apply(trips_by_block, args=(gtfs,stops_geoid,date,))
                    print("Success")
                except:
                    print("Failed")
                output_wkd.append(unique_geoid)




            except:
                print("Loading failed :(")


            print(time.time() - start_time)

            print("-----------------------------")




    # creating single dataframe for all outupts
    output_sat = pd.concat(output_sat)
    output_wkd = pd.concat(output_wkd)

    # group by block group, summing total trips
    output_sat = output_sat.groupby(['GEOID']).sum()
    output_wkd = output_wkd.groupby(['GEOID']).sum()


    # merging data to the total set of block groups
    output_sat = blocks.merge(output_sat,how="outer",on="GEOID")
    output_wkd = blocks.merge(output_wkd,how="outer",on="GEOID")

    # filling in 0 to block groups without transit
    output_sat = output_sat.fillna(0)
    output_wkd = output_wkd.fillna(0)

    # updating column names
    output_sat.columns = ["bg_id","score"]
    output_wkd.columns = ["bg_id","score"]

    # add column for measure name
    output_sat["score_key"] = "los_trips_SAT"
    output_wkd["score_key"] = "los_trips_WKD"

    # adding in a field for date
    output_sat["date"] = gtfs_date
    output_wkd["date"] = gtfs_date


    output = pd.concat([output_wkd, output_sat])

    output["score"] = output["score"] / 24

    # saving output
    output.to_csv(output_file_path, index = False)

    print(output)

    print("meow :)")





def transit_accessibility(region, date, period):
    '''
    computes all transit accessibility measures for our study for a study region for a single week
    i.e. this will have to be repeated weekly
    '''

    # <dest>_<measure>_<param>_<period>_<autoFlag>_<fareFlag>_<date>

    # autoY autoN
    # fareY fareN

    # make sure the data directory is as follows
    #
    # data
    # --region
    # ----input
    # ------boundary_data
    # --------block_group_poly.geojson
    # --------block_group_pts.csv
    # ------destination_data
    # --------employment.csv
    # --------groceries_snap.sv
    # --------healthcare.csv
    # --------education.csv
    # --------greenspace.csv

    start_time = time.time()

    # get complete list of block groups
    dfo = pd.read_csv("data/" + region + "/input/boundary_data/" + "block_group_pts.csv", dtype=str)
    dfo = dfo[["GEOID"]]

    # get path to spatial boundaries of block groups
    spatial_boundaries = "data/" + region + "/input/boundary_data/" + "block_group_poly.geojson"


    # get intrazonal times, for appending to travel time matrix later on
    dfint = dfo
    from tracc.spatial import radius
    radius_poly = radius(
        spatial_data_file_path = spatial_boundaries,
        id_field = "GEOID"
    )
    radius_poly["GEOID"] = radius_poly["GEOID"].astype(str)
    dfint = dfint.merge(radius_poly, on = "GEOID")
    del radius_poly
    dfint["o_block"] = dfint["GEOID"]
    dfint["d_block"] = dfint["GEOID"]
    del dfint['GEOID']
    dfint["fare_all"] = 0
    dfint["fare_lowcost"] = 0
    dfint["time_all"] = dfint["radius"] / 0.1
    dfint["time_lowcost"] = dfint["time_all"]
    del dfint["radius"]


    # zones with weird data that we need to smooth that didnt come up originally:
    also_missing = {
        "New York": ["360470193003","360610001001","340170103004"],
        "Boston": [],
        "Chicago": [],
        "District of Columbia": ["110010092031"],
        "Los Angeles": [],
        "Philadelphia": [],
        "San Francisco-Oakland": ["060411242001"]
    }
    also_missing = also_missing[region]



    # get fare threshold, and the date for fares
    acc_config_region = pd.read_csv('accessibility/acc_config_regional.csv')
    acc_config_region = acc_config_region[acc_config_region["region"] == region]
    fare_threshold = float(acc_config_region["fare_threshold"])
    fare_date = acc_config_region["fare_date"].iloc[0]

    # getting a neighbourds matrix
    from tracc.spatial import get_neighbours
    neighbours = get_neighbours(
        spatial_data_file_path = spatial_boundaries,
        weight_type = "KNN",
        idVariable = "GEOID",
        param = 10
    )

    # load in supply data
    dftemp = pd.read_csv("data/" + region + "/input/destination_data/employment.csv", dtype=str)
    dfo = pd.merge(dfo,dftemp, how='left', left_on="GEOID", right_on="block_group_id")
    for dataset in ["groceries_snap.csv","healthcare.csv","education.csv","greenspace.csv"]:
        dftemp = pd.read_csv("data/" + region + "/input/destination_data/" + dataset, dtype=str)
        dfo = pd.merge(dfo,dftemp, how='left', left_on="GEOID", right_on="GEOID")
    del dftemp
    dfo["schools"] = dfo["count"]
    dfo["parks"] = dfo["area"].astype(float) * 247.10538161 # from km2 to acres
    dfo["urgentcare"] = dfo["urgent_care_facilities"]
    del dfo["count"], dfo["area"], dfo["urgent_care_facilities"]
    dfo = dfo.fillna(0)

    # load in supply data
    dftemp = pd.read_csv("data/" + region + "/input/destination_data/employment.csv", dtype=str)


    # loading in the accessibility config file
    acc_config = pd.read_csv('accessibility/acc_config.csv')
    destination_types = list(acc_config.destination.unique())

    # unique impedence to compute
    impedences_times = acc_config[acc_config["type_code"] != "M"]
    impedences_times =  impedences_times[["function_name","function","cost","params","fare"]]
    impedences_times = impedences_times.drop_duplicates()

    # access_measures
    access_measures = acc_config

    # create the supply object
    dfo = tracc.supply(
        supply_df = dfo,
        columns = ["GEOID"] + destination_types
    )
    # making sure the destinations are integers
    for dest in destination_types:
        dfo.data[dest] = dfo.data[dest].astype(float)
        dfo.data[dest] = dfo.data[dest].astype(int)


    # chunking the origins
    geoids = dfo.data["GEOID"].to_list()
    n_chunks = 2000
    def chunks(lst,n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    geoid_chunks = list(chunks(geoids,n_chunks))


    start_time = time.time()



    # setting the periods for input and output
    period_times = period
    period_access = period
    if period_access == "MP":
        period_access = "AM"


    # load in the fares data for this time period
    try:
        dff = pd.read_csv("data/" + region + "/otp/itinerary/fares/period_MP.csv", dtype={'origin_block': str, 'destination_block': str, 'fare_all': float, 'fare_lowcost': float})
        dff = dff[["origin_block","destination_block","fare_all","fare_lowcost"]]
        dff = dff.rename(columns = {"origin_block":"o_block"})
        dff = dff.rename(columns = {"destination_block":"d_block"})

        if region == "Los Angeles" or region == "San Francisco-Oakland":

            dff["o_block"] = "0" + dff["o_block"]

            dff["d_block"] = "0" + dff["d_block"]

    except:
        dff = pd.read_csv("accessibility/fake_fares.csv", dtype={'origin_block': str, 'destination_block': str, 'fare_all': float, 'fare_lowcost': float})
        dff = dff[["origin_block","destination_block","fare_all","fare_lowcost"]]
        dff = dff.rename(columns = {"origin_block":"o_block"})
        dff = dff.rename(columns = {"destination_block":"d_block"})

    # take the fare data into rounded floats for saving space
    # dff["fare_all"] = dff["fare_all"].astype(float)
    # dff["fare_lowcost"] = dff["fare_lowcost"].astype(float)
    dff["fare_all"] = dff["fare_all"].round(1)
    dff["fare_lowcost"] = dff["fare_lowcost"].round(1)

    print("n fares:", dff.shape[0])

    # creating the output dataframe
    dfout_P = pd.DataFrame(columns=['GEOID', 'measure', 'value'])
    dfout_M = pd.DataFrame(columns=['GEOID', 'measure', 'value'])

    print("number of chunks:", len(geoid_chunks))
    print("chunk size", n_chunks)

    # loop over the (8 or 2) travel time matrices in the study period (e.g. in AM, PM, WE)
    j = 0
    directory = "data/" + region + "/otp/itinerary/travel_times/" + date + "/period" + period_times + "/"
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):

            travel_time_matrix = os.path.join(directory, filename)

            # only use 2 travel time matrices
            if j == 2:
                break
            j += 1


            print("input time matrix", travel_time_matrix)

            # read in the travel time matrix
            dftall = pd.read_csv(travel_time_matrix, dtype=str)

            if region == "Los Angeles" or region == "San Francisco-Oakland":

                dftall["o_block"] = "0" + dftall["o_block"]

                dftall["d_block"] = "0" + dftall["d_block"]

            print("n times:", dftall.shape[0])

            # looping over chunks
            i = 0
            while i < len(geoid_chunks):

                print(i, " -- ", start_time - time.time())

                # get the geoids for this chunk
                geoids = geoid_chunks[i]

                # subset the full matrix by this set of geoids
                dft = dftall[dftall.o_block.isin(geoids)]

                dft["time_all"] = dft["time_all"].astype(float)
                dft["time_lowcost"] = dft["time_lowcost"].astype(float)

                # filling Na values with a large travel time
                dft = dft.fillna(9999)

                # if by chance the lowcost is a shorter travel time, set it to be the time_all time
                dft['time_all'] = dft[['time_all','time_lowcost']].min(axis=1)

                # subsetting the data to only have times less than 90 min
                dft = dft[dft["time_all"] < 5400]

                # removing existing intrazonal times
                dft = dft[dft["o_block"] != dft["d_block"]]

                # merge in fares
                dft = dft.merge(dff, how='left', on=['o_block','d_block'])





                # take any NA values for fares, and apply a very high fare
                dft['fare_all'] = dft['fare_all'].fillna(99)
                dft['fare_lowcost'] = dft['fare_lowcost'].fillna(99)


                # filling in missing costs, at the origin:

                li1 = list(dft['o_block'].unique())
                li2 = geoids
                missing = [x for x in li2 if x not in li1]

                for extra_missing in also_missing:
                    if extra_missing in geoids:
                        missing.append(extra_missing)

                if len(missing) >= 1:

                    dft = dft[~dft['o_block'].isin(missing)]

                    new_times = []

                    # for each zone, compute average travel times to other zones based on neighbours
                    for location in missing:

                        locneigh = neighbours[str(location)]

                        temp = dft[dft['o_block'].isin(locneigh)]

                        temp = pd.DataFrame(temp.groupby(['d_block'], as_index=False)['time_all','time_lowcost','fare_all','fare_lowcost'].mean())

                        temp['o_block'] = str(location)

                        new_times.append(temp)

                    # combine the outputs, and concat to the input times
                    new_times = pd.concat(new_times)

                    dft = pd.concat([dft, new_times])

                    # max value just in access_measures
                    dft = dft.groupby(["o_block","d_block"])['time_all','time_lowcost','fare_all','fare_lowcost'].max().reset_index()

                    del new_times
                    del temp
                    del locneigh
                del li1
                del li2
                del missing


                # filling in missing costs, at the destination:

                li1 = list(dft['d_block'].unique())
                li2 = geoids
                missing = [x for x in li2 if x not in li1]

                for extra_missing in also_missing:
                    if extra_missing in geoids:
                        missing.append(extra_missing)

                if len(missing) >= 1:

                    dft = dft[~dft['d_block'].isin(missing)]

                    new_times = []
                    for location in missing:

                        locneigh = neighbours[str(location)]

                        temp = dft[dft['d_block'].isin(locneigh)]

                        temp = pd.DataFrame(temp.groupby(['o_block'], as_index=False)['time_all','time_lowcost','fare_all','fare_lowcost'].mean())

                        temp['d_block'] = str(location)

                        new_times.append(temp)

                    # # combine the outputs, and concat to the input times
                    new_times = pd.concat(new_times)
                    dft = pd.concat([dft, new_times])

                    # max value just in access_measures
                    dft = dft.groupby(["o_block","d_block"])['time_all','time_lowcost','fare_all','fare_lowcost'].max().reset_index()

                    del new_times
                    del temp
                    del locneigh
                del li1
                del li2
                del missing

                # removing existing intrazonal times
                dft = dft[dft["o_block"] != dft["d_block"]]


                # convert time to minutes
                dft["time_all"] = dft["time_all"] / 60
                dft["time_all"] = dft["time_all"].round(1)
                dft["time_lowcost"] = dft["time_lowcost"] / 60
                dft["time_lowcost"] = dft["time_lowcost"].round(1)

                # # add in pre-computed intrazonal times
                dfint_sub = dfint[dfint.o_block.isin(geoids)]
                dft = pd.concat([dft,dfint_sub])


                # convert to a tracc costs object
                dft = tracc.costs(dft)


                # calc base fare impedence, i.e. assigning 1 or 0 if under or over the fare threshold
                dft.impedence_calc(
                    cost_column = "fare_all",
                    impedence_func = "cumulative",
                    impedence_func_params = fare_threshold,
                    output_col_name = "fare_all_cum",
                    prune_output = False
                )
                dft.impedence_calc(
                    cost_column = "fare_lowcost",
                    impedence_func = "cumulative",
                    impedence_func_params = fare_threshold,
                    output_col_name = "fare_lowcost_cum",
                    prune_output = False
                )


                # create a time field for mintraveltime measures with a fare threshold
                dft.data["temp1"] = dft.data["time_all"] * dft.data["fare_all_cum"]
                dft.data["temp1"] = dft.data["temp1"].replace(0, 999)
                dft.data["temp2"] = dft.data["time_lowcost"] * dft.data["fare_lowcost_cum"]
                dft.data["temp2"] = dft.data["temp2"].replace(0, 999)
                dft.data["time_for_min_lowcost"] = dft.data[["temp1","temp2"]].max(axis=1)
                del dft.data["temp1"]
                del dft.data["temp2"]
                # thte result of "time_for_min_lowcost" is the minimum travel time


                # loop over acc config file, computing all impedences
                impedence_fare_names = []
                for index, row in impedences_times.iterrows():

                    # when we are not including a fare threshold
                    if row["fare"] == 0:

                        impedence_fare_name = row["function_name"] + "_fareN"
                        if impedence_fare_name not in impedence_fare_names:
                            impedence_fare_names.append(impedence_fare_name)
                            if row["function"] == "cumulative":
                                theta = float(row["params"])
                                dft.impedence_calc(
                                    cost_column = "time_all",
                                    impedence_func = row["function"],
                                    impedence_func_params = theta,
                                    output_col_name = impedence_fare_name,
                                    prune_output = False
                                )
                            elif row["function"] == "exponential":
                                beta = get_nexp_beta(row["params"],region)
                                dft.impedence_calc(
                                    cost_column = "time_all",
                                    impedence_func = row["function"],
                                    impedence_func_params = beta,
                                    output_col_name = impedence_fare_name,
                                    prune_output = False
                                )
                            else:
                                None

                    # when we are additionally including a fare threshold
                    elif row["fare"] == 1:

                        # impedences for complete network
                        impedence_fare_name = row["function_name"] + "_fareN"
                        if impedence_fare_name not in impedence_fare_names:
                            impedence_fare_names.append(impedence_fare_name)
                            if row["function"] == "cumulative":
                                theta = float(row["params"])
                                dft.impedence_calc(
                                    cost_column = "time_all",
                                    impedence_func = row["function"],
                                    impedence_func_params = theta,
                                    output_col_name = impedence_fare_name,
                                    prune_output = False
                                )
                            elif row["function"] == "exponential":
                                beta = get_nexp_beta(row["params"],region)
                                dft.impedence_calc(
                                    cost_column = "time_all",
                                    impedence_func = row["function"],
                                    impedence_func_params = beta,
                                    output_col_name = impedence_fare_name,
                                    prune_output = False
                                )
                            else:
                                None

                        # impedences for lowcost network
                        impedence_fare_name = row["function_name"] + "_lowcost"
                        if row["function"] == "cumulative":
                            theta = float(row["params"])
                            dft.impedence_calc(
                                cost_column = "time_lowcost",
                                impedence_func = row["function"],
                                impedence_func_params = theta,
                                output_col_name = impedence_fare_name,
                                prune_output = False
                            )
                        elif row["function"] == "exponential":
                            beta = get_nexp_beta(row["params"],region)
                            dft.impedence_calc(
                                cost_column = "time_lowcost",
                                impedence_func = row["function"],
                                impedence_func_params = beta,
                                output_col_name = impedence_fare_name,
                                prune_output = False
                            )
                        else:
                            None

                        # find the
                        impedence_fare_name = row["function_name"] + "_fareY"
                        impedence_fare_names.append(impedence_fare_name)

                        # combine the fare and time impedences (e.g. if either are 0, then the overarll impedence is 0)
                        dft.impedence_combine(
                            columns = [row["function_name"] + "_fareN", "fare_all_cum"],
                            how = "product",
                            output_col_name = "imp_all",
                            prune_output = True
                        )
                        dft.impedence_combine(
                            columns = [row["function_name"] + "_lowcost","fare_lowcost_cum"],
                            how = "product",
                            output_col_name = "imp_lowcost",
                            prune_output = True
                        )

                        # return the trip with the greater impedence value
                        dft.data[impedence_fare_name] = dft.data[["imp_lowcost","imp_all"]].max(axis=1)

                        del dft.data[row["function_name"] + "_lowcost"]
                        del dft.data["imp_lowcost"]
                        del dft.data["imp_all"]

                    else:
                        None





                # deleting for sake of memory
                del dft.data["fare_all"]
                del dft.data["fare_lowcost"]
                del dft.data["fare_all_cum"]
                del dft.data["fare_lowcost_cum"]
                del dft.data["time_lowcost"]

                # creating the tracc accessibility object
                acc = tracc.accessibility(
                    travelcosts_df = dft.data,
                    supply_df = dfo.data,
                    travelcosts_ids = ["o_block","d_block"],
                    supply_ids = "GEOID"
                )

                # deleting for sake of memory
                del dft

                # looping over acc config file, computing all access measures
                for index, row in access_measures.iterrows():

                    # naming convention for the measure
                    # <dest>_<measure>_<param>_<period>_<autoFlag>_<fareFlag>_<date>

                    # if we are not including a fare threshold for this measure
                    if row["fare"] == 0:

                        # measure ename
                        output_measure_name = row["destination"] + "_" + row["type_code"] + "_" + row["function_name"] + "_" + period_access  + "_" + "fareN"

                        # if it is a measure of potential accessibility
                        if row["type_code"] == "P":

                            imp_name = row["function_name"] + "_fareN"
                            temp_acc = acc.potential(
                                opportunity = row["destination"],
                                impedence = imp_name,
                                output_col_name = "value"
                            )
                            temp_acc["measure"] = output_measure_name
                            temp_acc.rename(columns = {'o_block':'GEOID'}, inplace = True)
                            dfout_P = pd.concat([dfout_P,temp_acc])
                            del temp_acc

                        # if it is a minimum travel time measure
                        elif row["type_code"] == "M":

                            temp_acc = acc.mintravelcost(
                                travelcost = "time_all",
                                opportunity = row["destination"],
                                min_n = float(row["params"]),
                                output_col_name = "value",
                                fill_na_value = 100
                            )
                            temp_acc["measure"] = output_measure_name
                            temp_acc.rename(columns = {'o_block':'GEOID'}, inplace = True)
                            dfout_M = pd.concat([dfout_M,temp_acc])
                            del temp_acc

                    # if we are additionally incorporating a fare threshold for this measure
                    elif row["fare"] == 1:

                        # name of measure
                        output_measure_name = row["destination"] + "_" + row["type_code"] + "_" + row["function_name"] + "_" + period_access + "_" + "fareN"

                        # if it is a measure of potential accessibility
                        if row["type_code"] == "P":

                            imp_name = row["function_name"] + "_fareN"
                            temp_acc = acc.potential(
                                opportunity = row["destination"],
                                impedence = imp_name,
                                output_col_name = "value"
                            )
                            temp_acc["measure"] = output_measure_name
                            temp_acc.rename(columns = {'o_block':'GEOID'}, inplace = True)
                            dfout_P = pd.concat([dfout_P,temp_acc])
                            del temp_acc

                        # if it is a minimum travel time measure
                        elif row["type_code"] == "M":

                            temp_acc = acc.mintravelcost(
                                travelcost = "time_all",
                                opportunity = row["destination"],
                                min_n = float(row["params"]),
                                output_col_name = "value",
                                fill_na_value = 100
                            )
                            temp_acc["measure"] = output_measure_name
                            temp_acc.rename(columns = {'o_block':'GEOID'}, inplace = True)
                            dfout_M = pd.concat([dfout_M,temp_acc])
                            del temp_acc


                        # name of measure
                        output_measure_name = row["destination"] + "_" + row["type_code"] + "_" + row["function_name"] + "_" + period_access  + "_" + "fareY"

                        # if it is a measure of potential accessibility
                        if row["type_code"] == "P":

                            imp_name = row["function_name"] + "_fareY"
                            temp_acc = acc.potential(
                                opportunity = row["destination"],
                                impedence = imp_name,
                                output_col_name = "value"
                            )
                            temp_acc["measure"] = output_measure_name
                            temp_acc.rename(columns = {'o_block':'GEOID'}, inplace = True)
                            dfout_P = pd.concat([dfout_P,temp_acc])
                            del temp_acc

                        # if it is a minimum travel time measure
                        elif row["type_code"] == "M":

                            temp_acc = acc.mintravelcost(
                                travelcost = "time_for_min_lowcost",
                                opportunity = row["destination"],
                                min_n = float(row["params"]),
                                output_col_name = "value",
                                fill_na_value = 100
                            )
                            temp_acc["measure"] = output_measure_name
                            temp_acc.rename(columns = {'o_block':'GEOID'}, inplace = True)
                            dfout_M = pd.concat([dfout_M,temp_acc])
                            del temp_acc


                # deleting the access object for sake of memory
                del acc

                i = i + 1



                # # break for testing
                # break



    # delete the fare table since we dont need it anymore
    del dff




    # average over the multiple time periods
    dfout_P["value"] = dfout_P["value"].astype(float)
    dfout_M["value"] = dfout_M["value"].astype(float)
    dfout_P = dfout_P.groupby(['GEOID', 'measure'], as_index=False).mean()
    dfout_M = dfout_M.groupby(['GEOID', 'measure'], as_index=False).mean()


    # setting anything with less than -1 to -1
    # this is for M measures where we have data, but no trip to a zone with X destinations less than 90 minutes
    dfout_M.loc[dfout_M['value'] > 90, 'value'] = -1

    # joing the two
    dfout = pd.concat([dfout_P,dfout_M])

    # round result to 3 points
    dfout["value_transit"] = dfout["value"].round(3)
    del dfout["value"]


    # setting up variable names for joining with auto accessibility
    dfout[["variable","fare_info"]] = dfout["measure"].str.rsplit("_", n = 1, expand=True)
    del dfout["measure"]


    # read in the auto accessibility data
    dfa = pd.read_csv("data/" + region + "/input/auto_travel_times/auto_accessibility.csv", dtype=str)
    dfa = pd.melt(dfa, id_vars = ["GEOID"])
    dfa["GEOID"] = dfa['GEOID'].astype(str)
    dfa["value"] = dfa["value"].astype(float)
    dfa = dfa.replace(0, 1) # replacing 0 with 1 so we arent dividing by 0!)


    # merging the auto accessibility
    dfout = dfout.merge(dfa, how = "left", on = ["GEOID","variable"])


    # creating the dataframe of only the plain transit measures (no auto ratio)
    dfout_transit = dfout[["GEOID","variable","fare_info","value_transit"]]
    # creating the proper measure name for this
    dfout_transit["measure"] = dfout_transit["variable"] + "_autoN_" + dfout_transit["fare_info"]
    del dfout_transit["variable"]
    del dfout_transit["fare_info"]
    dfout_transit.rename(columns = {'value_transit':'value'}, inplace = True)

    # creating the measures as a ratio to auto times
    dfout["value"] = dfout["value"].astype(float)
    dfout["value"] = dfout["value_transit"] / dfout["value"]
    dfout["measure"] = dfout["variable"] + "_autoY_" + dfout["fare_info"]
    del dfout["variable"]
    del dfout["fare_info"]
    del dfout["value_transit"]

    # joining the two
    dfout = pd.concat([dfout_transit, dfout])

    # adding in a field for the date
    dfout["date"] = str(date)

    # anything less than 0 to the -1 flag
    dfout.loc[dfout['value'] < 0, 'value'] = -1

    # round value outputs
    dfout["value"] = dfout["value"].round(3)

    # updating column names
    dfout = dfout.rename(columns = {"GEOID":"bg_id"})
    dfout = dfout.rename(columns = {"value":"score"})
    dfout = dfout.rename(columns = {"measure":"score_key"})

    # is in LA islands (these had weird results)
    if region == "Los Angeles":
        la_islands = ["061110036121","061119800001","060375991001","060375991002","060375990003","060375990004","060375990002","060375990001"]
        for island in la_islands:
            dfout.loc[dfout['bg_id'] == island, 'score'] = np.nan


    # data output
    out_file_name = "data/" + region + "/output/" + "measures_" + date + "_" + period + ".csv"
    dfout.to_csv(out_file_name, index = False)

    print(time.time() - start_time)
    print(dfout)

    # <dest>_<measure>_<param>_<period>_<autoFlag>_<fareFlag>_<date>






def auto_accessibility(region, input_matrix):

    # function for computing auto accessibility
    # set up to run via compute_auto_accessibility.py

    # make sure the data directory is as follows
    #
    # data
    # --region
    # ----input
    # ------boundary_data
    # --------block_group_poly.geojson
    # --------block_group_pts.csv
    # ------destination_data
    # --------employment.csv
    # --------groceries_snap.sv
    # --------healthcare.csv
    # --------education.csv
    # --------greenspace.csv
    # ------auto_travel_times
    # --------AM.csv.gzp
    # --------PM.csv.gzp
    # --------WE.csv.gzp
    # --------accessibility_chunks_AM/
    # --------accessibility_chunks_PM/
    # --------accessibility_chunks_WE/


    # mkdir accessibility_chunks_AM accessibility_chunks_PM accessibility_chunks_WE

    start_time = time.time()


    # get complete list of block groups
    dfo = pd.read_csv("data/" + region + "/input/boundary_data/" + "block_group_pts.csv", dtype=str)
    dfo = dfo[["GEOID"]]


    # zones with weird data that we need to smooth that didnt come up originally:
    also_missing = {
        "New York": [360610020001,360610020002,360050516005,360050504001,360610001001],
        "Boston": [],
        "Chicago": [170319800001],
        "District of Columbia": [],
        "Philadelphia": [],
        "San Francisco-Oakland": [],
        "Los Angeles": ["061110074053","061110001001"]
    }
    also_missing = also_missing[region]

    print(also_missing)
    #

    # load in supply data
    dftemp = pd.read_csv("data/" + region + "/input/destination_data/employment.csv", dtype=str)

    dfo = pd.merge(dfo,dftemp, how='left', left_on="GEOID", right_on="block_group_id")
    for dataset in ["groceries_snap.csv","healthcare.csv","education.csv","greenspace.csv"]:
        dftemp = pd.read_csv("data/" + region + "/input/destination_data/" + dataset, dtype=str)
        dfo = pd.merge(dfo,dftemp, how='left', left_on="GEOID", right_on="GEOID")
    del dftemp
    dfo["schools"] = dfo["count"]
    dfo["parks"] = dfo["area"].astype(float) * 247.10538161 # from km2 to acres
    dfo["urgentcare"] = dfo["urgent_care_facilities"]
    del dfo["count"], dfo["area"], dfo["urgent_care_facilities"]
    dfo = dfo.fillna(0)


    # generating intrazonal times based on radius and a 30km/hr travel speed
    from tracc.spatial import radius
    speed_value = 1 / 0.5 # 30 km / hour
    dfint = radius("data/" + region + "/input/boundary_data/" + "block_group_poly.geojson","GEOID")
    dfint['OriginName'] = dfint["GEOID"].astype(int)
    dfint['DestinationName'] = dfint["GEOID"].astype(int)
    del dfint["GEOID"]
    dfint["Total_Time"] = speed_value * dfint["radius"]
    dfint["Total_Time"] = dfint["Total_Time"].round(1)
    del dfint["radius"]


    # getting a neighbourds matrix
    from tracc.spatial import get_neighbours
    neighbours = get_neighbours(
        spatial_data_file_path = "data/" + region + "/input/boundary_data/" + "block_group_poly.geojson",
        weight_type = "KNN",
        idVariable = "GEOID",
        param = 10
    )


    # chunking the origins
    geoids = dfo["GEOID"].to_list()
    n_chunks = 500
    def chunks(lst,n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    geoid_chunks = list(chunks(geoids,n_chunks))



    # loading in the accessibility config file
    acc_config = pd.read_csv('accessibility/acc_config.csv')
    destination_types = list(acc_config.destination.unique())

    # unique impedence to compute
    impedences = acc_config[acc_config["type_code"] != "M"]
    impedences =  impedences[["function_name","function","cost","params"]]
    impedences = impedences.drop_duplicates()

    # access_measures
    access_measures = acc_config

    # create the supply object
    dfo = tracc.supply(
        supply_df = dfo,
        columns = ["GEOID"] + destination_types
    )
    # making sure the destinations are integers
    for dest in destination_types:
        dfo.data[dest] = dfo.data[dest].astype(float)
        dfo.data[dest] = dfo.data[dest].astype(int)

    # load in the full matrix
    dftall = pd.read_csv("data/" + region + "/input/auto_travel_times/" + input_matrix + ".csv.gzip", compression = "gzip", dtype=str)

    dftall["Total_Time"] = dftall["Total_Time"].astype(float)
    dftall["Total_Time"] = dftall["Total_Time"].astype(int)



    # adding in extra data for LA
    if region == "Los Angeles":

        dftextra1 = pd.read_csv("data/" + region + "/input/auto_travel_times/" + input_matrix + "_30toN.csv", dtype=str)

        dftextra2 = pd.read_csv("data/" + region + "/input/auto_travel_times/" + input_matrix + "_Nto30.csv", dtype=str)

        dftextra = pd.concat([dftextra1,dftextra2])

        del dftextra1, dftextra2

        # correcting columns
        dftextra["Total_Time"] = dftextra["Total_TravelTime"].astype(float)
        dftextra["Total_Time"] = dftextra["Total_Time"].astype(int)
        dftextra["OriginName"] = dftextra['Name'].str[:11]
        dftextra["DestinationName"] = dftextra['Name'].str[-11:]

        dftextra = dftextra.loc[:,["Total_Time","OriginName","DestinationName"]]

        dftall = pd.concat([dftall,dftextra])

        del dftextra





    if region == "Los Angeles" or region == "San Francisco-Oakland":

        dftall["OriginName"] = "0" + dftall["OriginName"]



    # # print total zones from spatial file
    print("Total zones:", len(neighbours))

    # # checking how many unique
    print("Total Origins in matrix: ",len(pd.unique(dftall['OriginName'])))

    # printing number of chunks to output
    print("Total Chunks: ", len(geoid_chunks))

    i = 0

    # looping over chunks
    while i < len(geoid_chunks):

        print(i, " -- ", start_time - time.time())

        # get the geoids for this chunk
        geoids = geoid_chunks[i]

        # have these set as the output dataframe
        outdf = pd.DataFrame(geoids, columns = ['GEOID'])
        outdf["GEOID"] = outdf["GEOID"].astype(str)

        # subset the full matrix by this set of geoids
        dft = dftall[dftall.OriginName.isin(geoids)]

        # convert times to float
        dft["Total_Time"] = dft["Total_Time"].astype(float)


        if region == "Los Angeles" or region == "San Francisco-Oakland":

            dft["DestinationName"] = "0" + dft["DestinationName"]


        # set all intrazonal times to 0
        dft.loc[dft['OriginName'] == dft["DestinationName"], 'Total_Time'] = -1

        # remove these
        dft = dft[dft["Total_Time"] >= 0]

        # filling in missing costs, at the origin:

        li1 = list(dft['OriginName'].unique())
        li2 = geoids
        missing = [x for x in li2 if x not in li1]

        for extra_missing in also_missing:
            if extra_missing in geoids:
                missing.append(extra_missing)

        print(missing)

        if len(missing) >= 1:

            dft = dft[~dft['OriginName'].isin(missing)]

            new_times = []

            # for each zone, compute average travel times to other zones based on neighbours
            for location in missing:

                locneigh = neighbours[str(location)]

                temp = dft[dft['OriginName'].isin(locneigh)]

                temp = pd.DataFrame(temp.groupby(['DestinationName'], as_index=False)['Total_Time'].mean())

                temp['OriginName'] = str(location)

                new_times.append(temp)

            # combine the outputs, and concat to the input times
            new_times = pd.concat(new_times)
            dft = pd.concat([dft, new_times])

            # max value just in access_measures
            dft = dft.groupby(["OriginName","DestinationName"])["Total_Time"].max().reset_index()

            del new_times
            del temp
            del locneigh
        del li1
        del li2
        del missing


        # filling in missing costs, at the destination:

        li1 = list(dft['DestinationName'].unique())
        li2 = geoids
        missing = [x for x in li2 if x not in li1]

        for extra_missing in also_missing:
            if extra_missing in geoids:
                missing.append(extra_missing)

        if len(missing) >= 1:

            dft = dft[~dft['DestinationName'].isin(missing)]

            new_times = []
            for location in missing:

                locneigh = neighbours[str(location)]

                temp = dft[dft['DestinationName'].isin(locneigh)]

                temp = pd.DataFrame(temp.groupby(['OriginName'], as_index=False)['Total_Time'].mean())

                temp['DestinationName'] = str(location)

                new_times.append(temp)

            # # combine the outputs, and concat to the input times
            new_times = pd.concat(new_times)
            dft = pd.concat([dft, new_times])

            # max value just in access_measures
            dft = dft.groupby(["OriginName","DestinationName"])["Total_Time"].max().reset_index()

            del new_times
            del temp
            del locneigh
        del li1
        del li2
        del missing


        # set all intrazonal times to -1
        dft.loc[dft['OriginName'] == dft["DestinationName"], 'Total_Time'] = -1

        # remove these
        dft = dft[dft["Total_Time"] >= 0]

        # get the estimated intrazonal times
        dfintsub = dfint[dfint.OriginName.isin(geoids)]
        dft = pd.concat([dft,dfintsub])

        # add 2 min for parking, remove times over 90 min
        dft["Total_Time"] = dft["Total_Time"] + 2
        dft = dft[dft["Total_Time"] <= 90]


        # update name of time column for accessibility
        dft.rename(columns = {'Total_Time':'time'}, inplace = True)

        # convert to a tracc costs object
        dft = tracc.costs(dft)

        # loop over acc config file, computing all impedences
        for index, row in impedences.iterrows():

            if row["function"] == "cumulative":
                theta = float(row["params"])
                dft.impedence_calc(
                    cost_column = row["cost"],
                    impedence_func = row["function"],
                    impedence_func_params = theta,
                    output_col_name = row["function_name"],
                    prune_output = False
                )
            elif row["function"] == "exponential":
                beta = get_nexp_beta(row["params"],region)
                dft.impedence_calc(
                    cost_column = row["cost"],
                    impedence_func = row["function"],
                    impedence_func_params = beta,
                    output_col_name = row["function_name"],
                    prune_output = False
                )
            else:
                None


        # creating the tracc accessibility object
        acc = tracc.accessibility(
            travelcosts_df = dft.data,
            supply_df = dfo.data,
            travelcosts_ids = ["OriginName","DestinationName"],
            supply_ids = "GEOID"
        )

        # deleting for sake of memory
        del dft

        # looping over acc config file, computing all access measures
        for index, row in access_measures.iterrows():

            output_measure_name = row["destination"] + "_" + row["type_code"] + "_" + row["function_name"] + "_" + input_matrix

            if row["type"] == "potential":

                temp_acc = acc.potential(
                    opportunity = row["destination"],
                    impedence = row["function_name"],
                    output_col_name = output_measure_name

                )
                temp_acc.rename(columns = {'OriginName':'GEOID'}, inplace = True)

                outdf = outdf.merge(temp_acc, how = "left", on = "GEOID")

            elif row["type"] == "mintraveltime":

                temp_acc = acc.mintravelcost(
                    travelcost = row["cost"],
                    opportunity = row["destination"],
                    min_n = float(row["params"]),
                    output_col_name = output_measure_name,
                    fill_na_value = 999
                )

                # temp_acc.index.name = 'GEOID'
                # temp_acc.reset_index(inplace=True)

                temp_acc.rename(columns = {'OriginName':'GEOID'}, inplace = True)

                outdf = outdf.merge(temp_acc, how = "left", on = "GEOID")

            else:
                None

        # deleting more for sake of memory
        del acc
        del temp_acc


        # saving to file
        outdf.to_csv("data/" + region + "/input/auto_travel_times/accessibility_chunks_" + input_matrix + "/auto_accessibility_" + input_matrix + "_chunk" + str(i) + ".csv", index = False)

        # again, deleting for memeory
        del outdf

        i += 1


def auto_accessibility_matrix_test(region,input_matrix):

    dftall = pd.read_csv("data/" + region + "/input/auto_travel_times/" + input_matrix + ".csv.gzip", compression = "gzip", dtype=str)

    if region == "Los Angeles" or region == "San Francisco-Oakland":

        dftall["OriginName"] = "0" + dftall["OriginName"]


    test = dftall['OriginName'].value_counts().to_frame()

    print(test)

    test.to_csv("test2.csv")


def auto_accessibility_join_single(region, period):

    """
    takes chunks of auto accessibility results and combines them into a single files

    for only one time period
    """


    data_dir = "data/" + region + "/input/auto_travel_times/accessibility_chunks_" + period

    dfs = []
    for filename in os.listdir(data_dir):
        if filename.endswith(".csv"):
            df = pd.read_csv(data_dir + "/" + filename, dtype=str)
            dfs.append(df)
    dfs = pd.concat(dfs)

    print(dfs)

    dfs.to_csv("data/" + region + "/input/auto_travel_times/test.csv")


def auto_accessibility_join(region):

    """
    takes chunks of auto accessibility results and combines them into a single files
    """


    data_dir = "data/" + region + "/input/auto_travel_times/accessibility_chunks_AM"

    dfs = []
    for filename in os.listdir(data_dir):
        if filename.endswith(".csv"):
            df = pd.read_csv(data_dir + "/" + filename, dtype=str)
            dfs.append(df)
    dfs = pd.concat(dfs)

    dfo = dfs

    data_dir = "data/" + region + "/input/auto_travel_times/accessibility_chunks_PM"

    dfs = []
    for filename in os.listdir(data_dir):
        if filename.endswith(".csv"):
            df = pd.read_csv(data_dir + "/" + filename, dtype=str)
            dfs.append(df)
    dfs = pd.concat(dfs)


    dfo = dfo.merge(dfs,how="outer",on="GEOID")

    data_dir = "data/" + region + "/input/auto_travel_times/accessibility_chunks_WE"

    dfs = []
    for filename in os.listdir(data_dir):
        if filename.endswith(".csv"):
            df = pd.read_csv(data_dir + "/" + filename, dtype=str)
            dfs.append(df)
    dfs = pd.concat(dfs)

    dfo = dfo.merge(dfs,how="outer",on="GEOID")


    dfo.to_csv("data/" + region + "/input/auto_travel_times/" + "auto_accessibility.csv", index = False)
