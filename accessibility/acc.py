


import pandas as pd
import numpy as np
import os
import tracc
import time



def get_nexp_beta(in_param, in_region):
    '''
    function for generating the value for beta for a negative exponential decay function based on median travel time
    '''


    acc_config_region = pd.read_csv('accessibility/acc_config_regional.csv')

    acc_config_region = acc_config_region[acc_config_region["region"] == in_region]

    median_time = float(acc_config_region[in_param])

    return np.log(0.5) / median_time



def transit_accessibility(region):
    '''
    computes all transit accessibility measures for our study for a study region for a single week
    i.e. this will have to be repeated weekly
    '''


    # get complete list of block groups
    dfo = pd.read_csv("data/" + region + "/input/boundary_data/" + "block_group_pts.csv")
    dfo = dfo[["GEOID"]]


    # load in supply data
    for dataset in ["groceries_snap.csv","healthcare.csv","education.csv","greenspace.csv","employment.csv"]:
        dftemp = pd.read_csv("data/" + region + "/input/destination_data/" + dataset)
        dfo = pd.merge(dfo,dftemp, how='left', left_on="GEOID", right_on="GEOID")
    del dftemp
    dfo["schools"] = dfo["count"]
    dfo["greenspace"] = dfo["area"]
    dfo["urgentcare"] = dfo["urgent_care_facilities"]
    del dfo["count"], dfo["area"], dfo["urgent_care_facilities"]
    dfo = dfo.fillna(0)


    # loading in the accessibility config file
    with open('accessibility/acc_config.json', 'r') as myfile:
        accessibility_config=myfile.read()
    # parse file
    accessibility_config = json.loads(accessibility_config)


    # get a unique lists of destinations and impedences
    destinations = []
    impedences = []
    for a in accessibility_config["measures"]:
        if a["destination"] not in destinations:
            destinations.append(a["destination"])
        if a["impedence"] not in impedences:
            impedences.append(a["impedence"])


    # create the supply object
    dfo = tracc.supply(
        supply_df = dfo,
        columns = ["GEOID"] + destinations
    )


    #
    dfa = []



    # begin loop for three time periods here

        # load in the fares data for this time period

        # accessibility output for time period

        # loop over the 8 times

            # load in the travel time data for this time period

            # remove intrazonal

            # fill in missing

            # compute intrazonal

            # merge with fares, join left with travel times

            # loop over impedences

                # compute impedences

                # gen max impedence to reduce

            # create accessibility object

            # loop over types of accessibility

                # compute accessibility

        # generate mean accessibility

        # output to dfa


def auto_accessibility(region, input_matrix):

    # make sure directory is
    # auto_travel_times
    # --

    start_time = time.time()


    # get complete list of block groups
    dfo = pd.read_csv("data/" + region + "/input/boundary_data/" + "block_group_pts.csv")
    dfo = dfo[["GEOID"]]


    # zones with weird data that we need to smooth that didnt come up originally:
    also_missing = {
        "New York": [360610020001,360610020002,360050516005,360050504001,360610001001]
    }
    also_missing = also_missing[region]


    # load in supply data
    dftemp = pd.read_csv("data/" + region + "/input/destination_data/employment.csv")
    dfo = pd.merge(dfo,dftemp, how='left', left_on="GEOID", right_on="block_group_id")
    for dataset in ["groceries_snap.csv","healthcare.csv","education.csv","greenspace.csv"]:
        dftemp = pd.read_csv("data/" + region + "/input/destination_data/" + dataset)
        dfo = pd.merge(dfo,dftemp, how='left', left_on="GEOID", right_on="GEOID")
    del dftemp
    dfo["schools"] = dfo["count"]
    dfo["parks"] = dfo["area"]
    dfo["urgentcare"] = dfo["urgent_care_facilities"]
    del dfo["count"], dfo["area"], dfo["urgent_care_facilities"]
    dfo = dfo.fillna(0)
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


    # i = 0
    # while i < len(geoid_chunks):
    #     if 360050504001 in geoid_chunks[i]:
    #         print(i)
    #     i += 1


    # loading in the accessibility config file
    acc_config = pd.read_csv('accessibility/acc_config.csv')
    destination_types = list(acc_config.destination.unique())


    # unique impedence to compute
    impedences = acc_config[acc_config.fare.isnull()]
    impedences = impedences[["function_name","function","cost","params"]]
    impedences = impedences.drop_duplicates()


    # access_measures
    access_measures = acc_config[acc_config.fare.isnull()]
    access_measures = access_measures[access_measures["fare"] != 1]

    # create the supply object
    dfo = tracc.supply(
        supply_df = dfo,
        columns = ["GEOID"] + destination_types
    )

    # load in the full matrix
    dftall = pd.read_csv("data/" + region + "/input/auto_travel_times/" + input_matrix + ".csv.gzip", compression = "gzip")

    # # checking how many unique
    print("Total Origins: ",len(pd.unique(dftall['OriginName'])))

    # printing number of chunks to output
    print("Total Chunks: ", len(geoid_chunks))

    i = 24 # 11 (big, island),  18 (downtown)

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


        # set all intrazonal times to 0
        dft.loc[dft['OriginName'] == dft["DestinationName"], 'Total_Time'] = -1

        # remove these
        dft = dft[dft["Total_Time"] >= 0]

        # get the estimated intrazonal times
        dfintsub = dfint[dfint.OriginName.isin(geoids)]
        dft = pd.concat([dft,dfintsub])

        # add 2 min for parking, remove times over 120 min
        dft["Total_Time"] = dft["Total_Time"] + 2
        dft = dft[dft["Total_Time"] <= 120]


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

            if row["type"] == "potential":

                temp_acc = acc.potential(
                    opportunity = row["destination"],
                    impedence = row["function_name"],
                    output_col_name = row["name"]

                )
                temp_acc.rename(columns = {'OriginName':'GEOID'}, inplace = True)

                outdf = outdf.merge(temp_acc, how = "left", on = "GEOID")

            elif row["type"] == "mintraveltime":

                temp_acc = acc.mintravelcost(
                    travelcost = row["cost"],
                    opportunity = row["destination"],
                    min_n = float(row["params"]),
                    output_col_name = row["name"],
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

        break






def auto_accessibility_join(region, input_matrix):


    data_dir = "data/" + region + "/input/auto_travel_times/accessibility_chunks_" + input_matrix

    dfs = []
    for filename in os.listdir(data_dir):
        if filename.endswith(".csv"):
            print(filename)
            df = pd.read_csv(data_dir + "/" + filename)



            dfs.append(df)

    dfs = pd.concat(dfs)

    dfs.to_csv("data/" + region + "/input/auto_travel_times/" + "auto_accessibility_" + input_matrix + ".csv", index = False)
