


import pandas as pd
import json
import tracc


def transit_accessibility(region):
    '''
    computes all transit accessibility measures for our study for a study region for a single week
    i.e. this will have to be repeated weekly
    '''


    # get complete list of block groups
    dfo = pd.read_csv("data/" + region + "/input/boundary_data/" + "block_group_pts.csv")
    dfo = dfo[["GEOID"]]


    # load in supply data
    for dataset in ["groceries.csv","healthcare.csv","education.csv","greenspace.csv","employment.csv"]:
        dftemp = pd.read_csv("data/" + region + "/input/destination_data/" + dataset)
        dfo = pd.merge(dfo,dftemp, how='left', left_on="GEOID", right_on="GEOID")
    del dftemp
    dfo["schools"] = dfo["count"]
    dfo["greenspace"] = dfo["area"]
    del dfo["count"], dfo["area"]
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


def auto_accessibility(region):

    # get complete list of block groups
    dfo = pd.read_csv("data/" + region + "/input/boundary_data/" + "block_group_pts.csv")
    dfo = dfo[["GEOID"]]

    # load in supply data
    for dataset in ["employment.csv"]:
        dftemp = pd.read_csv("data/" + region + "/input/destination_data/" + dataset)
        dfo = pd.merge(dfo,dftemp, how='left', left_on="GEOID", right_on="GEOID")
    del dftemp
    # dfo["schools"] = dfo["count"]
    # dfo["greenspace"] = dfo["area"]
    # del dfo["count"], dfo["area"]
    dfo = dfo.fillna(0)


    # # loading in the accessibility config file
    # with open('accessibility/acc_config.json', 'r') as myfile:
    #     accessibility_config=myfile.read()
    # # parse file
    # accessibility_config = json.loads(accessibility_config)


    dft = pd.read_csv("data/" + region + "/input/auto_travel_times/NY_8am.csv.gzip", compression = "gzip")

    print(dft)


    

auto_accessibility("New York")
