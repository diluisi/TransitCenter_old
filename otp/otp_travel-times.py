

from org.opentripplanner.scripting.api import *
import time
import csv 
import sys
import optparse
import datetime
import os
import ConfigParser


# jython -Dpython.path=otp-1.4.0-shaded.jar otp_travel-times.py --date 2020-02-29

start_time = time.time()
config = ConfigParser.RawConfigParser()

# input args from otp_handler
parser = optparse.OptionParser()
parser.add_option("-d", '--date', default = datetime.datetime.now().strftime('%Y-%m-%d'), help="input date for otp")
parser.add_option("-t", '--hour', default = 8, help="hour")
parser.add_option("-y", '--minute', default = '00', help="minute")
parser.add_option("-m", '--mode', default = 'TRANSIT', help="mode to check")
parser.add_option("-o", '--o_path',  help="path for origin points")
parser.add_option("-p", '--d_path',  help="path for dest points")
parser.add_option("-n", '--num',  help="iteration num")
parser.add_option("-x", '--out',  help="out path")
parser.add_option("-r", '--region',  help="region")
parser.add_option("-g", '--graph',  help="graph")
parser.add_option("-a", '--o_date',  help="date of trip")
parser.add_option("-b", '--lowcost', help="if evaluating its a low cost network, will bann premium routes")

(options, args) = parser.parse_args()

#reading arguments
date = options.date
hr = int(options.hour)
mode = options.mode
o_path = options.o_path
d_path = options.d_path
num = str(options.num)
outpath = options.out
print (date, hr, mode)
minute = int(options.minute)
region = options.region
graph_path = options.graph
o_date = options.date
lowcost = options.lowcost


dt = datetime.datetime.strptime(o_date, '%Y-%m-%d') # date in string form
dt_tm = datetime.datetime.combine(dt, datetime.time(hr, minute)) #date in datetime

#calling otp jython
otp = OtpsEntryPoint.fromArgs([ "--graphs", graph_path, "--router", "graphs-"+date ])

router = otp.getRouter()

#using csv population from the otp jython api
csv_otp = otp.createCSVOutput()
csv_otp.setHeader(['datetime', 'o_block', 'd_block','time'])

#loading the origin to run as bulk in otp
points = otp.loadCSVPopulation(o_path, 'LATITUDE', 'LONGITUDE') # census tracts
router = otp.getRouter()
i = 0
for origin in points:

    i = i + 1

 
    mode_str = mode+',WALK'
    max_time = 7200
    initial_wait = 0
    walk_dist = 5000

    #all possible arguments from the api are in the code, however some are commented out
    r = otp.createRequest()
    r.setDateTime(dt.year, dt.month, dt.day, hr, minute, 00) # departure date / time
    r.setModes(mode_str) # modes to include
    r.setMaxTimeSec(max_time) # time out (in seconds)
    r.setClampInitialWait(initial_wait) #initial wait
    r.setMaxWalkDistance (walk_dist) #maximum walk distance
    r.setOrigin(origin) # set origin
    r.setBannedRoutes(lowcost) #banning premium routes if any
    # r.setWalkSpeedMs() #speed of pedestrian
    # r.setWheelchairAccessible()
    
    spt = router.plan(r) 
    pop = otp.loadCSVPopulation(d_path, 'LATITUDE', 'LONGITUDE')
    try:
        result = spt.eval(pop)
        for r in result:
            csv_otp.addRow( [str(dt_tm), origin.getStringData('GEOID'), r.getIndividual().getStringData('GEOID'), r.getTime()] )
    except:
        continue
    
    # for testing purposes, limits number of iterations
    # if i ==1:
    #     break

# saving output
csv_otp.save(outpath + '/' + num + 'otp_times.csv')


run_time = time.time() - start_time
# print computation time
print ("----------------")
print (run_time)

#logging the input in a config file
run_no = 'Run '+num

config.add_section(run_no)
config.set(run_no, 'region', region)
config.set(run_no, 'o_path', o_path)
config.set(run_no, 'run_time', str(datetime.datetime.now()))
config.set(run_no, 'mode', mode_str)
config.set(run_no, 'max_time', str(max_time))
config.set(run_no, 'initial_wait', str(initial_wait))
config.set(run_no, 'max_walk', str(walk_dist))
config.set(run_no, 'date', str(date))
config.set(run_no, 'region', region)
config.set(run_no, 'hour', str(hr))
config.set(run_no, 'minute', str(minute))
config.set(run_no, 'o_date', str(o_date))
config.set(run_no, 'run_time', run_time)

filename = region+ '_' + str(date)+ '_' + mode+'_otp_log.cfg'

if os.path.exists(outpath + '/' +filename):

    with open(outpath + '/' +filename, 'a') as configfile:
        config.write(configfile)
else:
    with open(outpath + '/' +filename, 'w') as configfile:
        config.write(configfile)

