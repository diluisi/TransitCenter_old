import requests
import json

# Other imports
from google.transit import gtfs_realtime_pb2
from gtfslite import GTFS

# Local imports
from config import mtaURL

class Realtime:
    pass

class MTARealtime(Realtime):

    def __init__(self, url):
        self.url = url
        response = requests.get(url, allow_redirects=True)
        self.feed = gtfs_realtime_pb2.FeedMessage()
        self.feed.ParseFromString(response.content)
        updates = dict()
        stops = []
        for f in self.feed.entity:
            if f.HasField('trip_update'):
                trip_id = f.trip_update.trip.trip_id
                stop_id = f.trip_update.stop_time_update[0].stop_id
                arrival = f.trip_update.stop_time_update[0].arrival.time
                updates[trip_id] = {'stop_id': stop_id, 'arrival': arrival}
                stops.append(stop_id)

        # Now let's load up the MTA Static Feeds and do some checking
        static = GTFS.load_zip('mta_bronx_2020_05_01.zip')
        for key, val in updates.items():
            s = static.stop_times[(static.stop_times.stop_id == val['stop_id']) & (static.stop_times.trip_id == key)]
            if s.shape[0] > 0:
                print(s.head())
        

if __name__ == "__main__":
    r = MTARealtime(mtaURL)