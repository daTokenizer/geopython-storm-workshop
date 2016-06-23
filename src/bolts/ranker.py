from __future__ import absolute_import, print_function, unicode_literals

import redis
from streamparse.bolt import Bolt
from geopy.distance import vincenty

REDIS_LOCATION_DATA_KEY = "%s_location_data"

class Ranker(Bolt):

    def initialize(self, conf, ctx):
        self._redis = redis.Redis()

    def process(self, tup):
        top_x, location_a = tup.values
        a_name , a_lat, a_lon, a_rad = location_a

        for b_name, b_coord in top_x:
            # get from datastore
            distance_km = vincenty((a_lat,a_lon), b_coord).meters /1000
            b_rad = self._redis.get(REDIS_LOCATION_DATA_KEY % b_name)
            if distance_km < min(a_rad, b_rad):
                # self.emit([a_name, b_name], stream="output")
                self.log('Match found: %s, %s' % (a_name, b_name))
