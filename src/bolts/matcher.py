from __future__ import absolute_import, print_function, unicode_literals

import redis
from collections import Counter
from streamparse.bolt import Bolt
from geopy.distance import vincenty

REDIS_LOCATION_DATA_KEY = "%s_location_data"
MAX_NUM_OF_LOCATIONS = 50 # this is X from TopX

class TopX(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()
        self._redis = redis.Redis()


    def process(self, tup):
        name ,lat, lon, rad = tup.values[0]

        # query redis
        top_locations = self._redis.georadius("locations", lat, lon,
                            rad, unit='km',
                            withcoord=True,
                            # withdist=True, # commented out for performance reasons
                            sort='ASC')

        self._redis.geoadd("locations", lat, lon, name) # demonstrate seperating key to the r-tree and data to a different set
        self._redis.set(REDIS_LOCATION_DATA_KEY % name, rad)

        if top_locations:
            self.emit([top_locations[:MAX_NUM_OF_LOCATIONS], tup.values[0]])
            # self.log('%s: %d' % (name, len(top_locations)))
