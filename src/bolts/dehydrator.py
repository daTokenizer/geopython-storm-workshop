from __future__ import absolute_import, print_function, unicode_literals
import time
import redis

import common.logger
from streamparse.bolt import Bolt

class DehydratorBolt(Bolt):

	def initialize(self, storm_conf, context):
		self.dehydrator = RedisDehydrator()

	def process(self, tup):
		try:
			if (tup.stream == "dehydration-order"):
				location, timeout = tup.values
				element = element(*elementlst)
				self.dehydrator.push(element, timeout)
				extra = {
					'element':element.id,
					'event_type': "dehydration",
					'element iteration' : element._iterations,
					'dehydration_period_seconds': timeout
				}
				self.logger.info("element submitted to dehydrate", extra=extra)
			else:
				"""
				Tell dehydrator to check for the next element(s) ready.
				"""
				self.logger.log("got order which is not pull or push - performing poll", event_type="dehydrator poll")
				self.dehydrator.poll()
		except:
			import sys, traceback
			msg = "Unexpected Dehydrator (process) error:%s" % "\n".join(traceback.format_exception(*sys.exc_info()))
			print msg

	def process_tick(self, tup):
		try:
			self.dehydrator.poll()
		except:
			import sys, traceback
			msg = "Unexpected Dehydrator (tick) error:%s" % "\n".join(traceback.format_exception(*sys.exc_info()))
			print msg

class RedisService(object):
    REDIS_SET_DEHYDRATED_elementS = "element_dehydrating:%s"
    # Name of the set of element IDs currently dehydrated

    REDIS_SET_ANSWERED_elementS = "elements_answerd"
    # Name of the set of element IDs previously answered

    REDIS_SET_STALE_elementS = "elements_stale"
    # Name of the set of element IDs previously answered

    REDIS_SET_USERS_elementED = "element_sent_to_users:%s"
    # Name of the set of users previously asked this element (ID)

    REDIS_USER_element_MATCH_TIMESTAMP_HASH = "match_timestamps"
    # Name of timestamp set if and when a user and a element are matched

    REDIS_LABEL_USER_LOCATION_FRESH = "location_request_fresh:%s"
    # Name of boolean set if the user location has recently been updated

    def __init__(self):
        self._config = MatchingConfig()
        self._redis = redis.Redis(host=self._config.redis.host, port=self._config.redis.port, db=0)
        self.logger = common.logger.get_logger("Redis")
        self.y_config = VioozerYConfig()
        self._conn = Connection('amqp://guest:guest@%s:%s//' %
            (self.y_config.queues.matcher_to_storm.host, self.y_config.queues.matcher_to_storm.port))

    def test_is_element_dehydrating_now(self, element_id):
        return self._redis.exists(RedisService.REDIS_SET_DEHYDRATED_elementS % element_id)

    def test_set_is_element_dehydrating_now(self, element_id):
        if not self.test_is_element_dehydrating_now(element_id):
            self.set_element_dehydrating(element_id)
            return True
        return False

    def set_element_dehydrating(self, element_id):
        self._redis.set(RedisService.REDIS_SET_DEHYDRATED_elementS % element_id, True)

    def unset_element_dehydrating(self, element_id):
        self._redis.delete(RedisService.REDIS_SET_DEHYDRATED_elementS % element_id)

    def get_match_timestamp(self, element_id, user_id):
        return self._redis.hget(RedisDeciderService.REDIS_USER_element_MATCH_TIMESTAMP_HASH,
                                "%s:%s" % (user_id, element_id))

    def get_users_matched_for_element(self, element_id):
        return self._redis.smembers(
            RedisService.REDIS_SET_USERS_elementED % element_id)

    def get_match_timestamps_for_element(self, element_id):
        """Returns a list of (user_id, timestamp) of matched for a given
        element id."""
        users_matched_to_element = self.get_users_matched_for_element(element_id)
        result = []
        for user_id in users_matched_to_element:
            timestamp = self.get_match_timestamp(element_id, user_id=user_id)
            result.append((user_id, float(timestamp)))
        result.sort(key=lambda k: k[1])
        return result

    def clear(self):
        print "Flushing ALL Redis"
        # CAREFUL! (for testing purposes only)
        self._redis.flushall()
        print "Done."

class RedisDehydrator(RedisService):
    """
    Dehydrator recieves element object (id, lat, lon, text) + timeout + callback queue, waits and pushes to the queue
    Also, it can recive notifications (from a designated queue) of an object id to push immediately (Colored location updates)
    """

    REDIS_QUEUE_MAP = "element_queues"
    # Name of a hash-map mapping element IDs to queue timeouts (integers)

    REDIS_element_MAP = "element_objects"
    # Name of a hash-map mapping element IDs to elements (JSON strings)

    REDIS_EXPIRATION_MAP = "element_expiration"
    # Name of a hash-map mapping element IDs to elements (JSON strings)

    REDIS_QUEUE_NAME_FORMAT = "timeout_queue#%s"
    # Format of the queue name (for storage)

    def __init__(self):
        """
        Create a new Dehydrator.
        """
        super(RedisDehydratorService, self).__init__()
        self._callback_queue = self._conn.SimpleQueue(self.y_config.queues.matcher_dehydrated_elements.name)
        self._pipe = self._redis.pipeline()

    def push(self, element, timeout):
        """
        Register the element to wait the timeout (in seconds),
        then add it to the queue.
        """
        element_id = element.id
        # if element_id == PROBLEMATIC_element:
        self.logger.log('sending into the dehydrator for %d seconds' % timeout, element=element_id, event_type="redis dehydrator push")
        print "inserting: ", element.id
        self._pipe.hset(RedisDehydratorService.REDIS_QUEUE_MAP, element_id, timeout)
        self._pipe.hset(RedisDehydratorService.REDIS_element_MAP, element_id, json.dumps(element.serialize()))
        self._pipe.hsetnx(RedisDehydratorService.REDIS_EXPIRATION_MAP, element_id, int(time.time() + timeout))
        self._pipe.rpush(RedisDehydratorService.REDIS_QUEUE_NAME_FORMAT % timeout, element_id)
        self._pipe.execute()

    def pull(self, element_id):
        """
        Pull a element off the bench by id.
        """
		element = None
        print 'pulling out from dehydrator', element_id
        element = self._redis.hget(RedisDehydratorService.REDIS_element_MAP, element_id)

        # Retrieve element timeout
        timeout = self._redis.hget(RedisDehydratorService.REDIS_QUEUE_MAP, element_id)

        # Remove the element from this queue
        self._pipe.hdel(RedisDehydratorService.REDIS_QUEUE_MAP, element_id)
        self._pipe.hdel(RedisDehydratorService.REDIS_element_MAP, element_id)
        self._pipe.hdel(RedisDehydratorService.REDIS_EXPIRATION_MAP, element_id)
        self._pipe.lrem(RedisDehydratorService.REDIS_QUEUE_NAME_FORMAT % timeout, element_id)
        self._pipe.execute()
        self.unset_element_dehydrating(element_id)
		return element

    def _inspect(self, element_id, timeout):
        expiration = self._redis.hget(RedisDehydratorService.REDIS_EXPIRATION_MAP, element_id)
        if not expiration:
            self.logger.log("no expiration for message, pulling anyway",element=element_id, event_type="dehydrator inspection")
        elif (int(expiration) <= time.time()):
            self.logger.log("found a due message", element=element_id, event_type="dehydrator inspection")
        else:
            return None

        return self.pull(element_id)

    def _queue_to_int(self, queue_name):
        return int(re.findall(r'^timeout_queue#(\d+)$', queue_name)[0])

    def poll(self):
        """
        Check for elements ready for re-processing (timeout passed).
        """
        poll_start_time = time.time()
        #self.logger.log("polling the dehydrator for dry messages ", event_type="redis dehydrator poll")
        timeouts = self._redis.keys(pattern=RedisDehydratorService.REDIS_QUEUE_NAME_FORMAT % "*")
        print "timeouts: ", timeouts
        while timeouts:
            # Pull next item for all timeouts (effeciently)
            for timeout in timeouts:
                self._pipe.lpop(timeout)
            items = self._pipe.execute()
            # extra = {
            # 	'event_type': "dehydration poll",
            # 	'items': str(items),
            # 	'timeouts': str(list(timeouts))
            # }
            # self.logger.info("queried timeouts and items", extra=extra)
            print "items: ", items
            print "timeouts: ",list(timeouts)
            # Check what's ready, and what queues need more inspection
			elements = []
			next_timeouts = set()
            for timeout, element_id in zip(timeouts, items):
                if element_id:
					element = self._inspect(element_id, self._queue_to_int(timeout))
                    if element is not None:
                        # element was rehydrated, return to this queue to see if
                        # there are more rehydratable elements
						elements.append(element)
                        print "%s returned" % element
                        next_timeouts.add(timeout)
                    else:
                        # this element needs to dehydrate longer, push it back
                        # to the front of the queue
                        print "%s inspect is false" % element_id
                        self._pipe.lpush(timeout, element_id)
            self._pipe.execute()
            timeouts = next_timeouts

		return elements
        # if time.time() - poll_start_time > 60:
        # 	self.logger.log('dehydrator poll took more then a minute', event_type="rediso dehydrator poll")

    def get_element_in_timeout_queues(self, element_id):
        """Returns a tuple or each Dehydration queue:
        (queue_name, element-in_queue, queue length)"""
        queues = self._redis.keys("timeout_queue#*")
        result = []
        for queue in queues:
            elements = self._redis.lrange(queue, 0, -1)
            if element_id in elements:
                result.append(
                    (queue, True, elements.index(element_id), len(elements)))
            else:
                result.append(
                    (queue, False, "u/a", len(elements)))
        return result

    def get_all_elements_in_timeout_queue(self, element_id):
        """Returns a tuple or each Dehydration queue:
        (queue_name, element-in_queue, queue length)"""
        queues = self._redis.keys("timeout_queue#*")
        result = []
        for queue in queues:
            elements = self._redis.lrange(queue, 0, -1)
            i = 0
            element_location = "u/a"
            if element_id in elements:
                for element in elements:
                    if element == element_id:
                        result.append(
                            (queue, ">%d<" % i, element))
                    else:
                        result.append(
                            (queue, i, element))
                        # break
                i += 1
        return result
