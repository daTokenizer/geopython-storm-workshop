from __future__ import absolute_import, print_function, unicode_literals
import uuid
import itertools
import random
from streamparse.spout import Spout
import string
printable = set(string.printable)

def generate_random_location():
    lat, lon = random.uniform(40, 70), random.uniform(40, 70)
    return (lat,lon)

def get_rand_word(words):
    return filter(lambda x: x in printable, random.choice(words))

def generate_random_name(words):
    return get_rand_word(words) + "_" + get_rand_word(words) + "_" + get_rand_word(words)

class LocationSpout(Spout):

    def initialize(self, stormconf, context):
        word_file = "/usr/share/dict/words"
        self.words = open(word_file).read().splitlines()


    def next_tuple(self):
        name = generate_random_name(self.words)
        lat ,lon = generate_random_location()
        rad = random.uniform(0.1, 1)
        location = (name ,lat, lon, rad)
        self.emit([location])
