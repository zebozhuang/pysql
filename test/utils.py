# coding: utf-8

import time
import random


def get_milliontimestamp():
    return int(time.time()*1000)


def get_timestamp():
    return int(time.time())


def get_test_name():
    return "name_%d_%d" % (get_milliontimestamp(), random.randint(1, 1000))

def get_test_age():
    return random.randint(10, 50)