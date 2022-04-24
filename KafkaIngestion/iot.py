#!/usr/bin/env python
from sympy import python
from kafka import KafkaProducer
import numpy as np
from sys import argv, exit
from time import time, sleep
# different device "profiles" with different
# distributions of values to make things interesting
DEVICE_PROFILES = {
    "boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1019.9, 9.5)},
    "denver": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1012.0, 41.3)},
    "losang": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1015.9, 11.3)}
}

if len(argv) != 2 or argv[1] not in DEVICE_PROFILES.keys():
    print("please provide a valid device name:")
    for key in DEVICE_PROFILES.keys():
        print(f" *{key}")
        print(f"\nformat: {argv[0]} DEVICE_NAME")
        exit(1)

profile_name = argv[1]
profile = DEVICE_PROFILES[profile_name]

producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True:
    temp = np.random.normal(profile['temp'][0], profile['temp'][1])
    humd = max(0, min(np.random.normal(
        profile['humd'][0], profile['humd'][1]), 100))
    pres = np.random.normal(profile['pres'][0], profile['pres'][1])
    msg = f'{time()},{profile_name},{temp}, {humd}, {pres}'
    producer.send('weather', bytes(msg, encoding='utf8'))
    print('sending data to kafka')
    sleep(.5)
