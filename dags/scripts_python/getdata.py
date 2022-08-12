# Importing Python Libraries
from datetime import datetime, timedelta
import json
import time
import os
import pandas as pd
import glob
import requests
from geopy.geocoders import Nominatim
from airflow.hooks.base import BaseHook


# set past 5 days
days=[]
i = 1
while i < 5:
  fivedaysago =  time.mktime((datetime.today() - timedelta(days=1)).timetuple())  
  days.append(fivedaysago)
  i += 1

api_connection = BaseHook.get_connection("api")

geolocator = Nominatim(user_agent="metadata")

locations = [['30.318878','-81.690173'],['-15.7801','-47.9292'],
             ['40.6643','-73.9385'],['30.0446','31.2456'],['19.328','-103.602'],
             ['51.5072','-0.1275'],['-11.2999','-41.8568'],['48.8032','2.3511']]

def api():
    count = 0
    for i in range(len(locations)):
        for dt in days:
            api_parameters = {
                'lat':locations[count][0],
                'lon':locations[count][1],
                'units':'metric',
                'dt':int(dt),
                'appid':api_connection.password
                #'appid':'f03d3da3e4aa605d6d5c5a01a13ca69f'
            }
            #r = requests.get(url = 'http://api.openweathermap.org/data/2.5/onecall/timemachine', params = api_parameters)
            r = requests.get(url = api_connection.host + '/data/2.5/onecall/timemachine', params = api_parameters)
            data = r.json()
            time = datetime.today().strftime('%Y%m%d%H%M%S%f')
            with open(f"data/weather/raw/weather_output_{time}.json", "w") as outfile:
                json.dump(data, outfile)
        count += 1
