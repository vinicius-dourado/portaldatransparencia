import json
import pandas as pd
import glob
import os
from pandas import json_normalize
from scripts_python.location import city_state_country
#from location import city_state_country
import sys
from geopy.geocoders import Nominatim
import time

def convert_time(timedf):
    timetup = time.gmtime(timedf)
    timetup = time.strftime('%Y-%m-%d %H:%M:%S', timetup)
    return timetup  

def execute():
    df_current = pd.DataFrame(columns=['lat','lon','dt','temp','feels_like','pressure','humidity',
        'dew_point','uvi','clouds','visibility','wind_speed',
        'wind_deg']
        )
    df_hourly = pd.DataFrame(columns=['lat','lon','dt','temp','feels_like','pressure','humidity',
        'dew_point','uvi','clouds','visibility','wind_speed'
        'wind_deg'])

    columns = ['lat','lon','dt','temp','feels_like','pressure','humidity',
        'dew_point','uvi','clouds','visibility','wind_speed',
        'wind_deg']


    for file in list(glob.glob(f'data/weather/raw/*.json')):
        filename = os.path.basename(file).replace('.json','.csv')
        r = open(file)
        d = json.load(r)
        df_current = pd.json_normalize(d , max_level=1)
        df_current = df_current[['lat','lon','current.dt','current.temp','current.feels_like',
        'current.pressure','current.humidity','current.dew_point','current.uvi',
        'current.clouds','current.visibility','current.wind_speed','current.wind_deg']]
        df_current.columns = columns

        df_hourly = pd.json_normalize(d, record_path=['hourly'],meta=['lat', 'lon', 'timezone'])
        df_hourly = df_hourly[['lat','lon','dt', 'temp','feels_like',
        'pressure','humidity','dew_point','uvi',
        'clouds','visibility','wind_speed','wind_deg']]
        df_hourly.columns = columns               
    
        df_current = df_current.apply(city_state_country, axis=1)
        df_hourly = df_hourly.apply(city_state_country, axis=1)

        df_current = df_current.drop(['lat', 'lon'], axis=1)
        df_current.drop_duplicates(keep='first')    
        df_hourly = df_hourly.drop(['lat', 'lon'], axis=1)        
        df_hourly.drop_duplicates(keep='first')    


        df_current['dt'] = df_current['dt'].apply(convert_time)
        df_hourly['dt'] = df_hourly['dt'].apply(convert_time)

        df_current.to_csv('data/weather/staging/current_'+filename, na_rep='NULL', index=False)
        df_hourly.to_csv('data/weather/staging/hourly_'+filename, na_rep='NULL', index=False)     


  
    
