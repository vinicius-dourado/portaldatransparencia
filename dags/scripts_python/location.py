from geopy.geocoders import Nominatim
import numpy as np

geolocator = Nominatim(user_agent="metadata")
def city_state_country(row):
    coord = f"{row['lat']}, {row['lon']}"
    if not np.isnan(row['lat']):
        location = geolocator.reverse(coord, exactly_one=True)
        address = location.raw['address']
        city = address.get('city', '')
        state = address.get('state', '')
        country = address.get('country', '')
        row['city'] = str(city)
        row['state'] = str(state)
        row['country'] = str(country)
    return row
