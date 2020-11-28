!apt-get install libgeos-3.5.0
!apt-get install libgeos-dev
!pip install https://github.com/matplotlib/basemap/archive/master.zip
!pip install pyproj==1.9.6

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mlt
from mpl_toolkits.basemap import Basemap
%matplotlib inline

from google.colab import files
uploaded = files.upload()

from geopy.geocoders import Nominatim

def get_latitude(city):
  geolocator = Nominatim(user_agent='myapplication')
  location = geolocator.geocode(city)
  if location:
    return location.raw.get('lat')
  else:
    return 0

def get_longitude(city):
  geolocator = Nominatim(user_agent='myapplication')
  location = geolocator.geocode(city)
  if location:
    return location.raw.get('lon')
  else:
    return 0

import csv
import io

# get the set of intersecting cities - can do in preprocessing step
# todo: modify climate3.py to do it there instead
pm_cities = set()
temp_cities = set()
final_cities = set()

pm25_results = pd.read_csv(io.BytesIO(uploaded["final_1000_pm25_results.csv"]))
temp_results = pd.read_csv(io.BytesIO(uploaded["final_1000_temp_results.csv"]))

for index, row in pm25_results.iterrows():
  pm_cities.add(row['city'])

for index, row in temp_results.iterrows():
  temp_cities.add(row['city'])

final_cities = pm_cities.intersection(temp_cities)

print(len(final_cities)) # 547

# combine the CIs of both variables in one dataset
temp_CI = {}
for index, row in temp_results.iterrows():
  city = row['city']
  if city in final_cities:
    temp_CI[city] = row['CI']

pm_CI = {}
for index, row in pm25_results.iterrows():
  city = row['city']
  if city in final_cities:
    pm_CI[city] = row['CI']

# get the latitude and longitude of each city in final_cities
lat = {}
for key in final_cities:
  lat[key] = get_latitude(key)

lon = {}
for key in final_cities:
  lon[key] = get_longitude(key)

# get colour variable from CI
def get_colour(CI):
  if (CI >= 95):
    colour = 'orange'
  elif (CI >= 90):
    colour = 'gold'
  elif (CI >= 85):
    colour = 'midnightblue'
  elif (CI >= 80):
    colour = 'deepskyblue'
  elif (CI >= 50):
    colour = 'lightskyblue'
  else:
    colour = 'azure'
  return colour

# schema: City, Lat, Long, PM25_CI, Temp_CI
results = pd.DataFrame(columns = ['City', 
                        'Latitude',
                        'Longitude', 
                        'PM25_CI', 
                        'PM25_colour', 
                        'Temp_CI', 
                        'Temp_colour'])

# combine the results
for city in final_cities:
  results = results.append({'City': city, 
                          'Latitude': lat[city], 
                          'Longitude': lon[city], 
                          'PM25_CI': pm_CI[city], 
                          'PM25_colour': get_colour(pm_CI[city]), 
                          'Temp_CI': temp_CI[city], 
                          'Temp_colour': get_colour(temp_CI[city])},
                          ignore_index=True)

# export data
results.to_csv("results.csv", index=False, encoding='utf-8-sig')

# get lists of data for plotting
latitudes = list(lat.values())
longitudes = list(lon.values())
pm25_colours = results['PM25_colour'].values
temp_colours = results['Temp_colour'].values

# import final_results
final_results = pd.read_csv(io.BytesIO(uploaded["final_results.csv"]))
print(final_results)

# change the colours of the imported results
temp_colours = final_results['Temp_CI'].map(lambda x: get_colour(x))
pm25_colours = final_results['PM25_CI'].map(lambda x: get_colour(x))
print(temp_colours)
print(pm25_colours)
final_results['Temp_Str_Col'] = temp_colours
final_results['PM25_Str_Col'] = pm25_colours

# plot pm25_colours
fig = plt.figure()
m = Basemap()
m.drawcountries(color='dimgrey')
m.fillcontinents(color='lightgrey')
for index, row in final_results.iterrows():
  city = row['City']
  latitude = row['Latitude']
  longitude = row['Longitude']
  pm25_colour = row['PM25_Str_Col']
  m.plot(longitude, latitude, marker='.', color=pm25_colour)
plt.show()
fig.savefig("pm25_colours.png", dpi=600)

# plot temp_colours
fig2 = plt.figure()
m = Basemap()
m.drawcountries(color='dimgrey')
m.fillcontinents(color='lightgrey')
for index, row in final_results.iterrows():
  city = row['City']
  latitude = row['Latitude']
  longitude = row['Longitude']
  temp_colour = row['Temp_Str_Col']
  m.plot(longitude, latitude, marker='.', color=temp_colour)
plt.show()
fig2.savefig("temp_colours.png", dpi=600)

# updated code for processing
# import final_results
final_results = pd.read_csv(io.BytesIO(uploaded["final_results.csv"]))
# read latitudes and longitudes
latitudes = results['Latitude'].values
longitudes = results['Longitude'].values
# pm25_colours = results['PM25_colour'].values
# temp_colours = results['Temp_colour'].values
# get new colours based on updated get_colour function if required
temp_colours = final_results['Temp_CI'].map(lambda x: get_colour(x))
pm25_colours = final_results['PM25_CI'].map(lambda x: get_colour(x))
print(temp_colours)
print(pm25_colours)
# save updated colours back into new column (can I overwrite?)
final_results['Temp_Str_Col'] = temp_colours
final_results['PM25_Str_Col'] = pm25_colours

# import diff_temp
diff_pm25 = pd.read_csv(io.BytesIO(uploaded["diffpm25Final.csv"]))
diff_temp = pd.read_csv(io.BytesIO(uploaded["difftemp25Final.csv"]))
diff_pm25.set_index("city", inplace=True)
# print(diff_pm25)
diff_pm25_dict = diff_pm25.to_dict()['diff']
# print(diff_pm25_dict)
diff_temp.set_index("city", inplace=True)
diff_temp_dict = diff_temp.to_dict()['diff']
# print(diff_temp_dict)

def get_colour(diff):
  if diff > 0:
    return 'orange'
  else:
    return 'midnightblue'

# get latitudes, longitudes and colours
latitudes = {}
longitudes = {}
for city, temp_diff in diff_temp_dict.items():
  latitude = float(get_latitude(city))
  latitudes[city] = latitude
  longitude = float(get_longitude(city))
  longitudes[city] = latitude
  temp_mag_colour = get_colour(temp_diff)

for city in diff_pm25_dict.keys():
  if city not in latitudes:
    latitude = float(get_latitude(city))
    latitudes[city] = latitude
    longitude = float(get_longitude(city))
    longitudes[city] = longitude

# save latitudes and longitudes

# plot pm25_colours, scaled by absolute value of change
fig = plt.figure()
m = Basemap()
m.drawcountries(color='dimgrey')
m.fillcontinents(color='lightgrey')
for lon, lat, col, size in zip(longitudes_plotting, latitudes_plotting, pm25_colours, pm25_diff):
  m.plot(lon, lat, marker='.', markersize=abs(size), color=col, alpha=0.4)
plt.show()
fig.savefig("pm25_colours_scaled.png", dpi=600)

# plot temp_colours, scaled by absolute value of change
fig = plt.figure()
m = Basemap()
m.drawcountries(color='dimgrey')
m.fillcontinents(color='lightgrey')
for lon, lat, col, size in zip(longitudes_plotting, latitudes_plotting, temp_colours, temp_diff):
  m.plot(lon, lat, marker='.', markersize=abs(size), color=col, alpha=0.4)
plt.show()
fig.savefig("temp_colours_scaled.png", dpi=600)