# Climate, Traffic and COVID-19

This repository consists of all the files involved in data preparation/pre-processing, processing and analysis for the climate and traffic sections of our project.

### Climate

- **main.py** <br>
Filter only PM2.5 and temperature for all cities; compute the monthly average for both ECVs and combine the results across various datasets.

- **climate.py** <br>
Compute the cumulative sum and carry out the bootstrap approach to compute the confidence level for each ECV for each city.

- **austin.py** <br>
Filter for only Austin PM2.5 and temperature in the city of Austin.

- **graph.py** <br>
Filter (PM2.5 and temperature, Jan-Oct 2019 and Jan-Oct 2020) and process data to obtain month-on-month differences in cities all around the world.

- **worldmap.py** <br>
Given the confidence level and average change computed for each city, plot the bubble maps for each ECV.


### Traffic

- **traffic-count-cleaning.scala** <br>
Filter for only 'THRU' intersection movements and combine average speed and volumes across heavy and non-heavy vehicles at each intersection

- **traffic-count-detector-merge.scala** <br>
Enrich traffic count data with information about traffic detector at each intersection (includes details such as latitude, longitude and device status)

- **ts-by-intersection-and-day.scala** <br>
Calculates the traffic score for each intersection for each day. This is done by agreggating speeds, volume, and capacity for all intervals in a day using our proposed Traffic Score formula

- **ts-by-day.scala** <br>
Calculates the traffic score for each day (across all intersections). This is done by using a weighted average of the traffic score for each intersections using the volume of vehicles for that intersection for that day. 

- **ts-by-month.scala** <br>
Calculates the traffic score for each month. This is done using a simple aggregation of the daily traffic scores for the particular month. 

- **sum-vol-by-month.scala** <br>
Calculates total volume per month

- **overpass_speed.py** <br>
Script to obtain speed limits given intersection coordinates via the Overpass API

- **interval-combine-volume-speed-with-limits.scala** <br>
Calculation of overall speed for an intersection for one 15 minute interval using a weighted average of speeds using volume. Also join speed limit data.

- **road-danger-score.scala** <br>
Calculation of monthly Road Danger Score based on the assessed severity level of each incident type
