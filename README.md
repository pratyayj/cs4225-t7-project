# Big Data in Austin

This repository consists of all the files involved in data preparation/pre-processing, processing and analysis for the climate and traffic sections of our project.

### Climate

- 

### Traffic

- **traffic-count-cleaning.scala**
Filter for only 'THRU' intersection movements and combine average speed and volumes across heavy and non-heavy vehicles at each intersection
<br>

- **traffic-count-detector-merge.scala**
Enrich traffic count data with information about traffic detector at each intersection (includes details such as latitude, longitude and device status)
<br>

- **ts-by-intersection-and-day.scala**
Calculates the traffic score for each intersection for each day. This is done by agreggating speeds, volume, and capacity for all intervals in a day using our proposed Traffic Score formula
<br>

- **sum-vol-by-month.scala**
Calculates total volume per month
<br>

- **overpass_speed.py**
Script to obtain speed limits given intersection coordinates via the Overpass API
<br>

- **road-danger-score.scala**
Calculation of monthly Road Danger Score based on the assessed severity level of each incident type