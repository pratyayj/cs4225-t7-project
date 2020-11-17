from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
import pandas as pd
import os
import glob
from pyspark.sql import Row
import spark


sc = SparkContext("local", "First App")
sqlContext = SQLContext(sc)

allFiles = ['waqi-covid19-airqualitydata-2020.csv', 'waqi-covid19-airqualitydata-2019Q1.csv',
            'waqi-covid19-airqualitydata-2019Q2.csv', 'waqi-covid19-airqualitydata-2019Q3.csv',
            'waqi-covid19-airqualitydata-2019Q4.csv']

def dateFormat(str):
    return str[:-3]

for f in allFiles:

    pm25 = sc.textFile(f) \
        .map(lambda line: line.split(","))\
        .filter(lambda line: len(line)>1)\
        .filter(lambda line: line[3] == "pm25") \
        .filter(lambda line: line[0] < '2019-11' or line[0] > '2020') \
        .map(lambda line: (dateFormat(line[0]),line[2], line[3], line[7]))\
        .collect()

    temperature = sc.textFile(f) \
        .map(lambda line: line.split(","))\
        .filter(lambda line: len(line)>1)\
        .filter(lambda line: line[3] == "temperature")\
        .filter(lambda line: line[0] < '2019-11' or line[0] > '2020')\
        .map(lambda line: (dateFormat(line[0]),line[2], line[3], line[7]))\
        .collect()


    pm25Df = sqlContext.createDataFrame(pm25, ['date', 'city', 'env', 'median'])
    pm25Median = pm25Df.groupBy('city', 'date').agg(functions.mean('median')).withColumnRenamed('avg(median)', 'avgMedian')
    pm25Median.toPandas().to_csv('pm25' + f[:-4] + '.csv')

    tempDf = sqlContext.createDataFrame(temperature, ['date', 'city', 'env', 'median'])
    tempMedian = tempDf.groupBy('city', 'date').agg(functions.mean('median')).withColumnRenamed('avg(median)', 'avgMedian')
    tempMedian.toPandas().to_csv('temp' + f[:-4] + '.csv')

    print("done processing " + f)


os.chdir("./")
extension ='csv'
pm25_all_filenames = [i for i in glob.glob('pm25*.{}'.format(extension))]
temp_all_filenames = [i for i in glob.glob('temp*.{}'.format(extension))]
pm25_combined_csv = pd.concat([pd.read_csv(f) for f in pm25_all_filenames ])
temp_combined_csv = pd.concat([pd.read_csv(f) for f in temp_all_filenames ])
pm25_combined_csv.to_csv( "pm25_combined_csv.csv", index=False, encoding='utf-8-sig')
temp_combined_csv.to_csv( "temp_combined_csv.csv", index=False, encoding='utf-8-sig')




