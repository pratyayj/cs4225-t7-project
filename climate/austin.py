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

    pm25_aus = sc.textFile(f) \
        .map(lambda line: line.split(","))\
        .filter(lambda line: len(line)>1)\
        .filter(lambda line: line[3] == "pm25") \
        .filter(lambda line: line[2] == "Austin") \
        .filter(lambda line: line[0] < '2019-11' or line[0] > '2020') \
        .map(lambda line: (dateFormat(line[0]),line[2], line[3], line[7]))\
        .collect()

    temperature_aus = sc.textFile(f) \
        .map(lambda line: line.split(","))\
        .filter(lambda line: len(line)>1)\
        .filter(lambda line: line[3] == "temperature") \
        .filter(lambda line: line[2] == "Austin") \
        .filter(lambda line: line[0] < '2019-11' or line[0] > '2020')\
        .map(lambda line: (dateFormat(line[0]),line[2], line[3], line[7]))\
        .collect()


    pm25AusDf = sqlContext.createDataFrame(pm25_aus, ['date', 'city', 'env', 'median'])
    pm25AusMedian = pm25AusDf.groupBy('city', 'date').agg(functions.mean('median')).withColumnRenamed('avg(median)', 'avgMedian')
    pm25AusMedian.toPandas().to_csv('auspm25' + f[:-4] + '.csv')

    tempAusDf = sqlContext.createDataFrame(temperature_aus, ['date', 'city', 'env', 'median'])
    tempAusMedian = tempAusDf.groupBy('city', 'date').agg(functions.mean('median')).withColumnRenamed('avg(median)', 'avgMedian')
    tempAusMedian.toPandas().to_csv('austemp' + f[:-4] + '.csv')

    print("done processing " + f)


os.chdir("./")
extension ='csv'
auspm25_all_filenames = [i for i in glob.glob('auspm25*.{}'.format(extension))]
austemp_all_filenames = [i for i in glob.glob('austemp*.{}'.format(extension))]
auspm25_combined_csv = pd.concat([pd.read_csv(f) for f in auspm25_all_filenames ])
austemp_combined_csv = pd.concat([pd.read_csv(f) for f in austemp_all_filenames ])
auspm25_combined_csv.to_csv( "auspm25_combined_csv.csv", index=False, encoding='utf-8-sig')
austemp_combined_csv.to_csv( "austemp_combined_csv.csv", index=False, encoding='utf-8-sig')




