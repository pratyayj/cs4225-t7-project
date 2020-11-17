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


for f in allFiles:

    combined = sc.textFile(f) \
        .map(lambda line: line.split(","))\
        .filter(lambda line: len(line)>1)\
        .filter(lambda line: line[3] == "pm25" or line[3] == "temperature") \
        .filter(lambda line: line[2] == "Austin" ) \
        .map(lambda line: (line[0],line[2], line[3], line[7]))

    combinedDf = sqlContext.createDataFrame(combined, ['date', 'city', 'env', 'median'])
    combinedDf.toPandas().to_csv('combined' + f[:-4] + '.csv')

    print("done processing " + f)


os.chdir("./")
extension ='csv'
combined_all_filenames = [i for i in glob.glob('combined*.{}'.format(extension))]
combined_combined_csv = pd.concat([pd.read_csv(f) for f in combined_all_filenames ])
combined_combined_csv.to_csv( "all_combined_csv.csv", index=False, encoding='utf-8-sig')


